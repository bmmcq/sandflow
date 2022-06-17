use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc::{Receiver, Sender};
use futures::future::BoxFuture;
use futures::{ready, Sink, Stream};
use pin_project_lite::pin_project;

use crate::stages::utils::ErrorHook;
use crate::FError;

pin_project! {
    pub struct AsyncStage {
        worker_id: u32,
        stage_id: u32,
        error_hook: Arc<ErrorHook>,
        #[pin]
        task: BoxFuture<'static, Result<(), FError>>
    }
}

impl AsyncStage {
    pub fn new<F>(worker_id: u32, stage_id: u32, task: F, error_hook: &Arc<ErrorHook>) -> Self
    where
        F: Future<Output = Result<(), FError>> + Send + 'static,
    {
        Self { worker_id, stage_id, task: Box::pin(task), error_hook: error_hook.clone() }
    }
}

impl Future for AsyncStage {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.error_hook.has_error() {
            warn!("worker[{}]: abort stage({}) as error occurred;", self.worker_id, self.stage_id);
            return Poll::Ready(());
        }

        let this = self.project();
        match ready!(this.task.poll(cx)) {
            Ok(()) => {
                debug!("worker[{}]: stage({}) is finished;", this.worker_id, this.stage_id);
                Poll::Ready(())
            }
            Err(e) => {
                error!("worker[{}]: stage({}) executed fail: {};", this.worker_id, this.stage_id, e);
                this.error_hook.set_error(e);
                Poll::Ready(())
            }
        }
    }
}

pub struct StageSource<T>(Receiver<T>);

impl<T> StageSource<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        StageSource(receiver)
    }
}

impl<T> Stream for StageSource<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().0).poll_next(cx)
    }
}

pub struct StageSink<T>(Sender<T>);

impl<T> StageSink<T> {
    pub fn new(sender: Sender<T>) -> Self {
        StageSink(sender)
    }
}

impl<T> Sink<T> for StageSink<T> {
    type Error = FError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!((*self.get_mut()).poll_ready(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(FError::ChSend(e))),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        match (*self.get_mut()).start_send(item) {
            Ok(()) => Ok(()),
            Err(e) => Err(FError::ChSend(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(Pin::new(&mut self.get_mut().0).poll_flush(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(FError::ChSend(e))),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(Pin::new(&mut self.get_mut().0).poll_close(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(FError::ChSend(e))),
        }
    }
}

impl<T> Deref for StageSink<T> {
    type Target = Sender<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for StageSink<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub(crate) mod sink;
pub(crate) mod source;
pub(crate) mod utils;
