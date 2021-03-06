use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc::Receiver;
use futures::{ready, Stream};
use pin_project_lite::pin_project;

use crate::stages::utils::ErrorHook;
use crate::FError;

pin_project! {
    pub struct SourceStage<F> {
        job_id: u64,
        error_hook: Arc<ErrorHook>,
        #[pin]
        task: F
    }
}

impl<F> SourceStage<F> {
    pub fn new(job_id: u64, error_hook: Arc<ErrorHook>, task: F) -> Self {
        Self { job_id, error_hook, task }
    }
}

impl<F> Future for SourceStage<F>
where
    F: Future<Output = Result<(), FError>>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.error_hook.has_error() {
            return Poll::Ready(());
        }

        let this = self.project();
        match ready!(this.task.poll(cx)) {
            Ok(()) => {
                debug!("source of job({}) is exhausted;", this.job_id);
                Poll::Ready(())
            }
            Err(e) => {
                error!("source of job({}) poll fail: {}", this.job_id, e);
                this.error_hook.set_error(e);
                Poll::Ready(())
            }
        }
    }
}

pub struct StageInput<T>(Receiver<T>);

impl<T> StageInput<T> {
    pub fn new(receiver: Receiver<T>) -> Self {
        StageInput(receiver)
    }
}

impl<T> Stream for StageInput<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().0).poll_next(cx)
    }
}
