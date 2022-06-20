use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::BoxFuture;
use futures::ready;
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

pub(crate) mod sink;
pub(crate) mod source;
pub(crate) mod utils;
