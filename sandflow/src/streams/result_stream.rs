use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc::Receiver;
use futures::{ready, Stream};
use pin_project_lite::pin_project;

use crate::{ErrorHook, FError};

pin_project! {
    pub struct ResultStream<T> {
        error_hook: Arc<ErrorHook>,
        #[pin]
        rx: Receiver<T>
    }
}

impl<T> ResultStream<T> {
    pub fn new(error_hook: Arc<ErrorHook>, rx: Receiver<T>) -> Self {
        ResultStream { error_hook, rx }
    }
}

impl<T> Stream for ResultStream<T> {
    type Item = Result<T, FError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(err) = self.error_hook.take_error() {
            return Poll::Ready(Some(Err(err)));
        }
        let this = self.project();
        let a = ready!(this.rx.poll_next(cx));
        Poll::Ready(a.map(|y| Ok(y)))
    }
}
