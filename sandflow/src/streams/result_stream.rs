use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc::Receiver;
use futures::Stream;
use pin_project_lite::pin_project;

pin_project! {
    pub struct ResultStream<T> {
        #[pin]
        rx: Receiver<T>
    }
}

impl<T> ResultStream<T> {
    pub fn new(rx: Receiver<T>) -> Self {
        ResultStream { rx }
    }
}

impl<T> Stream for ResultStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.rx.poll_next(cx)
    }
}
