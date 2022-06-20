use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Stream};
use pin_project_lite::pin_project;

use crate::stages::sink::TrySink;

pin_project! {
    #[project = SelectForwardProj]
    pub struct SelectForward<St, Ts, Item> {
        #[pin]
        sink: Option<Ts>,
        #[pin]
        stream: St,
        buffered_item: Option<Item>
    }
}

impl<St, Ts, Item> SelectForward<St, Ts, Item> {
    pub fn new(stream: St, sink: Ts) -> Self {
        Self { sink: Some(sink), stream, buffered_item: None }
    }
}

impl<St, Ms, Item, E> Future for SelectForward<St, Ms, Item>
where
    St: Stream<Item = Result<Item, E>>,
    Ms: TrySink<Item, Error = E>,
{
    type Output = Result<(), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let SelectForwardProj { mut sink, mut stream, buffered_item } = self.project();
        let mut si = sink
            .as_mut()
            .as_pin_mut()
            .expect("polled 'MultiForward' after completion");
        loop {
            if let Some(item) = buffered_item.take() {
                if let Some(pending) = si.as_mut().try_sink(item, cx)? {
                    *buffered_item = Some(pending);
                    return Poll::Pending;
                }
            }

            match stream.as_mut().poll_next(cx)? {
                Poll::Ready(Some(item)) => {
                    *buffered_item = Some(item);
                }
                Poll::Ready(None) => {
                    ready!(si.poll_close(cx))?;
                    sink.set(None);
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => {
                    ready!(si.poll_flush(cx))?;
                    return Poll::Pending;
                }
            }
        }
    }
}
