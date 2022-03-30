use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Stream};
use pin_project_lite::pin_project;

use crate::channels::multi_sink::MultiSink;

pin_project! {
    #[project = MultiForwardProj]
    pub struct MultiForward<St, Ms, Item> {
        #[pin]
        sink: Option<Ms>,
        #[pin]
        stream: St,
        buffered_item: Option<Item>
    }
}

impl<St, Ms, Item> MultiForward<St, Ms, Item> {
    pub fn new(stream: St, sink: Ms) -> Self {
        Self { sink: Some(sink), stream, buffered_item: None }
    }
}

impl<St, Ms, Item, E> Future for MultiForward<St, Ms, Item>
where
    St: Stream<Item = Result<Item, E>>,
    Ms: MultiSink<Item, Error = E>,
{
    type Output = Result<(), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let MultiForwardProj { mut sink, mut stream, buffered_item } = self.project();
        let mut si = sink
            .as_mut()
            .as_pin_mut()
            .expect("polled 'MultiForward' after completion");
        loop {
            if let Some(item) = buffered_item.take() {
                if let Some(pending) = si.as_mut().try_send(item, cx)? {
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
