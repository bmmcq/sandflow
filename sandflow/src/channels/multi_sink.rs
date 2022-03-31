use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Sink;
use pin_project_lite::pin_project;

pub trait MultiSink<Item>: Sink<Item> {
    fn try_send(self: Pin<&mut Self>, item: Item, cx: &mut Context<'_>) -> Result<Option<Item>, Self::Error>;
}

pin_project! {
    #[derive(Debug)]
    pub struct RoundRobinSink<S> {
        #[pin]
        sinks: Vec<S>,
        cursor: usize,
    }
}

impl<Si> RoundRobinSink<Si> {
    pub fn new(sinks: Vec<Si>) -> Self {
        Self { sinks, cursor: 0 }
    }
}

impl<Si, Item> Sink<Item> for RoundRobinSink<Si>
where
    Si: Sink<Item> + Unpin,
{
    type Error = Si::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        for si in this.sinks.get_mut() {
            if !Pin::new(si).poll_ready(cx)?.is_ready() {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let this = self.project();
        let index = *this.cursor;
        let sinks = this.sinks.get_mut();
        Pin::new(&mut sinks[index]).start_send(item)?;
        *this.cursor += 1;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        for si in this.sinks.get_mut() {
            if !Pin::new(si).poll_flush(cx)?.is_ready() {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        for si in this.sinks.get_mut() {
            if !Pin::new(si).poll_close(cx)?.is_ready() {
                return Poll::Pending;
            }
        }

        Poll::Ready(Ok(()))
    }
}

impl<Si, Item> MultiSink<Item> for RoundRobinSink<Si>
where
    Si: Sink<Item> + Unpin,
{
    fn try_send(self: Pin<&mut Self>, item: Item, cx: &mut Context<'_>) -> Result<Option<Item>, Self::Error> {
        let this = self.project();
        let cursor = *this.cursor;
        let sinks = this.sinks.get_mut();
        let index = cursor % sinks.len();
        if Pin::new(&mut sinks[index]).poll_ready(cx)?.is_ready() {
            Pin::new(&mut sinks[index]).start_send(item)?;
            *this.cursor += 1;
            Ok(None)
        } else {
            Ok(Some(item))
        }
    }
}
