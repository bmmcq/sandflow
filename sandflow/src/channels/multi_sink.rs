use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Sink;
use pin_project_lite::pin_project;

pub trait MultiSink<Item>: Sink<Item> {
    fn try_send(self: Pin<&mut Self>, item: Item, cx: &mut Context<'_>) -> Result<Option<Item>, Self::Error>;
}

#[derive(Debug, Copy, Clone)]
enum Modulo {
    ByAnd(u64),
    ByMod(u64),
}

impl Modulo {
    fn new(length: usize) -> Self {
        if length & (length - 1) == 0 {
            Modulo::ByAnd(length as u64 - 1)
        } else {
            Modulo::ByMod(length as u64)
        }
    }

    #[inline(always)]
    fn exec(&self, a: u64) -> usize {
        let r = match self {
            Modulo::ByAnd(b) => a & *b,
            Modulo::ByMod(b) => a % *b,
        };
        r as usize
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct RoundRobinSink<S> {
        #[pin]
        sinks: Vec<S>,
        cursor: u64,
        md: Modulo,
    }
}

impl<Si> RoundRobinSink<Si> {
    pub fn new(sinks: Vec<Si>) -> Self {
        let md = Modulo::new(sinks.len());
        Self { sinks, cursor: 0, md }
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
        let index = this.md.exec(*this.cursor);
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
        let index = this.md.exec(cursor);
        if Pin::new(&mut sinks[index]).poll_ready(cx)?.is_ready() {
            Pin::new(&mut sinks[index]).start_send(item)?;
            *this.cursor += 1;
            Ok(None)
        } else {
            Ok(Some(item))
        }
    }
}

pub trait Router<T> {
    fn route(&self, item: &T) -> u64;
}

impl<T, F> Router<T> for F
where
    F: Fn(&T) -> u64,
{
    fn route(&self, item: &T) -> u64 {
        (self)(item)
    }
}

pin_project! {
    pub struct RouteSink<S, R> {
        #[pin]
        sinks: Vec<S>,
        #[pin]
        router: R,
        md: Modulo,
    }
}

impl<Si, R> RouteSink<Si, R> {
    pub fn new(router: R, sinks: Vec<Si>) -> Self {
        let md = Modulo::new(sinks.len());
        Self { sinks, router, md }
    }
}

impl<Si, R, Item> Sink<Item> for RouteSink<Si, R>
where
    Si: Sink<Item> + Unpin,
    R: Router<Item>,
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
        let cursor = this.router.route(&item);
        let index = this.md.exec(cursor);
        let sinks = this.sinks.get_mut();
        Pin::new(&mut sinks[index]).start_send(item)?;
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

impl<Si, R, Item> MultiSink<Item> for RouteSink<Si, R>
where
    Si: Sink<Item> + Unpin,
    R: Router<Item>,
{
    fn try_send(self: Pin<&mut Self>, item: Item, cx: &mut Context<'_>) -> Result<Option<Item>, Self::Error> {
        let this = self.project();
        let cursor = this.router.route(&item);
        let sinks = this.sinks.get_mut();
        let index = this.md.exec(cursor);
        if Pin::new(&mut sinks[index]).poll_ready(cx)?.is_ready() {
            Pin::new(&mut sinks[index]).start_send(item)?;
            Ok(None)
        } else {
            Ok(Some(item))
        }
    }
}
