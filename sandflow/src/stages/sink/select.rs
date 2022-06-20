use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Sink};
use pin_project_lite::pin_project;

use crate::stages::sink::TrySink;

pub trait Selector<T> {
    /// Select a sink according to the item, return the id(index) which specific a sink;
    fn select_by(self: Pin<&mut Self>, item: &T) -> u64;
}

impl<F, T> Selector<T> for F
where
    F: FnMut(&T) -> u64 + Unpin,
{
    fn select_by(self: Pin<&mut Self>, item: &T) -> u64 {
        self.get_mut()(item)
    }
}

pub struct RoundSelector<T> {
    cursor: usize,
    _ph: std::marker::PhantomData<T>,
}

impl<T> Unpin for RoundSelector<T> {}

impl<T> Selector<T> for RoundSelector<T> {
    #[inline]
    fn select_by(self: Pin<&mut Self>, _: &T) -> u64 {
        let this = self.get_mut();
        this.cursor += 1;
        this.cursor as u64 - 1
    }
}

pin_project! {
    struct TagSink<Si> {
        #[pin]
        sink: Si,
        is_dirty: bool,
        is_closed: bool,
    }
}

impl<Si> TagSink<Si> {
    fn new(sink: Si) -> Self {
        Self { sink, is_dirty: false, is_closed: false }
    }
}

impl<Si, T> TrySink<T> for TagSink<Si>
where
    Si: Sink<T>,
{
    type Error = Si::Error;

    fn try_sink(self: Pin<&mut Self>, item: T, cx: &mut Context<'_>) -> Result<Option<T>, Self::Error> {
        let mut this = self.project();
        match this.sink.as_mut().poll_ready(cx) {
            Poll::Ready(Ok(_)) => {
                this.sink.as_mut().start_send(item)?;
                *this.is_dirty |= true;
                Ok(None)
            }
            Poll::Ready(Err(e)) => Err(e),
            Poll::Pending => Ok(Some(item)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if *this.is_dirty {
            match ready!(this.sink.poll_flush(cx)) {
                Ok(_) => {
                    *this.is_dirty = false;
                    Poll::Ready(Ok(()))
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        if !*this.is_closed {
            match ready!(this.sink.poll_close(cx)) {
                Ok(_) => {
                    *this.is_closed = true;
                    Poll::Ready(Ok(()))
                }
                Err(e) => Poll::Ready(Err(e)),
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }
}

pin_project! {
    pub struct SelectSink<Si, T> {
        sinks: Vec<TagSink<Si>>,
        #[pin]
        selector: T,
        rf: Rectifier,
        sink_size: u64
    }
}

impl<Si, T> SelectSink<Si, T> {
    pub fn new(sinks: Vec<Si>, selector: T) -> Self {
        let sink_size = sinks.len();
        let rf = Rectifier::new(sink_size);
        let mut tag_sinks = Vec::with_capacity(sink_size);
        for s in sinks {
            tag_sinks.push(TagSink::new(s));
        }

        Self { sinks: tag_sinks, selector, rf, sink_size: sink_size as u64 }
    }
}

impl<Si, Item> SelectSink<Si, RoundSelector<Item>> {
    pub fn round_select(sinks: Vec<Si>) -> Self
    where
        Si: Sink<Item>,
    {
        Self::new(sinks, RoundSelector { cursor: 0, _ph: std::marker::PhantomData })
    }
}

impl<Si, Item, T> TrySink<Item> for SelectSink<Si, T>
where
    Si: Sink<Item>,
    T: Selector<Item>,
{
    type Error = Si::Error;

    fn try_sink(self: Pin<&mut Self>, item: Item, cx: &mut Context<'_>) -> Result<Option<Item>, Self::Error> {
        let this = self.project();
        let index = this.selector.select_by(&item);
        let rf_index = if index < *this.sink_size { index as usize } else { this.rf.get(index) };
        // if safe here, because no `&mut sinks[rf_index]` is moved;
        let sink = unsafe { Pin::new_unchecked(&mut this.sinks[rf_index]) };
        sink.try_sink(item, cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        for sink in this.sinks.iter_mut() {
            let pinned = unsafe { Pin::new_unchecked(sink) };
            if let Err(e) = ready!(pinned.poll_flush(cx)) {
                return Poll::Ready(Err(e));
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        for sink in this.sinks.iter_mut() {
            let pinned = unsafe { Pin::new_unchecked(sink) };
            if let Err(e) = ready!(pinned.poll_close(cx)) {
                return Poll::Ready(Err(e));
            }
        }
        Poll::Ready(Ok(()))
    }
}

enum Rectifier {
    And(u64),
    Mod(u64),
}

impl Rectifier {
    fn new(length: usize) -> Self {
        if length & (length - 1) == 0 {
            Rectifier::And(length as u64 - 1)
        } else {
            Rectifier::Mod(length as u64)
        }
    }

    #[inline]
    fn get(&self, v: u64) -> usize {
        let r = match self {
            Rectifier::And(b) => v & *b,
            Rectifier::Mod(b) => v % *b,
        };
        r as usize
    }
}
