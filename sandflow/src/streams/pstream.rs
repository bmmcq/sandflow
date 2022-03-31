use std::future::Future;

use futures::stream::{Forward, Map, Then};
use futures::{Sink, Stream, StreamExt, TryStream};

use crate::SandFlowBuilder;


pub type DynStream<Item> = Box<dyn Stream<Item = Item> + Send + Unpin>;
//pub type PStream<Item> = PartialStream<Box<dyn Stream<Item = Item> + Send + Unpin>>;

pub struct PartialStream<Ps> {
    stream: Ps,
    fb: SandFlowBuilder,
}

impl<Ps> PartialStream<Ps> {
    pub fn new(fb: SandFlowBuilder, stream: Ps) -> Self {
        Self { stream, fb }
    }
}

impl<Ps> PartialStream<Ps>
where
    Ps: Stream + Send,
{
    pub fn to_box_dyn(self) -> PartialStream<DynStream<Ps::Item>>
    where
        Ps: 'static,
    {
        PartialStream::new(self.fb, Box::new(Box::pin(self.stream)) as Box<dyn Stream<Item = Ps::Item> + Send + Unpin>)
    }

    pub fn map<T, F>(self, f: F) -> PartialStream<Map<Ps, F>>
    where
        F: FnMut(Ps::Item) -> T,
        Ps: Sized,
    {
        let mapped = self.stream.map(f);
        PartialStream::new(self.fb, mapped)
    }

    pub fn then<Fut, F>(self, f: F) -> PartialStream<Then<Ps, Fut, F>>
    where
        F: FnMut(Ps::Item) -> Fut,
        Fut: Future,
        Self: Sized,
    {
        let then = self.stream.then(f);
        PartialStream::new(self.fb, then)
    }

    pub fn forward<S>(self, sink: S) -> Forward<Ps, S>
    where
        S: Sink<Ps::Ok, Error = Ps::Error>,
        Ps: TryStream + Sized,
    {
        self.stream.forward(sink)
    }
}
