use std::future::Future;
use std::pin::Pin;

use futures::stream::{FlatMap, Forward, Inspect, Map, Then};
use futures::{Sink, Stream, StreamExt, TryStream};

use crate::channels::multi_sink::RouteSink;
use crate::channels::GeneralReceiver;
use crate::{FError, SandData, SandFlowBuilder, StreamExtend};

pub type DynStream<Item> = Pin<Box<dyn Stream<Item = Item> + Send>>;

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
        PartialStream::new(self.fb, Box::pin(self.stream) as Pin<Box<dyn Stream<Item = Ps::Item> + Send>>)
    }

    pub fn map<T, F>(self, f: F) -> PartialStream<Map<Ps, F>>
    where
        F: FnMut(Ps::Item) -> T,
        Ps: Sized,
    {
        let mapped = self.stream.map(f);
        PartialStream::new(self.fb, mapped)
    }

    pub fn inspect<F>(self, f: F) -> PartialStream<Inspect<Ps, F>>
    where
        F: FnMut(&Ps::Item),
        Ps: Sized,
    {
        let inspected = self.stream.inspect(f);
        PartialStream::new(self.fb, inspected)
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

    pub fn flat_map<U, F>(self, f: F) -> PartialStream<FlatMap<Ps, U, F>>
    where
        F: FnMut(Ps::Item) -> U,
        U: Stream,
        Ps: Sized,
    {
        let fm = self.stream.flat_map(f);
        PartialStream::new(self.fb, fm)
    }

    pub fn forward<S>(self, sink: S) -> Forward<Ps, S>
    where
        S: Sink<Ps::Ok, Error = Ps::Error>,
        Ps: TryStream + Sized,
    {
        self.stream.forward(sink)
    }
}

impl<Si, Item> PartialStream<Si>
where
    Item: SandData,
    Si: Stream<Item = Result<Item, FError>> + Send + 'static,
{
    pub fn exchange<R>(self, route: R) -> PartialStream<GeneralReceiver<Item>>
    where
        R: Fn(&Item) -> u64 + Send + 'static,
    {
        let (senders, receiver) = self.fb.allocate::<Item>();
        let sink = RouteSink::new(route, senders);
        let st = self.stream.multi_forward(sink);
        self.fb.add_stage(st);
        PartialStream::new(self.fb, receiver)
    }
}
