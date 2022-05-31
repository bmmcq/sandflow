use std::future::Future;

use futures::stream::{FlatMap, Forward, Inspect, Map, Then};
use futures::{Sink, Stream, StreamExt, TryStream};

use crate::channels::multi_sink::RouteSink;
use crate::stages::StageSource;
use crate::{FError, SandData, SandFlowBuilder, StreamExtend};

pub struct PStream<St> {
    stream: St,
    fb: SandFlowBuilder,
}

impl<St> PStream<St> {
    pub fn new(fb: SandFlowBuilder, stream: St) -> Self {
        Self { stream, fb }
    }
}

pub type SourceStream<T> = PStream<StageSource<T>>;

impl<St> PStream<St>
where
    St: Stream + Send,
{
    pub fn map<T, F>(self, f: F) -> PStream<Map<St, F>>
    where
        F: FnMut(St::Item) -> T,
        St: Sized,
    {
        let mapped = self.stream.map(f);
        PStream::new(self.fb, mapped)
    }

    pub fn inspect<F>(self, f: F) -> PStream<Inspect<St, F>>
    where
        F: FnMut(&St::Item),
        St: Sized,
    {
        let inspected = self.stream.inspect(f);
        PStream::new(self.fb, inspected)
    }

    pub fn then<Fut, F>(self, f: F) -> PStream<Then<St, Fut, F>>
    where
        F: FnMut(St::Item) -> Fut,
        Fut: Future,
        Self: Sized,
    {
        let then = self.stream.then(f);
        PStream::new(self.fb, then)
    }

    pub fn flat_map<U, F>(self, f: F) -> PStream<FlatMap<St, U, F>>
    where
        F: FnMut(St::Item) -> U,
        U: Stream,
        St: Sized,
    {
        let fm = self.stream.flat_map(f);
        PStream::new(self.fb, fm)
    }

    pub fn forward<S>(self, sink: S) -> Forward<St, S>
    where
        S: Sink<St::Ok, Error = St::Error>,
        St: TryStream + Sized,
    {
        self.stream.forward(sink)
    }
}

impl<Si, Item> PStream<Si>
where
    Item: SandData,
    Si: Stream<Item = Result<Item, FError>> + Send + 'static,
{
    pub fn exchange<R>(self, route: R) -> SourceStream<Item>
    where
        R: Fn(&Item) -> u64 + Send + 'static,
    {
        let (senders, receiver) = self.fb.allocate::<Item>();
        let sink = RouteSink::new(route, senders);
        let st = self.stream.multi_forward(sink);
        self.fb.add_stage(st);
        PStream::new(self.fb, receiver)
    }
}
