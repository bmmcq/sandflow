use futures::{SinkExt, Stream};

use crate::channels::multi_sink::RoundRobinSink;
use crate::errors::FError;
use crate::flow::SandFlowBuilder;
use crate::streams::pstream::{DynStream, PartialStream};
use crate::streams::result_stream::ResultStream;
use crate::streams::StreamExtend;

pub trait SandData: Send + Sync + 'static {}

impl<T> SandData for T where T: Send + Sync + 'static {}

mod channels;
mod errors;
mod flow;
mod streams;
mod test;

pub fn spawn<Si, So, DI, DO, F, FF>(parallel: usize, source: Si, func: F) -> ResultStream<DO>
where
    DI: SandData,
    DO: SandData,
    Si: Stream<Item = Result<DI, FError>> + Send + Unpin + 'static,
    So: Stream<Item = Result<DO, FError>> + Send + 'static,
    F: Fn() -> FF,
    FF: FnOnce(PartialStream<DynStream<DI>>) -> PartialStream<So>,
{
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    let mut fbs = Vec::new();

    for i in 0..parallel {
        let (tx, rx) = futures::channel::mpsc::channel::<DI>(1024);
        txs.push(tx.sink_map_err(|e| FError::ChSend(e)));
        rxs.push(rx);
        fbs.push(SandFlowBuilder::new(parallel, i));
    }

    let source_fut = source.multi_forward(RoundRobinSink::new(txs));
    fbs[0].add_stage(source_fut);

    let (tx, rx) = futures::channel::mpsc::channel::<DO>(1024);
    for (i, r) in rxs.into_iter().enumerate() {
        let fb = fbs[i].clone();
        let st = PartialStream::new(fb, r).to_box_dyn();
        let progress = func();
        let last = progress(st).to_box_dyn();
        let sink = tx.clone().sink_map_err(|e| FError::ChSend(e));
        let last_fut = last.forward(sink);
        fbs[i].add_stage(last_fut);
    }

    for fb in fbs {
        let task = fb.build();
        task.run();
    }

    ResultStream::new(rx)
}
