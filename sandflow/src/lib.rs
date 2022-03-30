use futures::{SinkExt, Stream, StreamExt};

use crate::channels::multi_sink::RoundRobinSink;
use crate::errors::FError;
use crate::flow::SandFlowBuilder;
use crate::streams::pstream::SPStream;
use crate::streams::StreamExtend;

pub trait SandReq: Send + Sync + 'static {}

impl<T> SandReq for T where T: Send + Sync + 'static {}

mod channels;
mod errors;
mod flow;
mod streams;

pub fn spawn<St, DI, DO, F, FF>(parallel: usize, stream: St, func: F) -> Box<dyn Stream<Item = Result<DO, FError>> + Unpin>
where
    DI: SandReq,
    DO: SandReq,
    St: Stream<Item = Result<DI, FError>> + Send + 'static,
    F: Fn() -> FF,
    FF: FnOnce(SPStream<St::Item>) -> SPStream<Result<DO, FError>>,
{
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
    let mut fbs = Vec::new();

    for i in 0..parallel {
        let (tx, rx) = futures::channel::mpsc::channel(1024);
        txs.push(tx.sink_map_err(|e| FError::ChSend(e)));
        rxs.push(rx);
        fbs.push(SandFlowBuilder::new(parallel, i));
    }

    let source = stream.multi_forward(RoundRobinSink::new(txs));
    fbs[0].add_stage(source);

    todo!()
}
