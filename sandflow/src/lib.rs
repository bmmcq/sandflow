#[macro_use]
extern crate log;

pub use flow::worker_index;
use futures::Stream;

use crate::errors::FError;
use crate::flow::SandFlowBuilder;
use crate::stages::sink::select::SelectSink;
use crate::stages::sink::LocalStageSink;
use crate::stages::source::{SourceStage, StageInput};
use crate::stages::utils::ErrorHook;
use crate::streams::pstream::{InputStream, PStream};
use crate::streams::result_stream::ResultStream;
use crate::streams::StreamExtend;

pub trait SandData: Send + Sync + 'static {}

impl<T> SandData for T where T: Send + Sync + 'static {}

mod channels;
mod errors;
mod flow;
mod stages;
mod streams;
mod test;

const DEFAULT_PARALLEL: u32 = 2;

pub fn spawn<Si, So, DI, DO, F, FF>(source: Si, func: F) -> ResultStream<DO>
where
    DI: SandData,
    DO: SandData,
    Si: Stream<Item = Result<DI, FError>> + Send + Unpin + 'static,
    So: Stream<Item = Result<DO, FError>> + Send + 'static,
    F: Fn() -> FF,
    FF: FnOnce(InputStream<DI>) -> PStream<So>,
{
    let parallel = std::env::var("SANDFLOW_DEFAULT_PARALLEL")
        .map(|val| val.parse::<u32>().unwrap_or(DEFAULT_PARALLEL))
        .unwrap_or(DEFAULT_PARALLEL) as usize;
    spawn_job(0, parallel, source, func)
}

pub fn spawn_job<Si, So, DI, DO, F, FF>(job_id: u64, parallel: usize, source: Si, func: F) -> ResultStream<DO>
where
    DI: SandData,
    DO: SandData,
    Si: Stream<Item = Result<DI, FError>> + Send + Unpin + 'static,
    So: Stream<Item = Result<DO, FError>> + Send + 'static,
    F: Fn() -> FF,
    FF: FnOnce(InputStream<DI>) -> PStream<So>,
{
    let mut txs = Vec::new();
    let mut rxs = Vec::new();

    for _ in 0..parallel {
        let (tx, rx) = futures::channel::mpsc::channel::<DI>(1024);
        txs.push(LocalStageSink::<DI>::new(tx));
        rxs.push(rx);
    }

    let source_fut = source.select_forward(SelectSink::round_select(txs));
    let (tx, rx) = futures::channel::mpsc::channel::<DO>(1024);

    let mut primary = SandFlowBuilder::new(job_id, parallel);
    let mut mirrors = Vec::with_capacity(parallel - 1);

    for (i, r) in rxs.into_iter().enumerate() {
        let fb = if i == 0 {
            primary.clone()
        } else {
            let mirror = primary.fork_mirror();
            mirrors.push(mirror.clone());
            mirror
        };

        let st = PStream::new(fb.clone(), StageInput::new(r));
        let progress = func();
        let last = progress(st);
        let sink = LocalStageSink::<DO>::new(tx.clone());
        let last_fut = last.forward(sink);
        fb.add_stage(last_fut);
    }

    let error_hook = primary.get_error_hook().clone();
    sandflow_executor::spawn(SourceStage::new(job_id, error_hook.clone(), source_fut));
    sandflow_executor::spawn(primary.build());
    for m in mirrors {
        sandflow_executor::spawn(m.build());
    }

    ResultStream::new(error_hook, rx)
}
