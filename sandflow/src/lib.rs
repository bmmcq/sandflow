#[macro_use]
extern crate log;

use std::sync::Arc;

pub use flow::worker_id;
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
    let mut fbs = Vec::new();

    let err_hook = Arc::new(ErrorHook::new());
    for i in 0..parallel {
        let (tx, rx) = futures::channel::mpsc::channel::<DI>(1024);
        txs.push(LocalStageSink::<DI>::new(tx));
        rxs.push(rx);
        fbs.push(SandFlowBuilder::new(job_id, parallel, i, err_hook.clone()));
    }

    let source_fut = source.select_forward(SelectSink::round_select(txs));

    let (tx, rx) = futures::channel::mpsc::channel::<DO>(1024);
    for (i, r) in rxs.into_iter().enumerate() {
        let fb = fbs[i].clone();
        let st = PStream::new(fb, StageInput::new(r));
        let progress = func();
        let last = progress(st);
        let sink = LocalStageSink::<DO>::new(tx.clone());
        let last_fut = last.forward(sink);
        fbs[i].add_stage(last_fut);
    }

    for fb in fbs {
        debug!("spawn worker[{}] of job({}) with {} stages;", fb.get_index(), fb.get_job_id(), fb.stage_size());
        let task = fb.build();
        sandflow_executor::spawn(task);
    }

    sandflow_executor::spawn(SourceStage::new(job_id, err_hook.clone(), source_fut));

    ResultStream::new(err_hook, rx)
}
