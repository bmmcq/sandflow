use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::JoinAll;

use crate::channels::Port;
use crate::stages::utils::ErrorHook;
use crate::stages::{AsyncStage, StageSink, StageSource};
use crate::{FError, SandData};

#[derive(Clone)]
pub struct SandFlowBuilder {
    job_id: u64,
    parallel: usize,
    index: usize,
    stages: Rc<RefCell<Vec<AsyncStage>>>,
    ports: Rc<RefCell<Port>>,
    error_hook: Arc<ErrorHook>,
}

impl SandFlowBuilder {
    pub fn new(job_id: u64, parallel: usize, index: usize, error_hook: Arc<ErrorHook>) -> Self {
        SandFlowBuilder {
            job_id,
            parallel,
            index,
            stages: Rc::new(RefCell::new(vec![])),
            ports: Rc::new(RefCell::new(0)),
            error_hook,
        }
    }

    pub fn get_job_id(&self) -> u64 {
        self.job_id
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    pub fn stage_size(&self) -> usize {
        self.stages.borrow().len()
    }

    pub fn add_stage<F>(&self, stage: F)
    where
        F: Future<Output = Result<(), FError>> + Send + 'static,
    {
        let mut stages_borrow = self.stages.borrow_mut();
        let next_stage_id = stages_borrow.len() as u32;
        stages_borrow.push(AsyncStage::new(self.index as u32, next_stage_id, stage, &self.error_hook));
    }

    pub fn allocate<T: SandData>(&self) -> (Vec<StageSink<T>>, StageSource<T>) {
        let mut bp = self.ports.borrow_mut();
        let port = *bp;
        let (ts, r) = crate::channels::local::bind::<T>(port, self.parallel, 1024);
        *bp += 1;
        let mut senders = Vec::with_capacity(self.parallel);
        for t in ts {
            senders.push(StageSink::new(t));
        }
        (senders, StageSource::new(r))
    }

    pub fn build(self) -> SandFlow {
        let mut st = self.stages.borrow_mut();
        let mut stages_final = Vec::with_capacity(st.len());

        for fut in st.drain(..) {
            stages_final.push(fut);
        }

        let task = futures::future::join_all(stages_final);
        SandFlow { parallel: self.parallel, worker_id: self.index as u32, task }
    }
}

pub struct SandFlow {
    parallel: usize,
    worker_id: u32,
    task: JoinAll<AsyncStage>,
}

impl SandFlow {
    pub fn get_parallel(&self) -> usize {
        self.parallel
    }

    pub fn get_index(&self) -> u32 {
        self.worker_id
    }
}

thread_local! {
    static WORKER_INDEX : Cell<isize> = Cell::new(-1);
}

#[allow(dead_code)]
struct WorkerIndexGuard {
    index: u32,
}

impl WorkerIndexGuard {
    pub fn new(index: u32) -> Self {
        let set_idx = index as isize;
        WORKER_INDEX.with(|idx| idx.set(set_idx));
        WorkerIndexGuard { index }
    }
}

impl Drop for WorkerIndexGuard {
    fn drop(&mut self) {
        WORKER_INDEX.with(|idx| idx.set(-1));
    }
}

pub fn worker_id() -> isize {
    WORKER_INDEX.with(|idx| idx.get())
}

impl Future for SandFlow {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = WorkerIndexGuard::new(this.worker_id);
        Pin::new(&mut this.task).poll(cx).map(|_| ())
    }
}
