use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use futures::future::JoinAll;

use futures::FutureExt;

use crate::channels::{GeneralReceiver, GeneralSender, Port};
use crate::{FError, SandData};

pub type DynFuture<D> = Pin<Box<dyn Future<Output = D> + Send + 'static>>;

#[derive(Clone)]
pub struct SandFlowBuilder {
    parallel: usize,
    index: usize,
    stages: Rc<RefCell<Vec<DynFuture<()>>>>,
    ports: Rc<RefCell<Port>>,
}

impl SandFlowBuilder {
    pub fn new(parallel: usize, index: usize) -> Self {
        SandFlowBuilder { parallel, index, stages: Rc::new(RefCell::new(vec![])), ports: Rc::new(RefCell::new(0)) }
    }

    pub fn add_stage<F>(&self, stage: F)
    where
        F: Future<Output = Result<(), FError>> + Send + 'static,
    {
        let ok_st = stage.map(|f| {
            if let Err(e) = f {
                // TODO: sink error;
                eprintln!("get error: {}", e)
            }
            ()
        });

        self.stages.borrow_mut().push(Box::pin(ok_st));
    }

    pub fn allocate<T: SandData>(&self) -> (Vec<GeneralSender<T>>, GeneralReceiver<T>) {
        let mut bp = self.ports.borrow_mut();
        let port = *bp;
        let (ts, r) = crate::channels::local::bind::<T>(port, self.parallel, 1024);
        *bp += 1;
        let mut senders = Vec::with_capacity(self.parallel);
        for t in ts {
            senders.push(GeneralSender::Local(t));
        }
        (senders, GeneralReceiver::Local(r))
    }

    pub fn build(self) -> SandFlow {
        let mut st = self.stages.borrow_mut();
        let mut stages_final = Vec::with_capacity(st.len());

        for fut in st.drain(..) {
            stages_final.push(fut);
        }

        let task = futures::future::join_all(stages_final);
        SandFlow { parallel: self.parallel, index: self.index as u32, task }
    }
}

pub struct SandFlow {
    parallel: usize,
    index: u32,
    task: JoinAll<DynFuture<()>>
}

impl SandFlow {
    pub fn get_parallel(&self) -> usize {
        self.parallel
    }

    pub fn get_index(&self) -> u32 {
        self.index
    }
}

thread_local! {
    static WORKER_INDEX : Cell<isize> = Cell::new(-1);
}

#[allow(dead_code)]
struct WorkerIndexGuard {
    index:  u32,
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
        let _guard = WorkerIndexGuard::new(this.index);
        Pin::new(&mut this.task).poll(cx).map(|_| ())
    }
}


