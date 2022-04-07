use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use futures::FutureExt;

use crate::channels::{GeneralReceiver, GeneralSender, Port};
use crate::{FError, SandData};

pub type DynFuture<D> = Box<dyn Future<Output = D> + Send + Unpin + 'static>;

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
        F: Future<Output = Result<(), FError>> + Send + Unpin + 'static,
    {
        let ok_st = stage.map(|f| {
            if let Err(e) = f {
                // TODO: sink error;
                eprintln!("get error: {}", e)
            }
            ()
        });
        self.stages.borrow_mut().push(Box::new(ok_st));
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

        SandFlow { parallel: self.parallel, index: self.index, stages: stages_final }
    }
}

pub struct SandFlow {
    parallel: usize,
    index: usize,
    stages: Vec<DynFuture<()>>,
}

impl SandFlow {
    pub fn get_parallel(&self) -> usize {
        self.parallel
    }

    pub fn get_index(&self) -> usize {
        self.index
    }

    pub fn run(self) {
        for st in self.stages {
            sandflow_executor::spawn(st);
        }
    }
}
