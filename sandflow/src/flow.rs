use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use futures::FutureExt;

use crate::FError;

pub type DynFuture<D> = Box<dyn Future<Output = D> + Send + Unpin + 'static>;

#[derive(Clone)]
pub struct SandFlowBuilder {
    parallel: usize,
    index: usize,
    stages: Rc<RefCell<Vec<DynFuture<()>>>>,
}

impl SandFlowBuilder {
    pub fn new(parallel: usize, index: usize) -> Self {
        SandFlowBuilder { parallel, index, stages: Rc::new(RefCell::new(vec![])) }
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
