use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use crate::FError;

pub type DynFuture<D> = Box<dyn Future<Output = Result<D, FError>> + Send + 'static>;

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
        F: Future<Output = Result<(), FError>> + Send + 'static,
    {
        self.stages.borrow_mut().push(Box::new(stage));
    }
}

pub struct SandFlow {
    parallel: usize,
    index: usize,
    stages: Vec<DynFuture<()>>,
}
