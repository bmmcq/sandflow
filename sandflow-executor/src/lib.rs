#[macro_use]
extern crate lazy_static;

use std::future::Future;

use futures::executor::{ThreadPool, ThreadPoolBuilder};

lazy_static! {
    static ref EXECUTOR: SandFlowExecutor = init();
}

pub fn spawn<F>(task: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    EXECUTOR.spawn(task)
}

pub struct SandFlowExecutor {
    pool: ThreadPool,
}

impl SandFlowExecutor {
    pub fn spawn<F>(&self, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.pool.spawn_ok(task)
    }
}

pub fn init() -> SandFlowExecutor {
    let mut pool_builder = ThreadPoolBuilder::new();
    pool_builder.pool_size(16);
    let pool = pool_builder.create().expect("create executor failure;");
    SandFlowExecutor { pool }
}
