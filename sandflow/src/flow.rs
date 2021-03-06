use std::any::Any;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::future::JoinAll;
use sandflow_cluster::ServerId;

use crate::channels::local::LocalChannel;
use crate::errors::FError;
use crate::stages::sink::LocalStageSink;
use crate::stages::source::StageInput;
use crate::stages::utils::ErrorHook;
use crate::stages::AsyncStage;
use crate::SandData;

#[derive(Clone)]
pub struct SandFlowBuilder {
    job_id: u64,
    local_peers: usize,
    worker_index: usize,
    server_index: usize,
    stages: Rc<RefCell<Vec<AsyncStage>>>,
    next_ch_index: Rc<RefCell<usize>>,
    alloc_channels: Rc<RefCell<Vec<VecDeque<Box<dyn Any>>>>>,
    next_worker_index: usize,
    error_hook: Arc<ErrorHook>,
    servers: Arc<Vec<ServerId>>,
}

impl SandFlowBuilder {
    pub fn new(job_id: u64, parallel: usize) -> Self {
        Self::with_servers(job_id, parallel, 0, Arc::new(vec![]))
    }

    pub fn with_servers(job_id: u64, parallel: usize, server_index: usize, servers: Arc<Vec<ServerId>>) -> Self {
        Self {
            job_id,
            local_peers: parallel,
            worker_index: 0,
            server_index,
            stages: Rc::new(RefCell::new(Vec::new())),
            next_ch_index: Rc::new(RefCell::new(0)),
            alloc_channels: Rc::new(RefCell::new(Vec::new())),
            next_worker_index: 0,
            error_hook: Arc::new(ErrorHook::new()),
            servers,
        }
    }

    pub fn fork_mirror(&mut self) -> Self {
        if self.worker_index == 0 {
            let worker_index = self.next_worker_index + 1;
            if worker_index >= self.local_peers {
                panic!("can't fork more peers;");
            }
            self.next_worker_index += 1;
            Self {
                job_id: self.job_id,
                local_peers: self.local_peers,
                worker_index,
                server_index: self.server_index,
                stages: Rc::new(RefCell::new(vec![])),
                next_ch_index: Rc::new(RefCell::new(0)),
                alloc_channels: self.alloc_channels.clone(),
                next_worker_index: 0,
                error_hook: self.error_hook.clone(),
                servers: self.servers.clone(),
            }
        } else {
            panic!("can't fork mirror from mirror;")
        }
    }

    pub fn get_job_id(&self) -> u64 {
        self.job_id
    }

    pub fn get_index(&self) -> usize {
        self.worker_index
    }

    pub fn get_local_peers(&self) -> usize {
        self.local_peers
    }

    pub fn stage_size(&self) -> usize {
        self.stages.borrow().len()
    }

    pub fn server_size(&self) -> usize {
        self.servers.len()
    }

    pub fn get_error_hook(&self) -> &Arc<ErrorHook> {
        &self.error_hook
    }

    pub fn add_stage<F>(&self, stage: F)
    where
        F: Future<Output = Result<(), FError>> + Send + 'static,
    {
        let mut stages_borrow = self.stages.borrow_mut();
        let next_stage_id = stages_borrow.len() as u32;
        stages_borrow.push(AsyncStage::new(self.worker_index as u32, next_stage_id, stage, &self.error_hook));
    }

    pub fn alloc_local<T: SandData>(&self) -> (Vec<LocalStageSink<T>>, StageInput<T>) {
        assert!(self.local_peers > 1, "local peers should be larger than 1");
        let mut next_ch_index = self.next_ch_index.borrow_mut();
        let ch_index = *next_ch_index;
        let (senders, receiver) = if self.worker_index == 0 {
            let mut channels = crate::channels::local::alloc::<T>(self.local_peers, 1024);
            assert_eq!(channels.len(), self.local_peers);
            let (t, r) = channels.pop_front().unwrap().take();
            let mut queue = VecDeque::with_capacity(channels.len());
            for ch in channels {
                queue.push_back(Box::new(ch) as Box<dyn Any>);
            }
            let mut alloc_chs = self.alloc_channels.borrow_mut();
            assert_eq!(alloc_chs.len(), ch_index);
            alloc_chs.push(queue);
            (t, r)
        } else {
            let mut alloc_chs = self.alloc_channels.borrow_mut();
            assert!(alloc_chs.len() > ch_index);
            let ch_any = alloc_chs[ch_index].pop_front().expect("local channel lost;");
            ch_any
                .downcast::<LocalChannel<T>>()
                .expect("type cast fail;")
                .take()
        };
        *next_ch_index += 1;
        let mut sinks = Vec::with_capacity(self.local_peers);
        for t in senders {
            sinks.push(LocalStageSink::new(t));
        }
        (sinks, StageInput::new(receiver))
    }

    pub fn build(self) -> SandFlow {
        let mut st = self.stages.borrow_mut();
        let mut stages_final = Vec::with_capacity(st.len());

        for fut in st.drain(..) {
            stages_final.push(fut);
        }

        let task = futures::future::join_all(stages_final);
        SandFlow { local_peers: self.local_peers, worker_index: self.worker_index, task }
    }
}

pub struct SandFlow {
    local_peers: usize,
    worker_index: usize,
    task: JoinAll<AsyncStage>,
}

impl SandFlow {
    pub fn local_peers(&self) -> usize {
        self.local_peers
    }

    pub fn get_index(&self) -> usize {
        self.worker_index
    }
}

thread_local! {
    static WORKER_INDEX : RefCell<Option<usize>> = RefCell::new(None);
}

#[allow(dead_code)]
struct WorkerIndexGuard {
    index: usize,
}

impl WorkerIndexGuard {
    pub fn new(index: usize) -> Self {
        WORKER_INDEX.with(|idx| idx.borrow_mut().replace(index));
        WorkerIndexGuard { index }
    }
}

impl Drop for WorkerIndexGuard {
    fn drop(&mut self) {
        WORKER_INDEX.with(|idx| idx.borrow_mut().take());
    }
}

pub fn worker_index() -> Option<usize> {
    WORKER_INDEX.with(|idx| *idx.borrow())
}

impl Future for SandFlow {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let _guard = WorkerIndexGuard::new(this.worker_index);
        Pin::new(&mut this.task).poll(cx).map(|_| ())
    }
}
