use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};

use futures::channel::mpsc::{Receiver, Sender};

use crate::channels::Port;

thread_local! {
    static LOCAL_CHANNEL_TABLE : LocalChannelTable = LocalChannelTable::new();
}

pub fn bind<T: 'static>(port: Port, peers: usize, capacity: usize) -> (Vec<Sender<T>>, Receiver<T>) {
    let opt = LOCAL_CHANNEL_TABLE.with(|tb| tb.take(port));
    if let Some(ch) = opt {
        let casted = ch.downcast::<LocalChannel<T>>().expect("downcast failure");
        casted.take()
    } else {
        let mut txs = Vec::with_capacity(peers);
        let mut rxs = Vec::with_capacity(peers);
        for _ in 0..peers {
            let (tx, rx) = futures::channel::mpsc::channel::<T>(capacity);
            txs.push(tx);
            rxs.push(rx)
        }
        let mut channels = VecDeque::with_capacity(peers - 1);
        for rx in rxs.drain(1..) {
            let txs = txs.clone();
            let ch = LocalChannel::<T>::new(txs, rx);
            channels.push_back(Box::new(ch) as Box<dyn Any>);
        }

        // pop first
        let first = rxs.pop().expect("at least one channel");
        let first_ch = LocalChannel::<T>::new(txs, first);
        LOCAL_CHANNEL_TABLE.with(|tb| tb.register(port, channels));
        first_ch.take()
    }
}

struct LocalChannel<T> {
    senders: Vec<Sender<T>>,
    receiver: Receiver<T>,
}

impl<T> LocalChannel<T> {
    fn new(senders: Vec<Sender<T>>, receiver: Receiver<T>) -> Self {
        Self { senders, receiver }
    }

    fn take(self) -> (Vec<Sender<T>>, Receiver<T>) {
        (self.senders, self.receiver)
    }
}

struct LocalChannelTable {
    table: RefCell<HashMap<Port, VecDeque<Box<dyn Any>>>>,
}

impl LocalChannelTable {
    fn new() -> Self {
        Self { table: RefCell::new(HashMap::new()) }
    }

    fn register(&self, port: Port, channel: VecDeque<Box<dyn Any>>) {
        self.table.borrow_mut().insert(port, channel);
    }

    fn take(&self, port: Port) -> Option<Box<dyn Any>> {
        let mut borrow = self.table.borrow_mut();
        let ch = borrow.get_mut(&port)?;
        ch.pop_front()
    }
}
