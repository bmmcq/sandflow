use std::collections::VecDeque;

use futures::channel::mpsc::{Receiver, Sender};

pub fn alloc<T: 'static>(peers: usize, capacity: usize) -> VecDeque<LocalChannel<T>> {
    let mut txs = Vec::with_capacity(peers);
    let mut rxs = Vec::with_capacity(peers);
    for _ in 0..peers {
        let (tx, rx) = futures::channel::mpsc::channel::<T>(capacity);
        txs.push(tx);
        rxs.push(rx)
    }
    let mut channels = VecDeque::with_capacity(peers);
    for receiver in rxs {
        let senders = txs.clone();
        channels.push_back(LocalChannel { senders, receiver });
    }

    channels
}

pub struct LocalChannel<T> {
    senders: Vec<Sender<T>>,
    receiver: Receiver<T>,
}

impl<T> LocalChannel<T> {
    pub fn take(self) -> (Vec<Sender<T>>, Receiver<T>) {
        (self.senders, self.receiver)
    }
}
