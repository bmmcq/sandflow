use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc::{Receiver, Sender};
use futures::{Sink, Stream};

use crate::FError;

pub mod local;
pub mod multi_sink;

pub type Port = u32;

pub enum GeneralSender<T> {
    Local(Sender<T>),
}

pub enum GeneralReceiver<T> {
    Local(Receiver<T>),
}

impl<T> Sink<T> for GeneralSender<T> {
    type Error = FError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            GeneralSender::Local(tx) => Pin::new(tx).poll_ready(cx).map_err(|e| FError::ChSend(e)),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        match self.get_mut() {
            GeneralSender::Local(tx) => Pin::new(tx).start_send(item).map_err(|e| FError::ChSend(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            GeneralSender::Local(tx) => Pin::new(tx).poll_flush(cx).map_err(|e| FError::ChSend(e)),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match self.get_mut() {
            GeneralSender::Local(tx) => Pin::new(tx).poll_close(cx).map_err(|e| FError::ChSend(e)),
        }
    }
}

impl<T> Stream for GeneralReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            GeneralReceiver::Local(rx) => Pin::new(rx).poll_next(cx),
        }
    }
}
