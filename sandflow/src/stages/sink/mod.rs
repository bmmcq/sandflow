use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc::Sender;
use futures::{ready, Sink};

use crate::FError;

pub trait TrySink<T> {
    type Error;

    /// Try to  sink item , return `Some(T)` back if sink not ready, otherwise return `None` if no error occurs;
    fn try_sink(self: Pin<&mut Self>, item: T, cx: &mut Context<'_>) -> Result<Option<T>, Self::Error>;

    /// Same as `Sink`'s poll_flush
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;

    /// Same as `Sink`'s poll_close
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}

/// Sink data between local stages;
pub struct LocalStageSink<T>(Sender<T>);

impl<T> LocalStageSink<T> {
    pub fn new(sender: Sender<T>) -> Self {
        LocalStageSink(sender)
    }
}

impl<T> Sink<T> for LocalStageSink<T> {
    type Error = FError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(self.get_mut().0.poll_ready(cx)) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(FError::ChSend(e))),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        match self.get_mut().0.start_send(item) {
            Ok(_) => Ok(()),
            Err(e) => Err(FError::ChSend(e)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(Pin::new(&mut self.get_mut().0).poll_flush(cx)) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(FError::ChSend(e))),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(Pin::new(&mut self.get_mut().0).poll_close(cx)) {
            Ok(_) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(FError::ChSend(e))),
        }
    }
}

pub(crate) mod select;
