use futures::stream::Fuse;
use futures::{Stream, StreamExt, TryStream};

use crate::channels::multi_sink::MultiSink;
use crate::streams::multi_forward::MultiForward;

pub trait StreamExtend: Stream {
    fn multi_forward<Si>(self, sink: Si) -> MultiForward<Fuse<Self>, Si, Self::Ok>
    where
        Si: MultiSink<Self::Ok, Error = Self::Error>,
        Self: TryStream + Sized,
    {
        let fuse = self.fuse();
        MultiForward::new(fuse, sink)
    }
}

impl<T: ?Sized> StreamExtend for T where T: Stream {}

pub mod multi_forward;
pub mod pstream;
pub mod result_stream;
