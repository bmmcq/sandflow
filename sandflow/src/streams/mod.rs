use futures::stream::Fuse;
use futures::{Stream, StreamExt, TryStream};

use crate::stages::sink::TrySink;
use crate::streams::select_forward::SelectForward;

pub trait StreamExtend: Stream {
    fn select_forward<Si>(self, sink: Si) -> SelectForward<Fuse<Self>, Si, Self::Ok>
    where
        Si: TrySink<Self::Ok, Error = Self::Error>,
        Self: TryStream + Sized,
    {
        let fuse = self.fuse();
        SelectForward::new(fuse, sink)
    }
}

impl<T: ?Sized> StreamExtend for T where T: Stream {}

pub mod pstream;
pub mod result_stream;
pub mod select_forward;
