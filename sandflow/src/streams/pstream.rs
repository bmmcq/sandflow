use crate::streams::DynStream;
use crate::SandReq;

pub struct SPStream<D: SandReq> {
    stream: DynStream<D>,
    peers: usize,
}
