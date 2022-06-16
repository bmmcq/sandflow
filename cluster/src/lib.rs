#[macro_use]
extern crate lazy_static;

use std::net::SocketAddr;

use valley::codec::Encode;
use valley::errors::VError;
use valley::name_service::NameService;
pub use valley::name_service::StaticNameService;
pub use valley::ServerId;
pub use valley::{ChannelId, VPollSender, VReceiverStream};

use crate::name_service::NameServiceImpl;
use crate::server::Server;

mod name_service;
mod server;

pub fn get_local_server_id() -> Option<ServerId> {
    crate::server::get_server_ref().map(|s| s.get_server_id())
}

pub fn get_local_server_addr() -> Option<SocketAddr> {
    crate::server::get_server_ref().map(|s| s.get_address())
}

pub async fn alloc_channel<T>(ch_id: ChannelId, servers: &[ServerId]) -> Result<(VPollSender<T>, VReceiverStream), VError>
where
    T: Encode + Send + 'static,
{
    if let Some(server) = crate::server::get_server_ref() {
        server.alloc_channel(ch_id, servers).await
    } else {
        Err(VError::ServerNotStart(0))
    }
}

#[allow(dead_code)]
enum CommProto {
    TCP(i32),
    QUIC,
}

#[allow(dead_code)]
pub struct ClusterConfig {
    /// The unique id of local server;
    local_server_id: ServerId,
    /// The socket addr of local server before bind;
    local_server_addr: SocketAddr,
    /// The kind of communication connection;
    protocol: CommProto,
    /// The size of threads used to do communicating;
    thread_size: usize,
}

pub async fn start_server_with_hosts(config: ClusterConfig, hosts: Vec<(ServerId, SocketAddr)>) -> Result<(), VError> {
    let name_service = NameServiceImpl::StaticConfig(StaticNameService::new(hosts));
    match config.protocol {
        CommProto::TCP(p_size) => {
            if p_size < 0 {
                let mut server = valley::server::new_tcp_server(config.local_server_id, config.local_server_addr, name_service);
                server.start().await?;
                Server::set_tcp_server(server);
            } else {
                unimplemented!("pooled  tcp connections");
            }
        }
        CommProto::QUIC => {
            let mut server = valley::server::new_quic_server(config.local_server_id, config.local_server_addr, name_service);
            server.start().await?;
            Server::set_quic_server(server);
        }
    }
    Ok(())
}

pub async fn start_server<N>(_config: ClusterConfig, _name_service: N) -> Result<(), VError>
where
    N: NameService + Send + 'static,
{
    todo!()
}
