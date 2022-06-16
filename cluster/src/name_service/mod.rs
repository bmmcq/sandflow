use std::net::SocketAddr;

use async_trait::async_trait;
use valley::errors::VError;
use valley::name_service::{NameService, StaticNameService};
use valley::ServerId;

mod etcd;

#[allow(dead_code)]
pub enum NameServiceImpl {
    StaticConfig(StaticNameService),
    ETCD(()),
    Custom(Box<dyn NameService + Send + Sync + 'static>),
}

#[async_trait]
impl NameService for NameServiceImpl {
    async fn register(&self, server_id: ServerId, addr: SocketAddr) -> Result<(), VError> {
        match self {
            NameServiceImpl::StaticConfig(n) => n.register(server_id, addr).await,
            NameServiceImpl::ETCD(_) => {
                unimplemented!()
            }
            NameServiceImpl::Custom(n) => n.register(server_id, addr).await,
        }
    }

    async fn get_registered(&self, server_id: ServerId) -> Result<Option<SocketAddr>, VError> {
        match self {
            NameServiceImpl::StaticConfig(n) => n.get_registered(server_id).await,
            NameServiceImpl::ETCD(_) => {
                unimplemented!()
            }
            NameServiceImpl::Custom(n) => n.get_registered(server_id).await,
        }
    }
}
