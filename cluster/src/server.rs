use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, TryLockError, Weak};

use valley::codec::Encode;
use valley::connection::quic::QUIConnBuilder;
use valley::connection::tcp::TcpConnBuilder;
use valley::errors::VError;
use valley::server::ValleyServer;
use valley::{ChannelId, ServerId, VPollSender, VReceiverStream};

use crate::name_service::NameServiceImpl;

#[allow(dead_code)]
pub(crate) enum Server {
    TCPServer(ValleyServer<NameServiceImpl, TcpConnBuilder>),
    QUICServer(ValleyServer<NameServiceImpl, QUIConnBuilder>),
}

lazy_static! {
    static ref GLOBAL_SERVER_REF: RwLock<Option<Arc<Server>>> = RwLock::new(None);
}

thread_local! {
    static SERVER_CACHE: RefCell<Option<Weak<Server>>> = RefCell::new(None);
}

pub(crate) fn get_server_ref() -> Option<Arc<Server>> {
    let server_ref = SERVER_CACHE.with(|cache| {
        let mut br = cache.borrow_mut();
        if let Some(server) = br.as_ref() {
            Some(server.clone())
        } else {
            match GLOBAL_SERVER_REF.try_read() {
                Ok(r_lock) => {
                    if let Some(arc) = r_lock.as_ref() {
                        let server = Arc::downgrade(arc);
                        *br = Some(server.clone());
                        Some(server)
                    } else {
                        None
                    }
                }
                Err(TryLockError::WouldBlock) => None,
                Err(TryLockError::Poisoned(e)) => {
                    panic!("read lock of global server fail: {}", e);
                }
            }
        }
    });
    server_ref.and_then(|weak| weak.upgrade())
}

impl Server {
    pub fn set_tcp_server(server: ValleyServer<NameServiceImpl, TcpConnBuilder>) {
        let mut w_lock = GLOBAL_SERVER_REF
            .write()
            .expect("lock global server for write fail");
        let g_server = Arc::new(Server::TCPServer(server));
        w_lock.replace(g_server);
    }

    pub fn set_quic_server(server: ValleyServer<NameServiceImpl, QUIConnBuilder>) {
        let mut w_lock = GLOBAL_SERVER_REF
            .write()
            .expect("lock global server for write fail");
        let g_server = Arc::new(Server::QUICServer(server));
        w_lock.replace(g_server);
    }

    pub fn get_server_id(&self) -> ServerId {
        match self {
            Server::TCPServer(s) => s.get_server_id(),
            Server::QUICServer(s) => s.get_server_id(),
        }
    }

    pub fn get_address(&self) -> SocketAddr {
        match self {
            Server::TCPServer(s) => s.get_address(),
            Server::QUICServer(s) => s.get_address(),
        }
    }

    pub async fn alloc_channel<T>(
        &self, ch_id: ChannelId, servers: &[ServerId],
    ) -> Result<(VPollSender<T>, VReceiverStream), VError>
    where
        T: Encode + Send + 'static,
    {
        let (tx, rx) = match self {
            Server::TCPServer(s) => s.alloc_bi_symmetry_channel(ch_id, servers).await?,
            Server::QUICServer(s) => s.alloc_bi_symmetry_channel(ch_id, servers).await?,
        };
        Ok((tx.into(), rx.into()))
    }
}
