use std::net::SocketAddr;
use std::sync::Arc;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::utils::runit::Worker;
use crate::crab::proto::HandshakeRet;
use crate::crab::{CrabError, Node, node::NodeStatus};

struct RemoteNodeInner {
    node_id: String,
    conn: quinn::Connection,
    local_addr: SocketAddr,
    status_tx: watch::Sender<NodeStatus>,
    status_rx: watch::Receiver<NodeStatus>,
    client: bool,
}
pub(super) struct RemoteNode {
    inner: Arc<RemoteNodeInner>,
}
impl RemoteNode {
    pub(super) fn new(ret: &HandshakeRet, conn: quinn::Connection) -> Self {
        let (status_tx, status_rx) = watch::channel(NodeStatus::Ready);
        Self {
            inner: Arc::new(RemoteNodeInner {
                node_id: ret.node_id.clone(),
                local_addr: conn.remote_address(),
                conn: conn,
                status_tx: status_tx,
                status_rx: status_rx,
                client: false,
            }),
        }
    }
}
#[async_trait::async_trait]
impl Worker for RemoteNode {
    async fn serve(&self, _: CancellationToken) -> Result<(), CrabError> {
        todo!("RemoteNode serve")
    }
}
impl Node for RemoteNode {
    fn id(&self) -> &str {
        return &self.inner.node_id;
    }
    fn status(&self) -> NodeStatus {
        *self.inner.status_rx.borrow()
    }
    fn addr(&self) -> SocketAddr {
        self.inner.local_addr
    }
}
