use super::Node;
use super::proto::AsyncTask;
use super::types::{NodeMetadata, NodeStatus};
use std::net::SocketAddr;
use tokio::sync::{mpsc, watch};
use std::sync::Arc;

struct HandleInner {
    meta: Arc<NodeMetadata>,
    status_rx: watch::Receiver<NodeStatus>,
    cmd_tx: mpsc::Sender<Box<dyn AsyncTask>>,
}
pub struct Handle {
    inner: Arc<HandleInner>,
}
impl Handle {
    pub(super) fn new(
        meta: Arc<NodeMetadata>,
        status_rx: watch::Receiver<NodeStatus>,
        cmd_tx: mpsc::Sender<Box<dyn AsyncTask>>,
    ) -> Self {
        Self {
            inner: Arc::new(HandleInner {
                meta,
                status_rx,
                cmd_tx,
            }),
        }
    }
}
impl Clone for Handle {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl Node for Handle {
    fn id(&self) -> &str {
        &self.inner.meta.node_id
    }

    fn status(&self) -> NodeStatus {
        *self.inner.status_rx.borrow()
    }

    fn addr(&self) -> SocketAddr {
        self.inner.meta.remote_addr
    }

    fn as_client(&self) -> bool {
        self.inner.meta.as_client
    }
}
