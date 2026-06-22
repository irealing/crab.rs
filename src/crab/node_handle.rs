use super::proto::{AsyncJob, AsyncTask};
use super::types::{NodeMetadata, NodeStatus};
use super::CrabError;
use super::Node;
use crate::proto::Executor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, watch};

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
    pub async fn exec<CE, T>(&self, callback: CE) -> Result<T, CrabError>
    where
        CE: Executor<T>,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let job = AsyncJob { callback, tx };
        self.inner
            .cmd_tx
            .send(Box::new(job))
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))?;
        rx.await
            .map_err(|_| CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT))?
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
