use tokio_util::sync::CancellationToken;

use crate::crab::{CrabError, Node};

use super::utils::runit::Worker;

pub(super) struct RemoteNode {
    node_id: String,
    conn: quinn::Connecting,
}
#[async_trait::async_trait]
impl Worker for RemoteNode {
    async fn serve(&self, _: CancellationToken) -> Result<(), CrabError> {
        todo!("RemoteNode serve")
    }
}
impl Node for RemoteNode {
    fn id(&self) -> &str {
        return &self.node_id;
    }
}
