use super::super::Manager;
use super::super::protocol::CommandExecutor;
use super::super::types::Handshake;
use super::super::workers::ApiWorker;
use super::super::workers::types::Ret;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::routing::{delete, get};
use crab::CrabError;
use crab::utils::runit::Worker;
use serde::Deserialize;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
pub struct CtrlWorker {
    manager: Manager,
}
impl CtrlWorker {
    pub(crate) fn new(m: Manager) -> Self {
        Self { manager: m }
    }
}
#[async_trait::async_trait]
impl Worker for CtrlWorker {
    async fn serve(&self, _: CancellationToken) -> Result<(), CrabError> {
        Ok(())
    }
}

impl ApiWorker for CtrlWorker {
    fn routers(&self) -> Router {
        Router::new()
            .route("/{node_id}", get(node_info))
            .route("/{node_id}/ping", get(node_ping))
            .route("/{node_id}/dir", delete(node_remove_dir))
            .with_state(self.manager.clone())
    }
    fn tag(&self) -> &str {
        "ctrl"
    }
}

async fn node_info(
    State(manager): State<Manager>,
    Path(node_id): Path<String>,
) -> Ret<Arc<Handshake>> {
    let Some((_, info)) = manager.get(&node_id) else {
        return Ret::error(CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT));
    };
    info.into()
}
async fn node_ping(State(m): State<Manager>, Path(node_id): Path<String>) -> Ret<()> {
    let Some((h, _)) = m.get(&node_id) else {
        return Ret::error(CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT));
    };
    h.ping().await.into()
}
#[derive(Deserialize)]
struct NodePathParam {
    path: String,
}
async fn node_remove_dir(
    State(m): State<Manager>,
    Path(node_id): Path<String>,
    Query(params): Query<NodePathParam>,
) -> Ret<()> {
    let Some((h, _)) = m.get(&node_id) else {
        return Ret::error(CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT));
    };
    h.delete(params.path, true).await.into()
}
