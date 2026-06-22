use super::super::Manager;
use crate::app::types::Handshake;
use crate::app::workers::ApiWorker;
use crate::app::workers::types::Ret;
use axum::Router;
use axum::extract::{Path, State};
use axum::routing::get;
use crab::CrabError;
use crab::utils::runit::Worker;
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
    Ret::success(Some(info))
}
