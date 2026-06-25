use super::super::Manager;
use super::super::protocol::CommandExecutor;
use super::super::types::Handshake;
use super::super::workers::ApiWorker;
use super::super::workers::types::Ret;
use super::types::StreamResponse;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::routing::{delete, get};
use crab::CrabError;
use crab::proto::Stream;
use crab::utils::runit::Worker;
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, copy, duplex};
use tokio_util::io::ReaderStream;
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
            .route("/{node_id}/file", get(read_node_file))
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
struct RemoveNodePath {
    path: String,
    #[serde(default)]
    dir: bool,
}
async fn node_remove_dir(
    State(m): State<Manager>,
    Path(node_id): Path<String>,
    Query(params): Query<RemoveNodePath>,
) -> Ret<()> {
    let Some((h, _)) = m.get(&node_id) else {
        return Ret::error(CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT));
    };
    h.delete(params.path, params.dir).await.into()
}
#[derive(Deserialize)]
struct ReadNodeFile {
    path: String,
}
async fn read_node_file(
    State(m): State<Manager>,
    Path(node_id): Path<String>,
    Query(params): Query<ReadNodeFile>,
) -> StreamResponse {
    let Some((h, _)) = m.get(&node_id) else {
        return StreamResponse::Error(Ret::error(CrabError::ErrorCode(
            CrabError::NODE_ALREADY_EXIT,
        )));
    };
    let (sender, metadata) = match h.read_file(params.path).await {
        Ok(data) => data,
        Err(e) => {
            return StreamResponse::Error(Ret::error(e));
        }
    };
    const BUF_SIZE: usize = 1024 * 16;
    let (writer, reader) = duplex(BUF_SIZE);
    if let Err(_) = sender.send(Ok(async move |_: CancellationToken, mut stream: Stream| {
        let mut writer = writer;
        if let Err(err) = stream.read_ack().await {
            log::warn!("Error while reading ack from stream: {}", err);
            let _ = writer
                .write(&format!("read ack error {}", &err).as_bytes())
                .await;
            return Err(err);
        }
        copy(&mut stream.reader, &mut writer)
            .await
            .inspect(|copied| log::debug!("read file copied {} bytes", copied))?;
        writer.shutdown().await?;
        return Ok(());
    })) {
        return StreamResponse::Error(Ret::error(CrabError::ErrorCode(CrabError::CANCELED_ERROR)));
    }
    let stream = ReaderStream::new(reader);
    StreamResponse::File((metadata, Body::from_stream(stream)))
}
