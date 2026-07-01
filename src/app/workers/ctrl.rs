use super::super::Manager;
use super::super::protocol::{CommandExecutor, WriteFile};
use super::super::types::Handshake;
use super::super::workers::ApiWorker;
use super::super::workers::types::Ret;
use super::types::StreamResponse;
use crate::app::ServiceProvider;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::routing::{delete, get, post};
use crab::CrabError;
use crab::proto::{ExecutorWrapper, Stream};
use crab::utils::runit::Worker;
use futures_util::TryStreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, copy, duplex};
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::sync::CancellationToken;

pub struct CtrlWorker {
    provider: ServiceProvider,
}
impl CtrlWorker {
    pub(crate) fn new(m: ServiceProvider) -> Self {
        Self { provider: m }
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
            .route("/{node_id}/file", post(node_write_file))
            .with_state(self.provider.manager())
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
async fn node_write_file(
    State(m): State<Manager>,
    Path(node_id): Path<String>,
    Query(param): Query<WriteFile>,
    body: Body,
) -> Ret<()> {
    let Some((h, _)) = m.get(&node_id) else {
        return Ret::error(CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT));
    };
    let body_stream = body.into_data_stream();
    let io_stream = body_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let reader = StreamReader::new(io_stream);

    let sender = match h.write_file(param).await {
        Ok((sender, _)) => sender,
        Err(err) => {
            return Ret::error(err);
        }
    };
    let executor =
        async move |cancel: CancellationToken, mut stream: Stream| -> Result<(), CrabError> {
            let mut reader = reader;
            tokio::select! {
                _=cancel.cancelled()=>{
                  return  Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR));
                }
                ret=copy(&mut reader,&mut stream.writer)=>{
                    let _=stream.writer.shutdown().await;
                    return match ret{
                        Ok(size)=>{
                            log::debug!("write file successfully {} bytes", size);
                            stream.read_ack().await
                        },
                        Err(e)=>{
                            log::warn!("write file failed {:?}", e);
                            Err(e.into())
                        }
                    }
                }
            }
        };
    let (executor, err_rx) = ExecutorWrapper::wrap(executor);
    if let Err(_) = sender.send(Ok(executor)) {
        return Ret::error(CrabError::ErrorCode(CrabError::CANCELED_ERROR));
    }
    match err_rx.await {
        Ok(ret) => Ret::from(ret),
        Err(_) => Ret::error(CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
    }
}
