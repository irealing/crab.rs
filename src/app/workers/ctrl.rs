use super::super::Manager;
use super::super::protocol::{CommandExecutor, WriteFile};
use super::super::types::Handshake;
use super::super::workers::ApiWorker;
use super::super::workers::types::Ret;
use super::types::{ProxyResponse, StreamResponse};
use crate::app::ServiceProvider;
use crate::app::utils::http::HttpRequest;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::Request;
use axum::routing::{any, delete, get, post};
use crab::CrabError;
use crab::proto::{ExecutorWrapper, Stream};
use crab::utils::runit::Worker;
use futures_util::TryStreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, copy, duplex};
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::sync::CancellationToken;
use url::Url;

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
            .route("/proxy/{*target_path}", any(http_proxy))
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
const HEADER_X_TARGET_URL: &str = "X-Target-URL";
const HEADER_X_NODE_ID: &str = "X-Node-ID";
const HEADER_HOST: &str = "Host";
async fn http_proxy(
    State(m): State<Manager>,
    Path(target_path): Path<String>,
    req: Request<Body>,
) -> ProxyResponse {
    let (Some(node_id), Some(target_url)) = (
        req.headers()
            .get(HEADER_X_NODE_ID)
            .and_then(|v| v.to_str().ok()),
        req.headers()
            .get(HEADER_X_TARGET_URL)
            .and_then(|v| v.to_str().ok()),
    ) else {
        return ProxyResponse::Err((400, CrabError::ErrorCode(CrabError::BAD_PARAMETER)));
    };

    let url = match Url::parse(target_url) {
        Ok(url) => url,
        Err(e) => {
            return ProxyResponse::Err((400, CrabError::ErrorCodeWithMessage(400, e.to_string())));
        }
    };
    let mut url = match url.join(&target_path) {
        Ok(url) => url,
        Err(err) => {
            return ProxyResponse::Err((
                400,
                CrabError::ErrorCodeWithMessage(400, err.to_string()),
            ));
        }
    };
    if let Some(query) = req.uri().query() {
        url.set_query(Some(&query));
    }
    let Ok(method) = req.method().as_str().try_into() else {
        return ProxyResponse::Err((400, CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR)));
    };
    let exclude_headers = vec![HEADER_X_TARGET_URL, HEADER_X_NODE_ID, HEADER_HOST];
    let http_req = HttpRequest {
        method,
        request_uri: url.to_string(),
        headers: req
            .headers()
            .iter()
            .filter(|(h, _)| !exclude_headers.contains(&h.as_str()))
            .map(|(k, v)| {
                (
                    String::from(k.as_str()),
                    String::from(v.to_str().unwrap_or("")),
                )
            })
            .collect(),
    };
    let Some((handle, _)) = m.get(node_id) else {
        log::debug!("node {} not found", node_id);
        return ProxyResponse::Err((502, CrabError::ErrorCode(CrabError::NODE_ALREADY_EXIT)));
    };
    let body_stream = req.into_body().into_data_stream();
    let io_stream = body_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let body = StreamReader::new(io_stream);
    match handle.http_proxy((http_req, body)).await {
        Ok((resp, body)) => ProxyResponse::Ok((resp, Body::from_stream(ReaderStream::new(body)))),
        Err(err) => ProxyResponse::Err((502, err)),
    }
}
