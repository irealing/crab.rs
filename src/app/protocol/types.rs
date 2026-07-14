use super::super::ServiceProvider;
use super::super::utils::http::{HttpRequest,HttpResponse};
use super::commands::{DeleteCommand, FileMetadata, ReadFile, WriteFile};
use super::tcp::{TCPForwardRequest, TCPForwarder};
use crab::CrabError;
use crab::proto::{Executor, MessageHeader, Stream, TaskHandle};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tokio::io::{AsyncRead, DuplexStream};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Serialize)]
pub enum Command {
    Ping,
    Pong,
    Delete(DeleteCommand),
    ReadFile(ReadFile),
    WriteFile(WriteFile),
    HttpProxy(HttpRequest),
    TCPForward(TCPForwardRequest),
}
impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Command::Ping => {
                write!(f, "ping")
            }
            Command::Pong => {
                write!(f, "pong")
            }
            Command::Delete(ref delete) => {
                write!(f, "delete({})", delete.path)
            }
            Command::ReadFile(ref read) => {
                write!(f, "read_file({})", read.path)
            }
            Command::WriteFile(ref write) => {
                write!(
                    f,
                    "write_file({},mkdir={},overwrite={})",
                    write.path, write.mkdir, write.overwrite
                )
            }
            Command::HttpProxy(ref http_request) => {
                write!(f, "http_proxy({})", http_request.request_uri)
            }
            Command::TCPForward(ref tcp_forward) => {
                write!(f, "tcp_forward({})", tcp_forward.target_address)
            }
        }
    }
}
#[async_trait::async_trait]
pub trait CommandExecutor {
    /// 发送`Command::Ping`并等待对端返回`Command::Pong`
    async fn ping(&self) -> Result<(), CrabError>;
    /// 从节点删除文件或文件夹
    async fn delete(&self, _: String, _: bool) -> Result<(), CrabError>;

    /// 从节点读取文件数据
    /// 任务分两阶段：
    /// 1. 返回用于发送处理后续操作的`Sender<Executor<Output=()>>` 和 `FileMetadata`;
    /// 2. 不需要处理则`drop`返回的`Sender`
    /// 3. 否则send `Executor`在Worker协程里处理流
    /// * `Executor` 返回的数据将会被忽略
    async fn read_file<E>(&self, _: String) -> TaskHandle<E, FileMetadata>
    where
        E: Executor<Output = ()>;
    /// 在节点指定位置写入文件内容
    /// 与`read_file`类似，写入分为两阶段
    async fn write_file<E>(&self, _: WriteFile) -> TaskHandle<E, ()>
    where
        E: Executor<Output = ()>;
    /// 发起HTTP代理请求
    async fn http_proxy<B>(
        &self,
        _: (HttpRequest, B),
    ) -> Result<(HttpResponse, DuplexStream), CrabError>
    where
        B: AsyncRead + Unpin + Send + 'static;
}
/// 处理远程节点发送的命令
#[async_trait::async_trait]
pub trait CommandHandler: Send {
    async fn handle(
        self: Box<Self>,
        _: CancellationToken,
        _: ServiceProvider,
        _: MessageHeader,
        _: Stream,
    ) -> Result<(), CrabError>;
}
#[async_trait::async_trait]
impl CommandHandler for Command {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        provider: ServiceProvider,
        header: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let cmd = *self;
        let handler: Option<Box<dyn CommandHandler>> = match cmd {
            Command::Ping => {
                return stream
                    .write_message(header.method, header.option, &Command::Pong)
                    .await;
            }
            Command::Pong => None,
            Command::Delete(delete) => Some(Box::new(delete)),
            Command::ReadFile(read) => Some(Box::new(read)),
            Command::WriteFile(write) => Some(Box::new(write)),
            Command::HttpProxy(req) => Some(Box::new(req)),
            Command::TCPForward(req) => Some(Box::new(TCPForwarder::new(req))),
        };
        if let Some(handler) = handler {
            handler
                .handle(cancel, provider, header, stream)
                .await
                .inspect_err(|e| log::warn!("handle command error: {}", e))
        } else {
            stream
                .write_error(
                    header.method,
                    header.option,
                    &CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR),
                )
                .await
        }
    }
}
/// 简单的命令-响应的命令处理
/// 接收命令-生成响应-返回给对端节点
#[async_trait::async_trait]
pub trait SimpleCommandHandler: DeserializeOwned + Send + 'static {
    type Response: Serialize + Send;
    async fn make_response(
        self,
        _: CancellationToken,
        _: ServiceProvider,
    ) -> Result<Self::Response, CrabError>;
}
#[async_trait::async_trait]
impl<C, T> CommandHandler for C
where
    C: SimpleCommandHandler<Response = T>,
    T: Serialize + Send + Sync + 'static,
{
    async fn handle(
        self: Box<Self>,
        c: CancellationToken,
        provider: ServiceProvider,
        h: MessageHeader,
        mut stream: Stream,
    ) -> Result<(), CrabError> {
        let this = *self;
        match this.make_response(c.clone(), provider.clone()).await {
            Ok(resp) => stream.write_message(h.method, h.option, &resp).await,
            Err(err) => stream.write_error(h.method, h.option, &err).await,
        }
    }
}
