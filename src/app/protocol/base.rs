use super::super::utils::http::HttpRequest;
use super::commands::{DeleteCommand, FileMetadata, ReadFile};
use super::types::{Command, CommandExecutor};
use crate::app::protocol::WriteFile;
use crate::app::utils::http::HttpResponse;
use crab::proto::{AckMessage, Executor, MessageHeader, Method, Stream, TaskHandle};
use crab::{CrabError, Handle, Node};
use tokio::io::{AsyncRead, DuplexStream};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
#[async_trait::async_trait]
impl CommandExecutor for Handle {
    async fn ping(&self) -> Result<(), CrabError> {
        log::debug!("ping node {}", self.id());
        self.exec(async |_: CancellationToken, mut stream: Stream| {
            stream
                .write_message(Method::Command, MessageHeader::OPTION_NONE, &Command::Ping)
                .await?;
            let _ = stream.read_message::<Command>().await?;
            Ok(())
        })
            .await
    }
    async fn delete(&self, p: String, dir: bool) -> Result<(), CrabError> {
        log::debug!("deleting node {} path {}", self.id(), dir);
        self.exec(async move |_: CancellationToken, mut stream: Stream| {
            stream
                .write_message(
                    Method::Command,
                    MessageHeader::OPTION_NONE,
                    &Command::Delete(DeleteCommand { path: p, dir }),
                )
                .await?;
            stream.read_ack().await
        })
            .await
    }
    async fn read_file<E>(&self, filename: String) -> TaskHandle<E, FileMetadata>
    where
        E: Executor<Output=()>,
    {
        self.exec_with_ack::<Command, FileMetadata, E>(Command::ReadFile(ReadFile {
            path: filename,
        }))
            .await
    }
    async fn write_file<E>(&self, cmd: WriteFile) -> TaskHandle<E, ()>
    where
        E: Executor<Output=()>,
    {
        let (sender, _) = self
            .exec_with_ack::<Command, AckMessage, E>(Command::WriteFile(cmd))
            .await?;
        Ok((sender, ()))
    }
    async fn http_proxy<B>(
        &self,
        (req, body): (HttpRequest, B),
    ) -> Result<(HttpResponse, DuplexStream), CrabError>
    where
        B: AsyncRead + Send,
    {
        let (ret_tx, ret_rx) = oneshot::channel();
        self.exec_base(
            async move |cancel: CancellationToken, mut stream: Stream| -> Result<(), CrabError> {
                todo!()
            },
        )
            .await?;
        match ret_rx.await {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => Err(e),
            Err(_) => Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
        }
    }
}
async fn exec_http_request_proxy<B>(
    cancel: CancellationToken,
    (req, mut body): (HttpRequest, B),
    mut stream: Stream,
) -> Result<HttpResponse, CrabError>
where
    B: AsyncRead + Send + Unpin,
{
    stream
        .write_message(Method::Command, MessageHeader::OPTION_NONE, &req)
        .await?;
    stream.read_ack().await?;
    let (mut reader, mut writer) = stream.split();
    let copy_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        let mut reader = reader;
    })
}
