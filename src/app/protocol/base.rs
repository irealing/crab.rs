use super::super::utils::http::HttpRequest;
use super::commands::{DeleteCommand, FileMetadata, ReadFile};
use super::types::{Command, CommandExecutor};
use crate::app::protocol::WriteFile;
use crate::app::utils::http::HttpResponse;
use crab::proto::{AckMessage, Executor, MessageHeader, MessageReader, Method, Stream, TaskHandle};
use crab::{CrabError, Handle, Node};
use tokio::io::{AsyncRead, DuplexStream, duplex};
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
        E: Executor<Output = ()>,
    {
        self.exec_with_ack::<Command, FileMetadata, E>(Command::ReadFile(ReadFile {
            path: filename,
        }))
        .await
    }
    async fn write_file<E>(&self, cmd: WriteFile) -> TaskHandle<E, ()>
    where
        E: Executor<Output = ()>,
    {
        let (sender, _) = self
            .exec_with_ack::<Command, AckMessage, E>(Command::WriteFile(cmd))
            .await?;
        Ok((sender, ()))
    }
    async fn http_proxy<B>(
        &self,
        req: (HttpRequest, B),
    ) -> Result<(HttpResponse, DuplexStream), CrabError>
    where
        B: AsyncRead + Unpin + Send + 'static,
    {
        let (ret_tx, ret_rx) = oneshot::channel();
        self.exec_base(
            async move |cancel: CancellationToken, stream: Stream| -> Result<(), CrabError> {
                do_http_request_proxy(cancel, req, stream, ret_tx).await
            },
        )
        .await?;
        match ret_rx.await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
        }
    }
}
async fn do_http_request_proxy<'a, B>(
    cancel: CancellationToken,
    (req, mut body): (HttpRequest, B),
    mut stream: Stream,
    resp_writer: oneshot::Sender<Result<(HttpResponse, DuplexStream), CrabError>>,
) -> Result<(), CrabError>
where
    B: AsyncRead + Unpin + Send + 'static,
{
    stream
        .write_message(
            Method::Command,
            MessageHeader::OPTION_NONE,
            &Command::HttpProxy(req),
        )
        .await?;
    stream
        .read_ack()
        .await
        .inspect_err(|e| log::debug!("read ack failed {}", e))?;
    let (mut writer, mut reader) = stream.split();
    let req_cancel = cancel.clone();
    let req_fut = tokio::spawn(async move {
        tokio::select! {
            _= req_cancel.cancelled() =>Err( CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
            res=tokio::io::copy(&mut body, &mut writer) => {
                if let Err(e) = res {
                    log::error!("http request proxy write body error: {}", e);
                    Err(e.into())
                }else{
                    Ok(())
                }
            }
        }
    });
    let mut body_writer = match reader.read_message::<HttpResponse>().await {
        Ok((_, resp)) => {
            let (body_writer, body_reader) = duplex(1024 * 16);
            resp_writer
                .send(Ok((resp, body_reader)))
                .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
            body_writer
        }
        Err(err) => {
            log::error!("http request proxy reader error: {}", err);
            let _ = resp_writer.send(Err(err));
            return Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR));
        }
    };
    tokio::select! {
        _=cancel.cancelled()=>{return Err( CrabError::ErrorCode(CrabError::CANCELED_ERROR))},
        res=tokio::io::copy(&mut reader, &mut body_writer) => {
            match res{
                Ok(copy_size)=>{
                    log::debug!("http request proxy read {} bytes", copy_size);
                }
                Err(err)=>{
                    log::error!("http request proxy response transport error: {}", err);
                    return Err(err.into());
                }
            }
        }
    }
    let _ = req_fut.await;
    Ok(())
}
