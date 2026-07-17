use super::tcp::tcp_forward;
use super::types::{Command, Forwarder};
use crate::app::protocol::TcpForwardParams;
use crate::app::utils::http::{HttpRequest, HttpResponse};
use crab::proto::{MessageReader, Stream};
use crab::{CrabError, Handle};
use tokio::io::{AsyncRead, DuplexStream, duplex};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

async fn do_http_request_proxy<B>(
    cancel: CancellationToken,
    mut body: B,
    stream: Stream,
    resp_writer: oneshot::Sender<Result<(HttpResponse, DuplexStream), CrabError>>,
) -> Result<(), CrabError>
where
    B: AsyncRead + Unpin + Send + 'static,
{
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

#[async_trait::async_trait]
impl Forwarder for Handle {
    async fn http_proxy<B>(
        &self,
        (req, body): (HttpRequest, B),
    ) -> Result<(HttpResponse, DuplexStream), CrabError>
    where
        B: AsyncRead + Unpin + Send + 'static,
    {
        let (ret_tx, ret_rx) = oneshot::channel();
        self.spawn(
            Command::HttpProxy(req),
            async move |cancel: CancellationToken, stream: Stream| -> Result<(), CrabError> {
                do_http_request_proxy(cancel, body, stream, ret_tx).await
            },
        )
        .await?;
        match ret_rx.await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
        }
    }
    async fn tcp_forward(
        &self,
        _: CancellationToken,
        param: TcpForwardParams,
        conn: TcpStream,
    ) -> Result<(), CrabError> {
        self.exec(
            Command::TCPForward(param),
            async move |cancel: CancellationToken, stream: Stream| -> Result<(), CrabError> {
                tcp_forward(cancel, stream, conn).await
            },
        )
        .await
    }
}
