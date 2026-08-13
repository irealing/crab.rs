use crate::app::ServiceProvider;
use crate::app::protocol::types::CommandHandler;
use crate::app::utils::http::{HttpRequest, HttpResponse};
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, MessageReader, MessageWriter, Stream};
use futures_util::TryStreamExt;
use http_body::Frame;
use http_body_util::{BodyExt,StreamBody};
use tokio::io::{AsyncRead, AsyncWriteExt, DuplexStream, duplex};
use tokio::sync::oneshot;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::sync::CancellationToken;

#[async_trait::async_trait]
impl CommandHandler for HttpRequest {
    async fn handle(
        self: Box<Self>,
        cancel: CancellationToken,
        provider: ServiceProvider,
        header: MessageHeader,
        stream: Stream,
    ) -> Result<(), CrabError> {
        let this = *self;
        let (mut writer, reader) = stream.split();
        let req_body = StreamBody::new(ReaderStream::new(reader).map_ok(Frame::data));
        let req = match provider.http_client().make_request(this, req_body.boxed()) {
            Ok(req) => {
                writer
                    .write_message(header.method, header.option, &AckMessage::success())
                    .await?;
                req
            }
            Err(e) => {
                log::warn!("make http request error {}", e);
                writer.write_error(header.method, header.option, &e).await?;
                return Err(e);
            }
        };
        let resp = tokio::select! {
            _=cancel.cancelled()=>{
                return Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR));
            }
            ret=provider.http_client().request(req)=>{
                ret
            }

        };
        let body = match resp {
            Ok((resp, body)) => {
                writer
                    .write_message(header.method, header.option, &resp)
                    .await?;
                body
            }
            Err(e) => {
                log::warn!("request error {}", e);
                writer.write_error(header.method, header.option, &e).await?;
                return Err(e);
            }
        };
        let mut resp_reader =
            StreamReader::new(body.into_data_stream().map_err(std::io::Error::other));
        tokio::select! {
            _=cancel.cancelled()=>{
                Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR))
            }
            res=tokio::io::copy(&mut resp_reader, &mut writer) => {
                let _=writer.shutdown().await;
                res?;
                Ok(())
            }
        }
    }
}

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
pub mod worker {
    use crate::app::protocol::forwarder::http::do_http_request_proxy;
    use crate::app::protocol::types::Command;
    use crate::app::utils::http::{HttpRequest, HttpResponse};
    use crab::proto::Stream;
    use crab::{CrabError, Handle};
    use tokio::io::{AsyncRead, DuplexStream};
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    #[async_trait::async_trait]
    pub trait HttpForwarder {
        /// 发起HTTP代理请求
        async fn http_proxy<B>(
            &self,
            _: (HttpRequest, B),
        ) -> Result<(HttpResponse, DuplexStream), CrabError>
        where
            B: AsyncRead + Unpin + Send + 'static;
    }
    #[async_trait::async_trait]
    impl HttpForwarder for Handle {
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
    }
}
