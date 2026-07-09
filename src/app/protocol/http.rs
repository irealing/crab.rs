use super::super::utils::http::HttpRequest;
use super::types::CommandHandler;
use crate::app::ServiceProvider;
use crab::CrabError;
use crab::proto::{AckMessage, MessageHeader, MessageWriter, Stream};
use futures_util::TryStreamExt;
use http_body::Frame;
use http_body_util::BodyExt;
use http_body_util::StreamBody;
use tokio::io::AsyncWriteExt;
use tokio_util::io::{ReaderStream, StreamReader};
use tokio_util::sync::CancellationToken;
pub struct HttpProxyHandler {
    pub req: HttpRequest,
}
#[async_trait::async_trait]
impl CommandHandler for HttpProxyHandler {
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
        let req = match provider
            .http_client()
            .make_request(this.req, req_body.boxed())
        {
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
