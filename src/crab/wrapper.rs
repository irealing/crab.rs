use super::CrabError;
use super::Handle;
use super::proto::{HandshakePacket, MessageHeader, Method, Protocol, Stream};
use crate::crab::proto::{AckMessage, Hook};
use crate::crab::types::NodeMetadata;
use quinn::Connection;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
pub(super) struct ProtoWrapper<P: Protocol> {
    protocol: P,
}
impl<P> ProtoWrapper<P>
where
    P: Protocol,
    P::Handshake: HandshakePacket + 'static,
    P::Heartbeat: DeserializeOwned + Serialize + Sync + Send + 'static,
    P::Command: DeserializeOwned + Serialize + Sync + Send + 'static,
{
    pub fn new(protocol: P) -> Self {
        Self { protocol }
    }
    async fn handle_heartbeat(
        &self,
        meta: &NodeMetadata,
        stream: &mut Stream,
    ) -> Result<P::Heartbeat, CrabError> {
        let (_, body) = stream.read_message().await?;
        self.protocol.on_heartbeat(meta, &body).await
    }
}
#[async_trait::async_trait]
impl<P> Hook for ProtoWrapper<P>
where
    P: Protocol,
    P::Handshake: HandshakePacket + 'static,
    P::Heartbeat: DeserializeOwned + Serialize + Sync + Send + 'static,
    P::Command: DeserializeOwned + Serialize + Sync + Send + 'static,
{
    async fn handshake(&self, conn: &Connection) -> Result<NodeMetadata, CrabError> {
        log::trace!("handshake with connection from {}", conn.remote_address());
        let mut session = Stream::accept(conn).await?;
        let (header, handshake) = session.read_message::<P::Handshake>().await.map_err(|e| {
            log::warn!("handshake failed,read header {:?}", e);
            e
        })?;
        if header.method != Method::Handshake {
            log::warn!(
                "invalid message method,accept {:?} receive {:?} ",
                Method::Handshake,
                header.method
            );
            return Err(CrabError::ErrorCode(CrabError::BAD_MESSAGE_HEADER));
        }
        let meta = NodeMetadata {
            node_id: handshake.node_id().to_string(),
            remote_addr: conn.remote_address(),
            as_client: true,
        };
        match self.protocol.on_handshake(&meta, &handshake).await {
            Err(err) => {
                session
                    .write_error(Method::Handshake, MessageHeader::OPTION_ERROR, &err)
                    .await?;
                Err(err)
            }
            Ok(ret) => {
                if let Err(err) = session
                    .write_message(Method::Handshake, header.option, &ret)
                    .await
                {
                    log::error!("write handshake message failed {:}", err);
                    Err(CrabError::ErrorCode(CrabError::IO_BAD_MESSAGE))
                } else {
                    Ok(meta)
                }
            }
        }
    }
    async fn handshake_as_client(&self, conn: &Connection) -> Result<NodeMetadata, CrabError> {
        log::trace!(
            "handshake_as_client with connection {}",
            conn.remote_address()
        );
        let handshake = self.protocol.make_handshake()?;
        let mut session = Stream::open(conn).await?;
        session
            .write_message(Method::Handshake, 0, &handshake)
            .await?;
        let (_, body) = session.read_message::<P::Handshake>().await?;
        log::trace!(
            "remote {} node id {}",
            conn.remote_address(),
            body.node_id()
        );
        let meta = NodeMetadata {
            node_id: body.node_id().to_string(),
            remote_addr: conn.remote_address(),
            as_client: false,
        };
        Ok(meta)
    }
    async fn heartbeat(&self, meta: &NodeMetadata, stream: &mut Stream) -> Result<(), CrabError> {
        match self
            .handle_heartbeat(meta, stream)
            .await
            .and_then(|_| self.protocol.make_heartbeat())
        {
            Err(err) => {
                stream
                    .write_error(Method::Heartbeat, MessageHeader::OPTION_ERROR, &err)
                    .await
            }
            Ok(ret) => {
                stream
                    .write_message(Method::Heartbeat, MessageHeader::OPTION_NONE, &ret)
                    .await?;
                let (_, ack) = stream.read_message::<AckMessage>().await?;
                if ack.code != CrabError::NO_ERROR {
                    Err(CrabError::ErrorCode(ack.code))
                } else {
                    Ok(())
                }
            }
        }
    }
    async fn heartbeat_as_client(
        &self,
        meta: &NodeMetadata,
        stream: &mut Stream,
    ) -> Result<(), CrabError> {
        match self.protocol.make_heartbeat() {
            Err(err) => {
                log::warn!("make heartbeat failed {},write ack with error", err);
                stream
                    .write_error(Method::Heartbeat, MessageHeader::OPTION_ERROR, &err)
                    .await?
            }
            Ok(ret) => {
                stream
                    .write_message(Method::Heartbeat, MessageHeader::OPTION_NONE, &ret)
                    .await?
            }
        }
        let handshake_ret = self.handle_heartbeat(meta, stream).await;
        if let Err(err) = handshake_ret {
            stream
                .write_error(Method::Heartbeat, MessageHeader::OPTION_ERROR, &err)
                .await?;
        } else {
            stream
                .write_message(
                    Method::Heartbeat,
                    MessageHeader::OPTION_NONE,
                    &AckMessage {
                        code: CrabError::NO_ERROR,
                        msg: None,
                    },
                )
                .await?;
        }
        Ok(())
    }
    async fn on_node_accepted(&self, meta: &NodeMetadata, h: Handle) -> Result<(), CrabError> {
        self.protocol.on_node_accepted(meta, h).await
    }
    async fn on_node_exited(&self, meta: &NodeMetadata) {
        self.protocol.on_node_exited(meta).await
    }
    async fn handle_stream(
        &self,
        meta: &NodeMetadata,
        cancel: CancellationToken,
        stream: &mut Stream,
    ) -> Result<(), CrabError> {
        let (header, cmd) = stream.read_message::<P::Command>().await?;
        self.protocol
            .handle_command(cancel, meta, (&header, &cmd), stream)
            .await
    }
}
