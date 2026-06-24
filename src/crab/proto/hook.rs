use std::any::Any;
use quinn::Connection;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use super::{CrabError, Handle, NodeMetadata};
use super::{MessageHeader, Stream};

pub trait HandshakePacket: DeserializeOwned + Serialize + Send + Sync {
    fn node_id(&self) -> &str;
}
#[async_trait::async_trait]
pub trait Protocol: Send + Sync {
    type Handshake: HandshakePacket + 'static;
    type Heartbeat: DeserializeOwned + Serialize + Send + Sync + 'static;
    type Command: DeserializeOwned + Serialize + Send + Sync + 'static;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError>;
    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError>;
    async fn on_handshake(
        &self,
        _: &NodeMetadata,
        _: &Self::Handshake,
    ) -> Result<Self::Handshake, CrabError> {
        self.make_handshake()
    }
    async fn on_heartbeat(
        &self,
        _: &NodeMetadata,
        _: &Self::Heartbeat,
    ) -> Result<Self::Heartbeat, CrabError> {
        self.make_heartbeat()
    }
    async fn on_node_accepted(
        &self,
        _: &NodeMetadata,
        _: Handle,
        _: Self::Handshake,
    ) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_exited(&self, _: &NodeMetadata) {}
    async fn handle_command(
        &self,
        _: CancellationToken,
        _: &NodeMetadata,
        _: (MessageHeader, Self::Command),
        _: Stream,
    ) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNKNOWN_ERROR))
    }
}

#[async_trait::async_trait]
pub trait Hook: Send + Sync {
    async fn handshake(
        &self,
        _: &Connection,
    ) -> Result<(NodeMetadata, Box<dyn Any + Send>), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn handshake_as_client(
        &self,
        _: &Connection,
    ) -> Result<(NodeMetadata, Box<dyn Any + Send>), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn heartbeat(&self, _: &NodeMetadata, _: &mut Stream) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn heartbeat_as_client(&self, _: &NodeMetadata, _: &mut Stream) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
    async fn on_connection_accepted(&self, _: &Connection) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_accepted(
        &self,
        _: &NodeMetadata,
        _: Handle,
        _: Box<dyn Any + Send>,
    ) -> Result<(), CrabError> {
        Ok(())
    }
    async fn on_node_exited(&self, meta: &NodeMetadata) {
        log::trace!("on_node_exited {}", meta.node_id);
    }
    async fn handle_stream(
        &self,
        _: &NodeMetadata,
        _: CancellationToken,
        _: Stream,
    ) -> Result<(), CrabError> {
        Err(CrabError::ErrorCode(CrabError::UNSUPPORTED_ERROR))
    }
}