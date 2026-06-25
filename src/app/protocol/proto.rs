use super::types::{Command, CommandHandler};
use crate::app::types::Handshake;
use crate::app::Manager;
use async_trait::async_trait;
use crab::proto::{MessageHeader, Protocol, Stream};
use crab::{CrabError, Handle, NodeMetadata};
use tokio_util::sync::CancellationToken;

pub struct AppProtocol {
    device_id: String,
    manager: Manager,
}
impl AppProtocol {
    pub fn new(device_id: &str, manager: Manager) -> Self {
        Self {
            device_id: device_id.to_string(),
            manager,
        }
    }
}
#[async_trait]
impl Protocol for AppProtocol {
    type Handshake = Handshake;
    type Heartbeat = Command;

    type Command = Command;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError> {
        Ok(Handshake::new(&self.device_id))
    }

    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError> {
        Ok(Command::Ping)
    }
    async fn on_handshake(
        &self,
        meta: &NodeMetadata,
        _: &Self::Handshake,
    ) -> Result<Self::Handshake, CrabError> {
        if self.manager.exists(&meta.node_id) {
            return Err(CrabError::ErrorCode(CrabError::NODE_EXISTS));
        }
        self.make_handshake()
    }
    async fn on_heartbeat(
        &self,
        _: &NodeMetadata,
        _: &Self::Heartbeat,
    ) -> Result<Self::Heartbeat, CrabError> {
        Ok(Command::Pong)
    }
    async fn on_node_accepted(
        &self,
        meta: &NodeMetadata,
        h: Handle,
        info: Self::Handshake,
    ) -> Result<(), CrabError> {
        self.manager.insert(&meta.node_id, h, info);
        Ok(())
    }
    async fn on_node_exited(&self, meta: &NodeMetadata) {
        self.manager.remove(&meta.node_id)
    }
    async fn handle_command(
        &self,
        cancel: CancellationToken,
        _: &NodeMetadata,
        (header, cmd): (MessageHeader, Self::Command),
        stream: Stream,
    ) -> Result<(), CrabError> {
        log::debug!("Received command: {}", cmd);
        Box::new(cmd).handle(cancel, header, stream).await
    }
}
