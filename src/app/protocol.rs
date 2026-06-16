use super::types::{Command, Handshake};
use async_trait::async_trait;
use crab::proto::Protocol;
use crab::{CrabError, NodeMetadata};

pub struct AppProtocol {
    device_id: String,
}
impl AppProtocol {
    pub fn new(device_id: &str) -> Self {
        Self {
            device_id: device_id.to_string(),
        }
    }
}
#[async_trait]
impl Protocol for AppProtocol {
    type Handshake = Handshake;
    type Heartbeat = Handshake;

    type Command = Command;
    fn make_handshake(&self) -> Result<Self::Handshake, CrabError> {
        Ok(Handshake::new(&self.device_id))
    }

    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError> {
        Ok(Handshake::new(&self.device_id))
    }
    async fn on_handshake(
        &self,
        _: &NodeMetadata,
        hs: &Self::Handshake,
    ) -> Result<Self::Handshake, CrabError> {
        log::info!("Handling handshake {:?}", hs);
        self.make_handshake()
    }
}
