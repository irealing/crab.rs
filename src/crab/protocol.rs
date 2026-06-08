use crate::crab::CrabError;
use crate::crab::proto::{HandshakePacket, Protocol};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct HandshakePayload {
    pub node_id: String,
}
impl HandshakePacket for HandshakePayload {
    fn node_id(&self) -> &str {
        &self.node_id
    }
}

pub struct SimpleProtocol;
impl SimpleProtocol {
    pub fn new() -> Self {
        Self {}
    }
}
impl Protocol for SimpleProtocol {
    type Handshake = HandshakePayload;
    type Heartbeat = HandshakePayload;

    fn make_handshake(&self) -> Result<Self::Handshake, CrabError> {
        todo!()
    }

    fn make_heartbeat(&self) -> Result<Self::Heartbeat, CrabError> {
        todo!()
    }
}
