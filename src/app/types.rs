use crab::proto::HandshakePacket;
use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Debug)]
pub struct Handshake {
    pub device_id: String,
    pub version: String,
}
impl HandshakePacket for Handshake {
    fn node_id(&self) -> &str {
        &self.device_id
    }
}
impl Handshake {
    pub fn new(device_id: &str) -> Self {
        Self {
            device_id: device_id.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}
#[derive(Deserialize, Serialize, Debug)]
pub enum Command {
    Ping,
}
