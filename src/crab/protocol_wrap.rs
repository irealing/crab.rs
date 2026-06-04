use std::sync::Arc;

use crate::crab::{CrabError, node::HandshakeRet, proto::Protocol};

pub(super) struct ProtocolWrapper<S, H> {
    protocol: Box<dyn Protocol<Handshake = S, Heartbeat = H>>,
}
impl<S, H> ProtocolWrapper<S, H> {
    pub fn new(protocol: Box<dyn Protocol<Handshake = S, Heartbeat = H>>) -> Arc<Self> {
        Arc::new(ProtocolWrapper { protocol: protocol })
    }
    pub async fn handshake(
        self: Arc<Self>,
        conn: quinn::Connection,
    ) -> Result<HandshakeRet, CrabError> {
        let (mut writer, mut reader) = conn.accept_bi().await.map_err(|err| {
            log::warn!("accept handshake stream fail, err: {}", err);
            CrabError::ErrorCode(CrabError::HANDSHAKE_ERROR)
        })?;
        todo!("")
    }
    pub async fn connect(self: Arc<Self>, _: quinn::Connection) -> Result<HandshakeRet, CrabError> {
        todo!("")
    }
}
