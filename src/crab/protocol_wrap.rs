use super::proto::HandshakeRet;
use crate::crab::{
    CrabError::{self, ErrorCode},
    proto::Protocol,
};
#[async_trait::async_trait]
trait Hook<'a> {
    async fn handshake(&self, _: &'a quinn::Connection) -> Result<HandshakeRet, CrabError> {
        Err(ErrorCode(CrabError::HANDSHAKE_ERROR))
    }
    async fn handshake_as_client(
        &self,
        _: &'a quinn::Connection,
    ) -> Result<HandshakeRet, CrabError> {
        Err(ErrorCode(CrabError::HANDSHAKE_ERROR))
    }
}
struct Inner<S, H> {
    protocol: Box<dyn Protocol<Handshake = S, Heartbeat = H>>,
}
impl<'a, S, H> Hook<'a> for Inner<S, H> {}
pub(super) struct ProtocolWrapper<'a>(Box<dyn Hook<'a>>);
