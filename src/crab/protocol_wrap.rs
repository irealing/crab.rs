use super::proto::HandshakeRet;
use crate::crab::{
    CrabError::{self, ErrorCode},
    proto::Protocol,
};
#[async_trait::async_trait]
trait Hook: Send + Sync {
    async fn handshake(&self, _: &quinn::Connection) -> Result<HandshakeRet, CrabError> {
        Err(ErrorCode(CrabError::HANDSHAKE_ERROR))
    }
    async fn handshake_as_client(&self, _: &quinn::Connection) -> Result<HandshakeRet, CrabError> {
        Err(ErrorCode(CrabError::HANDSHAKE_ERROR))
    }
}
struct Inner<S, H, P: Protocol<Handshake = S, Heartbeat = H>> {
    protocol: P,
}
impl<S, H, P> Hook for Inner<S, H, P> where P: Protocol<Handshake = S, Heartbeat = H> {}
pub(super) struct ProtocolWrapper(Box<dyn Hook>);
impl<'a> ProtocolWrapper {
    pub fn new<S, H, P>(protocol: P) -> Self
    where
        S: 'static,
        H: 'static,
        P: Protocol<Handshake = S, Heartbeat = H> + 'static,
    {
        Self(Box::new(Inner { protocol }))
    }
}
