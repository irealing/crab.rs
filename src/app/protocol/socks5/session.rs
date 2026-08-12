use crab::utils::runit::OnceWorker;
use crab::{CrabError, Handle};
use tokio_util::sync::CancellationToken;

pub enum Session {
    Tcp(TcpSession),
    Udp(UdpSession),
}
#[async_trait::async_trait]
impl OnceWorker for Session {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        match self {
            Session::Tcp(sess) => sess.serve(token).await,
            Session::Udp(sess) => sess.serve(token).await,
        }
    }
}
pub struct TcpSession {
    handle: Handle,
}
#[async_trait::async_trait]
impl OnceWorker for TcpSession {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        todo!()
    }
}
pub struct UdpSession {}
#[async_trait::async_trait]
impl OnceWorker for UdpSession {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        todo!()
    }
}
