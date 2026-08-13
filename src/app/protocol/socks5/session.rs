use crate::app::protocol::TcpForwardParams;
use crate::app::protocol::forwarder::Address;
use crab::utils::runit::OnceWorker;
use crab::{CrabError, Handle};
use socks5_server::Connect;
use socks5_server::connection::connect::state::NeedReply;
use socks5_server::proto::{Address as Socks5Addr, Reply};
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
    pub handle: Handle,
    pub conn: Connect<NeedReply>,
    pub address: Socks5Addr,
}
impl TcpSession {
    pub const DEFAULT_TIMEOUT: u8 = 10;
    pub const DEFAULT_KEEPALIVE_TIMEOUT: u8 = 60;
    pub const DEFAULT_KEEPALIVE_RETRY: u8 = 15;
    pub const DEFAULT_KEEPALIVE_RETRY_INTERVAL: u8 = 3;
}
#[async_trait::async_trait]
impl OnceWorker for TcpSession {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        let target_address = match self.address {
            Socks5Addr::SocketAddress(address) => Address::SocketAddress(address),
            Socks5Addr::DomainAddress(host, port) => match String::from_utf8(host) {
                Ok(host) => Address::StringAddress { host, port },
                Err(err) => {
                    log::error!("bad host format {}", err);
                    let _ = self
                        .conn
                        .reply(Reply::AddressTypeNotSupported, Socks5Addr::unspecified())
                        .await;
                    return Err(CrabError::ErrorCode(CrabError::BAD_PARAMETER));
                }
            },
        };
        let param = TcpForwardParams {
            target_address,
            connect_timeout: Self::DEFAULT_TIMEOUT,
            keepalive_timeout: Self::DEFAULT_KEEPALIVE_TIMEOUT,
            keepalive_interval: Self::DEFAULT_KEEPALIVE_RETRY_INTERVAL,
            keepalive_retries: Self::DEFAULT_KEEPALIVE_RETRY,
        };

        // self.handle.tcp_forward(token,param,)
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
