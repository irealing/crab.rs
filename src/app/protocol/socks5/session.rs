use super::super::forwarder::tcp_forward;
use crate::app::protocol::TcpForwardParams;
use crate::app::protocol::forwarder::Address;
use crate::app::protocol::types::Command;
use crab::proto::Stream;
use crab::utils::runit::OnceWorker;
use crab::{CrabError, Handle};
use socks5_server::Connect;
use socks5_server::connection::connect::state::NeedReply;
use socks5_server::proto::{Address as Socks5Addr, Reply};
use std::net::SocketAddr;
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
impl TryFrom<Socks5Addr> for Address {
    type Error = CrabError;
    fn try_from(addr: Socks5Addr) -> Result<Self, Self::Error> {
        match addr {
            Socks5Addr::SocketAddress(address) => Ok(Address::SocketAddress(address)),
            Socks5Addr::DomainAddress(host, port) => match String::from_utf8(host) {
                Ok(host) => Ok(Address::DomainAddress { host, port }),
                Err(err) => {
                    log::error!("invalid UTF-8 address: {}", err);
                    Err(CrabError::ErrorCode(CrabError::BAD_PARAMETER))
                }
            },
        }
    }
}
#[async_trait::async_trait]
impl OnceWorker for TcpSession {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        let target_address = match self.address.try_into() {
            Ok(address) => address,
            Err(err) => {
                log::error!("invalid address type: {}", err);
                let _ = self
                    .conn
                    .reply(Reply::AddressTypeNotSupported, Socks5Addr::unspecified())
                    .await;
                return Err(err);
            }
        };
        let param = TcpForwardParams {
            target_address,
            connect_timeout: Self::DEFAULT_TIMEOUT,
            keepalive_timeout: Self::DEFAULT_KEEPALIVE_TIMEOUT,
            keepalive_interval: Self::DEFAULT_KEEPALIVE_RETRY_INTERVAL,
            keepalive_retries: Self::DEFAULT_KEEPALIVE_RETRY,
        };
        let (handle, addr) = self
            .handle
            .exec_ack::<_, SocketAddr, _>(Command::TcpForward(param))
            .await
            .inspect_err(|err| log::warn!("socks5 proxy forward tcp error {}", err))?;
        log::debug!("tcp forward via addr: {}", addr);
        let reply_ret = self
            .conn
            .reply(Reply::Succeeded, Socks5Addr::SocketAddress(addr))
            .await
            .map_err(|err| {
                log::error!("socks5 proxy forward tcp reply error {}", err.0);
                CrabError::ErrorCodeWithMessage(
                    CrabError::BAD_STATUS_ERROR,
                    format!("socks5 proxy forward tcp reply error {}", err.0),
                )
            })?;
        let executor = async move |cancel: CancellationToken, stream: Stream| {
            tcp_forward(cancel, stream, reply_ret).await
        };
        handle
            .send(Ok(executor))
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
        Ok(())
    }
}
pub struct UdpSession {}
#[async_trait::async_trait]
impl OnceWorker for UdpSession {
    async fn serve(self, token: CancellationToken) -> Result<(), CrabError> {
        todo!()
    }
}
