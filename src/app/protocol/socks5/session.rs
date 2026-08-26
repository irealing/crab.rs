use super::super::forwarder::tcp_forward;
use crate::app::protocol::TcpForwardParams;
use crate::app::protocol::forwarder::{Address, copy_udp_stream};
use crate::app::protocol::types::Command;
use crab::proto::Stream;
use crab::utils::runit::OnceWorker;
use crab::{CrabError, Handle};
use socks5_server::connection::associate::state::NeedReply as AssociateNeedReply;
use socks5_server::connection::connect::state::NeedReply as ConnectNeedReply;
use socks5_server::proto::{Address as Socks5Addr, Reply};
use socks5_server::{Associate, Connect};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio::net::UdpSocket;
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
    pub conn: Connect<ConnectNeedReply>,
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
    async fn serve(self, _: CancellationToken) -> Result<(), CrabError> {
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
pub struct UdpSession {
    pub(super) handle: Handle,
    pub(super) associate: Associate<AssociateNeedReply>,
}
impl UdpSession {
    async fn prepare(&self) -> Result<(UdpSocket, SocketAddr), CrabError> {
        let peer_addr = self.associate.peer_addr()?;
        let sock =
            UdpSocket::bind(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))).await?;
        sock.connect(peer_addr).await?;
        let local_addr = sock.local_addr()?;
        Ok((sock, local_addr))
    }
}
#[async_trait::async_trait]
impl OnceWorker for UdpSession {
    async fn serve(self, _: CancellationToken) -> Result<(), CrabError> {
        let (sock, local_addr) = match self.prepare().await {
            Ok((sock, local_addr)) => (sock, local_addr),
            Err(err) => {
                log::error!("alloc local addr error {}", err);
                return Err(err);
            }
        };
        let (handle, _) = match self
            .handle
            .exec_ack::<_, SocketAddr, _>(Command::UdpForward(None))
            .await
        {
            Ok(result) => result,
            Err(err) => {
                log::warn!("exec udp forward command error {}", err);
                let _ = self
                    .associate
                    .reply(Reply::NetworkUnreachable, Socks5Addr::unspecified())
                    .await;
                return Err(err);
            }
        };
        let mut associate = self
            .associate
            .reply(Reply::Succeeded, Socks5Addr::SocketAddress(local_addr))
            .await
            .map_err(|(err, _)| {
                log::warn!("udp forward error {}", err);
                CrabError::ErrorCodeWithMessage(CrabError::NETWORK_ERROR, err.to_string())
            })?;
        let executor =
            async move |cancel: CancellationToken, stream: Stream| -> Result<(), CrabError> {
                tokio::select! {
                    _=cancel.cancelled() =>Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
                    _=associate.wait_close()=>Err(CrabError::ErrorCode(CrabError::CANCELED_ERROR)),
                    ret=copy_udp_stream(cancel.clone(), stream, sock)=>ret,
                }
            };
        handle
            .send(Ok(executor))
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?;
        Ok(())
    }
}
