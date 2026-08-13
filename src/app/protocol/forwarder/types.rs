use crab::CrabError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::SocketAddr;
use tokio::net::lookup_host;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Address {
    SocketAddress(SocketAddr),
    StringAddress { host: String, port: u16 },
}
impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::SocketAddress(addr) => {
                write!(f, "{}", addr)
            }
            Address::StringAddress { host, port } => {
                write!(f, "host:{},port:{}", host, port)
            }
        }
    }
}
impl Address {
    pub async fn resolve(&self) -> Result<SocketAddr, CrabError> {
        match self {
            Address::SocketAddress(addr) => Ok(*addr),
            Address::StringAddress { host, port } => {
                let mut address = lookup_host((host.as_str(), *port)).await?;
                match address.next() {
                    Some(addr) => Ok(addr),
                    None => Err(CrabError::ErrorCode(CrabError::DNS_RESOLVE_ERROR)),
                }
            }
        }
    }
}
pub struct ForwarderHandle<T> {
    pub(super) cancel: CancellationToken,
    pub(super) rx: oneshot::Receiver<Result<(), CrabError>>,
    pub(super) metadata: T,
}
impl<T> ForwarderHandle<T> {
    fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    fn cancel(&mut self) {
        self.cancel.cancel();
    }
    async fn wait(self) -> Result<(), CrabError> {
        self.rx
            .await
            .map_err(|_| CrabError::ErrorCode(CrabError::CANCELED_ERROR))?
    }
    fn metadata(&self) -> &T {
        &self.metadata
    }
}
