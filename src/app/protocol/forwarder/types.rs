use crab::CrabError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::net::SocketAddr;
use tokio::net::lookup_host;
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Address {
    SocketAddress(SocketAddr),
    DomainAddress { host: String, port: u16 },
}
impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Address::SocketAddress(addr) => addr.fmt(f),
            Address::DomainAddress { host, port } => write!(f, "host:{},port:{}", host, port),
        }
    }
}
impl Address {
    pub async fn resolve(&self) -> Result<SocketAddr, CrabError> {
        match self {
            Address::SocketAddress(addr) => Ok(*addr),
            Address::DomainAddress { host, port } => {
                let mut address = lookup_host((host.as_str(), *port)).await?;
                match address.next() {
                    Some(addr) => Ok(addr),
                    None => Err(CrabError::ErrorCode(CrabError::DNS_RESOLVE_ERROR)),
                }
            }
        }
    }
}
