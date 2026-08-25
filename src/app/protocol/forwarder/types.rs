use binrw::{BinRead, binrw};
use crab::CrabError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::io::{Cursor, Read};
use std::net::SocketAddr;
use tokio::net::lookup_host;
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
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
impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Self {
        Address::SocketAddress(addr)
    }
}
#[binrw]
pub struct DomainHost {
    #[br(temp)]
    #[bw(calc=data.len() as u8)]
    len: u8,
    #[br(count = len)]
    pub data: Vec<u8>,
}
#[binrw]
pub enum PackageHost {
    #[br(magic = 1u8)]
    Ipv4([u8; 4]),
    #[br(magic = 4u8)]
    Ipv6([u8; 16]),
    #[br(magic = 3u8)]
    Domain(DomainHost),
}
impl From<SocketAddr> for PackageHost {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(addr) => Self::Ipv4(addr.ip().octets()),
            SocketAddr::V6(addr) => Self::Ipv6(addr.ip().octets()),
        }
    }
}
#[binrw]
#[brw(big)]
pub struct PacketHeader {
    #[br(assert(rsv==0))]
    pub rsv: u16,
    pub frag: u8,
    pub addr: PackageHost,
    pub port: u16,
}
impl PacketHeader {
    pub fn unpack(buf: &[u8]) -> Result<(Self, usize), CrabError> {
        let mut cursor = Cursor::new(buf);
        let header = Self::read(&mut cursor).map_err(|err| {
            log::warn!("Failed to read packet: {}", err);
            CrabError::ErrorCode(CrabError::BAD_MESSAGE_HEADER)
        })?;
        Ok((header, cursor.position() as usize))
    }
}
impl From<SocketAddr> for PacketHeader {
    fn from(addr: SocketAddr) -> Self {
        let port = addr.port();
        Self {
            rsv: 0,
            frag: 0,
            addr: addr.into(),
            port,
        }
    }
}
impl TryFrom<PacketHeader> for Address {
    type Error = CrabError;
    fn try_from(header: PacketHeader) -> Result<Self, Self::Error> {
        match header.addr {
            PackageHost::Ipv4(addr) => Ok(Self::SocketAddress(SocketAddr::new(
                addr.into(),
                header.port,
            ))),
            PackageHost::Ipv6(addr) => Ok(Self::SocketAddress(SocketAddr::new(
                addr.into(),
                header.port,
            ))),
            PackageHost::Domain(host) => Ok(Self::DomainAddress {
                host: String::from_utf8(host.data).map_err(|err| {
                    log::warn!("bad utf8 array {}", err);
                    CrabError::ErrorCode(CrabError::ENCODING_ERROR)
                })?,
                port: header.port,
            }),
        }
    }
}
