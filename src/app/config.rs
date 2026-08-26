#[cfg(feature = "tcp_forward")]
use crate::app::protocol::forwarder::TcpForwardOption;
#[cfg(feature = "socks5")]
use crate::protocol::socks5::Config as SocksConfig;
use crab::{CrabError, EndpointConfig, utils::crypto::Config as TLSConfig};
use serde::Deserialize;
use std::net::SocketAddr;
use std::{fs, str::FromStr};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub node_id: String,
    #[cfg(feature = "api")]
    pub http_api: Option<SocketAddr>,
    pub endpoint: EndpointConfig,
    pub tls: TLSConfig,
    #[cfg(feature = "tcp_forward")]
    pub tcp_forward: Option<Vec<TcpForwardOption>>,
    #[cfg(feature = "socks5")]
    pub socks5_proxy: Option<Vec<SocksConfig>>,
}
impl FromStr for Config {
    type Err = CrabError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(filename) = s.strip_prefix("@") {
            let content = fs::read_to_string(filename)?;
            Ok(toml::from_str::<Self>(&content).map_err(|e| {
                log::error!("parse config file {} error: {}", s, e);
                CrabError::ErrorCode(CrabError::PARSE_ERROR)
            })?)
        } else {
            Ok(toml::from_str(s).map_err(|_| CrabError::ErrorCode(CrabError::PARSE_ERROR))?)
        }
    }
}
