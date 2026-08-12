use std::{fs, str::FromStr};

#[cfg(feature = "tcp_forward")]
use super::workers::forwarder::TcpForwarderOption;
#[cfg(feature = "socks5")]
use crate::protocol::Config as SocksConfig;
use crab::{CrabError, EndpointConfig, utils::crypto::Config as TLSConfig};
use serde::Deserialize;
#[derive(Deserialize, Debug)]
pub struct Config {
    pub node_id: String,
    pub endpoint: EndpointConfig,
    pub tls: TLSConfig,
    #[cfg(feature = "tcp_forward")]
    pub tcp_forward: Option<Vec<TcpForwarderOption>>,
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
