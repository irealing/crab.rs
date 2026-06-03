use std::{fs, str::FromStr};

use serde::Deserialize;

use crate::crab::{CrabError, LocalNodeConfig, utils::crypto::Config as TLSConfig};

#[derive(Deserialize, Debug)]
pub struct Config {
    pub node: LocalNodeConfig,
    pub tls: TLSConfig,
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
