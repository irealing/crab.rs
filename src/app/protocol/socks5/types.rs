use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Deserialize, Serialize)]
pub struct PasswordAuthConfig {
    pub username: String,
    pub password: String,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum AuthConfig {
    NoAuth,
    Password(PasswordAuthConfig),
}
impl Default for AuthConfig {
    fn default() -> Self {
        Self::NoAuth
    }
}
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub listen: SocketAddr,
    pub target: String,
    #[serde(default)]
    pub auth: AuthConfig,
}
