use crate::crab::utils::runit::Worker;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Ready,
    Running,
    Stopping,
    Stopped,
}
#[derive(Debug, Deserialize, Copy, Clone)]
#[serde(default)]
pub struct Options {
    pub connect_timeout: u64,
    pub handshake_timeout: u64,
    pub heartbeat_interval: u64,
    pub heartbeat_timeout: u64,
}
pub trait Endpoint: Worker {
    fn id(&self) -> &str;
    fn addr(&self) -> SocketAddr;
}
pub trait Node: Send + Sync {
    fn id(&self) -> &str;
    fn status(&self) -> NodeStatus;
    fn addr(&self) -> SocketAddr;
    fn as_client(&self) -> bool;
}
