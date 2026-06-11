use crate::crab::utils::runit::Worker;
use serde::Deserialize;
use std::fmt::Display;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Ready,
    Running,
    Stopping,
    Stopped,
}
impl Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            NodeStatus::Ready => {
                write!(f, "Ready")
            }
            NodeStatus::Running => {
                write!(f, "Running")
            }
            NodeStatus::Stopping => {
                write!(f, "Stopping")
            }
            NodeStatus::Stopped => {
                write!(f, "Stopped")
            }
        }
    }
}
#[derive(Debug, Deserialize, Copy, Clone)]
#[serde(default)]
pub struct Options {
    pub connect_timeout: u64,
    pub handshake_timeout: u64,
    pub first_heartbeat: u64,
    pub heartbeat_interval: u64,
    pub heartbeat_timeout: u64,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            connect_timeout: 10,
            handshake_timeout: 10,
            first_heartbeat: 3,
            heartbeat_interval: 15,
            heartbeat_timeout: 30,
        }
    }
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
