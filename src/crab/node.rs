use crate::crab::utils::runit::Worker;
use std::net::SocketAddr;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Ready,
    Running,
    Stopping,
    Stopped,
}

pub trait Node: Worker {
    fn id(&self) -> &str;
    fn status(&self) -> NodeStatus;
    fn addr(&self) -> SocketAddr;
}
