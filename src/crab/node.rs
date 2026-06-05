use crate::crab::{CrabError, utils::runit::Worker};
use quinn::Connection;
use std::net::SocketAddr;
use std::sync::Arc;
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
