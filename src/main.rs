use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
mod crab;

use crab::{
    CrabError,
    utils::runit::{Worker, wait_exit},
};

mod config;
struct WaitExitWorker;
impl WaitExitWorker {
    fn new() -> Self {
        Self {}
    }
}

const DEFAULT_CONFIG_FILE: &str = "@config.toml";
#[tokio::main]
async fn main() {
    logforth::starter_log::stderr().apply();
}
