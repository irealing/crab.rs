use super::super::errors::CrabError;
use std::{option::Option, sync::Arc};
use tokio::task::JoinSet;
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    async fn run(&self) -> Option<CrabError>;
}

struct WorkerGroup {
    workers: Vec<Arc<dyn Worker>>,
}
#[async_trait::async_trait]
impl Worker for WorkerGroup {
    async fn run(&self) -> Option<CrabError> {
        let mut join_set = JoinSet::new();
        for worker in &self.workers {
            let worker = worker.clone();
            join_set.spawn(async move { worker.run().await });
        }
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Some(err)) => return Some(err),
                Ok(None) => continue,
                Err(join_err) => {
                    log::warn!("{}:{} Worker task failed: {}", file!(), line!(), join_err);
                    return Some(CrabError::ErrorCode(CrabError::ASYNC_RUNTIME_ERROR));
                }
            }
        }
        None
    }
}
pub fn worker_group(workers: Vec<Arc<dyn Worker>>) -> impl Worker {
    WorkerGroup { workers }
}
