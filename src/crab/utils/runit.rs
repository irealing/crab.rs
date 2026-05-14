use super::super::errors::CrabError;
use std::{option::Option, sync::Arc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    async fn run(&self, token: CancellationToken) -> Option<CrabError>;
}

struct WorkerGroup {
    workers: Vec<Arc<dyn Worker>>,
}
#[async_trait::async_trait]
impl Worker for WorkerGroup {
    async fn run(&self, token: CancellationToken) -> Option<CrabError> {
        let mut join_set = JoinSet::new();
        for worker in &self.workers {
            let worker = worker.clone();
            let token = token.clone();
            join_set.spawn(async move { worker.run(token).await });
        }
        let mut ret = None;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Some(err)) => {
                    token.cancel();
                    if ret.is_none() {
                        ret = Some(err);
                    }
                }
                Ok(None) => continue,
                Err(join_err) => {
                    log::warn!("{}:{} Worker task failed: {}", file!(), line!(), join_err);
                    return Some(CrabError::ErrorCode(CrabError::ASYNC_RUNTIME_ERROR));
                }
            }
        }
        ret
    }
}
pub fn worker_group(workers: Vec<Arc<dyn Worker>>) -> impl Worker {
    WorkerGroup { workers }
}
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::select;
    use tokio_util::sync::CancellationToken;

    use crate::crab::CrabError;

    use super::{Worker, worker_group};
    struct LocalWorker(u64);
    #[async_trait::async_trait]
    impl Worker for LocalWorker {
        async fn run(&self, token: CancellationToken) -> Option<CrabError> {
            eprintln!("{}:{} LocalWorker:run {}", file!(), line!(), self.0);
            if self.0 % 2 != 0 {
                tokio::time::sleep(Duration::from_secs(self.0)).await;
                return Some(CrabError::ErrorCode(CrabError::UNKNOWN_ERROR));
            }
            select! {
                _=tokio::time::sleep(Duration::from_secs(self.0))=>{
                    eprintln!("{}:{} LocalWorker({})::run completed",file!(),line!(),self.0);
                }
                _=token.cancelled()=>{
                    eprintln!("{}:{} LocalWorker({})::run cancelled",file!(),line!(),self.0);
                }
            }
            None
        }
    }
    #[tokio::test]
    async fn test_worker_error() {
        worker_group(vec![
            Arc::new(LocalWorker(3)),
            Arc::new(LocalWorker(10)),
            Arc::new(LocalWorker(20)),
        ])
        .run(CancellationToken::new())
        .await
        .filter(|e| matches!(e, CrabError::ErrorCode(_)))
        .expect("expect error");
    }
}
