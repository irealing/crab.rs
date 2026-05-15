use dashmap::DashMap;

use crate::crab::utils::runit::Worker;

type Manager = DashMap<String, dyn Node>;
#[async_trait::async_trait]
pub trait Node: Worker {
    fn id(&self) -> &str;
}
