use axum::Router;
use crab::utils::runit::Worker;

pub trait ApiWorker: Worker {
    fn routers(&self) -> Router;
    fn tag(&self) -> &str;
}
