use tokio_util::sync::CancellationToken;
mod crab;

#[tokio::main]
async fn main() {
    let token = CancellationToken::new();
}
async fn wait_shutdown(token: CancellationToken) {}
