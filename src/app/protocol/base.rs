use crab::CrabError;

#[async_trait::async_trait]
pub trait CommandExecutor {
    async fn ping(self) -> Result<(), CrabError>;
}
