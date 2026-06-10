use super::CrabError;
use std::net::SocketAddr;
use tokio::net::lookup_host;

pub mod crypto;
pub mod runit;
pub async fn parse_remote_addr(addr: &str) -> Result<(&str, Vec<SocketAddr>), CrabError> {
    let (host, _) = addr
        .rsplit_once(':')
        .ok_or(CrabError::ErrorCode(CrabError::BAD_REMOTE_ADDR))?;
    let addr = lookup_host(addr).await?;
    Ok((host, addr.collect::<Vec<SocketAddr>>()))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_parse_remote_addr() {
        parse_remote_addr("localhost:443").await.unwrap();
    }
}
