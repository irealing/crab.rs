use crab::CrabError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::fs;
use tokio::time::Instant;

const TEMP_FILE_SUFFIX: &str = ".crab_temp";
pub async fn generate_temp_path(target_path: &str, mkdir: bool) -> Result<PathBuf, CrabError> {
    let filepath = Path::new(target_path);
    let base_dir = filepath.parent().unwrap_or(Path::new(""));
    let filename = filepath
        .file_name()
        .ok_or(CrabError::ErrorCode(CrabError::BAD_PARAMETER))?;
    if mkdir {
        fs::create_dir_all(base_dir).await?;
    }
    if !fs::try_exists(base_dir).await? || !fs::metadata(base_dir).await?.is_dir() {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "directory not exists",
        ))?;
    }
    let mut new_filename = OsString::from(filename);
    new_filename.push(TEMP_FILE_SUFFIX);
    Ok(base_dir.join(new_filename))
}
#[derive(Clone)]
pub struct IdleTracker {
    start_at: Instant,
    last_active_at: Arc<AtomicU64>,
    timeout: Duration,
}
impl IdleTracker {
    pub fn new(timeout: Duration) -> Self {
        Self {
            start_at: Instant::now(),
            last_active_at: Arc::new(AtomicU64::new(0)),
            timeout,
        }
    }
    #[inline]
    pub fn refresh(&self) {
        let ms = self.start_at.elapsed().as_millis() as u64;
        self.last_active_at.store(ms, Ordering::Relaxed);
    }
    pub fn is_timeout(&self) -> bool {
        let current_ms = self.start_at.elapsed().as_millis() as u64;
        let last_ms = self.last_active_at.load(Ordering::Relaxed);
        current_ms.saturating_sub(last_ms) >= self.timeout.as_millis() as u64
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_generate_temp_path() {
        generate_temp_path("./tmp/.crab", true).await.unwrap();
    }
}
