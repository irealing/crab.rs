use crab::CrabError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio::fs;
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

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_generate_temp_path() {
        generate_temp_path("./tmp/.crab", true).await.unwrap();
    }
}
