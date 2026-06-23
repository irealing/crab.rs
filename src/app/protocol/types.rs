use crab::CrabError;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::fs;

#[derive(Serialize, Deserialize)]
pub struct DeleteCommand(pub String);
impl DeleteCommand {
    pub fn exec(&self) -> Result<(), CrabError> {
        fs::remove_dir_all(&self.0)?;
        Ok(())
    }
}
#[derive(Deserialize, Serialize)]
pub enum Command {
    Ping,
    Pong,
    Delete(DeleteCommand),
}
impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Command::Ping => {
                write!(f, "ping")
            }
            Command::Pong => {
                write!(f, "pong")
            }
            Command::Delete(ref delete) => {
                write!(f, "delete({})", delete.0)
            }
        }
    }
}
