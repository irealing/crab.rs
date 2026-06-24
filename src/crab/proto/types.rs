use binrw::{binrw, BinRead, BinWrite};
use serde::{Deserialize, Serialize};
use crate::CrabError;

#[derive(BinRead, BinWrite, Debug, PartialEq, Eq, Clone, Copy)]
#[brw(repr = u16)]
#[brw(little)]
pub enum Method {
    Handshake = 0,
    Heartbeat = 1,
    Ack = 2,
    Error = 3,
    Command = 4,
}

#[binrw]
#[brw(little)]
#[brw(magic = b"CRAB")]
#[derive(Debug)]
pub struct MessageHeader {
    pub version: u8,
    pub option: u8,
    pub method: Method,
    pub length: u32,
}

impl MessageHeader {
    pub const OPTION_NONE: u8 = 0;
    pub const OPTION_ERROR: u8 = 1;
    pub fn ok(&self) -> bool {
        self.option & Self::OPTION_ERROR != Self::OPTION_ERROR
    }
}


#[derive(Serialize, Deserialize)]
pub struct AckMessage {
    pub code: u32,
    pub msg: Option<String>,
}
impl AckMessage {
    pub fn from_error(err: &CrabError) -> Self {
        match err {
            CrabError::ErrorCode(code) => Self {
                code: *code,
                msg: None,
            },
            _ => Self {
                code: CrabError::UNKNOWN_ERROR,
                msg: Some(err.to_string()),
            },
        }
    }
}
impl Into<Result<(), CrabError>> for AckMessage {
    fn into(self) -> Result<(), CrabError> {
        if self.code == CrabError::NO_ERROR {
            Ok(())
        } else {
            match self.msg {
                None => Err(CrabError::ErrorCode(self.code)),
                Some(msg) => Err(CrabError::ErrorCodeWithMessage(self.code, msg)),
            }
        }
    }
}
impl From<Result<(), CrabError>> for AckMessage {
    fn from(value: Result<(), CrabError>) -> Self {
        let err = match value {
            Ok(()) => CrabError::ErrorCode(CrabError::NO_ERROR),
            Err(e) => e,
        };
        Self::from_error(&err)
    }
}