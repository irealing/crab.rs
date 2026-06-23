use axum::Json;
use axum::response::{IntoResponse, Response};
use crab::CrabError;
use serde::Serialize;

#[derive(Serialize)]
pub struct Ret<D: Serialize> {
    pub err_no: u32,
    pub msg: String,
    pub data: Option<D>,
}
impl<D: Serialize> Ret<D> {
    pub fn success(data: Option<D>) -> Self {
        Self {
            err_no: CrabError::NO_ERROR,
            msg: "success".to_string(),
            data,
        }
    }
    pub fn error(e: CrabError) -> Self {
        Self {
            msg: e.to_string(),
            err_no: e.err_no(),
            data: None,
        }
    }
}
impl<T> IntoResponse for Ret<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        Json(self).into_response()
    }
}
impl<T> From<Result<T, CrabError>> for Ret<T>
where
    T: Serialize,
{
    fn from(value: Result<T, CrabError>) -> Self {
        match value {
            Ok(val) => Ret::success(Some(val)),
            Err(e) => Ret::error(e),
        }
    }
}
