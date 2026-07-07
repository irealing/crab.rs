use super::super::protocol::FileMetadata;
use crate::app::utils::http::HttpResponse;
use axum::Json;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
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
    pub fn error(e: CrabError) -> Self {
        Self {
            msg: e.to_string(),
            err_no: e.err_no(),
            data: None,
        }
    }
}
impl<T> From<T> for Ret<T>
where
    T: Serialize,
{
    fn from(t: T) -> Self {
        Self {
            err_no: CrabError::NO_ERROR,
            msg: "success".to_string(),
            data: Some(t),
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
            Ok(val) => val.into(),
            Err(e) => Ret::error(e),
        }
    }
}
pub enum StreamResponse {
    Error(Ret<()>),
    File((FileMetadata, Body)),
}
impl IntoResponse for StreamResponse {
    fn into_response(self) -> Response {
        match self {
            StreamResponse::Error(ret) => ret.into_response(),
            StreamResponse::File((metadata, body)) => (
                [
                    (header::CONTENT_TYPE, "application/octet-stream"),
                    (
                        header::CONTENT_LENGTH,
                        format!("{}", metadata.filesize).as_str(),
                    ),
                ],
                body,
            )
                .into_response(),
        }
    }
}
pub enum ProxyResponse {
    Err((u16, CrabError)),
    Ok((HttpResponse, Body)),
}
impl IntoResponse for ProxyResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Err((status, err)) => {
                let body: Ret<()> = Ret::error(err);
                (
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    Json(body),
                )
                    .into_response()
            }
            Self::Ok((header, body)) => {
                let mut resp_headers = HeaderMap::new();
                for (key, value) in header.headers {
                    if let (Ok(name), Ok(val)) = (
                        HeaderName::from_bytes(key.as_bytes()),
                        HeaderValue::from_str(&value),
                    ) {
                        resp_headers.insert(name, val);
                    }
                }
                (
                    StatusCode::from_u16(header.status_code)
                        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    resp_headers,
                    body,
                )
                    .into_response()
            }
        }
    }
}
