use bytes::Bytes;
use crab::utils::crypto::TLSProvider;
use crab::CrabError;
use http_body_util::combinators::BoxBody;
use hyper::body::Incoming;
use hyper::Request;
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;

pub type Headers = Vec<(String, String)>;
#[derive(Copy, Serialize, Deserialize, Clone)]
pub enum HttpMethod {
    Options,
    Get,
    Post,
    Put,
    Delete,
    Head,
    Trace,
    Connect,
    Patch,
}
impl From<HttpMethod> for hyper::Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::Options => hyper::Method::OPTIONS,
            HttpMethod::Get => hyper::Method::GET,
            HttpMethod::Post => hyper::Method::POST,
            HttpMethod::Put => hyper::Method::PUT,
            HttpMethod::Delete => hyper::Method::DELETE,
            HttpMethod::Head => hyper::Method::HEAD,
            HttpMethod::Trace => hyper::Method::TRACE,
            HttpMethod::Connect => hyper::Method::CONNECT,
            HttpMethod::Patch => hyper::Method::PATCH,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub request_uri: String,
    pub headers: Headers,
}
#[derive(Serialize, Deserialize)]
pub struct HttpResponse {
    pub status_code: u16,
    pub headers: Headers,
}
pub type RequestBody = BoxBody<Bytes, io::Error>;
pub type HyperClient = Client<HttpsConnector<HttpConnector>, RequestBody>;

pub struct HttpClient {
    inner: Arc<HyperClient>,
}
impl HttpClient {
    pub fn create(tls: &TLSProvider) -> Result<Self, CrabError> {
        let tls_config = tls.build_client_config()?;
        let https = HttpsConnectorBuilder::new()
            .with_tls_config(tls_config)
            .https_or_http()
            .enable_http2()
            .build();
        Ok(Self {
            inner: Arc::new(Client::builder(TokioExecutor::new()).build(https)),
        })
    }
    pub async fn request(
        &self,
        req: Request<RequestBody>,
    ) -> Result<(HttpResponse, Incoming), CrabError> {
        match self.inner.request(req).await {
            Ok(resp) => Ok((
                HttpResponse {
                    status_code: resp.status().as_u16(),
                    headers: resp
                        .headers()
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                        .collect(),
                },
                resp.into_body(),
            )),
            Err(e) => Err(CrabError::ErrorCodeWithMessage(
                CrabError::UNKNOWN_ERROR,
                e.to_string(),
            )),
        }
    }
    pub fn make_request(
        &self,
        req: HttpRequest,
        body: RequestBody,
    ) -> Result<Request<RequestBody>, CrabError> {
        let mut builder = Request::builder().method(req.method.clone());
        if req.headers.len() > 0 {
            for (k, v) in req.headers {
                builder = builder.header(k, v);
            }
        }
        Ok(builder.body(body).map_err(|err| {
            log::error!("failed to make http request {}", err);
            CrabError::ErrorCode(CrabError::BAD_PARAMETER)
        })?)
    }
}
