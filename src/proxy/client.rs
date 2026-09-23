//! HTTP client for upstream proxying

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Uri};
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

use crate::config::UpstreamConfig;

/// Upstream proxy client
#[derive(Clone)]
pub struct ProxyClient {
    client: Client<HttpConnector, http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
    base_url: String,
    api_key: String,
}

impl ProxyClient {
    /// Create a new proxy client
    pub fn new(config: &UpstreamConfig) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(config.connect_timeout_secs)));

        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(8)
            .build(connector);

        Self {
            client,
            base_url: config.normalized_base_url(),
            api_key: config.api_key.clone(),
        }
    }

    /// Build the upstream URI for a path
    pub fn upstream_uri(
        &self,
        path: &str,
    ) -> Result<Uri, Box<dyn std::error::Error + Send + Sync>> {
        let uri = format!("{}{}", self.base_url, path);
        Uri::try_from(uri).map_err(|e| e.into())
    }

    /// Proxy a request to the upstream
    pub async fn proxy(
        &self,
        method: Method,
        path: String,
        body: Bytes,
        headers: HeaderMap,
    ) -> Result<hyper::Response<hyper::body::Incoming>, Box<dyn std::error::Error + Send + Sync>>
    {
        let uri = self.upstream_uri(&path)?;

        // Build upstream request
        let mut builder = Request::builder().method(method).uri(uri);

        // Copy headers, replacing authorization with upstream key
        for (name, value) in headers.iter() {
            if name == http::header::AUTHORIZATION {
                continue; // replaced below
            }
            builder = builder.header(name.clone(), value.clone());
        }

        // Set upstream API key
        builder = builder.header(
            http::header::AUTHORIZATION,
            format!("Bearer {}", self.api_key),
        );

        // Set content type if not present
        if !headers.contains_key(http::header::CONTENT_TYPE) {
            builder = builder.header(http::header::CONTENT_TYPE, "application/json");
        }

        // Map Infallible error to hyper::Error to satisfy the client's body type
        let body = Full::new(body).map_err(|never| match never {});
        let body = http_body_util::combinators::BoxBody::new(body);
        let req = builder.body(body)?;

        let response = self.client.request(req).await?;

        Ok(response)
    }
}
