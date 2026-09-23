//! HTTP client for upstream proxying.
//!
//! # Design notes
//!
//! * **Upstream credentials are read per request**, not captured at startup, so
//!   rotating `upstream.api_key` or repointing `upstream.base_url` in
//!   `config.yaml` takes effect through hot reload without a restart. Only
//!   `connect_timeout_secs` is fixed at startup, because it configures the
//!   connector and its connection pool.
//! * **Hop-by-hop headers are stripped in both directions.** Forwarding
//!   `Connection`, `Transfer-Encoding`, `Upgrade` and friends breaks framing
//!   between two independent HTTP connections.
//! * **`Host` is dropped** so the upstream hostname from `base_url` is used;
//!   forwarding the proxy's own `Host` would be wrong for virtual-hosted APIs.
//! * **`Accept-Encoding` is dropped.** The response body is scanned for usage as
//!   it streams past; if the upstream compressed it, usage would be unreadable
//!   and every streaming request would be metered as unavailable. Losing
//!   compression on an inference API is a small price for correct accounting.

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Uri, header};
use http_body_util::{BodyExt, Full};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use std::time::Duration;

use crate::config::UpstreamConfig;

/// Headers that apply to a single transport hop and must not be forwarded.
///
/// RFC 7230 §6.1. The `Connection` header's own listed tokens are handled
/// separately, since it can name arbitrary extra headers.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Whether a header must not cross a proxy hop.
pub fn is_hop_by_hop(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    HOP_BY_HOP.contains(&lower.as_str())
}

/// Remove hop-by-hop headers, including any named by `Connection`.
///
/// Also drops `Host`, which must describe the connection being made rather than
/// the one it arrived on.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // Collect the tokens named by `Connection` before removing it.
    let mut named: Vec<String> = Vec::new();
    if let Some(conn) = headers.get(header::CONNECTION) {
        if let Ok(value) = conn.to_str() {
            for token in value.split(',') {
                let token = token.trim().to_ascii_lowercase();
                if !token.is_empty() {
                    named.push(token);
                }
            }
        }
    }

    for hop in HOP_BY_HOP {
        headers.remove(*hop);
    }
    for name in named {
        headers.remove(name.as_str());
    }
    headers.remove(header::HOST);
}

/// Upstream proxy client.
#[derive(Clone)]
pub struct ProxyClient {
    client: Client<HttpConnector, http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
    /// Fallback upstream used when a caller does not supply a live snapshot.
    default_base_url: String,
    /// Fallback credential, paired with `default_base_url`.
    default_api_key: String,
}

impl ProxyClient {
    /// Create a new proxy client.
    ///
    /// `config` seeds the connector (connect timeout, TCP settings) and the
    /// fallback upstream. Per-request values normally come from the live config
    /// snapshot so hot reload is honoured.
    pub fn new(config: &UpstreamConfig) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(
            config.connect_timeout_secs.max(1),
        )));
        // Without this, a DNS name resolving to several addresses would only
        // ever try the first — and hang for the full connect timeout if it is
        // blackholed.
        connector.set_happy_eyeballs_timeout(Some(Duration::from_millis(300)));

        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(16)
            .build(connector);

        Self {
            client,
            default_base_url: config.normalized_base_url(),
            default_api_key: config.api_key.clone(),
        }
    }

    /// Build an upstream URI for a request path.
    pub fn upstream_uri(
        &self,
        base_url: &str,
        path: &str,
    ) -> Result<Uri, Box<dyn std::error::Error + Send + Sync>> {
        let base = base_url.trim_end_matches('/');
        // `path` always starts with `/` for the routes this proxy serves.
        let uri = format!("{base}{path}");
        Uri::try_from(uri).map_err(|e| e.into())
    }

    /// Proxy a request to the upstream and return the streaming response.
    ///
    /// The response body is *not* collected: streaming responses stay
    /// incremental all the way to the client. Callers that need the whole body
    /// (non-streaming JSON) collect it themselves under a size cap.
    pub async fn proxy(
        &self,
        upstream: Option<&UpstreamConfig>,
        method: Method,
        path: String,
        body: Bytes,
        mut headers: HeaderMap,
    ) -> Result<hyper::Response<hyper::body::Incoming>, Box<dyn std::error::Error + Send + Sync>>
    {
        let (base_url, api_key) = match upstream {
            Some(cfg) => (cfg.normalized_base_url(), cfg.api_key.clone()),
            None => (self.default_base_url.clone(), self.default_api_key.clone()),
        };

        let uri = self.upstream_uri(&base_url, &path)?;

        strip_hop_by_hop(&mut headers);
        // Framing headers are recomputed by hyper, and the local credential is
        // replaced by the upstream credential below.
        headers.remove(header::CONTENT_LENGTH);
        headers.remove(header::AUTHORIZATION);
        headers.remove(header::ACCEPT_ENCODING);

        let mut builder = Request::builder().method(method).uri(uri);
        for (name, value) in headers.iter() {
            builder = builder.header(name.clone(), value.clone());
        }
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {api_key}"));
        if !headers.contains_key(header::CONTENT_TYPE) {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
        }
        if !headers.contains_key(header::ACCEPT) {
            builder = builder.header(header::ACCEPT, "application/json");
        }

        let body = Full::new(body).map_err(|never| match never {});
        let body = http_body_util::combinators::BoxBody::new(body);
        let request = builder.body(body)?;

        let response = self.client.request(request).await?;
        Ok(response)
    }

    /// Copy an upstream response's headers onto a new response builder, dropping
    /// hop-by-hop headers and any header the caller must not pass through.
    ///
    /// `Content-Length` is dropped because the body is re-framed by hyper: for
    /// buffered responses hyper computes it from the sized body, and for streamed
    /// responses it must use chunked encoding. Forwarding the upstream's value
    /// would either duplicate the header or declare a length the re-framed body
    /// does not have.
    pub fn forward_response_headers(
        builder: http::response::Builder,
        headers: &HeaderMap,
    ) -> http::response::Builder {
        let mut builder = builder;
        for (name, value) in headers.iter() {
            if is_hop_by_hop(name.as_str()) {
                continue;
            }
            if name == header::CONTENT_LENGTH {
                continue;
            }
            // Upstream session state is meaningless to this API's clients.
            if name == header::SET_COOKIE {
                continue;
            }
            builder = builder.header(name.clone(), value.clone());
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn client() -> ProxyClient {
        ProxyClient::new(&UpstreamConfig {
            base_url: "https://api.example.com/".into(),
            api_key: "upstream-secret".into(),
            timeout_secs: 120,
            connect_timeout_secs: 10,
        })
    }

    #[test]
    fn test_upstream_uri_joins_paths_without_doubling_slash() {
        let c = client();
        assert_eq!(
            c.upstream_uri("https://api.example.com", "/v1/models")
                .unwrap()
                .to_string(),
            "https://api.example.com/v1/models"
        );
        // A base URL with a trailing slash must not produce a double slash.
        assert_eq!(
            c.upstream_uri("https://api.example.com/", "/v1/models")
                .unwrap()
                .to_string(),
            "https://api.example.com/v1/models"
        );
    }

    #[test]
    fn test_upstream_uri_preserves_a_base_path() {
        let c = client();
        assert_eq!(
            c.upstream_uri("https://gateway.internal/openai/v1", "/v1/responses")
                .unwrap()
                .to_string(),
            "https://gateway.internal/openai/v1/v1/responses"
        );
    }

    #[test]
    fn test_hop_by_hop_headers_are_recognized() {
        for name in [
            "connection",
            "Keep-Alive",
            "TRANSFER-ENCODING",
            "Upgrade",
            "proxy-authorization",
            "te",
            "trailer",
            "proxy-authenticate",
        ] {
            assert!(is_hop_by_hop(name), "{name} must be hop-by-hop");
        }
        for name in ["authorization", "content-type", "accept", "x-request-id"] {
            assert!(!is_hop_by_hop(name), "{name} must be end-to-end");
        }
    }

    #[test]
    fn test_strip_removes_hop_by_hop_host_and_connection_named_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("close, x-custom-hop"),
        );
        headers.insert("x-custom-hop", HeaderValue::from_static("v"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(header::HOST, HeaderValue::from_static("proxy.local"));
        headers.insert("x-keep", HeaderValue::from_static("keep-me"));

        strip_hop_by_hop(&mut headers);

        assert!(!headers.contains_key(header::CONNECTION));
        assert!(
            !headers.contains_key("x-custom-hop"),
            "Connection-named headers must go"
        );
        assert!(!headers.contains_key(header::TRANSFER_ENCODING));
        assert!(
            !headers.contains_key(header::HOST),
            "Host must describe the new connection"
        );
        assert_eq!(headers.get("x-keep").unwrap(), "keep-me");
    }

    #[test]
    fn test_strip_is_case_insensitive_for_connection_tokens() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONNECTION, HeaderValue::from_static("X-MiXeD-CaSe"));
        headers.insert("x-mixed-case", HeaderValue::from_static("v"));
        strip_hop_by_hop(&mut headers);
        assert!(!headers.contains_key("x-mixed-case"));
    }

    #[test]
    fn test_forward_response_headers_drops_hop_by_hop_and_cookies() {
        let mut upstream = HeaderMap::new();
        upstream.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream"),
        );
        upstream.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        upstream.insert(header::SET_COOKIE, HeaderValue::from_static("session=leak"));
        upstream.insert(header::CONTENT_LENGTH, HeaderValue::from_static("1234"));
        upstream.insert("x-ratelimit-remaining", HeaderValue::from_static("42"));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let response =
            ProxyClient::forward_response_headers(http::Response::builder().status(200), &upstream)
                .body(())
                .unwrap()
                .into_parts()
                .0
                .headers;

        // The upstream content type must win over any default.
        assert_eq!(
            response.get(header::CONTENT_TYPE).unwrap(),
            "text/event-stream"
        );
        assert!(!response.contains_key(header::TRANSFER_ENCODING));
        assert!(
            !response.contains_key(header::CONTENT_LENGTH),
            "hyper must re-frame the body rather than inherit the upstream length"
        );
        assert!(
            !response.contains_key(header::SET_COOKIE),
            "upstream session must not leak"
        );
        assert_eq!(response.get("x-ratelimit-remaining").unwrap(), "42");
    }
}
