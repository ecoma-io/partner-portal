//! Authentication.
//!
//! Identity is **always derived server-side** from the presented credential and
//! the current config snapshot. Nothing a client sends — `consumer_id`,
//! `x-consumer-id`, metadata, or any other field — contributes to identity, so
//! no request can read or write another consumer's data by asserting it.
//!
//! Implemented as an axum extractor rather than a middleware layer: a handler
//! that takes `Authenticated` cannot be written without resolving an identity,
//! so a new route is authenticated by construction instead of by remembering to
//! attach a layer.

use axum::{
    extract::FromRequestParts,
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::auth::ConsumerContext;
use crate::proxy::handler::AppState;

/// Authentication error.
#[derive(Debug)]
pub enum AuthError {
    MissingAuthorization,
    InvalidFormat,
    InvalidKey,
}

impl AuthError {
    fn message(&self) -> &'static str {
        match self {
            AuthError::MissingAuthorization => "Missing Authorization header",
            AuthError::InvalidFormat => {
                "Invalid Authorization header format; expected 'Bearer <key>'"
            }
            AuthError::InvalidKey => "Invalid API key",
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let body = serde_json::json!({
            "error": {
                "message": self.message(),
                "type": "invalid_request_error",
                "code": "invalid_api_key",
            }
        });

        (
            StatusCode::UNAUTHORIZED,
            [
                (axum::http::header::CONTENT_TYPE, "application/json"),
                // Credentials must never be replayed against a cache.
                (axum::http::header::CACHE_CONTROL, "no-store"),
            ],
            serde_json::to_string(&body).unwrap_or_else(|_| r#"{"error":{}}"#.to_string()),
        )
            .into_response()
    }
}

/// Extract a Bearer token from request parts.
///
/// The scheme match is case-insensitive, as RFC 7235 requires.
fn extract_bearer_token(parts: &Parts) -> Result<&str, AuthError> {
    let header = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(AuthError::MissingAuthorization)?;

    let (scheme, token) = header.split_once(' ').ok_or(AuthError::InvalidFormat)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(AuthError::InvalidFormat);
    }

    let token = token.trim();
    if token.is_empty() {
        return Err(AuthError::InvalidFormat);
    }
    Ok(token)
}

/// Extract the consumer identity for a request, or reject it.
pub struct Authenticated(pub ConsumerContext);

impl FromRequestParts<Arc<AppState>> for Authenticated {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let token = extract_bearer_token(parts)?;

        // Read from the live snapshot, so a key added or revoked by hot reload
        // takes effect on the next request without a restart.
        let config = state.config.read();
        let key_config = config.config.find_key(token).ok_or(AuthError::InvalidKey)?;

        Ok(Authenticated(ConsumerContext::new(
            key_config.consumer_id().to_string(),
            key_config.name.clone(),
            key_config.metadata.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    fn parts_with_auth(value: Option<&str>) -> Parts {
        let mut builder = Request::builder();
        if let Some(v) = value {
            builder = builder.header(AUTHORIZATION, v);
        }
        let (parts, _) = builder.body(()).unwrap().into_parts();
        parts
    }

    #[test]
    fn test_extract_bearer_token_valid() {
        let parts = parts_with_auth(Some("Bearer my-secret-key"));
        assert_eq!(extract_bearer_token(&parts).unwrap(), "my-secret-key");
    }

    #[test]
    fn test_extract_bearer_token_scheme_is_case_insensitive() {
        for value in ["bearer k", "BEARER k", "BeArEr k"] {
            let parts = parts_with_auth(Some(value));
            assert_eq!(
                extract_bearer_token(&parts).unwrap(),
                "k",
                "{value} must be accepted"
            );
        }
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let parts = parts_with_auth(None);
        assert!(matches!(
            extract_bearer_token(&parts),
            Err(AuthError::MissingAuthorization)
        ));
    }

    #[test]
    fn test_extract_bearer_token_invalid_format() {
        for value in ["Basic dXNlcjpwYXNz", "Bearer", "BearerNoSpace", "Bearer   "] {
            let parts = parts_with_auth(Some(value));
            assert!(
                matches!(extract_bearer_token(&parts), Err(AuthError::InvalidFormat)),
                "{value} must be rejected as malformed"
            );
        }
    }

    #[test]
    fn test_extract_bearer_token_trims_surrounding_space() {
        let parts = parts_with_auth(Some("Bearer  padded-key "));
        assert_eq!(extract_bearer_token(&parts).unwrap(), "padded-key");
    }

    #[test]
    fn test_extract_bearer_token_preserves_internal_characters() {
        // Keys may legitimately contain characters that look like other syntax.
        let parts = parts_with_auth(Some("Bearer sk-a.b_c-d:e"));
        assert_eq!(extract_bearer_token(&parts).unwrap(), "sk-a.b_c-d:e");
    }

    #[test]
    fn test_non_utf8_authorization_is_rejected_not_panicked() {
        let mut builder = Request::builder();
        builder = builder.header(AUTHORIZATION, b"\xff\xfe".as_slice());
        let (parts, _) = builder.body(()).unwrap().into_parts();
        assert!(matches!(
            extract_bearer_token(&parts),
            Err(AuthError::MissingAuthorization)
        ));
    }

    /// The hard contract on this surface: a rejection never renders the
    /// credential it was shown, on either the `Debug` or the response path.
    /// This is where a credential enters the process, so a variant that started
    /// carrying the token would fail here.
    #[tokio::test]
    async fn test_rejections_never_render_the_presented_credential() {
        const SECRET: &str = "super-secret-key";

        // A credential offered under the wrong scheme is rejected *with* the
        // credential in hand — the case where an echo would be reachable.
        let parts = parts_with_auth(Some(&format!("Basic {SECRET}")));
        let err = extract_bearer_token(&parts).unwrap_err();
        assert!(
            !format!("{err:?}").contains(SECRET),
            "Debug rendered the presented credential: {err:?}"
        );

        // And what a client sees, which is also what any proxy log of the
        // response would carry.
        let response = AuthError::InvalidKey.into_response();
        let (parts, body) = response.into_parts();
        assert_eq!(parts.status, StatusCode::UNAUTHORIZED);
        let rendered =
            String::from_utf8_lossy(&axum::body::to_bytes(body, 64 * 1024).await.unwrap())
                .into_owned();
        assert!(!rendered.contains(SECRET), "{rendered}");
        assert!(rendered.contains("Invalid API key"), "{rendered}");
    }

    #[test]
    fn test_auth_error_never_leaks_the_credential() {
        let response = AuthError::InvalidKey.into_response();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .unwrap(),
            "no-store"
        );
    }
}
