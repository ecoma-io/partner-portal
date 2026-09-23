//! Authentication middleware

use axum::{
    extract::FromRequestParts,
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::auth::ConsumerContext;
use crate::config::ConfigSnapshot;

/// Authentication error
#[derive(Debug)]
pub enum AuthError {
    MissingAuthorization,
    InvalidFormat,
    InvalidKey,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let message = match self {
            AuthError::MissingAuthorization => "Missing Authorization header",
            AuthError::InvalidFormat => "Invalid Authorization header format",
            AuthError::InvalidKey => "Invalid API key",
        };

        let body = serde_json::json!({
            "error": {
                "message": message,
                "type": "invalid_request_error",
            }
        });

        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(&body).unwrap(),
        )
            .into_response()
    }
}

/// Extract Bearer token from request parts
fn extract_bearer_token(parts: &Parts) -> Result<String, AuthError> {
    let auth_header = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(AuthError::MissingAuthorization)?;

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(AuthError::InvalidFormat)?;

    Ok(token.to_string())
}

/// Extractor that resolves a `ConsumerContext` from the request's
/// Authorization header using the live config snapshot.
///
/// Derives consumer identity **server-side** — never from client-provided
/// fields like `consumer_id`.
pub struct Authenticated(pub ConsumerContext);

impl<S> FromRequestParts<S> for Authenticated
where
    S: Send + Sync,
{
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let config = parts
            .extensions
            .get::<Arc<parking_lot::RwLock<ConfigSnapshot>>>()
            .ok_or(AuthError::InvalidKey)?;

        let token = extract_bearer_token(parts)?;
        let snapshot = config.read();

        let key_config = snapshot
            .config
            .find_key(&token)
            .ok_or(AuthError::InvalidKey)?;

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
        let token = extract_bearer_token(&parts).unwrap();
        assert_eq!(token, "my-secret-key");
    }

    #[test]
    fn test_extract_bearer_token_missing() {
        let parts = parts_with_auth(None);
        let result = extract_bearer_token(&parts);
        assert!(matches!(result, Err(AuthError::MissingAuthorization)));
    }

    #[test]
    fn test_extract_bearer_token_invalid_format() {
        let parts = parts_with_auth(Some("Basic dXNlcjpwYXNz"));
        let result = extract_bearer_token(&parts);
        assert!(matches!(result, Err(AuthError::InvalidFormat)));
    }
}
