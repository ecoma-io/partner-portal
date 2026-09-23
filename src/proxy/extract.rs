//! Streaming body extraction helpers

use bytes::Bytes;

/// Errors from bounded collection
#[derive(Debug, thiserror::Error)]
pub enum CollectError {
    #[error("Body too large")]
    TooLarge,

    #[error("Body error: {0}")]
    Body(#[from] hyper::Error),
}

/// Convert an incoming body into a full collection of bytes (bounded)
pub async fn collect_bounded(
    body: hyper::body::Incoming,
    max_size: usize,
) -> Result<Bytes, CollectError> {
    use http_body_util::BodyExt;

    let mut collected = Vec::new();
    let mut stream = body.into_data_stream();

    use futures::StreamExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if collected.len() + chunk.len() > max_size {
            return Err(CollectError::TooLarge);
        }
        collected.extend_from_slice(&chunk);
    }

    Ok(Bytes::from(collected))
}
