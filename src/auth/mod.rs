//! Authentication module
//!
//! Local API-key authentication with server-side consumer identity derivation.

mod context;
mod middleware;

pub use context::{ConsumerContext, ConsumerIdentity, ManagerRole};
pub use middleware::{AuthError, Authenticated};
