//! Authentication module
//!
//! Local API-key authentication with server-side consumer identity derivation.

mod context;
mod middleware;
mod scope;

pub use context::{ConsumerContext, ConsumerIdentity, ManagerRole};
pub use middleware::{AuthError, Authenticated};
pub use scope::{Scope, resolve_scope, scope_clause};
