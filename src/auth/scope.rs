//! What an authenticated credential may see, and the one rendering of it.
//!
//! # Why this is not in the dashboard
//!
//! It used to be. The scope, the resolver and the SQL fragment lived beside the
//! dashboard's usage queries, because the dashboard was the only surface that
//! read more than one consumer's rows. Billing added two more readers — the
//! partner's own statements and the manager's statement list — and the choice was
//! between a second copy of the fragment and moving the definition somewhere both
//! could reach.
//!
//! A second copy is the one thing invariant 7 cannot survive. The invariant is
//! that identity comes from the credential and never from the request, and the
//! way it fails is not a missing `WHERE` that someone notices — it is a *correct*
//! `WHERE` in one query and a subtly wider one in another. So the definition
//! lives here, in the module that already owns "who is this credential", and
//! every scoped query renders its filter through [`scope_clause`].
//!
//! # Narrowing, never widening
//!
//! A consumer key's scope is its own consumer and the request cannot change it:
//! the `consumers` parameter is not consulted at all for a key context, so
//! `?consumers=someone-else` is inert. A manager's scope is every consumer, and
//! the same parameter can only *shrink* that — and it is read verbatim, so a name
//! that does not exist matches no rows rather than falling back to everything
//! (ADR 0013). There is no value of any parameter that widens what a credential
//! already has.

use rusqlite::types::Value;

use crate::auth::ConsumerContext;

/// The consumers one query may read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// A single consumer; `consumer_id = ?`.
    One(String),
    /// The consumers the manager asked to see, verbatim.
    List(Vec<String>),
    /// Every consumer.
    All,
}

/// Resolve the effective scope for an authenticated context and the raw
/// `consumers` parameter.
///
/// Takes the parameter as a string rather than a parsed query type so that every
/// surface that has one — the ledger views, the statement views, and whatever
/// comes next — resolves it the same way instead of each carrying its own copy of
/// the splitting and trimming rules.
pub fn resolve_scope(consumer: &ConsumerContext, consumers: &str) -> Scope {
    // A key context never consults the parameter: `consumers=acme` cannot widen a
    // key into another consumer's data.
    if !consumer.is_manager() {
        return Scope::One(consumer.consumer_id().to_string());
    }
    let requested: Vec<String> = consumers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if requested.is_empty() {
        Scope::All
    } else {
        // Names are taken verbatim, so an unknown name is an empty result at
        // query time — never a fall-back to everything.
        Scope::List(requested)
    }
}

/// Render a [`Scope`] into a `(WHERE fragment, params)` pair.
///
/// `One` yields `consumer_id = ?`; `List` yields `consumer_id IN (?,?,…)`; `All`
/// yields `1 = 1` — a constant the planner folds away, kept explicit rather than
/// omitting the `WHERE` entirely so the callers that always append `" AND …"`
/// after the fragment never produce a dangling `AND`. An empty `List` yields
/// `consumer_id IN ()` which SQLite evaluates to `FALSE` — the correct rendering
/// of a filter nothing matches.
///
/// The fragment names the column unqualified. Every caller queries a table that
/// has exactly one `consumer_id` and no join that would introduce a second, so
/// there is nothing for SQLite to resolve ambiguously; a query that did need
/// qualification would have to qualify it here, in one place, rather than in each
/// caller's own copy.
pub fn scope_clause(scope: &Scope) -> (String, Vec<Value>) {
    match scope {
        Scope::One(id) => ("consumer_id = ?".to_string(), vec![Value::Text(id.clone())]),
        Scope::List(list) => {
            let placeholders = vec!["?"; list.len()].join(",");
            let params = list.iter().map(|c| Value::Text(c.clone())).collect();
            (format!("consumer_id IN ({placeholders})"), params)
        }
        Scope::All => ("1 = 1".to_string(), Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::ConsumerContext;

    /// A consumer key context pinned to `acme`.
    fn consumer_ctx() -> ConsumerContext {
        use crate::billing::partner::BillingMode;
        use crate::billing::partner::PartnerRuntimeConfig;
        use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
        use crate::billing::status::ServiceStatus;
        use std::collections::BTreeMap;

        let models = [("gpt-4o", 95_000)]
            .iter()
            .map(|(name, price)| {
                (
                    name.to_string(),
                    PricingSnapshot::new(
                        PricePerMillion::new(*price),
                        PricePerMillion::new(*price),
                        PricePerMillion::new(*price),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();
        ConsumerContext::new(std::sync::Arc::new(PartnerRuntimeConfig::new(
            "acme".to_string(),
            "acme-key".to_string(),
            BillingMode::Invoice,
            "billing@acme.test".to_string(),
            ServiceStatus::Active,
            models,
        )))
    }

    fn manager_ctx() -> ConsumerContext {
        ConsumerContext::manager()
    }

    #[test]
    fn test_a_key_context_is_its_own_consumer_whatever_the_parameter_asks_for() {
        // The parameter is a manager's view filter. For a key it is not read at
        // all, which is what makes it inert rather than narrow.
        assert_eq!(
            resolve_scope(&consumer_ctx(), "competitor"),
            Scope::One("acme".to_string())
        );
        assert_eq!(
            resolve_scope(&consumer_ctx(), ""),
            Scope::One("acme".to_string())
        );
        assert_eq!(
            resolve_scope(&consumer_ctx(), "acme,competitor"),
            Scope::One("acme".to_string())
        );
    }

    #[test]
    fn test_a_manager_with_no_parameter_sees_everything() {
        assert_eq!(resolve_scope(&manager_ctx(), ""), Scope::All);
        // Whitespace and empty entries are not names.
        assert_eq!(resolve_scope(&manager_ctx(), " , ,"), Scope::All);
    }

    #[test]
    fn test_a_manager_may_narrow_to_the_consumers_it_names() {
        assert_eq!(
            resolve_scope(&manager_ctx(), "b,d"),
            Scope::List(vec!["b".to_string(), "d".to_string()])
        );
        assert_eq!(
            resolve_scope(&manager_ctx(), " a ,,b "),
            Scope::List(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn test_a_name_that_does_not_exist_is_an_empty_view_not_everything() {
        // The failure that matters: a manager asking for a consumer that has no
        // rows must see nothing, not everything.
        assert_eq!(
            resolve_scope(&manager_ctx(), "ghost"),
            Scope::List(vec!["ghost".to_string()])
        );
    }

    #[test]
    fn test_scope_clause_renders_sql_fragments() {
        let (one_sql, one_params) = scope_clause(&Scope::One("acme".into()));
        assert_eq!(one_sql, "consumer_id = ?");
        assert_eq!(one_params, vec![Value::Text("acme".into())]);

        let (list_sql, list_params) = scope_clause(&Scope::List(vec!["a".into(), "b".into()]));
        assert_eq!(list_sql, "consumer_id IN (?,?)");
        assert_eq!(
            list_params,
            vec![Value::Text("a".into()), Value::Text("b".into())]
        );

        let (all_sql, all_params) = scope_clause(&Scope::All);
        assert_eq!(all_sql, "1 = 1");
        assert!(all_params.is_empty());

        // An empty list is the filter that names nobody: `IN ()` is `FALSE` in
        // SQLite, and it must stay that way.
        let (empty_sql, empty_params) = scope_clause(&Scope::List(Vec::new()));
        assert_eq!(empty_sql, "consumer_id IN ()");
        assert!(empty_params.is_empty());
    }
}
