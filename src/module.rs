//! Module trait — the application logic interface.
//!
//! Every module type (trustline, etc.) implements Module.
//! The framework routes messages to the module by namespace (legacy)
//! or by SO type lookup (for SO-routed messages).
//!
//! SyncObjects (SOs) are pure data — ref-counted queues managed by
//! the framework. The Module owns all application logic.
//!
//! Account API (handle_account, status) is server-level routing,
//! not sync framework — handled separately.

use serde_json::Value;

/// The result of handling an incoming sync request.
pub struct HandleResult {
    /// Whether the request was accepted (ACK) or rejected (NACK).
    pub ok: bool,
}

/// Handler for a module.
///
/// Each module type implements this trait. The framework creates instances
/// and routes messages to them by namespace or SO type.
///
/// SyncObjects are data structures (queues + ref counts) managed by the
/// framework. The Module handles all logic — dispatch, state, cross-SO
/// effects, and account API.
pub trait Module: Send + Sync {
    fn on_connect(&mut self, peer: &str, payload: Option<Value>);
    fn on_close(&mut self, peer: &str, payload: Option<Value>);
    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>>;
    fn handle_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ok: bool,
        ts: i64,
    );
    fn handle_account(
        &mut self,
        _method: &str,
        _payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        Err("no account API".into())
    }
    fn status(&self) -> Option<Value> {
        None
    }
    fn force_remove(&mut self, peer: &str);
    fn reattach_sync_objects(&mut self, registry: &crate::registry::RegistryRef);
}
