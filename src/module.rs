//! Module trait — the application logic interface.
//!
//! Every module type (trustline, triad, etc.) implements Module.
//! The framework routes messages to the module by namespace (legacy)
//! or by SO type lookup (for SO-routed messages).
//!
//! SyncObjects (SOs) are pure data — ref-counted queues managed by
//! the framework. The Module owns all application logic.
//!
//! ## Connect/close lifecycle
//!
//! Connect and close are regular req/resp messages. The module declares
//! which method triggers connect (`connects_on`, default "connect") and
//! which methods trigger close (`closes_on`, default ["close"]).
//!
//! Flow:
//! 1. Framework receives ACK/NACK for the registered connect method
//! 2. Module's handle_request/handle_response runs (custom init/cleanup)
//! 3. Framework runs common state logic (Connected/Waiting, fallback SO)
//!
//! If no custom handler exists, the framework default handles it.
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
    /// Which method triggers connect for this module.
    /// Default: "connect". Triad overrides to "commit".
    /// The framework runs common state logic (set Connected/Waiting,
    /// create fallback SO) AFTER the module's handle_request/handle_response.
    fn connects_on(&self) -> &str {
        "connect"
    }

    /// Which methods trigger close for this module.
    /// Default: ["close"]. Triad overrides to ["finalize", "cleanup", "cancel"].
    /// The framework runs common state logic AFTER the module's handler.
    fn closes_on(&self) -> Vec<&str> {
        vec!["close"]
    }

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
