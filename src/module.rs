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
//! Connect and close are handled by the framework via fixed `link.connect`
//! and `link.close` methods. The module receives lifecycle callbacks:
//!
//! - `on_connect(peer, so)` — called when a module becomes bilaterally
//!   connected. Notification only — the module cannot reject.
//!   Modules with `has_so` receive a framework-managed SyncObject;
//!   others receive a zero-value (no-op send/has_pending).
//! - `on_close(peer)` — called when a module is disconnected, just
//!   before cleanup. The module should clean up per-peer state.
//!
//! ## Tristate responses
//!
//! `handle_request` returns `i8`: 1 = ack, 0 = nack (app-level),
//! error return = framework error (result -1, counter still advances).
//!
//! Account API (handle_account, status) is server-level routing,
//! not sync framework — handled separately.

use serde_json::Value;

use crate::sync_object::SyncObject;

/// Handler for a module.
///
/// Each module type implements this trait. The framework creates instances
/// and routes messages to them by namespace or SO type.
///
/// SyncObjects are data structures (queues + ref counts) managed by the
/// framework. The Module handles all logic — dispatch, state, cross-SO
/// effects, and account API.
pub trait Module: Send + Sync {
    /// Called when a module becomes bilaterally connected — on
    /// `link.connect` for the root module, on `module.connect` for
    /// child modules. Notification only (cannot reject).
    ///
    /// Modules registered with `has_so` receive a framework-managed
    /// SyncObject with send/has_pending wired. Others receive a
    /// zero-value SyncObject (no registry — send/has_pending are no-ops).
    /// Modules use this to create per-peer state and embed the SO.
    fn on_connect(&mut self, peer: &str, so: SyncObject<()>) {
        let _ = so; // default: ignore
    }

    /// Called when a module is disconnected — on `module.close` for
    /// child modules, on `link.close` for all connected modules
    /// (children first, then root), just before cleanup.
    /// Modules use this to clean up per-peer state.
    fn on_close(&mut self, peer: &str) {}

    /// Handle an incoming sync request. Returns tristate i8:
    /// 1 = ack (accept), 0 = nack (reject, app-level).
    /// Error return = framework error (counter advances, result -1).
    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<i8, Box<dyn std::error::Error>>;

    /// Handle a response to a previously sent request.
    /// `result` is the peer's tristate: 1 = ack, 0 = nack, -1 = error.
    fn handle_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        result: i8,
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
