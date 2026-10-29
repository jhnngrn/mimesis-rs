//! Module trait — the application interface.
//!
//! Modules implement sync methods and account API calls. The framework
//! dispatches by namespace: `trustline.payment` → module "trustline",
//! method "payment".
//!
//! Outbox management (collect_outbox, dequeue, outbox_has) is handled
//! entirely by the SyncRegistry — modules don't touch it.

use serde_json::Value;

/// The result of handling an incoming sync request.
pub struct HandleResult {
    /// Whether the request was accepted (ACK) or rejected (NACK).
    pub ok: bool,
}

/// Module processes sync methods and account API calls for a namespace.
///
/// In Rust-mimesis, modules use `SyncObject<T>` for their per-peer state.
/// SyncObjects register queues in the framework's SyncRegistry — the
/// framework collects, dequeues, and kicks automatically. Modules just
/// call `sync_object.send()` inside handlers.
pub trait Module: Send + Sync {
    /// Called when the module becomes bilaterally connected on a link.
    fn on_connect(&mut self, peer: &str);

    /// Called when the module is disconnected from a link.
    fn on_close(&mut self, peer: &str);

    /// Handle an incoming sync request.
    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>>;

    /// Handle a response to our outgoing sync request.
    fn handle_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ok: bool,
        ts: i64,
    );

    /// Handle an account API call (local, not sync).
    fn handle_account(
        &mut self,
        method: &str,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>>;

    /// Return module status for the account API.
    fn status(&self) -> Option<Value>;

    /// Unilateral cleanup — called when a link is force-removed.
    fn force_remove(&mut self, peer: &str);

    /// Reconnect SyncObjects to the registry after deserialization.
    /// The framework calls this after loading persisted state.
    fn reattach_sync_objects(&mut self, registry: &crate::registry::RegistryRef);
}
