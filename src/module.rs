//! Module trait — the sync dispatch interface.
//!
//! A Module is a container that owns SyncObjects for a namespace and
//! routes sync messages to the right peer's SO. The framework dispatches
//! by namespace: `trustline.payment` → module "trustline", method "payment".
//!
//! Account API (handle_account, status) is NOT part of this trait —
//! it's server-level routing, not sync framework.

use serde_json::Value;

/// The result of handling an incoming sync request.
pub struct HandleResult {
    /// Whether the request was accepted (ACK) or rejected (NACK).
    pub ok: bool,
}

/// Module owns SyncObjects for a namespace and routes sync messages.
///
/// Modules use `SyncObject<T>` for their per-peer state. The framework
/// collects, dequeues, and kicks automatically via the SyncRegistry.
pub trait Module: Send + Sync {
    /// Called when the module becomes bilaterally connected on a link.
    /// Payload comes from the connects_on method (e.g. commit payload).
    fn on_connect(&mut self, peer: &str, payload: Option<Value>);

    /// Called when the module is disconnected from a link.
    /// Payload comes from the closes_on method (e.g. finalize payload).
    fn on_close(&mut self, peer: &str, payload: Option<Value>);

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
    /// Default returns error — override in modules that expose account API.
    /// This is server-level routing, not sync framework, and will move out.
    fn handle_account(
        &mut self,
        _method: &str,
        _payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        Err("no account API".into())
    }

    /// Return module status for the account API.
    /// Default returns None — override in modules that expose status.
    fn status(&self) -> Option<Value> {
        None
    }

    /// Unilateral cleanup — called when a link is force-removed.
    fn force_remove(&mut self, peer: &str);

    /// Reconnect SyncObjects to the registry after deserialization.
    /// The framework calls this after loading persisted state.
    fn reattach_sync_objects(&mut self, registry: &crate::registry::RegistryRef);
}
