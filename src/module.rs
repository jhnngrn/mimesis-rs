//! SoHandler trait — the sync object dispatch interface.
//!
//! Every SO type (trustline, triad, novation, ...) implements SoHandler.
//! The framework dispatches messages directly to SO instances by their
//! bilateral ID, not by namespace. Each SO instance is an independent
//! message-receiving object.
//!
//! Account API (handle_account, status) is server-level routing,
//! not sync framework — handled separately.

use serde_json::Value;

/// The result of handling an incoming sync request.
pub struct HandleResult {
    /// Whether the request was accepted (ACK) or rejected (NACK).
    pub ok: bool,
}

/// Handler for a sync object instance.
///
/// Each SO type implements this trait. The framework creates instances
/// on connect and routes messages to them by so_id. on_connect is the
/// constructor (called with the connect payload), on_close is the
/// destructor.
pub trait SoHandler: Send + Sync {
    /// Constructor — called when this SO instance is created via connect.
    /// Payload carries the constructor parameters (e.g. amount for novation).
    fn on_connect(&mut self, peer: &str, payload: Option<Value>);

    /// Destructor — called when this SO instance is closed.
    /// Payload carries the close parameters (e.g. finalize reason).
    fn on_close(&mut self, peer: &str, payload: Option<Value>);

    /// Handle an incoming sync request routed to this SO instance.
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

    /// Unilateral cleanup — called when a link is force-removed.
    fn force_remove(&mut self, peer: &str);
}

// Keep Module as a type alias during migration so existing code compiles.
// TODO: remove once all call sites are migrated.
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
