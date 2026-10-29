//! Module trait — the application interface.
//!
//! Modules implement sync methods and account API calls. The framework
//! dispatches by namespace: `trustline.payment` → module "trustline",
//! method "payment".

use serde_json::Value;

/// The result of handling an incoming sync request.
pub struct HandleResult {
    /// Whether the request was accepted (ACK) or rejected (NACK).
    pub ok: bool,
}

/// Module processes sync methods and account API calls for a namespace.
///
/// In Rust-mimesis, modules use `SyncObject<T>` for their per-peer state
/// and outbox queues. The framework collects queues from registered
/// SyncObjects during `next_tx` — modules don't manage delivery.
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

    /// Collect outbox entries for a peer. The framework calls this during
    /// nextTx to gather pending messages from all modules.
    /// Returns (method, payload, queued_at, queue_id) tuples.
    fn collect_outbox(&self, peer: &str) -> Vec<OutboxCandidate>;

    /// Dequeue the head entry for a peer/queue_id after response received.
    fn dequeue(&mut self, peer: &str, queue_id: &str);

    /// True if the outbox has any entry for this peer.
    fn outbox_has(&self, peer: &str) -> bool;
}

/// A candidate message from a module's outbox, for nextTx selection.
pub struct OutboxCandidate {
    pub method: String,
    pub payload: Option<Value>,
    pub queued_at: i64,
    pub queue_id: String,
}
