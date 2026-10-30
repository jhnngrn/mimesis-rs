//! SoHandler trait — the sync object dispatch interface.
//!
//! Every SO type (trustline, triad, novation, ...) implements SoHandler.
//! The framework dispatches messages directly to SO instances by their
//! bilateral ID, not by namespace. Each SO instance is an independent
//! message-receiving object.
//!
//! SoContext is the handle passed into handler methods — it lets the
//! handler enqueue messages, create child SOs, and close itself.
//!
//! Account API (handle_account, status) is server-level routing,
//! not sync framework — handled separately.

use serde_json::Value;

use crate::link::ConnectMode;
use crate::registry::RegistryRef;

/// The result of handling an incoming sync request.
pub struct HandleResult {
    /// Whether the request was accepted (ACK) or rejected (NACK).
    pub ok: bool,
}

// ---------------------------------------------------------------------------
// SoContext — the handle a handler uses to interact with the framework
// ---------------------------------------------------------------------------

/// Intent recorded by a handler during a dispatch call.
/// The framework drains these after the handler returns.
#[derive(Debug, Clone)]
pub enum SoIntent {
    /// Create a child SO instance.
    ConnectChild {
        so_type: String,
        payload: Option<Value>,
    },
    /// Close this SO instance.
    Close {
        payload: Option<Value>,
    },
}

/// Context passed into SoHandler methods.
///
/// Provides the handler with:
/// - enqueue: send a message to the peer via this SO's queue
/// - connect_child: request a child SO to be created
/// - close: request this SO to be closed
///
/// Intents (connect_child, close) are collected and processed by the
/// framework after the handler method returns.
pub struct SoContext {
    /// This SO's bilateral instance ID.
    pub so_id: u64,
    /// This SO's registry queue ID (for enqueue).
    pub queue_id: u64,
    /// Peer identity.
    pub peer: String,
    /// Shared registry reference.
    registry: RegistryRef,
    /// Intents accumulated during this handler call.
    intents: Vec<SoIntent>,
}

impl SoContext {
    /// Create a new context for a handler call.
    pub fn new(so_id: u64, queue_id: u64, peer: &str, registry: RegistryRef) -> Self {
        SoContext {
            so_id,
            queue_id,
            peer: peer.to_string(),
            registry,
            intents: Vec::new(),
        }
    }

    /// Enqueue a message to the peer on this SO's queue.
    pub fn enqueue(&self, method: &str, payload: Option<Value>) {
        self.registry
            .lock()
            .unwrap()
            .enqueue(self.queue_id, method, payload);
    }

    /// Request a child SO to be created under this one.
    /// The framework allocates the so_id and sends the connect message.
    pub fn connect_child(&mut self, so_type: &str, payload: Option<Value>) {
        self.intents.push(SoIntent::ConnectChild {
            so_type: so_type.to_string(),
            payload,
        });
    }

    /// Request this SO to be closed.
    /// The framework sends the close message and calls on_close when acked.
    pub fn close(&mut self, payload: Option<Value>) {
        self.intents.push(SoIntent::Close { payload });
    }

    /// Drain accumulated intents (called by the framework after handler returns).
    pub fn drain_intents(&mut self) -> Vec<SoIntent> {
        std::mem::take(&mut self.intents)
    }
}

/// Handler for a sync object instance.
///
/// Each SO type implements this trait. The framework creates instances
/// on connect and routes messages to them by so_id. on_connect is the
/// constructor (called with the connect payload), on_close is the
/// destructor.
///
/// Methods that can interact with the framework receive a &mut SoContext.
pub trait SoHandler: Send + Sync {
    /// Constructor — called when this SO instance is created via connect.
    /// Payload carries the constructor parameters (e.g. amount for novation).
    fn on_connect(&mut self, ctx: &mut SoContext, peer: &str, payload: Option<Value>);

    /// Destructor — called when this SO instance is closed.
    /// Payload carries the close parameters (e.g. finalize reason).
    fn on_close(&mut self, peer: &str, payload: Option<Value>);

    /// Handle an incoming sync request routed to this SO instance.
    fn handle_request(
        &mut self,
        ctx: &mut SoContext,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>>;

    /// Handle a response to our outgoing sync request.
    fn handle_response(
        &mut self,
        ctx: &mut SoContext,
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
