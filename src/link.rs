//! Bilateral sync primitive.
//!
//! A Link holds the sync state between two identities. Each side has a
//! mirrored counter and a deterministic turn bit — both sides can send
//! simultaneously, but the turn bit resolves every collision
//! deterministically.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// Connection status / state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionStatus {
    Waiting,
    Incoming,
    Connected,
}

// ---------------------------------------------------------------------------
// ConnectMode — how a SyncObject connects
// ---------------------------------------------------------------------------

/// How a SyncObject type handles incoming connections.
/// - Auto: connected automatically when the first bilateral transaction succeeds.
/// - Invite: requires an explicit accept/reject handshake before connecting.
/// - Closed: does not accept incoming connections at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectMode {
    Auto,
    Invite,
    Closed,
}

impl Default for ConnectMode {
    fn default() -> Self {
        ConnectMode::Auto
    }
}

// ---------------------------------------------------------------------------
// ChildTracker — tracks child SyncObjects and their connected state
// ---------------------------------------------------------------------------

/// Tracks child SyncObjects registered under a ConnectionState.
/// Shared (via Arc) between the ConnectionState and its child SyncObjects.
/// Each child is identified by its queue_id and tracked as connected or not.
#[derive(Debug, Clone, Default)]
pub struct ChildTracker {
    inner: Arc<Mutex<ChildTrackerInner>>,
}

#[derive(Debug, Default)]
struct ChildTrackerInner {
    /// queue_ids of all registered children (connected or not).
    all: HashSet<u64>,
    /// queue_ids of connected children only.
    connected: HashSet<u64>,
}

impl ChildTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a child (starts as disconnected).
    pub fn register(&self, queue_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.all.insert(queue_id);
    }

    /// Unregister a child. Removes from both sets.
    pub fn unregister(&self, queue_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.all.remove(&queue_id);
        inner.connected.remove(&queue_id);
    }

    /// Mark a child as connected.
    pub fn set_connected(&self, queue_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        debug_assert!(inner.all.contains(&queue_id), "set_connected on unregistered child");
        inner.connected.insert(queue_id);
    }

    /// True if the given child is in the connected set.
    pub fn is_connected(&self, queue_id: u64) -> bool {
        self.inner.lock().unwrap().connected.contains(&queue_id)
    }

    /// Number of connected children.
    pub fn connected_count(&self) -> usize {
        self.inner.lock().unwrap().connected.len()
    }

    /// True if any child is connected.
    pub fn has_connected(&self) -> bool {
        self.connected_count() > 0
    }

    /// Total number of registered children (connected or not).
    pub fn total_count(&self) -> usize {
        self.inner.lock().unwrap().all.len()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConnectionState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ConnectionStatus>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub intent_connect: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub intent_close: bool,
    /// Tracks child SyncObjects — replaces the old atomic ref_count.
    /// Only connected children block link/module closure.
    #[serde(skip)]
    pub children: ChildTracker,
}

fn is_false(v: &bool) -> bool { !*v }

// ---------------------------------------------------------------------------
// SoInstance — a single SO instance on a link, identified by bilateral ID
// ---------------------------------------------------------------------------

/// A child SO at any lifecycle stage.
///
/// - `so_id: None` → pending, not yet sent (lives in parent's children list)
/// - `so_id: Some(id), status: None` → allocated but not bilateral yet
/// - `so_id: Some(id), status: Some(Waiting)` → sent, waiting for ACK
/// - `so_id: Some(id), status: Some(Connected)` → bilateral, active
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoChild {
    /// Bilateral ID. None = pending, not yet allocated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub so_id: Option<u64>,
    /// SO type name (e.g. "novation").
    pub so_type: String,
    /// Connection status. None = not yet bilateral.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ConnectionStatus>,
    /// Method to send as the creating message (e.g. "commit").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub method: String,
    /// Payload for the creating message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
}

/// A sync object instance on a link. Each has a bilateral ID (the link
/// counter at connect time), a type name, and an optional parent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoInstance {
    /// SO type name (e.g. "trustline", "novation").
    pub so_type: String,
    /// Parent SO's bilateral ID. None = root SO.
    pub parent_id: Option<u64>,
    /// Connection status.
    pub status: ConnectionStatus,
    /// Tracks child SO instances for lifecycle ref counting.
    #[serde(skip)]
    pub children: ChildTracker,
    /// Child SOs at all lifecycle stages (pending through connected).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub so_children: Vec<SoChild>,
}

/// Table of SO instances on a link, keyed by bilateral ID.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SoTable {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub instances: HashMap<u64, SoInstance>,
}

impl SoTable {
    pub fn new() -> Self {
        SoTable { instances: HashMap::new() }
    }

    /// Insert a new SO instance. Returns the old one if the ID was taken.
    pub fn insert(&mut self, so_id: u64, instance: SoInstance) -> Option<SoInstance> {
        self.instances.insert(so_id, instance)
    }

    /// Get an SO instance by bilateral ID.
    pub fn get(&self, so_id: u64) -> Option<&SoInstance> {
        self.instances.get(&so_id)
    }

    /// Get a mutable SO instance by bilateral ID.
    pub fn get_mut(&mut self, so_id: u64) -> Option<&mut SoInstance> {
        self.instances.get_mut(&so_id)
    }

    /// Remove an SO instance.
    pub fn remove(&mut self, so_id: u64) -> Option<SoInstance> {
        self.instances.remove(&so_id)
    }

    /// Find the root SO (parent_id = None).
    pub fn root(&self) -> Option<(u64, &SoInstance)> {
        self.instances.iter()
            .find(|(_, inst)| inst.parent_id.is_none())
            .map(|(&id, inst)| (id, inst))
    }

    /// All children of a given parent SO.
    pub fn children_of(&self, parent_id: u64) -> Vec<(u64, &SoInstance)> {
        self.instances.iter()
            .filter(|(_, inst)| inst.parent_id == Some(parent_id))
            .map(|(&id, inst)| (id, inst))
            .collect()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LinkState {
    #[serde(flatten)]
    pub conn: ConnectionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub modules: HashMap<String, ConnectionState>,
}

// ---------------------------------------------------------------------------
// Wire envelopes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxRequest {
    pub from: String,
    pub to: String,
    pub counter: u64,
    /// Target SO instance ID. 0 = lifecycle (link connect/close).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub so_id: u64,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    pub timestamp: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resp: Option<TxResponse>,
}

fn is_zero(v: &u64) -> bool { *v == 0 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxResponse {
    pub ok: bool,
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// Outbox entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEntry {
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    pub time: i64,
}

/// A message selected by nextTx for sending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedMsg {
    /// Target SO instance bilateral ID.
    #[serde(default)]
    pub so_id: u64,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(default)]
    pub queued_at: i64,
    /// Registry queue ID (for dequeue after response).
    #[serde(default)]
    pub queue_id: u64,
}

// ---------------------------------------------------------------------------
// Link
// ---------------------------------------------------------------------------

/// Maximum seconds a peer timestamp may be ahead of local clock.
pub const MAX_TIMESTAMP_FUTURE: i64 = 600;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub state: LinkState,
    /// SO instances on this link, keyed by bilateral ID.
    #[serde(default)]
    pub so_table: SoTable,
    pub counter: u64,
    pub turn_bit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<QueuedMsg>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_response: Option<TxResponse>,
    #[serde(default)]
    pub ts: i64,
}

#[derive(Debug, Clone)]
pub enum ValidateResult {
    /// Valid — proceed with dispatch.
    Ok,
    /// Stale counter but cached response exists — resend it.
    Cached(TxResponse),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SyncError {
    #[error("stale counter")]
    StaleCounter,
    #[error("counter ahead")]
    CounterAhead,
    #[error("collision")]
    Collision,
    #[error("timestamp out of range")]
    Timestamp,
    #[error("no link")]
    NoLink,
    #[error("max links")]
    MaxLinks,
    #[error("close with refs > 0")]
    RefsHeld,
    #[error("unknown method")]
    BadMethod,
    #[error("{0}")]
    Other(String),
}

impl Link {
    /// Create a new link with turn bit derived from identity ordering.
    pub fn new(self_id: &str, peer: &str) -> Self {
        let bit = if self_id > peer { 1 } else { 0 };
        Link {
            state: LinkState::default(),
            so_table: SoTable::new(),
            counter: 0,
            turn_bit: bit,
            pending: None,
            cached_response: None,
            ts: 0,
        }
    }

    /// True when this side owns the current counter slot.
    pub fn dominant(&self) -> bool {
        self.counter % 2 == self.turn_bit
    }

    /// True if the link is closing.
    pub fn closing(&self) -> bool {
        self.state.conn.intent_close
    }

    /// True if the named module is bilaterally connected and not closing.
    pub fn module_available(&self, name: &str) -> bool {
        if self.state.conn.intent_close {
            return false;
        }
        if self.state.root.as_deref() == Some(name) {
            return self.state.conn.status == Some(ConnectionStatus::Connected);
        }
        if let Some(c) = self.state.modules.get(name) {
            c.status == Some(ConnectionStatus::Connected) && !c.intent_close
        } else {
            false
        }
    }

    /// True if the module is bilaterally connected (ignoring IntentClose).
    pub fn module_connected(&self, name: &str) -> bool {
        if self.state.root.as_deref() == Some(name) {
            return self.state.conn.status == Some(ConnectionStatus::Connected);
        }
        if let Some(c) = self.state.modules.get(name) {
            c.status == Some(ConnectionStatus::Connected)
        } else {
            false
        }
    }

    /// Validate an incoming request at the given counter.
    pub fn validate_request(&self, counter: u64) -> Result<ValidateResult, SyncError> {
        if counter < self.counter {
            if let Some(ref cr) = self.cached_response {
                if counter == self.counter - 1 {
                    return Ok(ValidateResult::Cached(cr.clone()));
                }
            }
            return Err(SyncError::StaleCounter);
        }
        if counter > self.counter {
            return Err(SyncError::CounterAhead);
        }
        if self.pending.is_some() && self.dominant() {
            return Err(SyncError::Collision);
        }
        Ok(ValidateResult::Ok)
    }

    /// Validate that timestamp is within acceptable bounds.
    pub fn validate_timestamp(&self, ts: i64, now: i64) -> Result<(), SyncError> {
        if ts < self.ts {
            return Err(SyncError::Timestamp);
        }
        if ts > now + MAX_TIMESTAMP_FUTURE {
            return Err(SyncError::Timestamp);
        }
        Ok(())
    }

    /// Finalize an incoming request. Returns the response to send back.
    pub fn accept_request(&mut self, ok: bool, ts: i64) -> TxResponse {
        let resp = TxResponse { ok, timestamp: ts };
        self.cached_response = Some(resp.clone());
        self.counter += 1;
        self.ts = ts;
        self.pending = None;
        resp
    }

    /// True if an SO instance is connected.
    pub fn so_connected(&self, so_id: u64) -> bool {
        self.so_table.get(so_id)
            .map_or(false, |inst| inst.status == ConnectionStatus::Connected)
    }

    /// True if an SO instance is connected and the link is not closing.
    pub fn so_available(&self, so_id: u64) -> bool {
        if self.state.conn.intent_close {
            return false;
        }
        self.so_table.get(so_id)
            .map_or(false, |inst| inst.status == ConnectionStatus::Connected)
    }

    /// Finalize a received response to our outgoing message.
    pub fn accept_response(&mut self, ts: i64) {
        self.counter += 1;
        self.ts = ts;
        self.pending = None;
        self.cached_response = None;
    }
}

/// Current unix timestamp in seconds.
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
