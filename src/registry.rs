//! SyncRegistry — framework-owned outbox queues and kick tracking.
//!
//! The registry replaces per-SyncObject queues and per-module collect_outbox.
//! Each SyncObject registers a queue here; the framework collects from the
//! registry during nextTx, dequeues on response, and drains kicks after
//! each handler invocation.
//!
//! Queue IDs are internal u64 counters, persisted alongside the queue data
//! so that pending messages survive restarts.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::link::{ConnectMode, OutboxEntry};

/// Shared reference to the registry. Held by Node and by each SyncObject.
pub type RegistryRef = Arc<Mutex<SyncRegistry>>;

/// A single queue in the registry — 1:1 with a SyncObject and its peer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncQueue {
    /// Which peer this queue sends to.
    pub peer: String,
    /// Which module owns this queue (legacy).
    pub module: String,
    /// Bilateral SO instance ID (new routing). 0 = legacy/unset.
    #[serde(default)]
    pub so_id: u64,
    /// How this SO connects (Auto or Invite).
    #[serde(default)]
    pub connect_mode: ConnectMode,
    /// Pending outbox messages.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<OutboxEntry>,
}

/// Framework-owned registry of all SyncObject queues.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncRegistry {
    /// All registered queues, keyed by internal ID.
    queues: HashMap<u64, SyncQueue>,
    /// Next ID to assign.
    next_id: u64,
    /// Peers to kick after the current handler returns.
    #[serde(skip)]
    kicks: HashSet<String>,
}

impl SyncRegistry {
    pub fn new() -> Self {
        SyncRegistry {
            queues: HashMap::new(),
            next_id: 1,
            kicks: HashSet::new(),
        }
    }

    /// Register a new queue for a module bound to a peer. Returns the queue ID.
    /// Ref counting is managed by ConnectionState, not the registry.
    pub fn register(&mut self, peer: &str, module: &str, mode: ConnectMode) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.queues.insert(
            id,
            SyncQueue {
                peer: peer.to_string(),
                module: module.to_string(),
                so_id: 0,
                connect_mode: mode,
                entries: Vec::new(),
            },
        );
        id
    }

    /// Register a new queue for an SO instance. Returns the queue ID.
    pub fn register_so(&mut self, peer: &str, so_id: u64, so_type: &str, mode: ConnectMode) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.queues.insert(
            id,
            SyncQueue {
                peer: peer.to_string(),
                module: so_type.to_string(),
                so_id,
                connect_mode: mode,
                entries: Vec::new(),
            },
        );
        id
    }

    /// Re-register a queue with a known ID (for deserialization).
    /// The queue data should already be in the registry from persisted state.
    /// Ref counting is managed by ConnectionState via the SyncObject's reattach.
    pub fn reattach(&mut self, id: u64, peer: &str, module: &str, mode: ConnectMode) {
        // Ensure the queue exists (it should from deserialized state).
        self.queues.entry(id).or_insert_with(|| SyncQueue {
            peer: peer.to_string(),
            module: module.to_string(),
            so_id: 0,
            connect_mode: mode,
            entries: Vec::new(),
        });
        // Keep next_id above any reattached ID.
        if id >= self.next_id {
            self.next_id = id + 1;
        }
    }

    /// Unregister a queue. Removes queue data.
    /// Ref count decrement happens in SyncObject::Drop via the Arc<AtomicUsize>.
    pub fn unregister(&mut self, id: u64) {
        self.queues.remove(&id);
    }

    /// Look up a queue by ID (for transport-level decisions).
    pub fn get_queue(&self, id: u64) -> Option<&SyncQueue> {
        self.queues.get(&id)
    }

    /// Enqueue a message and mark the peer for kicking.
    pub fn enqueue(
        &mut self,
        id: u64,
        method: &str,
        payload: Option<serde_json::Value>,
    ) {
        if let Some(sq) = self.queues.get_mut(&id) {
            let outbox_entry = OutboxEntry {
                method: method.to_string(),
                payload,
                time: crate::link::now_secs(),
            };
            self.kicks.insert(sq.peer.clone());
            sq.entries.push(outbox_entry);
        }
    }

    /// Remove all queued entries for this queue.
    pub fn delete_queue(&mut self, id: u64) {
        self.queues.remove(&id);
    }

    /// Dequeue the head entry from this queue.
    pub fn dequeue(&mut self, id: u64) {
        if let Some(sq) = self.queues.get_mut(&id) {
            if !sq.entries.is_empty() {
                sq.entries.remove(0);
            }
            if sq.entries.is_empty() {
                self.queues.remove(&id);
            }
        }
    }

    /// Drain the kicks set — returns peers that need send_pending.
    pub fn drain_kicks(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.kicks)
    }

    /// True if a specific queue has pending entries.
    pub fn queue_has_pending(&self, id: u64) -> bool {
        self.queues.get(&id).map_or(false, |sq| !sq.entries.is_empty())
    }

    /// True if any queue for this module has a pending entry for the peer.
    pub fn has_pending(&self, module: &str, peer: &str) -> bool {
        self.queues.values().any(|sq| {
            sq.module == module && sq.peer == peer && !sq.entries.is_empty()
        })
    }

    /// Collect the oldest outbox entry per queue for a peer across a module's queues.
    /// Returns (queue_id, method, payload, queued_at) for the head entry of each queue.
    pub fn collect_outbox(&self, module: &str, peer: &str) -> Vec<OutboxCandidate> {
        let mut candidates = Vec::new();
        for (&id, sq) in &self.queues {
            if sq.module != module || sq.peer != peer {
                continue;
            }
            if let Some(head) = sq.entries.first() {
                candidates.push(OutboxCandidate {
                    queue_id: id,
                    method: head.method.clone(),
                    payload: head.payload.clone(),
                    queued_at: head.time,
                });
            }
        }
        candidates
    }

    /// Collect the oldest outbox entry per queue for a peer's SO instance.
    pub fn collect_outbox_by_so(&self, so_id: u64, peer: &str) -> Vec<OutboxCandidate> {
        let mut candidates = Vec::new();
        for (&id, sq) in &self.queues {
            if sq.so_id != so_id || sq.peer != peer {
                continue;
            }
            if let Some(head) = sq.entries.first() {
                candidates.push(OutboxCandidate {
                    queue_id: id,
                    method: head.method.clone(),
                    payload: head.payload.clone(),
                    queued_at: head.time,
                });
            }
        }
        candidates
    }

    /// True if any queue (any module) has pending entries for this peer.
    pub fn any_pending_for_peer(&self, peer: &str) -> bool {
        self.queues.values().any(|sq| {
            sq.peer == peer && !sq.entries.is_empty()
        })
    }
}

/// A candidate message from the registry, for nextTx selection.
pub struct OutboxCandidate {
    pub queue_id: u64,
    pub method: String,
    pub payload: Option<serde_json::Value>,
    pub queued_at: i64,
}

/// Create a new shared registry.
pub fn new_registry() -> RegistryRef {
    Arc::new(Mutex::new(SyncRegistry::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_enqueue() {
        let mut reg = SyncRegistry::new();
        let id = reg.register("alice", "triad", ConnectMode::Auto);
        reg.enqueue(id, "payment", None);

        assert!(reg.has_pending("triad", "alice"));
        assert!(!reg.has_pending("triad", "bob"));
    }

    #[test]
    fn dequeue_removes_head() {
        let mut reg = SyncRegistry::new();
        let id = reg.register("alice", "triad", ConnectMode::Auto);
        reg.enqueue(id, "a", None);
        reg.enqueue(id, "b", None);

        let candidates = reg.collect_outbox("triad", "alice");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].method, "a");

        reg.dequeue(id);
        let candidates = reg.collect_outbox("triad", "alice");
        assert_eq!(candidates[0].method, "b");

        reg.dequeue(id);
        assert!(!reg.has_pending("triad", "alice"));
    }

    #[test]
    fn kicks_tracked_on_enqueue() {
        let mut reg = SyncRegistry::new();
        let id_a = reg.register("alice", "triad", ConnectMode::Auto);
        let id_b = reg.register("bob", "triad", ConnectMode::Auto);
        reg.enqueue(id_a, "x", None);
        reg.enqueue(id_b, "y", None);

        let kicks = reg.drain_kicks();
        assert!(kicks.contains("alice"));
        assert!(kicks.contains("bob"));

        // Drained — next drain is empty.
        assert!(reg.drain_kicks().is_empty());
    }

    #[test]
    fn multiple_queues_same_peer() {
        let mut reg = SyncRegistry::new();
        let id1 = reg.register("alice", "triad", ConnectMode::Auto);
        let id2 = reg.register("alice", "triad", ConnectMode::Auto);

        reg.enqueue(id1, "pay1", None);
        reg.enqueue(id2, "pay2", None);

        let candidates = reg.collect_outbox("triad", "alice");
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn reattach_bumps_next_id() {
        let mut reg = SyncRegistry::new();
        reg.reattach(42, "alice", "triad", ConnectMode::Auto);
        assert!(reg.next_id > 42);
    }
}
