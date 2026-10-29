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

use crate::link::OutboxEntry;

/// Shared reference to the registry. Held by Node and by each SyncObject.
pub type RegistryRef = Arc<Mutex<SyncRegistry>>;

/// A single queue entry in the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    /// Which module owns this queue.
    pub module: String,
    /// Per-peer outbox: peer → pending messages.
    pub entries: HashMap<String, Vec<OutboxEntry>>,
}

/// Framework-owned registry of all SyncObject queues.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncRegistry {
    /// All registered queues, keyed by internal ID.
    queues: HashMap<u64, QueueEntry>,
    /// Next ID to assign.
    next_id: u64,
    /// Peers to kick after the current handler returns.
    #[serde(skip)]
    kicks: HashSet<String>,
    /// Ref counts per module (number of live SyncObjects).
    #[serde(skip)]
    ref_counts: HashMap<String, usize>,
}

impl SyncRegistry {
    pub fn new() -> Self {
        SyncRegistry {
            queues: HashMap::new(),
            next_id: 1,
            kicks: HashSet::new(),
            ref_counts: HashMap::new(),
        }
    }

    /// Register a new queue for a module. Returns the queue ID.
    pub fn register(&mut self, module: &str) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.queues.insert(
            id,
            QueueEntry {
                module: module.to_string(),
                entries: HashMap::new(),
            },
        );
        *self.ref_counts.entry(module.to_string()).or_insert(0) += 1;
        id
    }

    /// Re-register a queue with a known ID (for deserialization).
    /// The queue data should already be in the registry from persisted state;
    /// this just bumps the ref count.
    pub fn reattach(&mut self, id: u64, module: &str) {
        // Ensure the queue exists (it should from deserialized state).
        self.queues.entry(id).or_insert_with(|| QueueEntry {
            module: module.to_string(),
            entries: HashMap::new(),
        });
        *self.ref_counts.entry(module.to_string()).or_insert(0) += 1;
        // Keep next_id above any reattached ID.
        if id >= self.next_id {
            self.next_id = id + 1;
        }
    }

    /// Unregister a queue. Removes queue data and decrements ref count.
    pub fn unregister(&mut self, id: u64) {
        if let Some(entry) = self.queues.remove(&id) {
            if let Some(count) = self.ref_counts.get_mut(&entry.module) {
                *count = count.saturating_sub(1);
            }
        }
    }

    /// Enqueue a message and mark the peer for kicking.
    pub fn enqueue(
        &mut self,
        id: u64,
        peer: &str,
        method: &str,
        payload: Option<serde_json::Value>,
    ) {
        if let Some(entry) = self.queues.get_mut(&id) {
            let outbox_entry = OutboxEntry {
                method: method.to_string(),
                payload,
                time: crate::link::now_secs(),
            };
            entry
                .entries
                .entry(peer.to_string())
                .or_default()
                .push(outbox_entry);
            self.kicks.insert(peer.to_string());
        }
    }

    /// Remove all queued entries for a peer on a specific queue.
    pub fn delete_queue(&mut self, id: u64, peer: &str) {
        if let Some(entry) = self.queues.get_mut(&id) {
            entry.entries.remove(peer);
        }
    }

    /// Dequeue the head entry for a peer on a specific queue.
    pub fn dequeue(&mut self, id: u64, peer: &str) {
        if let Some(entry) = self.queues.get_mut(&id) {
            if let Some(q) = entry.entries.get_mut(peer) {
                if !q.is_empty() {
                    q.remove(0);
                }
                if q.is_empty() {
                    entry.entries.remove(peer);
                }
            }
        }
    }

    /// Drain the kicks set — returns peers that need send_pending.
    pub fn drain_kicks(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.kicks)
    }

    /// Ref count for a module (number of live SyncObjects).
    pub fn ref_count(&self, module: &str) -> usize {
        self.ref_counts.get(module).copied().unwrap_or(0)
    }

    /// True if a specific queue has a pending entry for the peer.
    pub fn queue_has_pending(&self, id: u64, peer: &str) -> bool {
        self.queues.get(&id).map_or(false, |entry| {
            entry.entries.get(peer).map_or(false, |q| !q.is_empty())
        })
    }

    /// True if any queue for this module has a pending entry for the peer.
    pub fn has_pending(&self, module: &str, peer: &str) -> bool {
        self.queues.values().any(|entry| {
            entry.module == module
                && entry
                    .entries
                    .get(peer)
                    .map_or(false, |q| !q.is_empty())
        })
    }

    /// Collect the oldest outbox entry per queue for a peer across a module's queues.
    /// Returns (queue_id, method, payload, queued_at) for the head entry of each queue.
    pub fn collect_outbox(&self, module: &str, peer: &str) -> Vec<OutboxCandidate> {
        let mut candidates = Vec::new();
        for (&id, entry) in &self.queues {
            if entry.module != module {
                continue;
            }
            if let Some(q) = entry.entries.get(peer) {
                if let Some(head) = q.first() {
                    candidates.push(OutboxCandidate {
                        queue_id: id,
                        method: head.method.clone(),
                        payload: head.payload.clone(),
                        queued_at: head.time,
                    });
                }
            }
        }
        candidates
    }

    /// True if any queue (any module) has pending entries for this peer.
    pub fn any_pending_for_peer(&self, peer: &str) -> bool {
        self.queues.values().any(|entry| {
            entry
                .entries
                .get(peer)
                .map_or(false, |q| !q.is_empty())
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
        let id = reg.register("triad");
        reg.enqueue(id, "alice", "payment", None);

        assert!(reg.has_pending("triad", "alice"));
        assert!(!reg.has_pending("triad", "bob"));
    }

    #[test]
    fn dequeue_removes_head() {
        let mut reg = SyncRegistry::new();
        let id = reg.register("triad");
        reg.enqueue(id, "alice", "a", None);
        reg.enqueue(id, "alice", "b", None);

        let candidates = reg.collect_outbox("triad", "alice");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].method, "a");

        reg.dequeue(id, "alice");
        let candidates = reg.collect_outbox("triad", "alice");
        assert_eq!(candidates[0].method, "b");

        reg.dequeue(id, "alice");
        assert!(!reg.has_pending("triad", "alice"));
    }

    #[test]
    fn kicks_tracked_on_enqueue() {
        let mut reg = SyncRegistry::new();
        let id = reg.register("triad");
        reg.enqueue(id, "alice", "x", None);
        reg.enqueue(id, "bob", "y", None);

        let kicks = reg.drain_kicks();
        assert!(kicks.contains("alice"));
        assert!(kicks.contains("bob"));

        // Drained — next drain is empty.
        assert!(reg.drain_kicks().is_empty());
    }

    #[test]
    fn ref_counts() {
        let mut reg = SyncRegistry::new();
        let id1 = reg.register("triad");
        let id2 = reg.register("triad");
        assert_eq!(reg.ref_count("triad"), 2);

        reg.unregister(id1);
        assert_eq!(reg.ref_count("triad"), 1);

        reg.unregister(id2);
        assert_eq!(reg.ref_count("triad"), 0);
    }

    #[test]
    fn multiple_queues_same_peer() {
        let mut reg = SyncRegistry::new();
        let id1 = reg.register("triad");
        let id2 = reg.register("triad");

        reg.enqueue(id1, "alice", "pay1", None);
        reg.enqueue(id2, "alice", "pay2", None);

        let candidates = reg.collect_outbox("triad", "alice");
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn reattach_restores_ref_count() {
        let mut reg = SyncRegistry::new();
        reg.reattach(42, "triad");
        assert_eq!(reg.ref_count("triad"), 1);
        assert!(reg.next_id > 42);
    }
}
