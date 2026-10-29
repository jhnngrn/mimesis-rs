//! SyncObject<T> — the core framework abstraction.
//!
//! A SyncObject wraps module data (like a Novation tracker) with
//! per-peer message queues and automatic cleanup via Drop. When the
//! object is dropped — removed from a map, parent struct dropped,
//! scope exited — Drop fires: queues are cleared, and the framework
//! is notified to kick each peer.
//!
//! This realizes the "NextTx" pattern from the old design: each module
//! owns its queues, and the sync loop collects from them. But Drop
//! guarantees cleanup — no stale outbox entries, no forgotten refs.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::link::OutboxEntry;

/// Callback the framework provides so SyncObject can notify it on drop.
/// Each call means "kick this peer — something changed."
pub type KickFn = Arc<dyn Fn(&str) + Send + Sync>;

/// Ref callback: called with `true` when a SyncObject is created (ref acquired),
/// `false` when dropped (ref released). The framework uses this to track how many
/// live SyncObjects a module has — replacing Module::Busy().
pub type RefFn = Arc<dyn Fn(bool) + Send + Sync>;

/// SyncObject<T> wraps module data with per-peer message queues.
///
/// - `data: T` — the module's own state (e.g. Novation, Pathfinding)
/// - `queues: HashMap<String, Vec<OutboxEntry>>` — peer → pending messages
/// - `kick: KickFn` — framework callback to kick peers on drop
/// - `ref_fn: RefFn` — framework callback for ref counting (constructor = ref, Drop = unref)
///
/// Implements `Deref<Target=T>` so module code accesses T's fields
/// transparently: `sync_obj.some_field` reads `sync_obj.data.some_field`.
pub struct SyncObject<T> {
    /// The module's own data.
    pub data: T,
    /// Per-peer outbox queues. The sync loop reads from these.
    pub queues: HashMap<String, Vec<OutboxEntry>>,
    /// Framework callback. Called for each peer on drop.
    kick: Option<KickFn>,
    /// Ref callback. Called with true on creation, false on drop.
    ref_fn: Option<RefFn>,
}

impl<T> SyncObject<T> {
    /// Create a new SyncObject wrapping the given data.
    /// `kick` notifies peers on drop; `ref_fn` tracks the ref lifecycle.
    pub fn new(data: T, kick: KickFn, ref_fn: RefFn) -> Self {
        ref_fn(true); // ref acquired
        SyncObject {
            data,
            queues: HashMap::new(),
            kick: Some(kick),
            ref_fn: Some(ref_fn),
        }
    }

    /// Create a SyncObject without callbacks (for testing).
    pub fn without_kick(data: T) -> Self {
        SyncObject {
            data,
            queues: HashMap::new(),
            kick: None,
            ref_fn: None,
        }
    }

    /// Enqueue a message for a peer.
    pub fn send(&mut self, peer: &str, method: &str, payload: Option<serde_json::Value>) {
        let entry = OutboxEntry {
            method: method.to_string(),
            payload,
            time: crate::link::now_secs(),
        };
        self.queues
            .entry(peer.to_string())
            .or_default()
            .push(entry);
        if let Some(ref kick) = self.kick {
            kick(peer);
        }
    }

    /// Dequeue the head entry for a peer (after response received).
    pub fn dequeue(&mut self, peer: &str) {
        if let Some(q) = self.queues.get_mut(peer) {
            if !q.is_empty() {
                q.remove(0);
            }
            if q.is_empty() {
                self.queues.remove(peer);
            }
        }
    }

    /// Remove all queued entries for a peer.
    pub fn delete_queue(&mut self, peer: &str) {
        self.queues.remove(peer);
    }

    /// True if there's a pending entry for this peer.
    pub fn has_pending(&self, peer: &str) -> bool {
        self.queues.get(peer).map_or(false, |q| !q.is_empty())
    }

    /// Get the oldest entry for a peer, if any.
    pub fn peek(&self, peer: &str) -> Option<&OutboxEntry> {
        self.queues.get(peer).and_then(|q| q.first())
    }

    /// Returns all peers that have pending entries.
    pub fn peers_with_pending(&self) -> Vec<&str> {
        self.queues
            .iter()
            .filter(|(_, q)| !q.is_empty())
            .map(|(p, _)| p.as_str())
            .collect()
    }

}

impl<T> Deref for SyncObject<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> DerefMut for SyncObject<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<T> Drop for SyncObject<T> {
    fn drop(&mut self) {
        // Release ref first — framework sees ref count decrease.
        if let Some(ref ref_fn) = self.ref_fn {
            ref_fn(false);
        }
        // Queues die with the object — no stale outbox possible.
        // Kick every peer so the sync loop picks up the removal.
        if let Some(ref kick) = self.kick {
            for peer in self.queues.keys() {
                kick(peer);
            }
        }
        self.queues.clear();
    }
}

// Serialize/Deserialize — only the data and queues are persisted.
// The kick callback is restored by the framework on load.
impl<T: Serialize> Serialize for SyncObject<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("SyncObject", 2)?;
        s.serialize_field("data", &self.data)?;
        s.serialize_field("queues", &self.queues)?;
        s.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SyncObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Inner<T> {
            data: T,
            #[serde(default)]
            queues: HashMap<String, Vec<OutboxEntry>>,
        }
        let inner = Inner::<T>::deserialize(deserializer)?;
        Ok(SyncObject {
            data: inner.data,
            queues: inner.queues,
            kick: None,    // framework restores this
            ref_fn: None,  // framework restores this
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn deref_accesses_inner_data() {
        #[derive(Default)]
        struct MyData {
            balance: i64,
        }
        let obj = SyncObject::without_kick(MyData { balance: 42 });
        assert_eq!(obj.balance, 42);
    }

    fn noop_ref_fn() -> RefFn {
        Arc::new(|_| {})
    }

    #[test]
    fn send_enqueues_and_kicks() {
        let kicked = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let kicked2 = kicked.clone();
        let kick: KickFn = Arc::new(move |peer: &str| {
            kicked2.lock().unwrap().push(peer.to_string());
        });

        let mut obj = SyncObject::new((), kick, noop_ref_fn());
        obj.send("alice@example.com", "payment", None);
        obj.send("alice@example.com", "payment", None);
        obj.send("bob@example.com", "payment", None);

        assert!(obj.has_pending("alice@example.com"));
        assert!(obj.has_pending("bob@example.com"));
        assert!(!obj.has_pending("carol@example.com"));

        let kicks = kicked.lock().unwrap();
        assert_eq!(kicks.len(), 3);
    }

    #[test]
    fn dequeue_removes_head() {
        let mut obj = SyncObject::without_kick(());
        obj.send("alice@example.com", "a", None);
        obj.send("alice@example.com", "b", None);

        assert_eq!(obj.peek("alice@example.com").unwrap().method, "a");
        obj.dequeue("alice@example.com");
        assert_eq!(obj.peek("alice@example.com").unwrap().method, "b");
        obj.dequeue("alice@example.com");
        assert!(!obj.has_pending("alice@example.com"));
    }

    #[test]
    fn drop_kicks_all_peers() {
        let kick_count = Arc::new(AtomicUsize::new(0));
        let kc = kick_count.clone();
        let kick: KickFn = Arc::new(move |_peer: &str| {
            kc.fetch_add(1, Ordering::SeqCst);
        });

        let mut obj = SyncObject::new((), kick, noop_ref_fn());
        obj.send("alice@example.com", "x", None);
        obj.send("bob@example.com", "y", None);

        // Reset counter (send already kicked twice)
        kick_count.store(0, Ordering::SeqCst);

        drop(obj);

        // Drop should kick once per peer with pending queues
        assert_eq!(kick_count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn ref_fn_called_on_new_and_drop() {
        let ref_count = Arc::new(AtomicUsize::new(0));
        let rc = ref_count.clone();
        let ref_fn: RefFn = Arc::new(move |acquired| {
            if acquired {
                rc.fetch_add(1, Ordering::SeqCst);
            } else {
                rc.fetch_sub(1, Ordering::SeqCst);
            }
        });
        let kick: KickFn = Arc::new(|_| {});

        assert_eq!(ref_count.load(Ordering::SeqCst), 0);

        let obj = SyncObject::new((), kick.clone(), ref_fn.clone());
        assert_eq!(ref_count.load(Ordering::SeqCst), 1);

        let obj2 = SyncObject::new((), kick, ref_fn);
        assert_eq!(ref_count.load(Ordering::SeqCst), 2);

        drop(obj);
        assert_eq!(ref_count.load(Ordering::SeqCst), 1);

        drop(obj2);
        assert_eq!(ref_count.load(Ordering::SeqCst), 0);
    }
}
