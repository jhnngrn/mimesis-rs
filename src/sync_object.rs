//! SyncObject<T> — the core framework abstraction.
//!
//! A SyncObject wraps module data (like a Novation tracker) with
//! automatic lifecycle management via the SyncRegistry. On creation
//! it registers a queue in the registry; on Drop it unregisters.
//!
//! Queues live in the registry, not in SyncObject — the framework
//! collects from the registry during nextTx, dequeues on response,
//! and drains kicks after each handler. Modules never touch outbox
//! plumbing directly.

use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

use crate::link::ChildTracker;
use crate::registry::RegistryRef;

/// SyncObject<T> wraps module data with a registry-managed queue.
///
/// - `data: T` — the module's own state (e.g. Novation, Pathfinding)
/// - `queue_id: u64` — internal registry queue ID
/// - `registry: RegistryRef` — shared reference to the registry
///
/// Implements `Deref<Target=T>` so module code accesses T's fields
/// transparently: `sync_obj.some_field` reads `sync_obj.data.some_field`.
pub struct SyncObject<T> {
    /// The module's own data.
    pub data: T,
    /// The peer this SyncObject is bound to (1:1).
    peer: String,
    /// Internal queue ID assigned by the registry.
    queue_id: u64,
    /// Shared reference to the registry.
    registry: Option<RegistryRef>,
    /// Parent's child tracker — this SO registers/unregisters itself here.
    parent: Option<ChildTracker>,
    /// Whether this SyncObject has been bilaterally confirmed.
    /// Only connected SyncObjects count in the parent's connected set.
    connected: bool,
}

impl<T> SyncObject<T> {
    /// Create a new SyncObject bound to a peer, registering a queue in the registry.
    /// The `parent` ChildTracker comes from the ConnectionState for this module+peer.
    /// Create a new SyncObject bound to a peer, registering a queue in the registry.
    /// Starts as disconnected — the parent's connected set is NOT updated until set_connected().
    pub fn new(
        data: T,
        peer: &str,
        registry: &RegistryRef,
        module: &str,
        parent: ChildTracker,
    ) -> Self {
        let queue_id = registry.lock().unwrap().register(peer, module);
        parent.register(queue_id);
        SyncObject {
            data,
            peer: peer.to_string(),
            queue_id,
            registry: Some(registry.clone()),
            parent: Some(parent),
            connected: false,
        }
    }

    /// Create a SyncObject without a registry (for testing).
    pub fn without_registry(data: T, peer: &str) -> Self {
        SyncObject {
            data,
            peer: peer.to_string(),
            queue_id: 0,
            registry: None,
            parent: None,
            connected: false,
        }
    }

    /// The peer this SyncObject is bound to.
    pub fn peer(&self) -> &str {
        &self.peer
    }

    /// Enqueue a message to this object's peer.
    pub fn send(&self, method: &str, payload: Option<serde_json::Value>) {
        if let Some(ref reg) = self.registry {
            reg.lock()
                .unwrap()
                .enqueue(self.queue_id, method, payload);
        }
    }

    /// Remove all queued entries and unregister this queue.
    pub fn delete_queue(&self) {
        if let Some(ref reg) = self.registry {
            reg.lock().unwrap().delete_queue(self.queue_id);
        }
    }

    /// The internal queue ID (for framework use).
    pub fn queue_id(&self) -> u64 {
        self.queue_id
    }

    /// Mark this SyncObject as bilaterally connected.
    /// Adds this SO to the parent's connected set. Panics if already connected.
    pub fn set_connected(&mut self) {
        assert!(!self.connected, "SyncObject already connected");
        self.connected = true;
        if let Some(ref p) = self.parent {
            p.set_connected(self.queue_id);
        }
    }

    /// Whether this SyncObject has been bilaterally confirmed.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// True if the outbox has pending entries for this sync object.
    pub fn has_pending(&self) -> bool {
        if let Some(ref reg) = self.registry {
            let r = reg.lock().unwrap();
            r.queue_has_pending(self.queue_id)
        } else {
            false
        }
    }

    /// Reconnect to a registry after deserialization.
    /// The queue data should already exist in the registry from persisted state.
    /// Registers this SO in the parent's child tracker (as disconnected).
    /// Caller should call set_connected() if this SO was previously connected.
    pub fn reattach(&mut self, registry: &RegistryRef, module: &str, parent: ChildTracker) {
        registry.lock().unwrap().reattach(self.queue_id, &self.peer, module);
        parent.register(self.queue_id);
        self.registry = Some(registry.clone());
        self.parent = Some(parent);
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
        if let Some(ref reg) = self.registry {
            reg.lock().unwrap().unregister(self.queue_id);
        }
        // Unregister from parent (removes from both all and connected sets).
        if let Some(ref p) = self.parent {
            p.unregister(self.queue_id);
        }
    }
}

// Serialize/Deserialize — data and queue_id are persisted.
// The registry reference is restored by reattach() after load.
impl<T: Serialize> Serialize for SyncObject<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("SyncObject", 3)?;
        s.serialize_field("data", &self.data)?;
        s.serialize_field("peer", &self.peer)?;
        s.serialize_field("queue_id", &self.queue_id)?;
        s.end()
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for SyncObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Inner<T> {
            data: T,
            #[serde(default)]
            peer: String,
            #[serde(default)]
            queue_id: u64,
        }
        let inner = Inner::<T>::deserialize(deserializer)?;
        Ok(SyncObject {
            data: inner.data,
            peer: inner.peer,
            queue_id: inner.queue_id,
            registry: None,   // restored by reattach()
            parent: None,     // restored by reattach()
            connected: false, // restored by set_connected() after reattach
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry;

    fn make_tracker() -> ChildTracker {
        ChildTracker::new()
    }

    #[test]
    fn deref_accesses_inner_data() {
        #[derive(Default)]
        struct MyData {
            balance: i64,
        }
        let obj = SyncObject::without_registry(MyData { balance: 42 }, "alice");
        assert_eq!(obj.balance, 42);
    }

    #[test]
    fn send_enqueues_in_registry() {
        let reg = registry::new_registry();
        let tracker = make_tracker();
        let obj = SyncObject::new((), "alice", &reg, "triad", tracker);

        obj.send("payment", None);

        let r = reg.lock().unwrap();
        assert!(r.has_pending("triad", "alice"));
        assert!(!r.has_pending("triad", "bob"));
    }

    #[test]
    fn send_tracks_kicks() {
        let reg = registry::new_registry();
        let t_a = make_tracker();
        let t_b = make_tracker();
        let obj_a = SyncObject::new((), "alice", &reg, "triad", t_a);
        let obj_b = SyncObject::new((), "bob", &reg, "triad", t_b);

        obj_a.send("x", None);
        obj_b.send("y", None);

        let kicks = reg.lock().unwrap().drain_kicks();
        assert!(kicks.contains("alice"));
        assert!(kicks.contains("bob"));
    }

    #[test]
    fn new_registers_but_not_connected() {
        let reg = registry::new_registry();
        let tracker = make_tracker();
        let _obj = SyncObject::new((), "alice", &reg, "triad", tracker.clone());
        // Registered but not connected.
        assert_eq!(tracker.total_count(), 1);
        assert_eq!(tracker.connected_count(), 0);
    }

    #[test]
    fn set_connected_adds_to_connected_set() {
        let reg = registry::new_registry();
        let tracker = make_tracker();
        let mut obj = SyncObject::new((), "alice", &reg, "triad", tracker.clone());
        assert!(!obj.is_connected());
        assert!(!tracker.has_connected());

        obj.set_connected();
        assert!(obj.is_connected());
        assert!(tracker.has_connected());
        assert_eq!(tracker.connected_count(), 1);
    }

    #[test]
    fn drop_unregisters_from_tracker() {
        let reg = registry::new_registry();
        let tracker = make_tracker();
        {
            // Disconnected — drop removes from all set.
            let _obj = SyncObject::new((), "alice", &reg, "triad", tracker.clone());
            assert_eq!(tracker.total_count(), 1);
        }
        assert_eq!(tracker.total_count(), 0);
        assert!(!tracker.has_connected());

        {
            // Connected — drop removes from both sets.
            let mut obj = SyncObject::new((), "bob", &reg, "triad", tracker.clone());
            obj.set_connected();
            assert_eq!(tracker.connected_count(), 1);
        }
        assert_eq!(tracker.total_count(), 0);
        assert_eq!(tracker.connected_count(), 0);
    }

    #[test]
    fn multiple_connected_sync_objects() {
        let reg = registry::new_registry();
        let tracker = make_tracker();
        let mut obj1 = SyncObject::new((), "alice", &reg, "triad", tracker.clone());
        let mut obj2 = SyncObject::new((), "bob", &reg, "triad", tracker.clone());
        obj1.set_connected();
        obj2.set_connected();
        assert_eq!(tracker.connected_count(), 2);

        drop(obj1);
        assert_eq!(tracker.connected_count(), 1);

        drop(obj2);
        assert_eq!(tracker.connected_count(), 0);
    }

    #[test]
    fn separate_trackers_per_module() {
        let reg = registry::new_registry();
        let t_triad = make_tracker();
        let t_loops = make_tracker();
        let mut obj1 = SyncObject::new((), "alice", &reg, "triad", t_triad.clone());
        let mut obj2 = SyncObject::new((), "alice", &reg, "loops", t_loops.clone());
        obj1.set_connected();
        obj2.set_connected();
        assert_eq!(t_triad.connected_count(), 1);
        assert_eq!(t_loops.connected_count(), 1);
    }
}
