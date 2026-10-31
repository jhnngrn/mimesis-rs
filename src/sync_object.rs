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
}

impl<T> SyncObject<T> {
    /// Create a new SyncObject bound to a peer, registering a queue in the registry.
    pub fn new(
        data: T,
        peer: &str,
        registry: &RegistryRef,
        module: &str,
    ) -> Self {
        let queue_id = registry.lock().unwrap().register(peer, module);
        SyncObject {
            data,
            peer: peer.to_string(),
            queue_id,
            registry: Some(registry.clone()),
        }
    }

    /// Create a SyncObject without a registry (for testing).
    pub fn without_registry(data: T, peer: &str) -> Self {
        SyncObject {
            data,
            peer: peer.to_string(),
            queue_id: 0,
            registry: None,
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
    pub fn reattach(&mut self, registry: &RegistryRef, module: &str) {
        registry.lock().unwrap().reattach(self.queue_id, &self.peer, module);
        self.registry = Some(registry.clone());
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry;

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
        let obj = SyncObject::new((), "alice", &reg, "triad");

        obj.send("payment", None);

        let r = reg.lock().unwrap();
        assert!(r.has_pending("triad", "alice"));
        assert!(!r.has_pending("triad", "bob"));
    }

    #[test]
    fn send_tracks_kicks() {
        let reg = registry::new_registry();
        let obj_a = SyncObject::new((), "alice", &reg, "triad");
        let obj_b = SyncObject::new((), "bob", &reg, "triad");

        obj_a.send("x", None);
        obj_b.send("y", None);

        let kicks = reg.lock().unwrap().drain_kicks();
        assert!(kicks.contains("alice"));
        assert!(kicks.contains("bob"));
    }

    #[test]
    fn drop_unregisters_from_registry() {
        let reg = registry::new_registry();
        {
            let obj = SyncObject::new((), "alice", &reg, "triad");
            obj.send("payment", None);
            assert!(reg.lock().unwrap().has_pending("triad", "alice"));
        }
        // After drop, queue is removed.
        assert!(!reg.lock().unwrap().has_pending("triad", "alice"));
    }

    #[test]
    fn multiple_queues_same_module() {
        let reg = registry::new_registry();
        let obj1 = SyncObject::new((), "alice", &reg, "triad");
        let obj2 = SyncObject::new((), "bob", &reg, "triad");

        obj1.send("x", None);
        obj2.send("y", None);

        let r = reg.lock().unwrap();
        assert!(r.has_pending("triad", "alice"));
        assert!(r.has_pending("triad", "bob"));
    }
}
