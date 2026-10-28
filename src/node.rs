//! Node — collection of bilateral links for an identity.
//!
//! Manages the set of links, the outbox (collected from SyncObject queues
//! during nextTx), and the sync loop that drives bilateral communication.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::link::{
    ConnectionStatus, Link, QueuedMsg, SyncError, TxRequest,
    ValidateResult, now_secs,
};

/// Maximum number of concurrent links per node.
pub const MAX_LINKS: usize = 64;

/// Invite mode — node-level policy for incoming connections.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InviteMode {
    Closed,
    Invite,
    Open,
}

impl Default for InviteMode {
    fn default() -> Self {
        InviteMode::Invite
    }
}

/// Node manages the set of bilateral links for an identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub links: HashMap<String, Link>,
    #[serde(default)]
    pub invite_mode: InviteMode,
    /// Root module declarations: root name → child module names.
    #[serde(skip)]
    roots: HashMap<String, Vec<String>>,
}

impl Node {
    pub fn new(id: String) -> Self {
        Node {
            id,
            links: HashMap::new(),
            invite_mode: InviteMode::default(),
            roots: HashMap::new(),
        }
    }

    /// Declare a root module with its child modules.
    pub fn declare_root(&mut self, name: &str, children: Vec<String>) {
        self.roots.insert(name.to_string(), children);
    }

    /// True if name is a declared root module.
    pub fn is_root(&self, name: &str) -> bool {
        self.roots.contains_key(name)
    }

    /// True if name is a child of the given root.
    pub fn is_child_module(&self, name: &str, root: &str) -> bool {
        self.roots
            .get(root)
            .map_or(false, |children| children.iter().any(|c| c == name))
    }

    /// Get an existing link.
    pub fn get(&self, peer: &str) -> Option<&Link> {
        self.links.get(peer)
    }

    /// Get a mutable reference to an existing link.
    pub fn get_mut(&mut self, peer: &str) -> Option<&mut Link> {
        self.links.get_mut(peer)
    }

    /// Create a new outgoing link to peer running the named root module.
    pub fn connect(&mut self, peer: &str, root: &str) -> Result<(), SyncError> {
        if !self.is_root(root) {
            return Err(SyncError::Other(format!("unknown root module {:?}", root)));
        }
        let peer = peer.to_lowercase();
        if self.links.contains_key(&peer) {
            return Err(SyncError::Other(format!("link to {} already exists", peer)));
        }
        if self.links.len() >= MAX_LINKS {
            return Err(SyncError::MaxLinks);
        }
        let mut lk = Link::new(&self.id, &peer);
        lk.state.root = Some(root.to_string());
        lk.state.conn.intent_connect = true;
        self.links.insert(peer, lk);
        Ok(())
    }

    /// Mark the link to peer for closing.
    pub fn close(&mut self, peer: &str) {
        if let Some(lk) = self.links.get_mut(peer) {
            lk.state.conn.intent_close = true;
        }
    }

    /// Request connection for child modules on a peer's link.
    pub fn connect_modules(&mut self, peer: &str, modules: &[String]) {
        let lk = match self.links.get(peer) {
            Some(lk) => lk,
            None => return,
        };
        let root = match &lk.state.root {
            Some(r) => r.clone(),
            None => return,
        };
        // Filter to valid child modules first (needs &self).
        let valid: Vec<String> = modules
            .iter()
            .filter(|name| self.is_child_module(name, &root))
            .cloned()
            .collect();
        // Now mutate the link.
        let lk = self.links.get_mut(peer).unwrap();
        for name in valid {
            let mod_state = lk
                .state
                .modules
                .entry(name)
                .or_default();
            if mod_state.status.is_none() {
                mod_state.intent_connect = true;
            }
        }
    }

    /// True if the module on the peer's link is available (connected and not closing).
    pub fn module_available(&self, peer: &str, module: &str) -> bool {
        self.links
            .get(peer)
            .map_or(false, |lk| lk.module_available(module))
    }

    /// Unilaterally remove a link.
    pub fn force_remove(&mut self, peer: &str) {
        self.links.remove(peer);
    }

    /// Determine the next message to send for this peer.
    /// This is the heart of the bilateral sync protocol.
    pub fn next_tx(&mut self, peer: &str) -> Option<QueuedMsg> {
        let lk = self.links.get_mut(peer)?;

        // Cancel opposing intents on established links.
        if lk.state.conn.intent_close
            && lk.state.conn.intent_connect
            && lk.state.conn.status.is_some()
        {
            lk.state.conn.intent_close = false;
            lk.state.conn.intent_connect = false;
        }

        // Link-level lifecycle messages have absolute priority.
        if lk.state.conn.intent_close {
            // TODO: check anyBusy when modules are integrated
            return Some(QueuedMsg {
                method: "link.close".into(),
                payload: None,
                queued_at: 0,
                module: String::new(),
                queue_id: String::new(),
            });
        }
        if lk.state.conn.intent_connect {
            let root = lk.state.root.clone().unwrap_or_default();
            return Some(QueuedMsg {
                method: "link.connect".into(),
                payload: Some(serde_json::json!({"name": root})),
                queued_at: 0,
                module: String::new(),
                queue_id: String::new(),
            });
        }

        if lk.state.conn.status != Some(ConnectionStatus::Connected) {
            return None;
        }

        // Module close/connect — cancel opposing, close first, then connect.
        for mod_state in lk.state.modules.values_mut() {
            if mod_state.intent_close && mod_state.intent_connect && mod_state.status.is_some() {
                mod_state.intent_close = false;
                mod_state.intent_connect = false;
            }
        }
        for (name, mod_state) in &lk.state.modules {
            if mod_state.intent_close {
                // TODO: check moduleBusy
                return Some(QueuedMsg {
                    method: "module.close".into(),
                    payload: Some(serde_json::json!({"name": name})),
                    queued_at: 0,
                    module: String::new(),
                    queue_id: String::new(),
                });
            }
        }
        for (name, mod_state) in &lk.state.modules {
            if mod_state.intent_connect {
                return Some(QueuedMsg {
                    method: "module.connect".into(),
                    payload: Some(serde_json::json!({"name": name})),
                    queued_at: 0,
                    module: String::new(),
                    queue_id: String::new(),
                });
            }
        }

        // Module messages are collected from SyncObject queues at runtime.
        // This base Node only handles lifecycle; the full framework wires
        // SyncObject queues into nextTx via a collector trait.
        None
    }

    /// Build the wire envelope for sending.
    pub fn build_request(&self, peer: &str, msg: &QueuedMsg) -> Option<TxRequest> {
        let lk = self.links.get(peer)?;
        let mut ts = 0i64;
        if lk.dominant() {
            ts = now_secs();
            if ts < lk.ts {
                ts = lk.ts;
            }
        }
        Some(TxRequest {
            from: self.id.clone(),
            to: peer.to_string(),
            counter: lk.counter,
            method: msg.method.clone(),
            payload: msg.payload.clone(),
            timestamp: ts,
            resp: lk.cached_response.clone(),
        })
    }

    /// Validate an incoming request.
    pub fn validate_incoming(&self, env: &TxRequest) -> Result<ValidateResult, SyncError> {
        let lk = self.links.get(&env.from).ok_or(SyncError::NoLink)?;
        let result = lk.validate_request(env.counter)?;
        if let ValidateResult::Ok = &result {
            lk.validate_timestamp(env.timestamp, now_secs())?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_creates_link_with_intent() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);

        node.connect("bob@b.com", "trustline").unwrap();
        let lk = node.get("bob@b.com").unwrap();
        assert!(lk.state.conn.intent_connect);
        assert_eq!(lk.state.root.as_deref(), Some("trustline"));
    }

    #[test]
    fn connect_rejects_unknown_root() {
        let mut node = Node::new("alice@a.com".into());
        let err = node.connect("bob@b.com", "unknown").unwrap_err();
        assert!(matches!(err, SyncError::Other(_)));
    }

    #[test]
    fn connect_rejects_duplicate() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.connect("bob@b.com", "trustline").unwrap();
        let err = node.connect("bob@b.com", "trustline").unwrap_err();
        assert!(matches!(err, SyncError::Other(_)));
    }

    #[test]
    fn close_sets_intent() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.connect("bob@b.com", "trustline").unwrap();
        node.close("bob@b.com");
        assert!(node.get("bob@b.com").unwrap().closing());
    }

    #[test]
    fn next_tx_returns_connect() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.connect("bob@b.com", "trustline").unwrap();

        let msg = node.next_tx("bob@b.com").unwrap();
        assert_eq!(msg.method, "link.connect");
    }

    #[test]
    fn next_tx_cancels_opposing_intents() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.connect("bob@b.com", "trustline").unwrap();

        // Simulate established then set both intents
        let lk = node.get_mut("bob@b.com").unwrap();
        lk.state.conn.status = Some(ConnectionStatus::Connected);
        lk.state.conn.intent_connect = true;
        lk.state.conn.intent_close = true;

        // nextTx should cancel both
        let msg = node.next_tx("bob@b.com");
        assert!(msg.is_none()); // both cancelled, no module messages
        let lk = node.get("bob@b.com").unwrap();
        assert!(!lk.state.conn.intent_connect);
        assert!(!lk.state.conn.intent_close);
    }

    #[test]
    fn turn_bit_is_deterministic() {
        let lk1 = Link::new("alice@a.com", "bob@b.com");
        let lk2 = Link::new("bob@b.com", "alice@a.com");
        // One must be 0, the other 1
        assert_ne!(lk1.turn_bit, lk2.turn_bit);
        // Together they cover both slots
        assert_eq!(lk1.turn_bit + lk2.turn_bit, 1);
    }

    #[test]
    fn connect_modules_sets_intent() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec!["routing".into()]);
        node.connect("bob@b.com", "trustline").unwrap();

        node.connect_modules("bob@b.com", &["routing".into()]);
        let lk = node.get("bob@b.com").unwrap();
        let m = lk.state.modules.get("routing").unwrap();
        assert!(m.intent_connect);
    }
}
