//! Node — collection of bilateral links for an identity.
//!
//! Manages the set of links, module dispatch, and the sync protocol
//! that drives bilateral communication. Outbox messages are collected
//! from registered modules' SyncObject queues during nextTx.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::link::{
    ConnectionState, ConnectionStatus, Link, QueuedMsg, SyncError, TxRequest,
    TxResponse, ValidateResult, now_secs,
};
use crate::module::Module;
use crate::registry::{self, RegistryRef};

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

/// Split "trustline.payment" → ("trustline", "payment").
/// If no dot, returns ("", method).
fn split_method(method: &str) -> (&str, &str) {
    match method.find('.') {
        Some(i) => (&method[..i], &method[i + 1..]),
        None => ("", method),
    }
}

/// Node manages the set of bilateral links for an identity.
#[derive(Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub links: HashMap<String, Link>,
    #[serde(default)]
    pub invite_mode: InviteMode,
    /// Root module declarations: root name → child module names.
    #[serde(skip)]
    roots: HashMap<String, Vec<String>>,
    /// Registered modules by name.
    #[serde(skip)]
    modules: HashMap<String, Box<dyn Module>>,
    /// Framework-owned outbox registry (queues, kicks, ref counts).
    #[serde(skip)]
    registry: RegistryRef,
}

impl Node {
    pub fn new(id: String) -> Self {
        Node {
            id,
            links: HashMap::new(),
            invite_mode: InviteMode::default(),
            roots: HashMap::new(),
            modules: HashMap::new(),
            registry: registry::new_registry(),
        }
    }

    /// Declare a root module with its child modules.
    pub fn declare_root(&mut self, name: &str, children: Vec<String>) {
        self.roots.insert(name.to_string(), children);
    }

    /// Register a module instance.
    pub fn register_module(&mut self, name: &str, module: Box<dyn Module>) {
        self.modules.insert(name.to_string(), module);
    }

    /// Get a clone of the registry reference (for passing to modules/SyncObjects).
    pub fn registry(&self) -> RegistryRef {
        self.registry.clone()
    }

    /// Get the ConnectionState for a module on a peer's link.
    /// Returns the root ConnectionState for the root module, or the
    /// child's ConnectionState from the modules map.
    pub fn connection_state(&self, peer: &str, module: &str) -> Option<&ConnectionState> {
        let lk = self.links.get(peer)?;
        if lk.state.root.as_deref() == Some(module) {
            Some(&lk.state.conn)
        } else {
            lk.state.modules.get(module)
        }
    }

    /// Get a mutable ConnectionState for a module on a peer's link.
    pub fn connection_state_mut(&mut self, peer: &str, module: &str) -> Option<&mut ConnectionState> {
        let lk = self.links.get_mut(peer)?;
        if lk.state.root.as_deref() == Some(module) {
            Some(&mut lk.state.conn)
        } else {
            lk.state.modules.get_mut(module)
        }
    }

    /// Get the ref count handle for a module on a peer (for passing to SyncObjects).
    /// Creates the ConnectionState entry if it doesn't exist yet.
    pub fn ref_count_handle(&mut self, peer: &str, module: &str) -> Option<crate::link::RefCountHandle> {
        self.connection_state(peer, module)
            .map(|cs| cs.ref_count.clone())
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

    /// Access module map (for cross-module access).
    pub fn module_map(&self) -> &HashMap<String, Box<dyn Module>> {
        &self.modules
    }

    /// Get a mutable reference to a module by name.
    pub fn module_mut(&mut self, name: &str) -> Option<&mut Box<dyn Module>> {
        self.modules.get_mut(name)
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
        let valid: Vec<String> = modules
            .iter()
            .filter(|name| self.is_child_module(name, &root))
            .cloned()
            .collect();
        let lk = self.links.get_mut(peer).unwrap();
        for name in valid {
            let mod_state = lk.state.modules.entry(name).or_default();
            if mod_state.status.is_none() {
                mod_state.intent_connect = true;
            }
        }
    }

    /// True if the module on the peer's link is available.
    pub fn module_available(&self, peer: &str, module: &str) -> bool {
        self.links
            .get(peer)
            .map_or(false, |lk| lk.module_available(module))
    }

    /// Unilaterally remove a link.
    pub fn force_remove(&mut self, peer: &str) {
        // Fire ForceRemove on all connected modules.
        if let Some(lk) = self.links.get(peer) {
            let root = lk.state.root.clone();
            let mod_names: Vec<String> = lk.state.modules.keys().cloned().collect();
            // Children first, then root.
            for name in &mod_names {
                if let Some(m) = self.modules.get_mut(name) {
                    m.force_remove(peer);
                }
            }
            if let Some(ref r) = root {
                if let Some(m) = self.modules.get_mut(r.as_str()) {
                    m.force_remove(peer);
                }
            }
        }
        self.links.remove(peer);
    }

    // -----------------------------------------------------------------------
    // Ref count checks — ConnectionState.ref_count per module per peer
    // -----------------------------------------------------------------------

    /// True if any module has active refs (live SyncObjects) for this peer.
    /// Used to NACK incoming link.close when we still hold refs.
    fn has_active_refs(&self, peer: &str) -> bool {
        let lk = match self.links.get(peer) {
            Some(lk) => lk,
            None => return false,
        };
        if lk.state.conn.ref_count.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            return true;
        }
        for cs in lk.state.modules.values() {
            if cs.ref_count.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                return true;
            }
        }
        false
    }

    // -----------------------------------------------------------------------
    // Dispatch — incoming request
    // -----------------------------------------------------------------------

    /// Dispatch an incoming sync request by namespace.
    pub fn dispatch_request(
        &mut self,
        peer: &str,
        full_method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<bool, SyncError> {
        let (ns, method) = split_method(full_method);

        if ns == "link" {
            return self.dispatch_link_request(peer, method, payload);
        }
        if ns == "module" {
            return self.dispatch_module_request(peer, method, payload);
        }
        // Application module message.
        let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
        if !lk.module_connected(ns) {
            return Err(SyncError::BadMethod);
        }
        let m = self
            .modules
            .get_mut(ns)
            .ok_or(SyncError::BadMethod)?;
        match m.handle_request(peer, method, payload, ts) {
            Ok(r) => Ok(r.ok),
            Err(e) => Err(SyncError::Other(e.to_string())),
        }
    }

    fn dispatch_link_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
    ) -> Result<bool, SyncError> {
        match method {
            "connect" => {
                let name = payload
                    .as_ref()
                    .and_then(|p| p.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if name.is_empty() || !self.is_root(name) {
                    return Err(SyncError::Other(format!(
                        "unknown root module {:?}",
                        name
                    )));
                }
                let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
                if lk.state.conn.status == Some(ConnectionStatus::Connected) {
                    return Err(SyncError::Other("link already connected".into()));
                }
                if lk.state.conn.status == Some(ConnectionStatus::Incoming) {
                    return Err(SyncError::Other("link already incoming".into()));
                }
                // Root mismatch — NACK.
                if lk.state.root.as_deref() != Some(name) {
                    return Ok(false);
                }
                // Accept: mutual add or open mode.
                let status = lk.state.conn.status.clone();
                let is_open = self.invite_mode == InviteMode::Open;
                if status == Some(ConnectionStatus::Waiting) || is_open {
                    let lk = self.links.get_mut(peer).unwrap();
                    lk.state.conn.status = Some(ConnectionStatus::Connected);
                    lk.state.conn.intent_connect = false;
                    let root = lk.state.root.clone().unwrap_or_default();
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        m.on_connect(peer);
                    }
                    return Ok(true);
                }
                // Invite mode — mark as incoming, NACK.
                let lk = self.links.get_mut(peer).unwrap();
                lk.state.conn.status = Some(ConnectionStatus::Incoming);
                Ok(false)
            }
            "close" => {
                if self.has_active_refs(peer) {
                    return Err(SyncError::RefsHeld);
                }
                let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
                if lk.state.conn.status == Some(ConnectionStatus::Connected) {
                    // OnClose: children first, then root.
                    let mod_names: Vec<String> = lk
                        .state
                        .modules
                        .iter()
                        .filter(|(_, c)| c.status == Some(ConnectionStatus::Connected))
                        .map(|(n, _)| n.clone())
                        .collect();
                    let root = lk.state.root.clone().unwrap_or_default();
                    for name in &mod_names {
                        if let Some(m) = self.modules.get_mut(name.as_str()) {
                            m.on_close(peer);
                        }
                    }
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        m.on_close(peer);
                    }
                }
                self.links.remove(peer);
                Ok(true)
            }
            _ => Err(SyncError::Other(format!(
                "unknown link method {:?}",
                method
            ))),
        }
    }

    fn dispatch_module_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
    ) -> Result<bool, SyncError> {
        let name = payload
            .as_ref()
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
        let root = lk.state.root.clone().unwrap_or_default();

        match method {
            "connect" => {
                if name.is_empty() || !self.is_child_module(name, &root) {
                    return Ok(false);
                }
                if !self.modules.contains_key(name) {
                    return Ok(false);
                }
                let lk = self.links.get_mut(peer).unwrap();
                let mod_state = lk.state.modules.entry(name.to_string()).or_default();

                // Already connected — idempotent ACK.
                if mod_state.status == Some(ConnectionStatus::Connected) {
                    return Ok(true);
                }
                // Mutual — we also want this module.
                if mod_state.intent_connect
                    || mod_state.status == Some(ConnectionStatus::Waiting)
                {
                    mod_state.status = Some(ConnectionStatus::Connected);
                    mod_state.intent_connect = false;
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_connect(peer);
                    }
                    return Ok(true);
                }
                // Open mode — auto-accept.
                if self.invite_mode == InviteMode::Open {
                    mod_state.status = Some(ConnectionStatus::Connected);
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_connect(peer);
                    }
                    return Ok(true);
                }
                // Invite — mark incoming, NACK.
                mod_state.status = Some(ConnectionStatus::Incoming);
                Ok(false)
            }
            "close" => {
                if name.is_empty() || !self.is_child_module(name, &root) {
                    return Ok(false);
                }
                // Check ref count before taking mutable borrow on links.
                if let Some(cs) = self.connection_state(peer, name) {
                    if cs.ref_count.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                        return Err(SyncError::RefsHeld);
                    }
                }
                let lk = self.links.get_mut(peer).unwrap();
                let was_connected = lk.state.modules.get(name)
                    .map_or(false, |s| s.status == Some(ConnectionStatus::Connected));
                if !lk.state.modules.contains_key(name) {
                    return Ok(true); // already gone
                }
                lk.state.modules.remove(name);
                if was_connected {
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_close(peer);
                    }
                }
                Ok(true)
            }
            _ => Err(SyncError::Other(format!(
                "unknown module method {:?}",
                method
            ))),
        }
    }

    // -----------------------------------------------------------------------
    // Dispatch — outgoing response
    // -----------------------------------------------------------------------

    /// Dispatch a response to our outgoing message.
    pub fn dispatch_response(
        &mut self,
        peer: &str,
        full_method: &str,
        payload: Option<Value>,
        ok: bool,
        ts: i64,
    ) {
        let (ns, method) = split_method(full_method);

        if ns == "link" {
            self.dispatch_link_response(peer, method, ok);
            return;
        }
        if ns == "module" {
            self.dispatch_module_response(peer, method, payload, ok);
            return;
        }
        // Application module response.
        if let Some(m) = self.modules.get_mut(ns) {
            m.handle_response(peer, method, payload, ok, ts);
        }
    }

    fn dispatch_link_response(&mut self, peer: &str, method: &str, ok: bool) {
        match method {
            "connect" => {
                let lk = match self.links.get_mut(peer) {
                    Some(lk) => lk,
                    None => return,
                };
                if ok {
                    lk.state.conn.status = Some(ConnectionStatus::Connected);
                    lk.state.conn.intent_connect = false;
                    let root = lk.state.root.clone().unwrap_or_default();
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        m.on_connect(peer);
                    }
                } else {
                    lk.state.conn.status = Some(ConnectionStatus::Waiting);
                    lk.state.conn.intent_connect = false;
                }
            }
            "close" => {
                if ok {
                    // OnClose: children first, then root.
                    if let Some(lk) = self.links.get(peer) {
                        let mod_names: Vec<String> = lk
                            .state
                            .modules
                            .iter()
                            .filter(|(_, c)| {
                                c.status == Some(ConnectionStatus::Connected)
                            })
                            .map(|(n, _)| n.clone())
                            .collect();
                        let root = lk.state.root.clone().unwrap_or_default();
                        for name in &mod_names {
                            if let Some(m) = self.modules.get_mut(name.as_str()) {
                                m.on_close(peer);
                            }
                        }
                        if let Some(m) = self.modules.get_mut(root.as_str()) {
                            m.on_close(peer);
                        }
                    }
                    self.links.remove(peer);
                } else {
                    if let Some(lk) = self.links.get_mut(peer) {
                        lk.state.conn.intent_close = false;
                    }
                }
            }
            _ => {}
        }
    }

    fn dispatch_module_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ok: bool,
    ) {
        let name = payload
            .as_ref()
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if name.is_empty() {
            return;
        }

        match method {
            "connect" => {
                let lk = match self.links.get_mut(peer) {
                    Some(lk) => lk,
                    None => return,
                };
                let mod_state = match lk.state.modules.get_mut(name) {
                    Some(s) => s,
                    None => return,
                };
                if ok {
                    mod_state.status = Some(ConnectionStatus::Connected);
                    mod_state.intent_connect = false;
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_connect(peer);
                    }
                } else {
                    mod_state.status = Some(ConnectionStatus::Waiting);
                    mod_state.intent_connect = false;
                }
            }
            "close" => {
                let lk = match self.links.get_mut(peer) {
                    Some(lk) => lk,
                    None => return,
                };
                let mod_state = match lk.state.modules.get(name) {
                    Some(s) => s,
                    None => return,
                };
                if ok {
                    let was_connected =
                        mod_state.status == Some(ConnectionStatus::Connected);
                    lk.state.modules.remove(name);
                    if was_connected {
                        if let Some(m) = self.modules.get_mut(name) {
                            m.on_close(peer);
                        }
                    }
                } else {
                    if let Some(s) = lk.state.modules.get_mut(name) {
                        s.intent_close = false;
                    }
                }
            }
            _ => {}
        }
    }

    // -----------------------------------------------------------------------
    // Handle incoming — the full sync receive path
    // -----------------------------------------------------------------------

    /// Handle a complete incoming sync request envelope.
    /// This is the main entry point for the receive side.
    pub fn handle_incoming(&mut self, env: &TxRequest) -> Result<TxResponse, SyncError> {
        let peer = env.from.to_lowercase();

        // No link — handle link.connect and link.close specially.
        if !self.links.contains_key(&peer) {
            if env.method == "link.close" {
                return Ok(TxResponse {
                    ok: true,
                    timestamp: now_secs(),
                });
            }
            if env.method != "link.connect" {
                return Err(SyncError::NoLink);
            }
            if self.invite_mode == InviteMode::Closed {
                return Ok(TxResponse {
                    ok: false,
                    timestamp: now_secs(),
                });
            }
            if self.links.len() >= MAX_LINKS {
                return Err(SyncError::MaxLinks);
            }
            // Validate root module name before creating state.
            let root_name = env
                .payload
                .as_ref()
                .and_then(|p| p.get("name"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if root_name.is_empty() || !self.is_root(root_name) {
                return Err(SyncError::Other(format!(
                    "unknown root module {:?}",
                    root_name
                )));
            }
            let mut lk = Link::new(&self.id, &peer);
            lk.state.root = Some(root_name.to_string());
            self.links.insert(peer.clone(), lk);
        }

        let now = now_secs();

        // Piggybacked response.
        if let Some(ref resp) = env.resp {
            let lk = self.links.get(&peer).unwrap();
            if lk.pending.is_some() && env.counter == lk.counter + 1 {
                let lk = self.links.get(&peer).unwrap();
                lk.validate_timestamp(resp.timestamp, now)?;

                // Extract pending info before mutable borrow.
                let pending_method;
                let pending_payload;
                let pending_queue_id;
                {
                    let lk = self.links.get(&peer).unwrap();
                    let p = lk.pending.as_ref().unwrap();
                    pending_method = p.method.clone();
                    pending_payload = p.payload.clone();
                    pending_queue_id = p.queue_id;
                }

                self.dispatch_response(
                    &peer,
                    &pending_method,
                    pending_payload,
                    resp.ok,
                    resp.timestamp,
                );

                // Dequeue from registry.
                if pending_queue_id != 0 {
                    self.registry.lock().unwrap().dequeue(pending_queue_id);
                }

                let lk = self.links.get_mut(&peer).unwrap();
                lk.accept_response(resp.timestamp);
            }
        }

        // Validate counter.
        {
            let lk = self.links.get(&peer).unwrap();
            match lk.validate_request(env.counter)? {
                ValidateResult::Ok => {}
                ValidateResult::Cached(cached) => return Ok(cached),
            }
        }

        // Determine timestamp.
        let lk = self.links.get(&peer).unwrap();
        let ts = if lk.dominant() {
            let mut t = now;
            if t < lk.ts {
                t = lk.ts;
            }
            t
        } else {
            env.timestamp
        };

        // Validate timestamp.
        self.links
            .get(&peer)
            .unwrap()
            .validate_timestamp(ts, now)?;

        // Dispatch request.
        let ok = self.dispatch_request(&peer, &env.method, env.payload.clone(), ts)?;

        // Accept and return response.
        let lk = self.links.get_mut(&peer).unwrap();
        let response = lk.accept_request(ok, ts);
        Ok(response)
    }

    // -----------------------------------------------------------------------
    // nextTx — collect from lifecycle + module outboxes
    // -----------------------------------------------------------------------

    /// Determine the next message to send for this peer.
    pub fn next_tx(&mut self, peer: &str) -> Option<QueuedMsg> {
        // Phase 1: Mutable link access — cancel opposing intents.
        {
            let lk = self.links.get_mut(peer)?;

            // Cancel opposing intents on established links.
            if lk.state.conn.intent_close
                && lk.state.conn.intent_connect
                && lk.state.conn.status.is_some()
            {
                lk.state.conn.intent_close = false;
                lk.state.conn.intent_connect = false;
            }

            // Module-level opposing intents.
            if lk.state.conn.status == Some(ConnectionStatus::Connected) {
                for mod_state in lk.state.modules.values_mut() {
                    if mod_state.intent_close
                        && mod_state.intent_connect
                        && mod_state.status.is_some()
                    {
                        mod_state.intent_close = false;
                        mod_state.intent_connect = false;
                    }
                }
            }
        }
        // Mutable borrow dropped — now we can call &self methods freely.

        // Phase 2: Read link state + ref count checks (immutable).
        let lk = self.links.get(peer)?;

        // Graceful link shutdown: close children individually, then link.
        if lk.state.conn.intent_close && lk.state.conn.status == Some(ConnectionStatus::Connected) {
            use std::sync::atomic::Ordering;
            // Close children one at a time, each waiting for RefCount == 0.
            for (name, mod_state) in &lk.state.modules {
                if mod_state.status == Some(ConnectionStatus::Connected)
                    && mod_state.ref_count.load(Ordering::Relaxed) == 0
                {
                    return Some(QueuedMsg {
                        method: "module.close".into(),
                        payload: Some(serde_json::json!({"name": name})),
                        queued_at: 0,
                        module: String::new(),
                        queue_id: 0,
                    });
                }
                if mod_state.status == Some(ConnectionStatus::Connected) {
                    return None; // Child still has refs — wait.
                }
            }
            // All children closed. Close link if root RefCount == 0.
            if lk.state.conn.ref_count.load(Ordering::Relaxed) == 0 {
                return Some(QueuedMsg {
                    method: "link.close".into(),
                    payload: None,
                    queued_at: 0,
                    module: String::new(),
                    queue_id: 0,
                });
            }
            return None; // Root still has refs — wait.
        }

        // Not-yet-connected link closing (e.g. cancel during handshake).
        if lk.state.conn.intent_close {
            return Some(QueuedMsg {
                method: "link.close".into(),
                payload: None,
                queued_at: 0,
                module: String::new(),
                queue_id: 0,
            });
        }

        if lk.state.conn.intent_connect {
            let root = lk.state.root.clone().unwrap_or_default();
            return Some(QueuedMsg {
                method: "link.connect".into(),
                payload: Some(serde_json::json!({"name": root})),
                queued_at: 0,
                module: String::new(),
                queue_id: 0,
            });
        }

        if lk.state.conn.status != Some(ConnectionStatus::Connected) {
            return None;
        }

        // Module close intents (from SetModules, not link-level shutdown).
        {
            use std::sync::atomic::Ordering;
            let close_name: Option<String> = lk
                .state
                .modules
                .iter()
                .find(|(_, s)| {
                    s.intent_close
                        && s.status == Some(ConnectionStatus::Connected)
                        && s.ref_count.load(Ordering::Relaxed) == 0
                })
                .map(|(name, _)| name.clone());
            if let Some(name) = close_name {
                return Some(QueuedMsg {
                    method: "module.close".into(),
                    payload: Some(serde_json::json!({"name": name})),
                    queued_at: 0,
                    module: String::new(),
                    queue_id: 0,
                });
            }
        }

        // Collect module connect intents.
        let connect_name: Option<String> = lk
            .state
            .modules
            .iter()
            .find(|(_, s)| s.intent_connect)
            .map(|(name, _)| name.clone());
        if let Some(name) = connect_name {
            return Some(QueuedMsg {
                method: "module.connect".into(),
                payload: Some(serde_json::json!({"name": name})),
                queued_at: 0,
                module: String::new(),
                queue_id: 0,
            });
        }

        // Phase 3: Collect from registry outboxes — oldest entry wins.
        let connected_modules: Vec<String> = self
            .modules
            .keys()
            .filter(|name| lk.module_connected(name))
            .cloned()
            .collect();

        let reg = self.registry.lock().unwrap();
        let mut best: Option<QueuedMsg> = None;
        for mod_name in &connected_modules {
            for candidate in reg.collect_outbox(mod_name, peer) {
                let msg = QueuedMsg {
                    method: format!("{}.{}", mod_name, candidate.method),
                    payload: candidate.payload,
                    queued_at: candidate.queued_at,
                    module: mod_name.clone(),
                    queue_id: candidate.queue_id,
                };
                let is_better = match &best {
                    None => true,
                    Some(b) => {
                        if msg.queued_at == 0 && b.queued_at == 0 {
                            false
                        } else if msg.queued_at == 0 {
                            false
                        } else if b.queued_at == 0 {
                            true
                        } else {
                            msg.queued_at < b.queued_at
                        }
                    }
                };
                if is_better {
                    best = Some(msg);
                }
            }
        }

        best
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::module::{HandleResult, Module};

    /// Minimal test module that tracks OnConnect/OnClose calls.
    struct TestModule {
        connected_peers: Vec<String>,
        closed_peers: Vec<String>,
    }

    impl TestModule {
        fn new() -> Self {
            TestModule {
                connected_peers: vec![],
                closed_peers: vec![],
            }
        }
    }

    impl Module for TestModule {
        fn on_connect(&mut self, peer: &str) {
            self.connected_peers.push(peer.to_string());
        }
        fn on_close(&mut self, peer: &str) {
            self.closed_peers.push(peer.to_string());
        }
        fn handle_request(
            &mut self,
            _peer: &str,
            _method: &str,
            _payload: Option<Value>,
            _ts: i64,
        ) -> Result<HandleResult, Box<dyn std::error::Error>> {
            Ok(HandleResult { ok: true })
        }
        fn handle_response(
            &mut self,
            _peer: &str,
            _method: &str,
            _payload: Option<Value>,
            _ok: bool,
            _ts: i64,
        ) {
        }
        fn handle_account(
            &mut self,
            _method: &str,
            _payload: Option<Value>,
        ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
            Err("unknown method".into())
        }
        fn status(&self) -> Option<Value> {
            None
        }
        fn force_remove(&mut self, peer: &str) {
            self.on_close(peer);
        }
        fn reattach_sync_objects(&mut self, _registry: &crate::registry::RegistryRef) {}
    }

    #[test]
    fn connect_creates_link_with_intent() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.register_module("trustline", Box::new(TestModule::new()));

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
    fn next_tx_returns_connect() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.register_module("trustline", Box::new(TestModule::new()));
        node.connect("bob@b.com", "trustline").unwrap();

        let msg = node.next_tx("bob@b.com").unwrap();
        assert_eq!(msg.method, "link.connect");
    }

    #[test]
    fn next_tx_cancels_opposing_intents() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.register_module("trustline", Box::new(TestModule::new()));
        node.connect("bob@b.com", "trustline").unwrap();

        let lk = node.get_mut("bob@b.com").unwrap();
        lk.state.conn.status = Some(ConnectionStatus::Connected);
        lk.state.conn.intent_connect = true;
        lk.state.conn.intent_close = true;

        let msg = node.next_tx("bob@b.com");
        assert!(msg.is_none());
        let lk = node.get("bob@b.com").unwrap();
        assert!(!lk.state.conn.intent_connect);
        assert!(!lk.state.conn.intent_close);
    }

    #[test]
    fn turn_bit_is_deterministic() {
        let lk1 = Link::new("alice@a.com", "bob@b.com");
        let lk2 = Link::new("bob@b.com", "alice@a.com");
        assert_ne!(lk1.turn_bit, lk2.turn_bit);
        assert_eq!(lk1.turn_bit + lk2.turn_bit, 1);
    }

    #[test]
    fn bilateral_connect_via_dispatch() {
        // Alice connects to Bob.
        let mut alice = Node::new("alice@a.com".into());
        alice.declare_root("trustline", vec![]);
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        // Alice builds link.connect.
        let msg = alice.next_tx("bob@b.com").unwrap();
        assert_eq!(msg.method, "link.connect");
        let env = alice.build_request("bob@b.com", &msg).unwrap();

        // Bob receives it (open mode for auto-accept).
        let mut bob = Node::new("bob@b.com".into());
        bob.declare_root("trustline", vec![]);
        bob.register_module("trustline", Box::new(TestModule::new()));
        bob.invite_mode = InviteMode::Open;

        let resp = bob.handle_incoming(&env).unwrap();
        assert!(resp.ok);

        // Bob's link should be Connected.
        let bob_lk = bob.get("alice@a.com").unwrap();
        assert_eq!(
            bob_lk.state.conn.status,
            Some(ConnectionStatus::Connected)
        );

        // Alice processes response.
        let lk = alice.get_mut("bob@b.com").unwrap();
        lk.pending = Some(msg.clone());
        alice.dispatch_response("bob@b.com", &msg.method, msg.payload, resp.ok, resp.timestamp);
        let lk = alice.get_mut("bob@b.com").unwrap();
        lk.accept_response(resp.timestamp);

        // Alice's link should be Connected.
        let alice_lk = alice.get("bob@b.com").unwrap();
        assert_eq!(
            alice_lk.state.conn.status,
            Some(ConnectionStatus::Connected)
        );
    }

    #[test]
    fn bilateral_close_via_dispatch() {
        // Set up connected link.
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.register_module("trustline", Box::new(TestModule::new()));
        node.connect("bob@b.com", "trustline").unwrap();
        let lk = node.get_mut("bob@b.com").unwrap();
        lk.state.conn.status = Some(ConnectionStatus::Connected);
        lk.state.conn.intent_connect = false;

        // Close.
        node.close("bob@b.com");
        let msg = node.next_tx("bob@b.com").unwrap();
        assert_eq!(msg.method, "link.close");

        // Simulate OK response.
        node.dispatch_response("bob@b.com", "link.close", None, true, 100);
        assert!(node.get("bob@b.com").is_none()); // link removed
    }
}
