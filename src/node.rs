//! Node — collection of bilateral links for an identity.
//!
//! Manages the set of links, SO instance dispatch, and the sync protocol
//! that drives bilateral communication. Messages are routed to SO instances
//! by their bilateral ID (so_id), not by namespace strings.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::link::{
    ConnectMode, ConnectionState, ConnectionStatus, Link, QueuedMsg, SoInstance,
    SyncError, TxRequest, TxResponse, ValidateResult, now_secs,
};
use crate::module::{HandleResult, Module, SoHandler};
use crate::registry::{self, RegistryRef};

/// Maximum number of concurrent links per node.
pub const MAX_LINKS: usize = 64;

/// Configuration for a declared SyncObject type.
#[derive(Debug, Clone)]
pub struct SoConfig {
    /// How this SO type connects (Auto or Invite).
    pub connect_mode: ConnectMode,
    /// Child SO types that can exist under this one.
    pub children: Vec<String>,
}

impl SoConfig {
    pub fn auto() -> Self {
        SoConfig {
            connect_mode: ConnectMode::Auto,
            children: vec![],
        }
    }

    pub fn invite() -> Self {
        SoConfig {
            connect_mode: ConnectMode::Invite,
            children: vec![],
        }
    }

    pub fn with_children(mut self, children: Vec<String>) -> Self {
        self.children = children;
        self
    }
}

/// True if `msg` is older than `best` (for oldest-first selection).
fn is_older(msg: &QueuedMsg, best: &Option<QueuedMsg>) -> bool {
    match best {
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

/// Key for looking up an SO handler: (peer, so_id).
type HandlerKey = (String, u64);

/// Node manages the set of bilateral links for an identity.
#[derive(Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub links: HashMap<String, Link>,
    /// Declared SO types and their configuration.
    #[serde(skip)]
    so_types: HashMap<String, SoConfig>,
    /// Registered modules by name (legacy — used during migration).
    #[serde(skip)]
    modules: HashMap<String, Box<dyn Module>>,
    /// SO handlers keyed by (peer, so_id).
    #[serde(skip)]
    handlers: HashMap<HandlerKey, Box<dyn SoHandler>>,
    /// Factory functions for creating SO handlers, keyed by SO type name.
    #[serde(skip)]
    so_factories: HashMap<String, Box<dyn Fn() -> Box<dyn SoHandler> + Send + Sync>>,
    /// Next SO instance ID to assign.
    #[serde(default)]
    next_so_id: u64,
    /// Framework-owned outbox registry (queues, kicks, ref counts).
    #[serde(skip)]
    registry: RegistryRef,
}

impl Node {
    pub fn new(id: String) -> Self {
        Node {
            id,
            links: HashMap::new(),
            so_types: HashMap::new(),
            modules: HashMap::new(),
            handlers: HashMap::new(),
            so_factories: HashMap::new(),
            next_so_id: 1, // 0 reserved for lifecycle
            registry: registry::new_registry(),
        }
    }

    /// Declare a SyncObject type with its configuration.
    pub fn declare_so(&mut self, name: &str, config: SoConfig) {
        self.so_types.insert(name.to_string(), config);
    }

    /// Declare a SO type with a handler factory.
    pub fn declare_so_with_factory<F>(&mut self, name: &str, config: SoConfig, factory: F)
    where
        F: Fn() -> Box<dyn SoHandler> + Send + Sync + 'static,
    {
        self.so_types.insert(name.to_string(), config);
        self.so_factories.insert(name.to_string(), Box::new(factory));
    }

    /// Backwards-compatible: declare a root with children (all Auto mode).
    pub fn declare_root(&mut self, name: &str, children: Vec<String>) {
        self.so_types
            .insert(name.to_string(), SoConfig::auto().with_children(children));
    }

    /// Register a module instance (legacy — use declare_so_with_factory instead).
    pub fn register_module(&mut self, name: &str, module: Box<dyn Module>) {
        self.modules.insert(name.to_string(), module);
    }

    /// Get a clone of the registry reference (for passing to modules/SyncObjects).
    pub fn registry(&self) -> RegistryRef {
        self.registry.clone()
    }

    /// Allocate a new bilateral SO instance ID.
    fn alloc_so_id(&mut self) -> u64 {
        let id = self.next_so_id;
        self.next_so_id += 1;
        id
    }

    /// Get an SO handler by (peer, so_id).
    pub fn handler(&self, peer: &str, so_id: u64) -> Option<&Box<dyn SoHandler>> {
        self.handlers.get(&(peer.to_string(), so_id))
    }

    /// Get a mutable SO handler by (peer, so_id).
    pub fn handler_mut(&mut self, peer: &str, so_id: u64) -> Option<&mut Box<dyn SoHandler>> {
        self.handlers.get_mut(&(peer.to_string(), so_id))
    }

    /// Create a new SO handler instance from a registered factory.
    fn create_handler(&self, so_type: &str) -> Option<Box<dyn SoHandler>> {
        self.so_factories.get(so_type).map(|f| f())
    }

    // -----------------------------------------------------------------------
    // Legacy methods — kept during migration
    // -----------------------------------------------------------------------

    /// Get the ConnectionState for a module on a peer's link.
    pub fn connection_state(&self, peer: &str, module: &str) -> Option<&ConnectionState> {
        let lk = self.links.get(peer)?;
        if lk.state.root.as_deref() == Some(module) {
            Some(&lk.state.conn)
        } else {
            lk.state.modules.get(module)
        }
    }

    pub fn connection_state_mut(
        &mut self,
        peer: &str,
        module: &str,
    ) -> Option<&mut ConnectionState> {
        let lk = self.links.get_mut(peer)?;
        if lk.state.root.as_deref() == Some(module) {
            Some(&mut lk.state.conn)
        } else {
            lk.state.modules.get_mut(module)
        }
    }

    pub fn child_tracker(
        &self,
        peer: &str,
        module: &str,
    ) -> Option<crate::link::ChildTracker> {
        self.connection_state(peer, module)
            .map(|cs| cs.children.clone())
    }

    /// True if name is a declared SO type.
    pub fn is_root(&self, name: &str) -> bool {
        self.so_types.contains_key(name)
    }

    /// True if name is a declared child of the given parent SO type.
    pub fn is_child_module(&self, name: &str, parent: &str) -> bool {
        self.so_types
            .get(parent)
            .map_or(false, |cfg| cfg.children.iter().any(|c| c == name))
    }

    pub fn so_config(&self, name: &str) -> Option<&SoConfig> {
        self.so_types.get(name)
    }

    pub fn connect_mode_for(&self, name: &str) -> ConnectMode {
        self.so_types
            .get(name)
            .map_or(ConnectMode::Invite, |cfg| cfg.connect_mode)
    }

    pub fn set_connect_mode(&mut self, name: &str, mode: ConnectMode) {
        if let Some(cfg) = self.so_types.get_mut(name) {
            cfg.connect_mode = mode;
        }
    }

    pub fn get(&self, peer: &str) -> Option<&Link> {
        self.links.get(peer)
    }

    pub fn get_mut(&mut self, peer: &str) -> Option<&mut Link> {
        self.links.get_mut(peer)
    }

    pub fn module_map(&self) -> &HashMap<String, Box<dyn Module>> {
        &self.modules
    }

    pub fn module_mut(&mut self, name: &str) -> Option<&mut Box<dyn Module>> {
        self.modules.get_mut(name)
    }

    /// Create a new outgoing link to peer running the named root SO type.
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

    pub fn module_available(&self, peer: &str, module: &str) -> bool {
        self.links
            .get(peer)
            .map_or(false, |lk| lk.module_available(module))
    }

    /// Unilaterally remove a link.
    pub fn force_remove(&mut self, peer: &str) {
        if let Some(lk) = self.links.get(peer) {
            let root = lk.state.root.clone();
            let mod_names: Vec<String> = lk.state.modules.keys().cloned().collect();
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
            // Also clean up SO handlers for this peer.
            let so_ids: Vec<u64> = lk.so_table.instances.keys().cloned().collect();
            for so_id in so_ids {
                if let Some(mut h) = self.handlers.remove(&(peer.to_string(), so_id)) {
                    h.force_remove(peer);
                }
            }
        }
        self.links.remove(peer);
    }

    // -----------------------------------------------------------------------
    // SO instance creation — framework creates handlers
    // -----------------------------------------------------------------------

    /// Create a root SO instance on a link when it connects.
    fn create_root_so(&mut self, peer: &str, so_type: &str, payload: Option<Value>) {
        let so_id = self.alloc_so_id();
        let instance = SoInstance {
            so_type: so_type.to_string(),
            parent_id: None,
            status: ConnectionStatus::Connected,
            children: Default::default(),
            intent_close: false,
        };
        if let Some(lk) = self.links.get_mut(peer) {
            lk.so_table.insert(so_id, instance);
        }
        // Create handler from factory and call on_connect.
        if let Some(mut handler) = self.create_handler(so_type) {
            handler.on_connect(peer, payload);
            self.handlers
                .insert((peer.to_string(), so_id), handler);
        }
    }

    // -----------------------------------------------------------------------
    // Ref checks
    // -----------------------------------------------------------------------

    fn has_active_refs(&self, peer: &str) -> bool {
        let lk = match self.links.get(peer) {
            Some(lk) => lk,
            None => return false,
        };
        if lk.state.conn.children.has_connected() {
            return true;
        }
        for cs in lk.state.modules.values() {
            if cs.children.has_connected() {
                return true;
            }
        }
        // Also check SO table.
        for inst in lk.so_table.instances.values() {
            if inst.children.has_connected() {
                return true;
            }
        }
        false
    }

    // -----------------------------------------------------------------------
    // Transport auto-connect
    // -----------------------------------------------------------------------

    fn auto_connect_on_response(&self, queue_id: u64, ok: bool, peer: &str) {
        if !ok || queue_id == 0 {
            return;
        }
        let module_name = {
            let reg = self.registry.lock().unwrap();
            match reg.get_queue(queue_id) {
                Some(q) => q.module.clone(),
                None => return,
            }
        };
        match self.so_types.get(&module_name) {
            Some(cfg) if cfg.connect_mode == ConnectMode::Auto => {}
            _ => return,
        }
        if let Some(cs) = self.connection_state(peer, &module_name) {
            if !cs.children.is_connected(queue_id) {
                cs.children.set_connected(queue_id);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Dispatch — incoming request
    // -----------------------------------------------------------------------

    /// Dispatch an incoming sync request.
    /// Routes by so_id for SO instance messages, by namespace for legacy.
    pub fn dispatch_request(
        &mut self,
        peer: &str,
        so_id: u64,
        full_method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<bool, SyncError> {
        // SO-id based routing: if so_id > 0, route to that instance.
        if so_id > 0 {
            let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
            if !lk.so_connected(so_id) {
                return Err(SyncError::BadMethod);
            }
            let key = (peer.to_string(), so_id);
            let handler = self.handlers.get_mut(&key).ok_or(SyncError::BadMethod)?;
            match handler.handle_request(peer, full_method, payload, ts) {
                Ok(r) => return Ok(r.ok),
                Err(e) => return Err(SyncError::Other(e.to_string())),
            }
        }

        // Legacy namespace-based routing for lifecycle and old-style modules.
        let (ns, method) = split_method(full_method);

        if ns == "link" {
            return self.dispatch_link_request(peer, method, payload);
        }
        if ns == "module" {
            return self.dispatch_module_request(peer, method, payload);
        }
        // Application module message (legacy).
        let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
        if !lk.module_connected(ns) {
            return Err(SyncError::BadMethod);
        }
        let m = self.modules.get_mut(ns).ok_or(SyncError::BadMethod)?;
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
                if lk.state.root.as_deref() != Some(name) {
                    return Ok(false);
                }
                let status = lk.state.conn.status.clone();
                let is_auto = self.connect_mode_for(name) == ConnectMode::Auto;
                if status == Some(ConnectionStatus::Waiting) || is_auto {
                    let lk = self.links.get_mut(peer).unwrap();
                    lk.state.conn.status = Some(ConnectionStatus::Connected);
                    lk.state.conn.intent_connect = false;
                    let root = lk.state.root.clone().unwrap_or_default();
                    // Create root SO instance.
                    self.create_root_so(peer, &root, payload.clone());
                    // Legacy: also call module on_connect.
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        m.on_connect(peer, payload);
                    }
                    return Ok(true);
                }
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
                    let mod_names: Vec<String> = lk
                        .state
                        .modules
                        .iter()
                        .filter(|(_, c)| c.status == Some(ConnectionStatus::Connected))
                        .map(|(n, _)| n.clone())
                        .collect();
                    let root = lk.state.root.clone().unwrap_or_default();
                    // Close SO handlers.
                    let so_ids: Vec<u64> = lk.so_table.instances.keys().cloned().collect();
                    for so_id in so_ids {
                        if let Some(mut h) =
                            self.handlers.remove(&(peer.to_string(), so_id))
                        {
                            h.on_close(peer, None);
                        }
                    }
                    // Legacy: close modules.
                    for name in &mod_names {
                        if let Some(m) = self.modules.get_mut(name.as_str()) {
                            m.on_close(peer, None);
                        }
                    }
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        m.on_close(peer, None);
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
                let mode = self.connect_mode_for(name);
                let lk = self.links.get_mut(peer).unwrap();
                let mod_state =
                    lk.state.modules.entry(name.to_string()).or_default();
                if mod_state.status == Some(ConnectionStatus::Connected) {
                    return Ok(true);
                }
                if mod_state.intent_connect
                    || mod_state.status == Some(ConnectionStatus::Waiting)
                {
                    mod_state.status = Some(ConnectionStatus::Connected);
                    mod_state.intent_connect = false;
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_connect(peer, payload);
                    }
                    return Ok(true);
                }
                if mode == ConnectMode::Auto {
                    mod_state.status = Some(ConnectionStatus::Connected);
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_connect(peer, payload);
                    }
                    return Ok(true);
                }
                if mode == ConnectMode::Closed {
                    return Ok(false);
                }
                mod_state.status = Some(ConnectionStatus::Incoming);
                Ok(false)
            }
            "close" => {
                if name.is_empty() || !self.is_child_module(name, &root) {
                    return Ok(false);
                }
                if let Some(cs) = self.connection_state(peer, name) {
                    if cs.children.has_connected() {
                        return Err(SyncError::RefsHeld);
                    }
                }
                let lk = self.links.get_mut(peer).unwrap();
                let was_connected = lk
                    .state
                    .modules
                    .get(name)
                    .map_or(false, |s| {
                        s.status == Some(ConnectionStatus::Connected)
                    });
                if !lk.state.modules.contains_key(name) {
                    return Ok(true);
                }
                lk.state.modules.remove(name);
                if was_connected {
                    if let Some(m) = self.modules.get_mut(name) {
                        m.on_close(peer, payload);
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

    pub fn dispatch_response(
        &mut self,
        peer: &str,
        so_id: u64,
        full_method: &str,
        payload: Option<Value>,
        ok: bool,
        ts: i64,
    ) {
        // SO-id based routing.
        if so_id > 0 {
            let key = (peer.to_string(), so_id);
            if let Some(handler) = self.handlers.get_mut(&key) {
                handler.handle_response(peer, full_method, payload, ok, ts);
            }
            return;
        }

        // Legacy namespace-based routing.
        let (ns, method) = split_method(full_method);
        if ns == "link" {
            self.dispatch_link_response(peer, method, payload, ok);
            return;
        }
        if ns == "module" {
            self.dispatch_module_response(peer, method, payload, ok);
            return;
        }
        if let Some(m) = self.modules.get_mut(ns) {
            m.handle_response(peer, method, payload, ok, ts);
        }
    }

    fn dispatch_link_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ok: bool,
    ) {
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
                    // Create root SO instance.
                    self.create_root_so(peer, &root, payload.clone());
                    // Legacy: also call module on_connect.
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        m.on_connect(peer, payload);
                    }
                } else {
                    lk.state.conn.status = Some(ConnectionStatus::Waiting);
                    lk.state.conn.intent_connect = false;
                }
            }
            "close" => {
                if ok {
                    let close_payload = payload;
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
                        // Close SO handlers.
                        let so_ids: Vec<u64> =
                            lk.so_table.instances.keys().cloned().collect();
                        for so_id in so_ids {
                            if let Some(mut h) =
                                self.handlers.remove(&(peer.to_string(), so_id))
                            {
                                h.on_close(peer, close_payload.clone());
                            }
                        }
                        // Legacy.
                        for name in &mod_names {
                            if let Some(m) = self.modules.get_mut(name.as_str()) {
                                m.on_close(peer, close_payload.clone());
                            }
                        }
                        if let Some(m) = self.modules.get_mut(root.as_str()) {
                            m.on_close(peer, close_payload);
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
                        m.on_connect(peer, payload);
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
                            m.on_close(peer, payload);
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
            if self.connect_mode_for(root_name) == ConnectMode::Closed {
                return Ok(TxResponse {
                    ok: false,
                    timestamp: now_secs(),
                });
            }
            if self.links.len() >= MAX_LINKS {
                return Err(SyncError::MaxLinks);
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
                lk.validate_timestamp(resp.timestamp, now)?;

                let pending_so_id;
                let pending_method;
                let pending_payload;
                let pending_queue_id;
                {
                    let lk = self.links.get(&peer).unwrap();
                    let p = lk.pending.as_ref().unwrap();
                    pending_so_id = p.so_id;
                    pending_method = p.method.clone();
                    pending_payload = p.payload.clone();
                    pending_queue_id = p.queue_id;
                }

                self.dispatch_response(
                    &peer,
                    pending_so_id,
                    &pending_method,
                    pending_payload,
                    resp.ok,
                    resp.timestamp,
                );

                self.auto_connect_on_response(pending_queue_id, resp.ok, &peer);

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

        self.links
            .get(&peer)
            .unwrap()
            .validate_timestamp(ts, now)?;

        // Dispatch request with so_id.
        let ok = self.dispatch_request(&peer, env.so_id, &env.method, env.payload.clone(), ts)?;

        let lk = self.links.get_mut(&peer).unwrap();
        let response = lk.accept_request(ok, ts);
        Ok(response)
    }

    // -----------------------------------------------------------------------
    // nextTx
    // -----------------------------------------------------------------------

    pub fn next_tx(&mut self, peer: &str) -> Option<QueuedMsg> {
        // Phase 1: cancel opposing intents.
        {
            let lk = self.links.get_mut(peer)?;
            if lk.state.conn.intent_close
                && lk.state.conn.intent_connect
                && lk.state.conn.status.is_some()
            {
                lk.state.conn.intent_close = false;
                lk.state.conn.intent_connect = false;
            }
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

        let lk = self.links.get(peer)?;

        // Graceful link shutdown.
        if lk.state.conn.intent_close
            && lk.state.conn.status == Some(ConnectionStatus::Connected)
        {
            // Close SO instances first.
            for (&so_id, inst) in &lk.so_table.instances {
                if inst.status == ConnectionStatus::Connected
                    && !inst.children.has_connected()
                    && !inst.intent_close
                {
                    // TODO: emit SO close message
                }
            }
            // Close legacy modules.
            for (name, mod_state) in &lk.state.modules {
                if mod_state.status == Some(ConnectionStatus::Connected)
                    && !mod_state.children.has_connected()
                {
                    return Some(QueuedMsg {
                        so_id: 0,
                        method: "module.close".into(),
                        payload: Some(serde_json::json!({"name": name})),
                        queued_at: 0,
                        queue_id: 0,
                    });
                }
                if mod_state.status == Some(ConnectionStatus::Connected) {
                    return None;
                }
            }
            if !lk.state.conn.children.has_connected() {
                return Some(QueuedMsg {
                    so_id: 0,
                    method: "link.close".into(),
                    payload: None,
                    queued_at: 0,
                    queue_id: 0,
                });
            }
            return None;
        }

        if lk.state.conn.intent_close {
            return Some(QueuedMsg {
                so_id: 0,
                method: "link.close".into(),
                payload: None,
                queued_at: 0,
                queue_id: 0,
            });
        }

        if lk.state.conn.intent_connect {
            let root = lk.state.root.clone().unwrap_or_default();
            return Some(QueuedMsg {
                so_id: 0,
                method: "link.connect".into(),
                payload: Some(serde_json::json!({"name": root})),
                queued_at: 0,
                queue_id: 0,
            });
        }

        if lk.state.conn.status != Some(ConnectionStatus::Connected) {
            return None;
        }

        // Module close intents.
        {
            let close_name: Option<String> = lk
                .state
                .modules
                .iter()
                .find(|(_, s)| {
                    s.intent_close
                        && s.status == Some(ConnectionStatus::Connected)
                        && !s.children.has_connected()
                })
                .map(|(name, _)| name.clone());
            if let Some(name) = close_name {
                return Some(QueuedMsg {
                    so_id: 0,
                    method: "module.close".into(),
                    payload: Some(serde_json::json!({"name": name})),
                    queued_at: 0,
                    queue_id: 0,
                });
            }
        }

        // Module connect intents.
        let connect_name: Option<String> = lk
            .state
            .modules
            .iter()
            .find(|(_, s)| s.intent_connect)
            .map(|(name, _)| name.clone());
        if let Some(name) = connect_name {
            return Some(QueuedMsg {
                so_id: 0,
                method: "module.connect".into(),
                payload: Some(serde_json::json!({"name": name})),
                queued_at: 0,
                queue_id: 0,
            });
        }

        // Collect from registry outboxes — oldest entry wins.
        let reg = self.registry.lock().unwrap();
        let mut best: Option<QueuedMsg> = None;

        // SO-instance queues (new system).
        for (&so_id, inst) in &lk.so_table.instances {
            if inst.status != ConnectionStatus::Connected {
                continue;
            }
            for candidate in reg.collect_outbox_by_so(so_id, peer) {
                let msg = QueuedMsg {
                    so_id,
                    method: candidate.method,
                    payload: candidate.payload,
                    queued_at: candidate.queued_at,
                    queue_id: candidate.queue_id,
                };
                if is_older(&msg, &best) {
                    best = Some(msg);
                }
            }
        }

        // Legacy module queues (namespace-routed).
        let connected_modules: Vec<String> = self
            .modules
            .keys()
            .filter(|name| lk.module_connected(name))
            .cloned()
            .collect();

        for mod_name in &connected_modules {
            for candidate in reg.collect_outbox(mod_name, peer) {
                let msg = QueuedMsg {
                    so_id: 0,
                    method: format!("{}.{}", mod_name, candidate.method),
                    payload: candidate.payload,
                    queued_at: candidate.queued_at,
                    queue_id: candidate.queue_id,
                };
                if is_older(&msg, &best) {
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
            so_id: msg.so_id,
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
    use crate::module::{HandleResult, Module, SoHandler};

    /// Minimal test module (legacy Module trait).
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
        fn on_connect(&mut self, peer: &str, _payload: Option<Value>) {
            self.connected_peers.push(peer.to_string());
        }
        fn on_close(&mut self, peer: &str, _payload: Option<Value>) {
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
        fn force_remove(&mut self, peer: &str) {
            self.on_close(peer, None);
        }
        fn reattach_sync_objects(&mut self, _registry: &crate::registry::RegistryRef) {}
    }

    /// Test SO handler for the new system.
    struct TestSoHandler {
        connected: bool,
        requests: Vec<String>,
    }

    impl TestSoHandler {
        fn new() -> Self {
            TestSoHandler {
                connected: false,
                requests: vec![],
            }
        }
    }

    impl SoHandler for TestSoHandler {
        fn on_connect(&mut self, _peer: &str, _payload: Option<Value>) {
            self.connected = true;
        }
        fn on_close(&mut self, _peer: &str, _payload: Option<Value>) {
            self.connected = false;
        }
        fn handle_request(
            &mut self,
            _peer: &str,
            method: &str,
            _payload: Option<Value>,
            _ts: i64,
        ) -> Result<HandleResult, Box<dyn std::error::Error>> {
            self.requests.push(method.to_string());
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
        fn force_remove(&mut self, peer: &str) {
            self.on_close(peer, None);
        }
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
        let mut alice = Node::new("alice@a.com".into());
        alice.declare_root("trustline", vec![]);
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        let msg = alice.next_tx("bob@b.com").unwrap();
        assert_eq!(msg.method, "link.connect");
        let env = alice.build_request("bob@b.com", &msg).unwrap();

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so("trustline", SoConfig::auto());
        bob.register_module("trustline", Box::new(TestModule::new()));

        let resp = bob.handle_incoming(&env).unwrap();
        assert!(resp.ok);

        let bob_lk = bob.get("alice@a.com").unwrap();
        assert_eq!(
            bob_lk.state.conn.status,
            Some(ConnectionStatus::Connected)
        );

        let lk = alice.get_mut("bob@b.com").unwrap();
        lk.pending = Some(msg.clone());
        alice.dispatch_response(
            "bob@b.com",
            msg.so_id,
            &msg.method,
            msg.payload,
            resp.ok,
            resp.timestamp,
        );
        let lk = alice.get_mut("bob@b.com").unwrap();
        lk.accept_response(resp.timestamp);

        let alice_lk = alice.get("bob@b.com").unwrap();
        assert_eq!(
            alice_lk.state.conn.status,
            Some(ConnectionStatus::Connected)
        );
    }

    #[test]
    fn bilateral_close_via_dispatch() {
        let mut node = Node::new("alice@a.com".into());
        node.declare_root("trustline", vec![]);
        node.register_module("trustline", Box::new(TestModule::new()));
        node.connect("bob@b.com", "trustline").unwrap();
        let lk = node.get_mut("bob@b.com").unwrap();
        lk.state.conn.status = Some(ConnectionStatus::Connected);
        lk.state.conn.intent_connect = false;

        node.close("bob@b.com");
        let msg = node.next_tx("bob@b.com").unwrap();
        assert_eq!(msg.method, "link.close");

        node.dispatch_response("bob@b.com", 0, "link.close", None, true, 100);
        assert!(node.get("bob@b.com").is_none());
    }

    #[test]
    fn so_handler_factory_creates_on_connect() {
        let mut alice = Node::new("alice@a.com".into());
        alice.declare_so_with_factory("trustline", SoConfig::auto(), || {
            Box::new(TestSoHandler::new())
        });
        // Also register legacy module for the connect handshake.
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        let msg = alice.next_tx("bob@b.com").unwrap();
        let env = alice.build_request("bob@b.com", &msg).unwrap();

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so_with_factory("trustline", SoConfig::auto(), || {
            Box::new(TestSoHandler::new())
        });
        bob.register_module("trustline", Box::new(TestModule::new()));

        let resp = bob.handle_incoming(&env).unwrap();
        assert!(resp.ok);

        // Bob should have a root SO instance in the table.
        let bob_lk = bob.get("alice@a.com").unwrap();
        assert_eq!(bob_lk.so_table.instances.len(), 1);
        let (_, inst) = bob_lk.so_table.root().unwrap();
        assert_eq!(inst.so_type, "trustline");
        assert_eq!(inst.status, ConnectionStatus::Connected);
    }
}
