//! Node — collection of bilateral links for an identity.
//!
//! Manages the set of links and the sync protocol that drives bilateral
//! communication. SyncObjects (SOs) are pure data (ref-counted queues);
//! the Module trait handles all application logic.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::link::{
    ConnectMode, ConnectionState, ConnectionStatus, Link, QueuedMsg, SoInstance,
    SyncError, TxRequest, TxResponse, ValidateResult, now_secs,
};
use crate::module::Module;
use crate::registry::{self, RegistryRef};

/// Maximum number of concurrent links per node.
pub const MAX_LINKS: usize = 64;

/// Configuration for a declared module/SO type.
#[derive(Debug, Clone)]
pub struct SoConfig {
    /// How this module connects (Auto or Invite).
    pub connect_mode: ConnectMode,
    /// Child module types that can exist under this one.
    pub children: Vec<String>,
    /// Whether the framework creates an SoInstance (queue + ref count) on connect.
    /// Modules with has_so=false get dispatch only — no queue, no SoInstance.
    pub has_so: bool,
}

impl SoConfig {
    pub fn auto() -> Self {
        SoConfig {
            connect_mode: ConnectMode::Auto,
            children: vec![],
            has_so: true,
        }
    }

    pub fn invite() -> Self {
        SoConfig {
            connect_mode: ConnectMode::Invite,
            children: vec![],
            has_so: true,
        }
    }

    /// Dispatch-only: module gets routed messages but no SoInstance/queue.
    pub fn dispatch_only() -> Self {
        SoConfig {
            connect_mode: ConnectMode::Auto,
            children: vec![],
            has_so: false,
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

/// Node manages the set of bilateral links for an identity.
#[derive(Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub links: HashMap<String, Link>,
    /// Declared SO types and their configuration.
    #[serde(skip)]
    so_types: HashMap<String, SoConfig>,
    /// Registered modules by name.
    #[serde(skip)]
    modules: HashMap<String, Box<dyn Module>>,
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
            next_so_id: 1, // 0 reserved for lifecycle
            registry: registry::new_registry(),
        }
    }

    /// Declare a SyncObject type with its configuration.
    pub fn declare_so(&mut self, name: &str, config: SoConfig) {
        self.so_types.insert(name.to_string(), config);
    }

    /// Declare a root module with children but no SO (dispatch only).
    /// The module gets on_connect/on_close and message routing,
    /// but no SoInstance or queue is created by the framework.
    pub fn declare_root(&mut self, name: &str, children: Vec<String>) {
        self.so_types
            .insert(name.to_string(), SoConfig::dispatch_only().with_children(children));
    }

    /// Register a module instance.
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

    // -----------------------------------------------------------------------
    // Accessors
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
            // Clean up SO instances for this peer.
            // (SOs are data only; Module.force_remove handles app-level cleanup.)
        }
        self.links.remove(peer);
    }

    // -----------------------------------------------------------------------
    // SO instance creation — SOs are pure data (queue + ref count)
    // -----------------------------------------------------------------------

    /// Create a root SO instance on a link when it connects.
    /// The SO is data only; the Module handles all logic.
    fn create_root_so(&mut self, peer: &str, so_type: &str) {
        let so_id = self.alloc_so_id();
        // Register a queue in the registry for this SO instance.
        self.registry
            .lock()
            .unwrap()
            .register_so(peer, so_id, so_type);

        let instance = SoInstance {
            so_type: so_type.to_string(),
            parent_id: None,
            status: ConnectionStatus::Connected,
            children: Default::default(),
            so_children: Vec::new(),
        };
        if let Some(lk) = self.links.get_mut(peer) {
            lk.so_table.insert(so_id, instance);
        }
    }

    // -----------------------------------------------------------------------
    // Pending child finalization — assign so_id = counter after transaction
    // -----------------------------------------------------------------------

    /// After a successful transaction, assign so_id to any pending child
    /// that was in Waiting state (sent but not yet confirmed).
    /// Both sender and receiver call this with the same counter value,
    /// so both sides derive the same child so_id.
    fn finalize_pending_children(&mut self, peer: &str, tx_counter: u64) {
        if let Some(lk) = self.links.get_mut(peer) {
            for inst in lk.so_table.instances.values_mut() {
                for child in inst.so_children.iter_mut() {
                    if child.so_id.is_none()
                        && child.status == Some(ConnectionStatus::Waiting)
                    {
                        child.so_id = Some(tx_counter);
                        child.status = Some(ConnectionStatus::Connected);
                        return; // one at a time
                    }
                }
            }
        }
    }

    /// Handle NACK on a pending child create message.
    /// - Auto: remove the child (shouldn't have been NACKed)
    /// - Invite/Closed: keep in Waiting (mutual-add / accept flow)
    fn handle_nacked_children(&mut self, peer: &str) {
        let so_types = &self.so_types;
        if let Some(lk) = self.links.get_mut(peer) {
            for inst in lk.so_table.instances.values_mut() {
                inst.so_children.retain(|child| {
                    if child.so_id.is_none()
                        && child.status == Some(ConnectionStatus::Waiting)
                    {
                        let mode = so_types
                            .get(&child.so_type)
                            .map_or(ConnectMode::Auto, |cfg| cfg.connect_mode);
                        // Auto → remove; Invite/Closed → keep in Waiting
                        mode != ConnectMode::Auto
                    } else {
                        true
                    }
                });
            }
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
        // SO-id based routing: if so_id > 0, route to Module by SO type.
        if so_id > 0 {
            let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
            if !lk.so_connected(so_id) {
                return Err(SyncError::BadMethod);
            }
            let so_type = lk.so_table.get(so_id)
                .map(|inst| inst.so_type.clone())
                .ok_or(SyncError::BadMethod)?;
            let m = self.modules.get_mut(&so_type).ok_or(SyncError::BadMethod)?;
            match m.handle_request(peer, full_method, payload, ts) {
                Ok(r) => return Ok(r.ok),
                Err(e) => return Err(SyncError::Other(e.to_string())),
            }
        }

        // Legacy namespace-based routing for lifecycle and old-style modules.
        let (ns, method) = split_method(full_method);

        if ns == "link" {
            return self.dispatch_link_request(peer, method, payload, ts);
        }
        if ns == "module" {
            return self.dispatch_module_request(peer, method, payload, ts);
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
        ts: i64,
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
                    let root = lk.state.root.clone().unwrap_or_default();
                    // 1. Call module handler first (custom init).
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        let method = m.connects_on().to_string();
                        match m.handle_request(peer, &method, payload, ts) {
                            Ok(r) if !r.ok => return Ok(false),
                            Err(_) => return Ok(false),
                            _ => {}
                        }
                    }
                    // 2. Common state logic.
                    let lk = self.links.get_mut(peer).unwrap();
                    lk.state.conn.status = Some(ConnectionStatus::Connected);
                    lk.state.conn.intent_connect = false;
                    if self.so_types.get(&root).map_or(false, |c| c.has_so) {
                        self.create_root_so(peer, &root);
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
                    // Call module handlers for close (cleanup).
                    for name in &mod_names {
                        if let Some(m) = self.modules.get_mut(name.as_str()) {
                            let close_method = m.closes_on().first()
                                .unwrap_or(&"close").to_string();
                            let _ = m.handle_request(peer, &close_method, None, ts);
                        }
                    }
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        let close_method = m.closes_on().first()
                            .unwrap_or(&"close").to_string();
                        let _ = m.handle_request(peer, &close_method, None, ts);
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
        ts: i64,
    ) -> Result<bool, SyncError> {
        let name: String = payload
            .as_ref()
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let lk = self.links.get(peer).ok_or(SyncError::NoLink)?;
        let root = lk.state.root.clone().unwrap_or_default();

        match method {
            "connect" => {
                if name.is_empty() || !self.is_child_module(&name, &root) {
                    return Ok(false);
                }
                if !self.modules.contains_key(&name) {
                    return Ok(false);
                }
                let mode = self.connect_mode_for(&name);
                let lk = self.links.get_mut(peer).unwrap();
                let mod_state =
                    lk.state.modules.entry(name.clone()).or_default();
                if mod_state.status == Some(ConnectionStatus::Connected) {
                    return Ok(true);
                }
                if mod_state.intent_connect
                    || mod_state.status == Some(ConnectionStatus::Waiting)
                {
                    // 1. Call module handler first.
                    if let Some(m) = self.modules.get_mut(name.as_str()) {
                        let method = m.connects_on().to_string();
                        match m.handle_request(peer, &method, payload, ts) {
                            Ok(r) if !r.ok => return Ok(false),
                            Err(_) => return Ok(false),
                            _ => {}
                        }
                    }
                    // 2. Common state logic.
                    let lk = self.links.get_mut(peer).unwrap();
                    let mod_state = lk.state.modules.get_mut(&name).unwrap();
                    mod_state.status = Some(ConnectionStatus::Connected);
                    mod_state.intent_connect = false;
                    return Ok(true);
                }
                if mode == ConnectMode::Auto {
                    // 1. Call module handler first.
                    if let Some(m) = self.modules.get_mut(name.as_str()) {
                        let method = m.connects_on().to_string();
                        match m.handle_request(peer, &method, payload, ts) {
                            Ok(r) if !r.ok => return Ok(false),
                            Err(_) => return Ok(false),
                            _ => {}
                        }
                    }
                    // 2. Common state logic.
                    let lk = self.links.get_mut(peer).unwrap();
                    let mod_state = lk.state.modules.get_mut(&name).unwrap();
                    mod_state.status = Some(ConnectionStatus::Connected);
                    return Ok(true);
                }
                if mode == ConnectMode::Closed {
                    return Ok(false);
                }
                mod_state.status = Some(ConnectionStatus::Incoming);
                Ok(false)
            }
            "close" => {
                if name.is_empty() || !self.is_child_module(&name, &root) {
                    return Ok(false);
                }
                if let Some(cs) = self.connection_state(peer, &name) {
                    if cs.children.has_connected() {
                        return Err(SyncError::RefsHeld);
                    }
                }
                let lk = self.links.get_mut(peer).unwrap();
                let was_connected = lk
                    .state
                    .modules
                    .get(&name)
                    .map_or(false, |s| {
                        s.status == Some(ConnectionStatus::Connected)
                    });
                if !lk.state.modules.contains_key(&name) {
                    return Ok(true);
                }
                lk.state.modules.remove(&name);
                if was_connected {
                    if let Some(m) = self.modules.get_mut(name.as_str()) {
                        let close_method = m.closes_on().first()
                            .unwrap_or(&"close").to_string();
                        let _ = m.handle_request(peer, &close_method, payload, ts);
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
        // SO-id based routing: route to Module by SO type.
        if so_id > 0 {
            let so_type = self.links.get(peer)
                .and_then(|lk| lk.so_table.get(so_id))
                .map(|inst| inst.so_type.clone());
            if let Some(so_type) = so_type {
                if let Some(m) = self.modules.get_mut(&so_type) {
                    m.handle_response(peer, full_method, payload, ok, ts);
                }
            }
            return;
        }

        // Legacy namespace-based routing.
        let (ns, method) = split_method(full_method);
        if ns == "link" {
            self.dispatch_link_response(peer, method, payload, ok, ts);
            return;
        }
        if ns == "module" {
            self.dispatch_module_response(peer, method, payload, ok, ts);
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
        ts: i64,
    ) {
        match method {
            "connect" => {
                let lk = match self.links.get_mut(peer) {
                    Some(lk) => lk,
                    None => return,
                };
                if ok {
                    let root = lk.state.root.clone().unwrap_or_default();
                    // 1. Call module handler first (notification).
                    if let Some(m) = self.modules.get_mut(root.as_str()) {
                        let method = m.connects_on().to_string();
                        m.handle_response(peer, &method, payload, true, ts);
                    }
                    // 2. Common state logic.
                    let lk = self.links.get_mut(peer).unwrap();
                    lk.state.conn.status = Some(ConnectionStatus::Connected);
                    lk.state.conn.intent_connect = false;
                    if self.so_types.get(&root).map_or(false, |c| c.has_so) {
                        self.create_root_so(peer, &root);
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
                        // Notify modules of close.
                        for name in &mod_names {
                            if let Some(m) = self.modules.get_mut(name.as_str()) {
                                let close_method = m.closes_on().first()
                                    .unwrap_or(&"close").to_string();
                                m.handle_response(peer, &close_method, close_payload.clone(), true, ts);
                            }
                        }
                        if let Some(m) = self.modules.get_mut(root.as_str()) {
                            let close_method = m.closes_on().first()
                                .unwrap_or(&"close").to_string();
                            m.handle_response(peer, &close_method, close_payload, true, ts);
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
        ts: i64,
    ) {
        let name: String = payload
            .as_ref()
            .and_then(|p| p.get("name"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if name.is_empty() {
            return;
        }
        match method {
            "connect" => {
                let lk = match self.links.get_mut(peer) {
                    Some(lk) => lk,
                    None => return,
                };
                let mod_state = match lk.state.modules.get_mut(&name) {
                    Some(s) => s,
                    None => return,
                };
                if ok {
                    // 1. Call module handler first (notification).
                    if let Some(m) = self.modules.get_mut(name.as_str()) {
                        let method = m.connects_on().to_string();
                        m.handle_response(peer, &method, payload, true, ts);
                    }
                    // 2. Common state logic.
                    let lk = self.links.get_mut(peer).unwrap();
                    let mod_state = lk.state.modules.get_mut(&name).unwrap();
                    mod_state.status = Some(ConnectionStatus::Connected);
                    mod_state.intent_connect = false;
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
                let mod_state = match lk.state.modules.get(&name) {
                    Some(s) => s,
                    None => return,
                };
                if ok {
                    let was_connected =
                        mod_state.status == Some(ConnectionStatus::Connected);
                    lk.state.modules.remove(&name);
                    if was_connected {
                        if let Some(m) = self.modules.get_mut(name.as_str()) {
                            let close_method = m.closes_on().first()
                                .unwrap_or(&"close").to_string();
                            m.handle_response(peer, &close_method, payload, true, ts);
                        }
                    }
                } else {
                    if let Some(s) = lk.state.modules.get_mut(&name) {
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

                let piggybacked_counter = {
                    let lk = self.links.get_mut(&peer).unwrap();
                    let counter = lk.counter;
                    lk.accept_response(resp.timestamp);
                    counter
                };
                if resp.ok {
                    self.finalize_pending_children(&peer, piggybacked_counter);
                } else {
                    self.handle_nacked_children(&peer);
                }
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

        let (tx_counter, response) = {
            let lk = self.links.get_mut(&peer).unwrap();
            let counter = lk.counter;
            let response = lk.accept_request(ok, ts);
            (counter, response)
        };

        if response.ok {
            self.finalize_pending_children(&peer, tx_counter);
        }

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
            for (_, inst) in &lk.so_table.instances {
                if inst.status == ConnectionStatus::Connected
                    && !inst.children.has_connected()
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

        // Pending child SO creates — children with so_id: None, status: None.
        // Emit message with PARENT's so_id (routes to parent on receiver).
        // Child's so_id = counter, derived after the transaction succeeds.
        {
            let lk = self.links.get_mut(peer)?;
            for (&parent_so_id, inst) in lk.so_table.instances.iter_mut() {
                for child in inst.so_children.iter_mut() {
                    if child.so_id.is_none() && child.status.is_none() {
                        child.status = Some(ConnectionStatus::Waiting);
                        return Some(QueuedMsg {
                            so_id: parent_so_id,
                            method: child.method.clone(),
                            payload: child.payload.clone(),
                            queued_at: 0,
                            queue_id: 0,
                        });
                    }
                }
            }
        }
        let lk = self.links.get(peer)?;

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
    use crate::module::{HandleResult, Module};

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
        fn handle_request(
            &mut self,
            peer: &str,
            method: &str,
            _payload: Option<Value>,
            _ts: i64,
        ) -> Result<HandleResult, Box<dyn std::error::Error>> {
            if method == self.connects_on() {
                self.connected_peers.push(peer.to_string());
            }
            if self.closes_on().contains(&method) {
                self.closed_peers.push(peer.to_string());
            }
            Ok(HandleResult { ok: true })
        }
        fn handle_response(
            &mut self,
            peer: &str,
            method: &str,
            _payload: Option<Value>,
            ok: bool,
            _ts: i64,
        ) {
            if method == self.connects_on() && ok {
                self.connected_peers.push(peer.to_string());
            }
            if self.closes_on().contains(&method) && ok {
                self.closed_peers.push(peer.to_string());
            }
        }
        fn force_remove(&mut self, peer: &str) {
            let close_method = self.closes_on().first()
                .unwrap_or(&"close").to_string();
            let _ = self.handle_request(peer, &close_method, None, 0);
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
    fn so_instance_created_on_connect() {
        let mut alice = Node::new("alice@a.com".into());
        alice.declare_so("trustline", SoConfig::auto());
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        let msg = alice.next_tx("bob@b.com").unwrap();
        let env = alice.build_request("bob@b.com", &msg).unwrap();

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so("trustline", SoConfig::auto());
        bob.register_module("trustline", Box::new(TestModule::new()));

        let resp = bob.handle_incoming(&env).unwrap();
        assert!(resp.ok);

        // Bob should have a root SO instance in the table (data only, no handler).
        let bob_lk = bob.get("alice@a.com").unwrap();
        assert_eq!(bob_lk.so_table.instances.len(), 1);
        let (_, inst) = bob_lk.so_table.root().unwrap();
        assert_eq!(inst.so_type, "trustline");
        assert_eq!(inst.status, ConnectionStatus::Connected);
    }

    #[test]
    fn pending_child_routes_via_parent_so_id() {
        use crate::link::SoChild;

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so("trustline", SoConfig::auto());
        bob.register_module("trustline", Box::new(TestModule::new()));

        // Connect alice → bob.
        let mut alice = Node::new("alice@a.com".into());
        alice.declare_root("trustline", vec![]);
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();
        let msg = alice.next_tx("bob@b.com").unwrap();
        let env = alice.build_request("bob@b.com", &msg).unwrap();
        let resp = bob.handle_incoming(&env).unwrap();
        assert!(resp.ok);

        // Get root SO id on bob.
        let root_so_id = {
            let lk = bob.get("alice@a.com").unwrap();
            let (id, _) = lk.so_table.root().unwrap();
            id
        };

        // Add a pending child (so_id: None, status: None) to the root SO.
        {
            let lk = bob.get_mut("alice@a.com").unwrap();
            let root = lk.so_table.get_mut(root_so_id).unwrap();
            root.so_children.push(SoChild {
                so_id: None,
                so_type: "novation".into(),
                status: None,
                method: "commit".into(),
                payload: Some(serde_json::json!({"amount": 100})),
            });
        }

        // next_tx should pick up the pending child.
        // The message should use PARENT's so_id (not allocate a new one).
        let next = bob.next_tx("alice@a.com");
        assert!(next.is_some());
        let next = next.unwrap();
        assert_eq!(next.method, "commit");
        assert_eq!(next.so_id, root_so_id, "creating message routes via parent's so_id");

        // The child should still have so_id: None but status: Waiting.
        let lk = bob.get("alice@a.com").unwrap();
        let root = lk.so_table.get(root_so_id).unwrap();
        let child = &root.so_children[0];
        assert!(child.so_id.is_none(), "so_id not assigned yet — derived from counter after tx");
        assert_eq!(child.status, Some(ConnectionStatus::Waiting));
    }

    #[test]
    fn child_so_id_equals_counter_after_transaction() {
        use crate::link::SoChild;

        let mut alice = Node::new("alice@a.com".into());
        alice.declare_so("trustline", SoConfig::auto());
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so("trustline", SoConfig::auto());
        bob.register_module("trustline", Box::new(TestModule::new()));

        // Bilateral connect.
        let msg = alice.next_tx("bob@b.com").unwrap();
        let env = alice.build_request("bob@b.com", &msg).unwrap();
        let resp = bob.handle_incoming(&env).unwrap();
        assert!(resp.ok);
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.pending = Some(msg.clone());
        }
        alice.dispatch_response("bob@b.com", msg.so_id, &msg.method, msg.payload, resp.ok, resp.timestamp);
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.accept_response(resp.timestamp);
        }

        // Get root SO id on alice.
        let root_so_id = {
            let lk = alice.get("bob@b.com").unwrap();
            let (id, _) = lk.so_table.root().unwrap();
            id
        };

        // Add a pending child on alice.
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            let root = lk.so_table.get_mut(root_so_id).unwrap();
            root.so_children.push(SoChild {
                so_id: None,
                so_type: "novation".into(),
                status: None,
                method: "commit".into(),
                payload: Some(serde_json::json!({"amount": 100})),
            });
        }

        // next_tx emits the creating message with parent's so_id.
        let creating_msg = alice.next_tx("bob@b.com").unwrap();
        assert_eq!(creating_msg.so_id, root_so_id);
        assert_eq!(creating_msg.method, "commit");

        // Record the counter BEFORE the transaction.
        let tx_counter = alice.get("bob@b.com").unwrap().counter;

        // Build and send to bob.
        let env = alice.build_request("bob@b.com", &creating_msg).unwrap();
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.pending = Some(creating_msg.clone());
        }

        // Bob receives — handle_incoming calls finalize_pending_children.
        // Bob's parent handler gets the request but doesn't create a child
        // (that's app-level logic, not tested here). The receiver-side
        // finalization only fires if the receiver also has a pending child.
        let bob_resp = bob.handle_incoming(&env).unwrap();
        assert!(bob_resp.ok);

        // Alice processes the response — finalize_pending_children assigns
        // child.so_id = tx_counter.
        alice.dispatch_response(
            "bob@b.com",
            creating_msg.so_id,
            &creating_msg.method,
            creating_msg.payload,
            bob_resp.ok,
            bob_resp.timestamp,
        );
        // Simulate accept_response + finalize (normally in handle_incoming
        // for piggybacked responses, here we do it manually).
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.accept_response(bob_resp.timestamp);
        }
        // Call finalize manually since we're outside handle_incoming.
        alice.finalize_pending_children("bob@b.com", tx_counter);

        // Verify: child.so_id = tx_counter.
        let lk = alice.get("bob@b.com").unwrap();
        let root = lk.so_table.get(root_so_id).unwrap();
        let child = &root.so_children[0];
        assert_eq!(child.so_id, Some(tx_counter), "child so_id should equal the transaction counter");
        assert_eq!(child.status, Some(ConnectionStatus::Connected));
    }

    #[test]
    fn nack_removes_auto_child() {
        use crate::link::SoChild;

        let mut alice = Node::new("alice@a.com".into());
        alice.declare_so("trustline", SoConfig::auto());
        alice.declare_so("novation", SoConfig::auto()); // Auto mode
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so("trustline", SoConfig::auto());
        bob.register_module("trustline", Box::new(TestModule::new()));

        // Bilateral connect.
        let msg = alice.next_tx("bob@b.com").unwrap();
        let env = alice.build_request("bob@b.com", &msg).unwrap();
        let resp = bob.handle_incoming(&env).unwrap();
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.pending = Some(msg.clone());
        }
        alice.dispatch_response("bob@b.com", msg.so_id, &msg.method, msg.payload, resp.ok, resp.timestamp);
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.accept_response(resp.timestamp);
        }

        let root_so_id = {
            let lk = alice.get("bob@b.com").unwrap();
            let (id, _) = lk.so_table.root().unwrap();
            id
        };

        // Add pending child.
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            let root = lk.so_table.get_mut(root_so_id).unwrap();
            root.so_children.push(SoChild {
                so_id: None,
                so_type: "novation".into(),
                status: None,
                method: "commit".into(),
                payload: None,
            });
        }

        // next_tx picks it up → status becomes Waiting.
        let creating_msg = alice.next_tx("bob@b.com").unwrap();
        assert_eq!(creating_msg.method, "commit");

        // Simulate NACK.
        alice.handle_nacked_children("bob@b.com");

        // Auto mode → child should be removed.
        let lk = alice.get("bob@b.com").unwrap();
        let root = lk.so_table.get(root_so_id).unwrap();
        assert!(root.so_children.is_empty(), "Auto child should be removed on NACK");
    }

    #[test]
    fn nack_keeps_invite_child() {
        use crate::link::SoChild;

        let mut alice = Node::new("alice@a.com".into());
        alice.declare_so("trustline", SoConfig::auto());
        alice.declare_so("payment", SoConfig::invite()); // Invite mode
        alice.register_module("trustline", Box::new(TestModule::new()));
        alice.connect("bob@b.com", "trustline").unwrap();

        let mut bob = Node::new("bob@b.com".into());
        bob.declare_so("trustline", SoConfig::auto());
        bob.register_module("trustline", Box::new(TestModule::new()));

        // Bilateral connect.
        let msg = alice.next_tx("bob@b.com").unwrap();
        let env = alice.build_request("bob@b.com", &msg).unwrap();
        let resp = bob.handle_incoming(&env).unwrap();
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.pending = Some(msg.clone());
        }
        alice.dispatch_response("bob@b.com", msg.so_id, &msg.method, msg.payload, resp.ok, resp.timestamp);
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            lk.accept_response(resp.timestamp);
        }

        let root_so_id = {
            let lk = alice.get("bob@b.com").unwrap();
            let (id, _) = lk.so_table.root().unwrap();
            id
        };

        // Add pending child with Invite-mode type.
        {
            let lk = alice.get_mut("bob@b.com").unwrap();
            let root = lk.so_table.get_mut(root_so_id).unwrap();
            root.so_children.push(SoChild {
                so_id: None,
                so_type: "payment".into(),
                status: None,
                method: "request".into(),
                payload: None,
            });
        }

        // next_tx picks it up → status becomes Waiting.
        alice.next_tx("bob@b.com").unwrap();

        // Simulate NACK.
        alice.handle_nacked_children("bob@b.com");

        // Invite mode → child should stay in Waiting.
        let lk = alice.get("bob@b.com").unwrap();
        let root = lk.so_table.get(root_so_id).unwrap();
        assert_eq!(root.so_children.len(), 1, "Invite child should stay on NACK");
        assert_eq!(root.so_children[0].status, Some(ConnectionStatus::Waiting));
    }
}
