//! HTTP server — multi-tenant p2p sync with ed25519 signing and argon2 auth.
//!
//! Mirrors the Go Server: /p2p/key, /p2p/tx, /api/auth, /api/admin, /api/account.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::identity::{identity_in_domain, parse_identity, valid_username};
use crate::link::{SyncError, TxRequest, TxResponse};
use crate::module::Module;
use crate::node::Node;
use crate::store::{FileStore, Store, UserRecord};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Server configuration.
#[derive(Clone)]
pub struct Config {
    pub domain: String,
    pub listen: String,
    pub data_dir: String,
    pub session_ttl: Duration,
    pub key_cache_ttl: Duration,
    pub http_timeout: Duration,
    pub peer_scheme: String,
    pub admin_user: String,
    pub admin_pass: String,
    pub verbose: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            domain: String::new(),
            listen: ":8471".into(),
            data_dir: "data".into(),
            session_ttl: Duration::from_secs(86400),
            key_cache_ttl: Duration::from_secs(3600),
            http_timeout: Duration::from_secs(10),
            peer_scheme: "https".into(),
            admin_user: String::new(),
            admin_pass: String::new(),
            verbose: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Module registration
// ---------------------------------------------------------------------------

/// Factory function that creates a module instance.
pub type ModuleFactory = Box<dyn Fn(&str, &str, &Node) -> Box<dyn Module> + Send + Sync>;

struct ModuleReg {
    name: String,
    factory: ModuleFactory,
}

// ---------------------------------------------------------------------------
// Signature helpers
// ---------------------------------------------------------------------------

const HDR_DOMAIN: &str = "x-p2ptx-domain";
const HDR_SIGNATURE: &str = "x-p2ptx-signature";
const SIG_PREFIX: &str = "P2PTX1\n";
const MAX_BODY: usize = 1 << 20;
const MAX_KEY_CACHE: usize = 256;
const MAX_USERS: usize = 1000;
const RETRY_DELAY: Duration = Duration::from_secs(60);

fn signable(path: &str, body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SIG_PREFIX.len() + path.len() + 1 + body.len());
    buf.extend_from_slice(SIG_PREFIX.as_bytes());
    buf.extend_from_slice(path.as_bytes());
    buf.push(b'\n');
    buf.extend_from_slice(body);
    buf
}

// ---------------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------------

struct Session {
    user: String,
    admin: bool,
    expires: Instant,
}

// ---------------------------------------------------------------------------
// Cached key
// ---------------------------------------------------------------------------

struct CachedKey {
    key: VerifyingKey,
    fetched: Instant,
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

/// Multi-tenant p2p server.
pub struct Server {
    pub cfg: Config,
    store: Box<dyn Store>,
    key: SigningKey,
    factories: Vec<ModuleReg>,
    roots: HashMap<String, Vec<String>>,

    nodes: RwLock<HashMap<String, Arc<Mutex<Node>>>>,

    users: Mutex<HashMap<String, UserRecord>>,
    sessions: Mutex<HashMap<String, Session>>,
    key_cache: Mutex<HashMap<String, CachedKey>>,

    client: reqwest::Client,
}

impl Server {
    /// Create a new server. Call register() and declare_root() before init().
    pub fn new(mut cfg: Config) -> Result<Self, Box<dyn std::error::Error>> {
        cfg.domain = cfg.domain.to_lowercase();
        if cfg.domain.is_empty() {
            return Err("Config.domain is required".into());
        }
        if cfg.listen.is_empty() {
            cfg.listen = ":8471".into();
        }

        let store = Box::new(FileStore::new(&cfg.data_dir)?);
        let key = load_or_create_key(store.as_ref())?;
        let users = store.load_users()?;

        let client = reqwest::Client::builder()
            .timeout(cfg.http_timeout)
            .build()?;

        Ok(Server {
            cfg,
            store,
            key,
            factories: Vec::new(),
            roots: HashMap::new(),
            nodes: RwLock::new(HashMap::new()),
            users: Mutex::new(users),
            sessions: Mutex::new(HashMap::new()),
            key_cache: Mutex::new(HashMap::new()),
            client,
        })
    }

    /// Register a module factory. Call before init().
    pub fn register(&mut self, name: &str, factory: ModuleFactory) {
        self.factories.push(ModuleReg {
            name: name.to_string(),
            factory,
        });
    }

    /// Declare a root module with its children. Call before init().
    pub fn declare_root(&mut self, name: &str, children: Vec<String>) {
        self.roots.insert(name.to_string(), children);
    }

    /// Load persisted nodes and bootstrap admin.
    pub fn init(&self) -> Result<(), Box<dyn std::error::Error>> {
        let ids = self.store.list_nodes()?;
        for id in ids {
            let (user, domain) = match parse_identity(&id) {
                Ok((u, d)) => (u, d),
                Err(_) => continue,
            };
            if domain != self.cfg.domain {
                continue;
            }
            let raw = self.store.load_node(&id)?;
            let node = self.new_node(&id, raw.as_deref());
            self.nodes.write().unwrap().insert(user, Arc::new(Mutex::new(node)));
        }

        if !self.cfg.admin_user.is_empty() && !self.cfg.admin_pass.is_empty() {
            let _ = self.create_user(&self.cfg.admin_user.clone(), &self.cfg.admin_pass.clone(), true);
        }

        Ok(())
    }

    fn new_node(&self, id: &str, raw: Option<&[u8]>) -> Node {
        let mut node: Node = match raw {
            Some(data) => serde_json::from_slice(data).unwrap_or_else(|_| Node::new(id.to_string())),
            None => Node::new(id.to_string()),
        };

        // Re-declare roots.
        for (name, children) in &self.roots {
            node.declare_root(name, children.clone());
        }

        // Re-register modules.
        for reg in &self.factories {
            let module = (reg.factory)(&reg.name, id, &node);
            node.register_module(&reg.name, module);
        }

        node
    }

    /// Handle an HTTP request — the main router.
    pub async fn handle_request(
        self: &Arc<Self>,
        req: Request<Incoming>,
    ) -> Result<Response<String>, hyper::Error> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();

        let resp = match (method, path.as_str()) {
            (Method::GET, "/p2p/key") => self.handle_key(),
            (Method::POST, "/p2p/tx") => self.handle_tx(req).await,
            (Method::POST, "/api/auth") => self.handle_auth(req).await,
            (Method::POST, "/api/admin") => self.handle_admin(req).await,
            (Method::POST, "/api/account") => self.handle_account(req).await,
            _ => json_response(StatusCode::NOT_FOUND, &serde_json::json!({"error": "not found"})),
        };

        Ok(resp)
    }

    // -----------------------------------------------------------------------
    // /p2p/key
    // -----------------------------------------------------------------------

    fn handle_key(&self) -> Response<String> {
        let pub_key = self.key.verifying_key();
        json_response(
            StatusCode::OK,
            &serde_json::json!({
                "domain": self.cfg.domain,
                "public_key": B64.encode(pub_key.as_bytes()),
            }),
        )
    }

    // -----------------------------------------------------------------------
    // /p2p/tx
    // -----------------------------------------------------------------------

    async fn handle_tx(self: &Arc<Self>, req: Request<Incoming>) -> Response<String> {
        let (sig_domain, sig, body) = match read_p2p(req).await {
            Ok(v) => v,
            Err(_) => return http_error(StatusCode::UNAUTHORIZED, "unauthorized"),
        };

        let env: TxRequest = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(_) => return http_error(StatusCode::BAD_REQUEST, "malformed"),
        };

        if !identity_in_domain(&env.from, &sig_domain) {
            return http_error(StatusCode::UNAUTHORIZED, "unauthorized");
        }

        let node = match self.node_by_identity(&env.to) {
            Some(n) => n,
            None => return http_error(StatusCode::UNAUTHORIZED, "unauthorized"),
        };

        // Verify signature.
        if let Err(_) = self.verify_p2p_signature(&sig_domain, "/p2p/tx", &sig, &body).await {
            return http_error(StatusCode::UNAUTHORIZED, "unauthorized");
        }

        // Handle incoming.
        let result = {
            let mut n = node.lock().unwrap();
            n.handle_incoming(&env)
        };

        match result {
            Ok(resp) => {
                // Persist and flush outgoing.
                let user = parse_identity(&env.to).map(|(u, _)| u).unwrap_or_default();
                let _ = self.commit_node(&env.to);
                self.flush_user(&user);
                json_response(StatusCode::OK, &resp)
            }
            Err(e) => match e {
                SyncError::NoLink => http_error(StatusCode::NOT_FOUND, "no link"),
                SyncError::MaxLinks => http_error(StatusCode::BAD_REQUEST, "max links"),
                SyncError::StaleCounter | SyncError::CounterAhead | SyncError::Collision => {
                    http_error(StatusCode::CONFLICT, &e.to_string())
                }
                SyncError::Timestamp => http_error(StatusCode::CONFLICT, &e.to_string()),
                SyncError::BadMethod => http_error(StatusCode::BAD_REQUEST, &e.to_string()),
                _ => http_error(StatusCode::BAD_REQUEST, &e.to_string()),
            },
        }
    }

    // -----------------------------------------------------------------------
    // /api/auth
    // -----------------------------------------------------------------------

    async fn handle_auth(&self, req: Request<Incoming>) -> Response<String> {
        let api_req = match read_api_request(req).await {
            Ok(r) => r,
            Err(_) => return http_error(StatusCode::BAD_REQUEST, "expected JSON {method, payload}"),
        };

        #[derive(Deserialize, Default)]
        struct Creds {
            #[serde(default)]
            username: String,
            #[serde(default)]
            password: String,
            #[serde(default)]
            old: String,
            #[serde(default)]
            new: String,
        }

        let creds: Creds = api_req
            .payload
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();

        match api_req.method.as_str() {
            "register" => {
                if let Err(e) = self.create_user(&creds.username, &creds.password, false) {
                    return api_error(&e.to_string());
                }
                api_ok(&serde_json::json!({"user": creds.username.to_lowercase()}))
            }
            "login" => {
                let username = creds.username.to_lowercase();
                let users = self.users.lock().unwrap();
                let rec = match users.get(&username) {
                    Some(r) => r,
                    None => return api_error("invalid credentials"),
                };
                if !verify_password(rec, &creds.password) {
                    return api_error("invalid credentials");
                }
                let token = self.new_session(&rec.name, rec.admin);
                let is_admin = rec.admin;
                drop(users);
                api_ok(&serde_json::json!({"token": token, "admin": is_admin}))
            }
            "logout" => {
                let token = match self.authenticate_raw(&api_req) {
                    Ok((_, t)) => t,
                    Err(e) => return api_error(&e),
                };
                self.sessions.lock().unwrap().remove(&token);
                api_ok(&serde_json::json!(true))
            }
            "change_password" => {
                let (sess_user, current_token) = match self.authenticate_raw(&api_req) {
                    Ok(v) => v,
                    Err(e) => return api_error(&e),
                };
                let mut users = self.users.lock().unwrap();
                let rec = match users.get(&sess_user) {
                    Some(r) => r,
                    None => return api_error("invalid credentials"),
                };
                if !verify_password(rec, &creds.old) {
                    return api_error("invalid credentials");
                }
                let (salt, hash) = hash_password(&creds.new);
                let rec = users.get_mut(&sess_user).unwrap();
                rec.salt = salt;
                rec.hash = hash;
                // Invalidate other sessions.
                let mut sessions = self.sessions.lock().unwrap();
                sessions.retain(|t, s| s.user != sess_user || *t == current_token);
                drop(sessions);
                if let Err(_) = self.store.save_users(&users) {
                    return api_error("persist failed");
                }
                drop(users);
                api_ok(&serde_json::json!(true))
            }
            _ => api_error("unknown method"),
        }
    }

    // -----------------------------------------------------------------------
    // /api/admin
    // -----------------------------------------------------------------------

    async fn handle_admin(&self, req: Request<Incoming>) -> Response<String> {
        let api_req = match read_api_request(req).await {
            Ok(r) => r,
            Err(_) => return http_error(StatusCode::BAD_REQUEST, "expected JSON {method, payload}"),
        };

        let (sess_user, _) = match self.authenticate_raw(&api_req) {
            Ok(v) => v,
            Err(e) => return http_error(StatusCode::UNAUTHORIZED, &e),
        };

        // Check admin.
        {
            let users = self.users.lock().unwrap();
            match users.get(&sess_user) {
                Some(r) if r.admin => {}
                _ => return http_error(StatusCode::FORBIDDEN, "admin only"),
            }
        }

        #[derive(Deserialize, Default)]
        struct Params {
            #[serde(default)]
            username: String,
            #[serde(default)]
            password: String,
        }
        let p: Params = api_req
            .payload
            .as_ref()
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let username = p.username.to_lowercase();

        match api_req.method.as_str() {
            "create_user" => {
                if let Err(e) = self.create_user(&username, &p.password, false) {
                    return api_error(&e.to_string());
                }
                api_ok(&serde_json::json!({"user": username}))
            }
            "delete_user" => {
                {
                    let mut users = self.users.lock().unwrap();
                    if !users.contains_key(&username) {
                        return api_error("no such user");
                    }
                    users.remove(&username);
                    let mut sessions = self.sessions.lock().unwrap();
                    sessions.retain(|_, s| s.user != username);
                    drop(sessions);
                    if let Err(_) = self.store.save_users(&users) {
                        return api_error("persist failed");
                    }
                }
                let node_id = format!("{}@{}", username, self.cfg.domain);
                self.nodes.write().unwrap().remove(&username);
                let _ = self.store.delete_node(&node_id);
                api_ok(&serde_json::json!(true))
            }
            "list_users" => {
                let users = self.users.lock().unwrap();
                let list: Vec<Value> = users
                    .values()
                    .map(|r| serde_json::json!({"name": r.name, "admin": r.admin}))
                    .collect();
                drop(users);
                api_ok(&serde_json::json!(list))
            }
            "reset_password" => {
                let mut users = self.users.lock().unwrap();
                let rec = match users.get_mut(&username) {
                    Some(r) => r,
                    None => return api_error("no such user"),
                };
                let (salt, hash) = hash_password(&p.password);
                rec.salt = salt;
                rec.hash = hash;
                if let Err(_) = self.store.save_users(&users) {
                    return api_error("persist failed");
                }
                drop(users);
                api_ok(&serde_json::json!(true))
            }
            _ => api_error("unknown method"),
        }
    }

    // -----------------------------------------------------------------------
    // /api/account
    // -----------------------------------------------------------------------

    async fn handle_account(self: &Arc<Self>, req: Request<Incoming>) -> Response<String> {
        let api_req = match read_api_request(req).await {
            Ok(r) => r,
            Err(_) => return http_error(StatusCode::BAD_REQUEST, "expected JSON {method, payload}"),
        };

        let (sess_user, _) = match self.authenticate_raw(&api_req) {
            Ok(v) => v,
            Err(e) => return http_error(StatusCode::UNAUTHORIZED, &e),
        };

        let node = match self.get_node(&sess_user) {
            Some(n) => n,
            None => return http_error(StatusCode::INTERNAL_SERVER_ERROR, "no node"),
        };

        // Dispatch to Node methods based on method namespace.
        let method = &api_req.method;
        let payload = api_req.payload;

        // "connect", "close", "status", "links" are node-level.
        // "<module>.<method>" dispatches to module.handle_account.
        let resp = {
            let mut n = node.lock().unwrap();
            match method.as_str() {
                "connect" => {
                    #[derive(Deserialize)]
                    struct P { peer: String, root: String }
                    let p: P = match payload.and_then(|v| serde_json::from_value(v).ok()) {
                        Some(p) => p,
                        None => return api_error("bad payload"),
                    };
                    match n.connect(&p.peer, &p.root) {
                        Ok(()) => api_ok(&serde_json::json!(true)),
                        Err(e) => api_error(&e.to_string()),
                    }
                }
                "close" => {
                    #[derive(Deserialize)]
                    struct P { peer: String }
                    let p: P = match payload.and_then(|v| serde_json::from_value(v).ok()) {
                        Some(p) => p,
                        None => return api_error("bad payload"),
                    };
                    n.close(&p.peer);
                    api_ok(&serde_json::json!(true))
                }
                "force_remove" => {
                    #[derive(Deserialize)]
                    struct P { peer: String }
                    let p: P = match payload.and_then(|v| serde_json::from_value(v).ok()) {
                        Some(p) => p,
                        None => return api_error("bad payload"),
                    };
                    n.force_remove(&p.peer);
                    api_ok(&serde_json::json!(true))
                }
                "links" => {
                    let links: HashMap<String, Value> = n
                        .links
                        .iter()
                        .map(|(peer, lk)| (peer.clone(), serde_json::to_value(lk).unwrap_or_default()))
                        .collect();
                    api_ok(&serde_json::json!(links))
                }
                _ => {
                    // Try module dispatch: "trustline.balance" → module "trustline", method "balance".
                    if let Some(dot) = method.find('.') {
                        let (mod_name, mod_method) = (&method[..dot], &method[dot + 1..]);
                        if let Some(m) = n.module_mut(mod_name) {
                            match m.handle_account(mod_method, payload) {
                                Ok(Some(result)) => api_ok(&result),
                                Ok(None) => api_ok(&serde_json::json!(true)),
                                Err(e) => api_error(&e.to_string()),
                            }
                        } else {
                            api_error("unknown module")
                        }
                    } else {
                        api_error("unknown method")
                    }
                }
            }
        };

        // Persist and flush outgoing after every account action.
        let node_id = format!("{}@{}", sess_user, self.cfg.domain);
        let _ = self.commit_node(&node_id);
        self.flush_user(&sess_user);

        resp
    }

    // -----------------------------------------------------------------------
    // P2P signing
    // -----------------------------------------------------------------------

    fn sign(&self, path: &str, body: &[u8]) -> Vec<u8> {
        let msg = signable(path, body);
        self.key.sign(&msg).to_bytes().to_vec()
    }

    async fn verify_p2p_signature(
        &self,
        domain: &str,
        path: &str,
        sig: &[u8],
        body: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let pub_key = self.peer_key(domain).await?;
        let sig_bytes: [u8; 64] = sig.try_into().map_err(|_| "bad signature length")?;
        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        pub_key
            .verify(&signable(path, body), &signature)
            .map_err(|e| format!("bad signature from {}: {}", domain, e).into())
    }

    async fn peer_key(
        &self,
        domain: &str,
    ) -> Result<VerifyingKey, Box<dyn std::error::Error>> {
        // Check cache.
        {
            let cache = self.key_cache.lock().unwrap();
            if let Some(ck) = cache.get(domain) {
                if ck.fetched.elapsed() < self.cfg.key_cache_ttl {
                    return Ok(ck.key);
                }
            }
        }

        // Fetch from peer.
        let url = format!("{}://{}/p2p/key", self.cfg.peer_scheme, domain);
        let resp = self.client.get(&url).send().await?;
        if !resp.status().is_success() {
            return Err(format!("key fetch {}: status {}", domain, resp.status()).into());
        }

        #[derive(Deserialize)]
        struct KeyResp {
            public_key: String,
        }
        let body: KeyResp = resp.json().await?;
        let raw = B64.decode(&body.public_key)?;
        if raw.len() != 32 {
            return Err(format!("key fetch {}: malformed", domain).into());
        }
        let key_bytes: [u8; 32] = raw.try_into().unwrap();
        let key = VerifyingKey::from_bytes(&key_bytes)?;

        // Cache.
        let mut cache = self.key_cache.lock().unwrap();
        if cache.len() >= MAX_KEY_CACHE {
            cache.retain(|_, v| v.fetched.elapsed() < self.cfg.key_cache_ttl);
        }
        if cache.len() < MAX_KEY_CACHE {
            cache.insert(
                domain.to_string(),
                CachedKey {
                    key,
                    fetched: Instant::now(),
                },
            );
        }

        Ok(key)
    }

    /// Post a signed sync message to a peer.
    pub async fn send_to_peer(
        &self,
        peer_domain: &str,
        msg: &TxRequest,
    ) -> Result<TxResponse, Box<dyn std::error::Error>> {
        let body = serde_json::to_vec(msg)?;
        let sig = self.sign("/p2p/tx", &body);
        let url = format!("{}://{}/p2p/tx", self.cfg.peer_scheme, peer_domain);
        let resp = self
            .client
            .post(&url)
            .header("content-type", "application/json")
            .header(HDR_DOMAIN, &self.cfg.domain)
            .header(HDR_SIGNATURE, B64.encode(&sig))
            .body(body)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(format!("peer {} returned {}", peer_domain, resp.status()).into());
        }
        let tx_resp: TxResponse = resp.json().await?;
        Ok(tx_resp)
    }

    // -----------------------------------------------------------------------
    // Send loop — event-driven flush (matches Go sendPending)
    // -----------------------------------------------------------------------

    /// Send all pending messages for a specific peer on a node.
    /// Loops: next_tx → build envelope → post → process response → repeat.
    async fn send_pending(self: Arc<Self>, node: Arc<Mutex<Node>>, peer: String) {
        use crate::link::now_secs;

        loop {
            // Phase 1: Lock, get next message, build envelope, unlock.
            let (env, _msg_module, msg_queue_id) = {
                let mut n = node.lock().unwrap();
                let msg = match n.next_tx(&peer) {
                    Some(m) => m,
                    None => return, // Nothing to send.
                };

                let lk = match n.links.get(&peer) {
                    Some(lk) => lk,
                    None => return,
                };

                let mut env = TxRequest {
                    from: n.id.clone(),
                    to: peer.clone(),
                    counter: lk.counter,
                    method: msg.method.clone(),
                    payload: msg.payload.clone(),
                    timestamp: 0,
                    resp: None,
                };

                // Dominant side sets timestamp.
                if lk.dominant() {
                    env.timestamp = now_secs();
                    if env.timestamp < lk.ts {
                        env.timestamp = lk.ts;
                    }
                }

                // Piggyback cached response.
                if let Some(ref cr) = lk.cached_response {
                    env.resp = Some(cr.clone());
                }

                let mod_name = msg.module.clone();
                let q_id = msg.queue_id.clone();

                // Promote to pending.
                if let Some(lk) = n.links.get_mut(&peer) {
                    lk.pending = Some(msg);
                }

                // Persist after promoting.
                let node_id = n.id.clone();
                drop(n);
                let _ = self.commit_node(&node_id);

                (env, mod_name, q_id)
            };

            // Phase 2: Send to peer (no lock held).
            let peer_dom = crate::identity::peer_domain(&peer);
            let resp = {
                let result = self.send_to_peer(peer_dom, &env).await;
                match result {
                    Ok(r) => Some(r),
                    Err(e) => {
                        if self.cfg.verbose {
                            eprintln!("[send] {}→{} {}: {}", env.from, peer, env.method, e);
                        }
                        None
                    }
                }
            }; // `e` dropped here.
            let resp = match resp {
                Some(r) => r,
                None => {
                    // Retry after delay (matches Go retryDelay = 60s).
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
            };

            // Phase 3: Lock, process response, dequeue, accept, continue loop.
            let need_retry = {
                let mut n = node.lock().unwrap();

                // Validate link still exists and pending matches.
                let counter_match = n
                    .links
                    .get(&peer)
                    .map(|lk| lk.counter == env.counter && lk.pending.is_some())
                    .unwrap_or(false);
                if !counter_match {
                    return; // Link changed while we were sending.
                }

                // Validate response timestamp.
                let bad_ts = n.links.get(&peer)
                    .map(|lk| lk.validate_timestamp(resp.timestamp, now_secs()).is_err())
                    .unwrap_or(false);
                if bad_ts {
                    if self.cfg.verbose {
                        eprintln!("[send] {}→{}: bad response timestamp", n.id, peer);
                    }
                    true // need retry
                } else {
                    // Dispatch response.
                    n.dispatch_response(&peer, &env.method, env.payload.clone(), resp.ok, resp.timestamp);

                    // Dequeue from registry.
                    if msg_queue_id != 0 {
                        let reg = n.registry();
                        reg.lock().unwrap().dequeue(msg_queue_id, &peer);
                    }

                    // Accept response (increments counter, clears pending/cached_response).
                    if let Some(lk) = n.links.get_mut(&peer) {
                        lk.accept_response(resp.timestamp);
                    }

                    // Persist.
                    let node_id = n.id.clone();
                    drop(n);
                    let _ = self.commit_node(&node_id);

                    false // no retry needed
                }
            }; // MutexGuard dropped here, before any await.

            if need_retry {
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }
            // Loop back to check for more messages.
        }
    }

    /// Drain kicks from the registry and send_pending only to kicked peers.
    fn flush_kicks(self: &Arc<Self>, node: Arc<Mutex<Node>>) {
        let kicks: Vec<String> = {
            let n = node.lock().unwrap();
            let reg_ref = n.registry();
            let mut reg = reg_ref.lock().unwrap();
            reg.drain_kicks().into_iter().collect()
        };
        for peer in kicks {
            let srv = Arc::clone(self);
            let nd = Arc::clone(&node);
            tokio::spawn(async move {
                srv.send_pending(nd, peer).await;
            });
        }
    }

    /// Drain kicks for a node by username — convenience for handlers.
    fn flush_user(self: &Arc<Self>, username: &str) {
        if let Some(node) = self.get_node(username) {
            self.flush_kicks(node);
        }
    }

    /// Flush all peers for a node — spawn a send_pending task per peer.
    /// Used at boot to resume all pending work.
    fn flush_node(self: &Arc<Self>, node: Arc<Mutex<Node>>) {
        let peers: Vec<String> = {
            let n = node.lock().unwrap();
            n.links.keys().cloned().collect()
        };
        for peer in peers {
            let srv = Arc::clone(self);
            let nd = Arc::clone(&node);
            tokio::spawn(async move {
                srv.send_pending(nd, peer).await;
            });
        }
    }

    /// Flush all nodes — resume pending work at boot (matches Go kickAll).
    pub fn flush_all(self: &Arc<Self>) {
        let nodes = self.nodes.read().unwrap();
        for (_, node) in nodes.iter() {
            self.flush_node(Arc::clone(node));
        }
    }

    // -----------------------------------------------------------------------
    // User management
    // -----------------------------------------------------------------------

    fn create_user(
        &self,
        username: &str,
        password: &str,
        admin: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let username = username.to_lowercase();
        if !valid_username(&username) {
            return Err(format!("invalid username {:?}", username).into());
        }
        let mut users = self.users.lock().unwrap();
        if users.contains_key(&username) {
            return Err("user already exists".into());
        }
        if users.len() >= MAX_USERS {
            return Err("max users reached".into());
        }
        let (salt, hash) = hash_password(password);
        users.insert(
            username.clone(),
            UserRecord {
                name: username.clone(),
                salt,
                hash,
                admin,
            },
        );
        self.store.save_users(&users)?;

        if !admin {
            drop(users);
            let node_id = format!("{}@{}", username, self.cfg.domain);
            let node = self.new_node(&node_id, None);
            // Persist immediately.
            let data = serde_json::to_vec_pretty(&node)?;
            self.store.save_node(&node_id, &data)?;
            self.nodes
                .write()
                .unwrap()
                .insert(username, Arc::new(Mutex::new(node)));
        }

        Ok(())
    }

    fn new_session(&self, user: &str, admin: bool) -> String {
        let mut token_bytes = [0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut token_bytes);
        let token = hex::encode(token_bytes);
        self.sessions.lock().unwrap().insert(
            token.clone(),
            Session {
                user: user.to_string(),
                admin,
                expires: Instant::now() + self.cfg.session_ttl,
            },
        );
        token
    }

    fn authenticate_raw(&self, _req: &ApiRequest) -> Result<(String, String), String> {
        // For API requests, token comes from the Authorization header,
        // but since we're dispatching from parsed body, check sessions
        // using a token field in payload or header.
        // Simplified: check "token" in payload.
        let token = _req
            .payload
            .as_ref()
            .and_then(|v| v.get("token"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if token.is_empty() {
            return Err("missing token".into());
        }
        let sessions = self.sessions.lock().unwrap();
        match sessions.get(token) {
            Some(s) if Instant::now() < s.expires => Ok((s.user.clone(), token.to_string())),
            _ => Err("invalid or expired session".into()),
        }
    }

    fn get_node(&self, user: &str) -> Option<Arc<Mutex<Node>>> {
        self.nodes.read().unwrap().get(user).cloned()
    }

    fn node_by_identity(&self, id: &str) -> Option<Arc<Mutex<Node>>> {
        let (user, domain) = parse_identity(id).ok()?;
        if domain != self.cfg.domain {
            return None;
        }
        self.get_node(&user)
    }

    /// Persist a node's state.
    pub fn commit_node(&self, id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let (user, _) = parse_identity(id)?;
        let node = self.get_node(&user).ok_or("node not found")?;
        let n = node.lock().unwrap();
        let data = serde_json::to_vec_pretty(&*n)?;
        self.store.save_node(id, &data)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Request parsing helpers
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ApiRequest {
    method: String,
    #[serde(default)]
    payload: Option<Value>,
}

async fn read_body(req: Request<Incoming>) -> Result<Vec<u8>, String> {
    let body = req
        .into_body()
        .collect()
        .await
        .map_err(|e| e.to_string())?
        .to_bytes();
    if body.len() > MAX_BODY {
        return Err("body too large".into());
    }
    Ok(body.to_vec())
}

async fn read_api_request(req: Request<Incoming>) -> Result<ApiRequest, String> {
    let body = read_body(req).await?;
    serde_json::from_slice(&body).map_err(|e| e.to_string())
}

async fn read_p2p(req: Request<Incoming>) -> Result<(String, Vec<u8>, Vec<u8>), String> {
    let domain = req
        .headers()
        .get(HDR_DOMAIN)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let sig_b64 = req
        .headers()
        .get(HDR_SIGNATURE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = read_body(req).await?;

    if domain.is_empty() {
        return Err("missing domain header".into());
    }
    let sig = B64
        .decode(&sig_b64)
        .map_err(|_| "bad signature encoding".to_string())?;
    if sig.len() != 64 {
        return Err("bad signature length".into());
    }

    Ok((domain, sig, body))
}

// ---------------------------------------------------------------------------
// HTTP response helpers
// ---------------------------------------------------------------------------

fn json_response(status: StatusCode, body: &impl Serialize) -> Response<String> {
    let json = serde_json::to_string(body).unwrap_or_else(|_| "{}".into());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(json)
        .unwrap()
}

fn http_error(status: StatusCode, msg: &str) -> Response<String> {
    json_response(status, &serde_json::json!({"error": msg}))
}

fn api_ok(result: &Value) -> Response<String> {
    json_response(StatusCode::OK, &serde_json::json!({"ok": true, "result": result}))
}

fn api_error(msg: &str) -> Response<String> {
    json_response(StatusCode::OK, &serde_json::json!({"ok": false, "error": msg}))
}

// ---------------------------------------------------------------------------
// Crypto helpers
// ---------------------------------------------------------------------------

fn load_or_create_key(store: &dyn Store) -> Result<SigningKey, Box<dyn std::error::Error>> {
    match store.load_server_key()? {
        Some(seed) => {
            if seed.len() != 32 {
                return Err(format!("bad seed length {}", seed.len()).into());
            }
            let bytes: [u8; 32] = seed.try_into().unwrap();
            Ok(SigningKey::from_bytes(&bytes))
        }
        None => {
            use rand::RngCore;
            let mut seed = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut seed);
            store.save_server_key(&seed)?;
            Ok(SigningKey::from_bytes(&seed))
        }
    }
}

/// Hash a password with argon2id.
fn hash_password(password: &str) -> (Vec<u8>, Vec<u8>) {
    use argon2::Argon2;
    use rand::RngCore;

    let mut salt = vec![0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);

    let argon = Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        argon2::Params::new(19 * 1024, 2, 1, Some(32)).unwrap(),
    );
    let mut hash = vec![0u8; 32];
    argon
        .hash_password_into(password.as_bytes(), &salt, &mut hash)
        .expect("argon2 hash failed");
    (salt, hash)
}

/// Verify a password against a stored record.
fn verify_password(rec: &UserRecord, password: &str) -> bool {
    use argon2::Argon2;

    let argon = Argon2::new(
        argon2::Algorithm::Argon2id,
        argon2::Version::V0x13,
        argon2::Params::new(19 * 1024, 2, 1, Some(rec.hash.len())).unwrap(),
    );
    let mut hash = vec![0u8; rec.hash.len()];
    if argon
        .hash_password_into(password.as_bytes(), &rec.salt, &mut hash)
        .is_err()
    {
        return false;
    }
    // Constant-time comparison.
    hash.len() == rec.hash.len() && hash.iter().zip(rec.hash.iter()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

/// Hex encode bytes (for session tokens).
mod hex {
    pub fn encode(bytes: [u8; 32]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signable_format() {
        let s = signable("/p2p/tx", b"hello");
        let expected = b"P2PTX1\n/p2p/tx\nhello";
        assert_eq!(s, expected);
    }

    #[test]
    fn sign_and_verify() {
        use rand::rngs::OsRng;
        let key = SigningKey::generate(&mut OsRng);
        let msg = signable("/p2p/tx", b"test body");
        let sig = key.sign(&msg);
        assert!(key.verifying_key().verify(&msg, &sig).is_ok());
    }

    #[test]
    fn password_hash_roundtrip() {
        let (salt, hash) = hash_password("testpass");
        let rec = UserRecord {
            name: "test".into(),
            salt,
            hash,
            admin: false,
        };
        assert!(verify_password(&rec, "testpass"));
        assert!(!verify_password(&rec, "wrongpass"));
    }

    #[test]
    fn load_or_create_key_persists() {
        let dir = std::env::temp_dir().join("mimesis_test_server_key");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FileStore::new(&dir).unwrap();

        let key1 = load_or_create_key(&store).unwrap();
        let key2 = load_or_create_key(&store).unwrap();
        assert_eq!(key1.to_bytes(), key2.to_bytes());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
