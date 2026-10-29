//! Triad — three-party novation to eliminate circular debt.
//!
//! A fully functional module (not a demo), meant to be simple.
//! Each Novation is its own SyncObject — Drop guarantees cleanup
//! of that novation's per-peer outbox queues when it's removed.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::module::{HandleResult, Module, OutboxCandidate};
use crate::sync_object::{KickFn, SyncObject};

/// Directional constants matching creditnet convention.
pub const IN: usize = 0;
pub const OUT: usize = 1;

/// Maximum concurrent novations per module instance.
pub const MAX_NOVATIONS: usize = 9;

// ---------------------------------------------------------------------------
// Novation state
// ---------------------------------------------------------------------------

/// A single three-party novation in progress.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Novation {
    pub amount: i64,
    /// Peer IDs: [IN, OUT].
    pub side: [String; 2],
    #[serde(default, skip_serializing_if = "is_false")]
    pub initiator: bool,
}

fn is_false(v: &bool) -> bool {
    !*v
}

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CommitMsg {
    novation_id: String,
    amount: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    next: String,
}

#[derive(Serialize, Deserialize)]
struct FinalizeMsg {
    novation_id: String,
}

#[derive(Serialize, Deserialize)]
struct RollbackMsg {
    novation_id: String,
}

// ---------------------------------------------------------------------------
// External dependencies (provided by the trustline layer)
// ---------------------------------------------------------------------------

/// Callback: available bandwidth on a peer in a direction.
pub type BandwidthFn = Box<dyn Fn(&str, usize) -> i64 + Send + Sync>;

/// Callback: pending balance on a peer in a direction.
pub type PendingBalanceFn = Box<dyn Fn(&str, usize) -> i64 + Send + Sync>;

/// Callback: add balance on a peer's trustline.
pub type AddBalanceFn = Box<dyn Fn(&str, i64, i64) + Send + Sync>;

/// Callback: check if a module is connected on a peer.
pub type IsConnectedFn = Box<dyn Fn(&str) -> bool + Send + Sync>;

/// Callback: list connected peers.
pub type ConnectedPeersFn = Box<dyn Fn() -> Vec<String> + Send + Sync>;

// ---------------------------------------------------------------------------
// Triad module
// ---------------------------------------------------------------------------

/// Triad implements Module. Each novation is its own SyncObject<Novation>,
/// so removing a novation automatically cleans up its queues via Drop.
pub struct Triad {
    /// Each novation owns its own SyncObject with per-peer queues.
    pub novations: HashMap<String, SyncObject<Novation>>,
    /// Kick callback, cloned into each new SyncObject<Novation>.
    kick: Option<KickFn>,
    pub name: String,
    pub node_id: String,
    bandwidth: Option<BandwidthFn>,
    pending_balance: Option<PendingBalanceFn>,
    add_balance: Option<AddBalanceFn>,
    is_connected: Option<IsConnectedFn>,
    connected_peers: Option<ConnectedPeersFn>,
}

impl Triad {
    /// Create a new Triad with full dependencies.
    pub fn new(
        name: String,
        node_id: String,
        kick: KickFn,
        bandwidth: BandwidthFn,
        pending_balance: PendingBalanceFn,
        add_balance: AddBalanceFn,
        is_connected: IsConnectedFn,
        connected_peers: ConnectedPeersFn,
    ) -> Self {
        Triad {
            novations: HashMap::new(),
            kick: Some(kick),
            name,
            node_id,
            bandwidth: Some(bandwidth),
            pending_balance: Some(pending_balance),
            add_balance: Some(add_balance),
            is_connected: Some(is_connected),
            connected_peers: Some(connected_peers),
        }
    }

    /// Create a minimal Triad for testing (no external deps).
    pub fn for_test(name: &str, node_id: &str) -> Self {
        Triad {
            novations: HashMap::new(),
            kick: None,
            name: name.to_string(),
            node_id: node_id.to_string(),
            bandwidth: None,
            pending_balance: None,
            add_balance: None,
            is_connected: None,
            connected_peers: None,
        }
    }

    /// Insert a new novation, wrapping it in a SyncObject with the kick callback.
    fn insert_novation(&mut self, id: String, nov: Novation) {
        let so = match &self.kick {
            Some(k) => SyncObject::new(nov, std::sync::Arc::clone(k)),
            None => SyncObject::without_kick(nov),
        };
        self.novations.insert(id, so);
    }

    /// How much capacity is reserved by in-flight novations on a peer+side.
    pub fn reserved(&self, peer: &str, side: usize) -> i64 {
        self.novations
            .values()
            .filter(|so| so.data.side[side] == peer)
            .map(|so| so.data.amount)
            .sum()
    }

    // -- helpers --

    fn check_connected(&self, peer: &str) -> bool {
        self.is_connected.as_ref().map_or(false, |f| f(peer))
    }

    fn get_bandwidth(&self, peer: &str, side: usize) -> i64 {
        self.bandwidth.as_ref().map_or(0, |f| f(peer, side))
    }

    fn get_pending_balance(&self, peer: &str, side: usize) -> i64 {
        self.pending_balance.as_ref().map_or(0, |f| f(peer, side))
    }

    fn do_add_balance(&self, peer: &str, amount: i64, ts: i64) {
        if let Some(ref f) = self.add_balance {
            f(peer, amount, ts);
        }
    }

    /// Send a message to a peer via the novation's own SyncObject queue.
    fn send(&mut self, nov_id: &str, peer: &str, method: &str, payload: Value) {
        if let Some(so) = self.novations.get_mut(nov_id) {
            so.send(peer, method, Some(payload));
        }
    }

    /// Clear queued messages for one side of a novation and delete the queue.
    fn remove_side(&mut self, nov_id: &str, side: usize) {
        if let Some(so) = self.novations.get_mut(nov_id) {
            let peer = so.data.side[side].clone();
            if !peer.is_empty() {
                so.delete_queue(&peer);
                so.data.side[side] = String::new();
            }
        }
    }

    // -- request handlers --

    fn req_commit(
        &mut self,
        peer: &str,
        payload: Option<Value>,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let msg: CommitMsg = match payload {
            Some(v) => serde_json::from_value(v)?,
            None => return Err("bad payload".into()),
        };
        if msg.amount <= 0 {
            return Ok(HandleResult { ok: false });
        }

        // Confirmation back to initiator — novation already exists.
        if self.novations.contains_key(&msg.novation_id) {
            let so = self.novations.get(&msg.novation_id).unwrap();
            if !so.data.side[IN].is_empty() {
                return Err(format!(
                    "novation {} already confirmed",
                    msg.novation_id
                )
                .into());
            }
            if self.get_pending_balance(peer, IN) < so.data.amount {
                return Ok(HandleResult { ok: false });
            }
            let out_peer = so.data.side[OUT].clone();
            let so = self.novations.get_mut(&msg.novation_id).unwrap();
            so.data.side[IN] = peer.to_string();
            let finalize =
                serde_json::to_value(FinalizeMsg {
                    novation_id: msg.novation_id.clone(),
                })
                .unwrap();
            self.send(&msg.novation_id, &out_peer, "finalize", finalize);
            return Ok(HandleResult { ok: true });
        }

        // New commit — verify connectivity and bandwidth.
        if msg.next.is_empty() || !self.check_connected(&msg.next) {
            return Ok(HandleResult { ok: false });
        }
        if self.novations.len() >= MAX_NOVATIONS {
            return Ok(HandleResult { ok: false });
        }
        let bw_in = self.get_bandwidth(peer, IN);
        let bw_out = self.get_bandwidth(&msg.next, OUT);
        if bw_in < msg.amount || bw_out < msg.amount {
            return Ok(HandleResult { ok: false });
        }

        let nov = Novation {
            amount: msg.amount,
            side: [peer.to_string(), String::new()],
            initiator: false,
        };
        self.insert_novation(msg.novation_id.clone(), nov);

        let next = msg.next.clone();
        let fwd = serde_json::to_value(CommitMsg {
            novation_id: msg.novation_id.clone(),
            amount: msg.amount,
            next: peer.to_string(),
        })
        .unwrap();
        self.send(&msg.novation_id, &next, "commit", fwd);
        Ok(HandleResult { ok: true })
    }

    fn resp_commit(&mut self, peer: &str, payload: Option<Value>, ok: bool) {
        let msg: CommitMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        if !ok {
            // Check if IN side is empty before removing.
            let in_empty = self.novations.get(&msg.novation_id)
                .map(|so| so.data.side[IN].is_empty())
                .unwrap_or(true);
            if in_empty {
                self.novations.remove(&msg.novation_id); // Drop cleans up queues.
                return;
            }
            let in_peer = self.novations.get(&msg.novation_id)
                .map(|so| so.data.side[IN].clone())
                .unwrap_or_default();
            let rollback = serde_json::to_value(RollbackMsg {
                novation_id: msg.novation_id.clone(),
            })
            .unwrap();
            self.send(&msg.novation_id, &in_peer, "rollback", rollback);
            return;
        }
        if let Some(so) = self.novations.get_mut(&msg.novation_id) {
            so.data.side[OUT] = peer.to_string();
        }
    }

    fn req_finalize(
        &mut self,
        peer: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let msg: FinalizeMsg = match payload {
            Some(v) => serde_json::from_value(v)?,
            None => return Err("bad payload".into()),
        };
        let (amount, out_peer) = {
            let so = self.novations.get(&msg.novation_id)
                .ok_or_else(|| format!("novation {} not found", msg.novation_id))?;
            if so.data.side[IN] != peer {
                return Err("wrong peer".into());
            }
            (so.data.amount, so.data.side[OUT].clone())
        };

        self.do_add_balance(peer, amount, ts);
        self.remove_side(&msg.novation_id, IN);

        if !out_peer.is_empty() {
            let fwd = serde_json::to_value(FinalizeMsg {
                novation_id: msg.novation_id.clone(),
            })
            .unwrap();
            self.send(&msg.novation_id, &out_peer, "finalize", fwd);
        }
        if out_peer.is_empty() {
            self.novations.remove(&msg.novation_id); // Drop cleans up.
        }
        Ok(HandleResult { ok: true })
    }

    fn resp_finalize(&mut self, peer: &str, payload: Option<Value>, ts: i64) {
        let msg: FinalizeMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        let (amount, in_peer) = {
            let so = match self.novations.get(&msg.novation_id) {
                Some(so) => so,
                None => return,
            };
            (so.data.amount, so.data.side[IN].clone())
        };

        self.do_add_balance(peer, -amount, ts);
        self.remove_side(&msg.novation_id, OUT);

        if in_peer.is_empty() {
            self.novations.remove(&msg.novation_id); // Drop cleans up.
        }
    }

    fn req_rollback(
        &mut self,
        peer: &str,
        payload: Option<Value>,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        let msg: RollbackMsg = match payload {
            Some(v) => serde_json::from_value(v)?,
            None => return Err("bad payload".into()),
        };
        let in_peer = {
            let so = self.novations.get(&msg.novation_id)
                .ok_or_else(|| format!("unknown novation {}", msg.novation_id))?;
            if so.data.side[OUT] != peer {
                return Err("wrong peer".into());
            }
            so.data.side[IN].clone()
        };
        self.remove_side(&msg.novation_id, OUT);

        if in_peer.is_empty() {
            self.novations.remove(&msg.novation_id); // Drop cleans up.
            return Ok(HandleResult { ok: true });
        }
        let fwd = serde_json::to_value(RollbackMsg {
            novation_id: msg.novation_id.clone(),
        })
        .unwrap();
        self.send(&msg.novation_id, &in_peer, "rollback", fwd);
        Ok(HandleResult { ok: true })
    }

    fn resp_rollback(&mut self, payload: Option<Value>) {
        let msg: RollbackMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        self.novations.remove(&msg.novation_id); // Drop cleans up queues.
    }

    /// Scan for circular debt and initiate novations (called periodically).
    pub fn try_novate(&mut self) {
        let peers = match &self.connected_peers {
            Some(f) => f(),
            None => return,
        };
        for debtor in &peers {
            if self.novations.len() >= MAX_NOVATIONS {
                return;
            }
            let mut avl_d = self.get_pending_balance(debtor, OUT);
            if avl_d <= 0 {
                continue;
            }
            for creditor in &peers {
                if creditor == debtor {
                    continue;
                }
                let avl_c = self.get_pending_balance(creditor, IN);
                if avl_c <= 0 {
                    continue;
                }
                let mut amount = avl_d;
                if avl_c < amount {
                    amount = avl_c;
                }
                if amount <= 0 {
                    continue;
                }
                let nov_id = random_novation_id();
                let nov = Novation {
                    initiator: true,
                    amount,
                    side: [String::new(), debtor.clone()],
                };
                self.insert_novation(nov_id.clone(), nov);
                let commit = serde_json::to_value(CommitMsg {
                    novation_id: nov_id.clone(),
                    amount,
                    next: creditor.clone(),
                })
                .unwrap();
                self.send(&nov_id, debtor, "commit", commit);
                avl_d -= amount;
                if avl_d <= 0 {
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Module trait
// ---------------------------------------------------------------------------

impl Module for Triad {
    fn on_connect(&mut self, _peer: &str) {}
    fn on_close(&mut self, _peer: &str) {}

    fn handle_request(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ts: i64,
    ) -> Result<HandleResult, Box<dyn std::error::Error>> {
        match method {
            "commit" => self.req_commit(peer, payload),
            "finalize" => self.req_finalize(peer, payload, ts),
            "rollback" => self.req_rollback(peer, payload),
            _ => Err("unknown method".into()),
        }
    }

    fn handle_response(
        &mut self,
        peer: &str,
        method: &str,
        payload: Option<Value>,
        ok: bool,
        ts: i64,
    ) {
        match method {
            "commit" => self.resp_commit(peer, payload, ok),
            "finalize" => self.resp_finalize(peer, payload, ts),
            "rollback" => self.resp_rollback(payload),
            _ => {}
        }
    }

    fn handle_account(
        &mut self,
        method: &str,
        payload: Option<Value>,
    ) -> Result<Option<Value>, Box<dyn std::error::Error>> {
        match method {
            "forceRollback" | "forceFinalize" => {
                let (side, msg_method) = if method == "forceRollback" {
                    (IN, "rollback")
                } else {
                    (OUT, "finalize")
                };
                let p: RollbackMsg = match payload {
                    Some(v) => serde_json::from_value(v)?,
                    None => return Err("bad payload".into()),
                };
                let (peer, other_empty) = {
                    let so = self.novations.get(&p.novation_id)
                        .ok_or_else(|| format!("unknown novation {}", p.novation_id))?;
                    if so.data.side[side].is_empty() {
                        return Err(format!("no peer on {} side", method).into());
                    }
                    let other = if side == OUT { IN } else { OUT };
                    if !so.data.side[other].is_empty() {
                        return Err(format!(
                            "{}: opposite side still has peer {}",
                            method, so.data.side[other]
                        )
                        .into());
                    }
                    (so.data.side[side].clone(), so.data.side[other].is_empty())
                };
                let _ = other_empty;
                let msg_payload = if msg_method == "finalize" {
                    serde_json::to_value(FinalizeMsg {
                        novation_id: p.novation_id.clone(),
                    })
                    .unwrap()
                } else {
                    serde_json::to_value(RollbackMsg {
                        novation_id: p.novation_id.clone(),
                    })
                    .unwrap()
                };
                self.send(&p.novation_id, &peer, msg_method, msg_payload);
                Ok(None)
            }
            _ => Err("unknown method".into()),
        }
    }

    fn status(&self) -> Option<Value> {
        let novs: HashMap<String, Value> = self
            .novations
            .iter()
            .map(|(id, so)| {
                (
                    id.clone(),
                    serde_json::json!({
                        "amount": so.data.amount,
                        "in": so.data.side[IN],
                        "out": so.data.side[OUT],
                    }),
                )
            })
            .collect();
        Some(serde_json::json!({"novations": novs}))
    }

    fn busy(&self, peer: &str) -> bool {
        self.novations
            .values()
            .any(|so| so.data.side[IN] == peer || so.data.side[OUT] == peer)
    }

    fn force_remove(&mut self, peer: &str) {
        let ids: Vec<String> = self.novations.keys().cloned().collect();
        for id in ids {
            let (is_out, is_in) = {
                let so = match self.novations.get(&id) {
                    Some(so) => so,
                    None => continue,
                };
                (so.data.side[OUT] == peer, so.data.side[IN] == peer)
            };
            if is_out {
                self.remove_side(&id, OUT);
                let in_peer = self.novations.get(&id)
                    .map(|so| so.data.side[IN].clone())
                    .unwrap_or_default();
                if !in_peer.is_empty() {
                    let rollback = serde_json::to_value(RollbackMsg {
                        novation_id: id.clone(),
                    })
                    .unwrap();
                    self.send(&id, &in_peer, "rollback", rollback);
                } else {
                    self.novations.remove(&id); // Drop cleans up.
                }
                continue;
            }
            if is_in {
                self.remove_side(&id, IN);
                let out_peer = self.novations.get(&id)
                    .map(|so| so.data.side[OUT].clone())
                    .unwrap_or_default();
                if out_peer.is_empty() {
                    self.novations.remove(&id); // Drop cleans up.
                }
            }
        }
    }

    fn collect_outbox(&self, peer: &str) -> Vec<OutboxCandidate> {
        // Find the first novation that has a pending entry for this peer.
        // queue_id = novation_id, so dequeue knows which SyncObject to use.
        for (id, so) in &self.novations {
            if let Some(entry) = so.peek(peer) {
                return vec![OutboxCandidate {
                    method: entry.method.clone(),
                    payload: entry.payload.clone(),
                    queued_at: entry.time,
                    queue_id: id.clone(),
                }];
            }
        }
        vec![]
    }

    fn dequeue(&mut self, peer: &str, queue_id: &str) {
        // queue_id is the novation_id.
        if let Some(so) = self.novations.get_mut(queue_id) {
            so.dequeue(peer);
        }
    }

    fn outbox_has(&self, peer: &str) -> bool {
        self.novations.values().any(|so| so.has_pending(peer))
    }
}

/// Generate a random novation ID (64 hex chars).
fn random_novation_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:032x}", ts)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: insert a raw novation (no kick) for testing.
    fn insert_test_nov(t: &mut Triad, id: &str, nov: Novation) {
        t.novations.insert(id.to_string(), SyncObject::without_kick(nov));
    }

    #[test]
    fn triad_reserved_counts() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        insert_test_nov(&mut t, "n1", Novation {
            amount: 100,
            side: ["bob@b.com".into(), "carol@c.com".into()],
            initiator: false,
        });
        insert_test_nov(&mut t, "n2", Novation {
            amount: 50,
            side: ["bob@b.com".into(), "".into()],
            initiator: false,
        });
        assert_eq!(t.reserved("bob@b.com", IN), 150);
        assert_eq!(t.reserved("carol@c.com", OUT), 100);
        assert_eq!(t.reserved("carol@c.com", IN), 0);
    }

    #[test]
    fn triad_busy_checks_sides() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        assert!(!t.busy("bob@b.com"));

        insert_test_nov(&mut t, "n1", Novation {
            amount: 100,
            side: ["bob@b.com".into(), "carol@c.com".into()],
            initiator: false,
        });
        assert!(t.busy("bob@b.com"));
        assert!(t.busy("carol@c.com"));
        assert!(!t.busy("dave@d.com"));
    }

    #[test]
    fn triad_commit_creates_novation() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        // Stub: always connected, unlimited bandwidth.
        t.is_connected = Some(Box::new(|_| true));
        t.bandwidth = Some(Box::new(|_, _| 1_000_000));

        let payload = serde_json::to_value(CommitMsg {
            novation_id: "test-nov".into(),
            amount: 100,
            next: "carol@c.com".into(),
        })
        .unwrap();

        let result = t
            .handle_request("bob@b.com", "commit", Some(payload), 0)
            .unwrap();
        assert!(result.ok);
        assert!(t.novations.contains_key("test-nov"));

        let so = &t.novations["test-nov"];
        assert_eq!(so.data.amount, 100);
        assert_eq!(so.data.side[IN], "bob@b.com");
        assert_eq!(so.data.side[OUT], "");
        // Should have forwarded commit to carol via this novation's queue.
        assert!(so.has_pending("carol@c.com"));
    }

    #[test]
    fn triad_rollback_clears_novation() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        insert_test_nov(&mut t, "n1", Novation {
            amount: 100,
            side: ["".into(), "bob@b.com".into()],
            initiator: false,
        });

        let payload = serde_json::to_value(RollbackMsg {
            novation_id: "n1".into(),
        })
        .unwrap();
        let result = t
            .handle_request("bob@b.com", "rollback", Some(payload), 0)
            .unwrap();
        assert!(result.ok);
        assert!(!t.novations.contains_key("n1"));
    }

    #[test]
    fn triad_force_remove_rolls_back_out_side() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        insert_test_nov(&mut t, "n1", Novation {
            amount: 100,
            side: ["charlie@c.com".into(), "bob@b.com".into()],
            initiator: false,
        });

        t.force_remove("bob@b.com");

        // OUT side cleared; rollback sent to IN peer via n1's queue.
        let so = &t.novations["n1"];
        assert_eq!(so.data.side[OUT], "");
        assert!(so.has_pending("charlie@c.com"));
    }

    #[test]
    fn triad_collect_outbox_returns_head() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        insert_test_nov(&mut t, "nov-a", Novation {
            amount: 100,
            side: ["bob@b.com".into(), "".into()],
            initiator: false,
        });
        // Queue two messages on nov-a's SyncObject for bob.
        t.novations.get_mut("nov-a").unwrap().send("bob@b.com", "commit", None);
        t.novations.get_mut("nov-a").unwrap().send("bob@b.com", "finalize", None);

        let candidates = t.collect_outbox("bob@b.com");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].method, "commit");
        assert_eq!(candidates[0].queue_id, "nov-a"); // queue_id = novation_id
    }

    #[test]
    fn triad_dequeue_uses_novation_id() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        insert_test_nov(&mut t, "nov-a", Novation {
            amount: 100,
            side: ["bob@b.com".into(), "".into()],
            initiator: false,
        });
        t.novations.get_mut("nov-a").unwrap().send("bob@b.com", "commit", None);
        t.novations.get_mut("nov-a").unwrap().send("bob@b.com", "finalize", None);

        // Dequeue using novation_id as queue_id.
        t.dequeue("bob@b.com", "nov-a");
        let entry = t.novations["nov-a"].peek("bob@b.com").unwrap();
        assert_eq!(entry.method, "finalize");
    }

    #[test]
    fn triad_drop_cleans_up_queues() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let kick_count = Arc::new(AtomicUsize::new(0));
        let kc = kick_count.clone();
        let kick: KickFn = Arc::new(move |_| {
            kc.fetch_add(1, Ordering::SeqCst);
        });

        let mut t = Triad::for_test("triad", "alice@a.com");
        t.kick = Some(kick);
        t.insert_novation("n1".into(), Novation {
            amount: 100,
            side: ["bob@b.com".into(), "".into()],
            initiator: false,
        });
        // Queue a message so there's something to clean up.
        t.novations.get_mut("n1").unwrap().send("bob@b.com", "commit", None);

        // Reset counter (send already kicked once).
        kick_count.store(0, Ordering::SeqCst);

        // Remove novation — Drop should kick bob.
        t.novations.remove("n1");
        assert_eq!(kick_count.load(Ordering::SeqCst), 1);
    }
}
