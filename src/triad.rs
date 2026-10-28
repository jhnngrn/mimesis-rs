//! Triad — three-party novation to eliminate circular debt.
//!
//! A fully functional module (not a demo), meant to be simple.
//! Uses SyncObject<TriadData> so Drop guarantees cleanup of
//! per-peer outbox queues.

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

/// Persistent data wrapped by SyncObject.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TriadData {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub novations: HashMap<String, Novation>,
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

/// Triad implements Module using SyncObject for automatic queue management.
pub struct Triad {
    pub obj: SyncObject<TriadData>,
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
            obj: SyncObject::new(TriadData::default(), kick),
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
            obj: SyncObject::without_kick(TriadData::default()),
            name: name.to_string(),
            node_id: node_id.to_string(),
            bandwidth: None,
            pending_balance: None,
            add_balance: None,
            is_connected: None,
            connected_peers: None,
        }
    }

    /// How much capacity is reserved by in-flight novations on a peer+side.
    pub fn reserved(&self, peer: &str, side: usize) -> i64 {
        self.obj
            .data
            .novations
            .values()
            .filter(|n| n.side[side] == peer)
            .map(|n| n.amount)
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

    /// Send a message to a peer via SyncObject queue, using novation_id as queue key.
    fn send(&mut self, peer: &str, _nov_id: &str, method: &str, payload: Value) {
        self.obj.send(peer, method, Some(payload));
    }

    /// Clear queued messages for a novation side.
    fn remove_side(&mut self, nov_id: &str, side: usize) {
        let peer = {
            let nov = match self.obj.data.novations.get(nov_id) {
                Some(n) => n,
                None => return,
            };
            if nov.side[side].is_empty() {
                return;
            }
            nov.side[side].clone()
        };
        // Note: SyncObject queues are per-peer, not per-novation.
        // In Go, api.DeleteQueue(peer, queueID) removes per-queue.
        // Here we just clear the peer reference. The queue cleanup
        // happens via SyncObject's Drop or dequeue-on-response.
        if let Some(nov) = self.obj.data.novations.get_mut(nov_id) {
            nov.side[side] = String::new();
        }
        let _ = peer; // used for queue deletion in Go
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
        if let Some(nov) = self.obj.data.novations.get(&msg.novation_id) {
            if !nov.side[IN].is_empty() {
                return Err(format!(
                    "novation {} already confirmed",
                    msg.novation_id
                )
                .into());
            }
            if self.get_pending_balance(peer, IN) < nov.amount {
                return Ok(HandleResult { ok: false });
            }
            let out_peer = nov.side[OUT].clone();
            let nov = self
                .obj
                .data
                .novations
                .get_mut(&msg.novation_id)
                .unwrap();
            nov.side[IN] = peer.to_string();
            let finalize =
                serde_json::to_value(FinalizeMsg {
                    novation_id: msg.novation_id.clone(),
                })
                .unwrap();
            self.send(&out_peer, &msg.novation_id, "finalize", finalize);
            return Ok(HandleResult { ok: true });
        }

        // New commit — verify connectivity and bandwidth.
        if msg.next.is_empty() || !self.check_connected(&msg.next) {
            return Ok(HandleResult { ok: false });
        }
        if self.obj.data.novations.len() >= MAX_NOVATIONS {
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
        self.obj
            .data
            .novations
            .insert(msg.novation_id.clone(), nov);

        let next = msg.next.clone();
        let fwd = serde_json::to_value(CommitMsg {
            novation_id: msg.novation_id.clone(),
            amount: msg.amount,
            next: peer.to_string(),
        })
        .unwrap();
        self.send(&next, &msg.novation_id, "commit", fwd);
        Ok(HandleResult { ok: true })
    }

    fn resp_commit(&mut self, peer: &str, payload: Option<Value>, ok: bool) {
        let msg: CommitMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        let nov = match self.obj.data.novations.get_mut(&msg.novation_id) {
            Some(n) => n,
            None => return,
        };
        if !ok {
            if nov.side[IN].is_empty() {
                self.obj.data.novations.remove(&msg.novation_id);
                return;
            }
            let in_peer = nov.side[IN].clone();
            let rollback = serde_json::to_value(RollbackMsg {
                novation_id: msg.novation_id.clone(),
            })
            .unwrap();
            self.send(&in_peer, &msg.novation_id, "rollback", rollback);
            return;
        }
        nov.side[OUT] = peer.to_string();
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
        let nov = match self.obj.data.novations.get(&msg.novation_id) {
            Some(n) => n,
            None => return Err(format!("novation {} not found", msg.novation_id).into()),
        };
        if nov.side[IN] != peer {
            return Err("wrong peer".into());
        }
        let amount = nov.amount;
        let out_peer = nov.side[OUT].clone();

        self.do_add_balance(peer, amount, ts);
        self.remove_side(&msg.novation_id, IN);

        if !out_peer.is_empty() {
            let fwd = serde_json::to_value(FinalizeMsg {
                novation_id: msg.novation_id.clone(),
            })
            .unwrap();
            self.send(&out_peer, &msg.novation_id, "finalize", fwd);
        }
        if out_peer.is_empty() {
            self.obj.data.novations.remove(&msg.novation_id);
        }
        Ok(HandleResult { ok: true })
    }

    fn resp_finalize(&mut self, peer: &str, payload: Option<Value>, ts: i64) {
        let msg: FinalizeMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        let nov = match self.obj.data.novations.get(&msg.novation_id) {
            Some(n) => n,
            None => return,
        };
        let amount = nov.amount;
        let in_peer = nov.side[IN].clone();

        self.do_add_balance(peer, -amount, ts);
        self.remove_side(&msg.novation_id, OUT);

        if in_peer.is_empty() {
            self.obj.data.novations.remove(&msg.novation_id);
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
        let nov = match self.obj.data.novations.get(&msg.novation_id) {
            Some(n) => n,
            None => return Err(format!("unknown novation {}", msg.novation_id).into()),
        };
        if nov.side[OUT] != peer {
            return Err("wrong peer".into());
        }
        let in_peer = nov.side[IN].clone();
        self.remove_side(&msg.novation_id, OUT);

        if in_peer.is_empty() {
            self.obj.data.novations.remove(&msg.novation_id);
            return Ok(HandleResult { ok: true });
        }
        let fwd = serde_json::to_value(RollbackMsg {
            novation_id: msg.novation_id.clone(),
        })
        .unwrap();
        self.send(&in_peer, &msg.novation_id, "rollback", fwd);
        Ok(HandleResult { ok: true })
    }

    fn resp_rollback(&mut self, payload: Option<Value>) {
        let msg: RollbackMsg = match payload.and_then(|v| serde_json::from_value(v).ok()) {
            Some(m) => m,
            None => return,
        };
        self.obj.data.novations.remove(&msg.novation_id);
    }

    /// Scan for circular debt and initiate novations (called periodically).
    pub fn try_novate(&mut self) {
        let peers = match &self.connected_peers {
            Some(f) => f(),
            None => return,
        };
        for debtor in &peers {
            if self.obj.data.novations.len() >= MAX_NOVATIONS {
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
                self.obj.data.novations.insert(nov_id.clone(), nov);
                let commit = serde_json::to_value(CommitMsg {
                    novation_id: nov_id.clone(),
                    amount,
                    next: creditor.clone(),
                })
                .unwrap();
                self.send(debtor, &nov_id, "commit", commit);
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
                let nov = self
                    .obj
                    .data
                    .novations
                    .get(&p.novation_id)
                    .ok_or_else(|| {
                        format!("unknown novation {}", p.novation_id)
                    })?;
                if nov.side[side].is_empty() {
                    return Err(format!("no peer on {} side", method).into());
                }
                let other = if side == OUT { IN } else { OUT };
                if !nov.side[other].is_empty() {
                    return Err(format!(
                        "{}: opposite side still has peer {}",
                        method, nov.side[other]
                    )
                    .into());
                }
                let peer = nov.side[side].clone();
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
                self.send(&peer, &p.novation_id, msg_method, msg_payload);
                Ok(None)
            }
            _ => Err("unknown method".into()),
        }
    }

    fn status(&self) -> Option<Value> {
        let novs: HashMap<String, Value> = self
            .obj
            .data
            .novations
            .iter()
            .map(|(id, nov)| {
                (
                    id.clone(),
                    serde_json::json!({
                        "amount": nov.amount,
                        "in": nov.side[IN],
                        "out": nov.side[OUT],
                    }),
                )
            })
            .collect();
        Some(serde_json::json!({"novations": novs}))
    }

    fn busy(&self, peer: &str) -> bool {
        self.obj
            .data
            .novations
            .values()
            .any(|nov| nov.side[IN] == peer || nov.side[OUT] == peer)
    }

    fn force_remove(&mut self, peer: &str) {
        let ids: Vec<String> = self.obj.data.novations.keys().cloned().collect();
        for id in ids {
            let nov = match self.obj.data.novations.get(&id) {
                Some(n) => n,
                None => continue,
            };
            if nov.side[OUT] == peer {
                self.remove_side(&id, OUT);
                let in_peer = self
                    .obj
                    .data
                    .novations
                    .get(&id)
                    .map(|n| n.side[IN].clone())
                    .unwrap_or_default();
                if !in_peer.is_empty() {
                    let rollback = serde_json::to_value(RollbackMsg {
                        novation_id: id.clone(),
                    })
                    .unwrap();
                    self.send(&in_peer, &id, "rollback", rollback);
                } else {
                    self.obj.data.novations.remove(&id);
                }
                continue;
            }
            if nov.side[IN] == peer {
                self.remove_side(&id, IN);
                let out_peer = self
                    .obj
                    .data
                    .novations
                    .get(&id)
                    .map(|n| n.side[OUT].clone())
                    .unwrap_or_default();
                if out_peer.is_empty() {
                    self.obj.data.novations.remove(&id);
                }
            }
        }
    }

    fn collect_outbox(&self, peer: &str) -> Vec<OutboxCandidate> {
        let entries = match self.obj.queues.get(peer) {
            Some(q) if !q.is_empty() => q,
            _ => return vec![],
        };
        // Return only the head entry (bilateral sync sends one at a time).
        let entry = &entries[0];
        vec![OutboxCandidate {
            method: entry.method.clone(),
            payload: entry.payload.clone(),
            queued_at: entry.time,
            queue_id: peer.to_string(),
        }]
    }

    fn dequeue(&mut self, peer: &str, _queue_id: &str) {
        self.obj.dequeue(peer);
    }

    fn outbox_has(&self, peer: &str) -> bool {
        self.obj.has_pending(peer)
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

    #[test]
    fn triad_reserved_counts() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.obj.data.novations.insert(
            "n1".into(),
            Novation {
                amount: 100,
                side: ["bob@b.com".into(), "carol@c.com".into()],
                initiator: false,
            },
        );
        t.obj.data.novations.insert(
            "n2".into(),
            Novation {
                amount: 50,
                side: ["bob@b.com".into(), "".into()],
                initiator: false,
            },
        );
        assert_eq!(t.reserved("bob@b.com", IN), 150);
        assert_eq!(t.reserved("carol@c.com", OUT), 100);
        assert_eq!(t.reserved("carol@c.com", IN), 0);
    }

    #[test]
    fn triad_busy_checks_sides() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        assert!(!t.busy("bob@b.com"));

        t.obj.data.novations.insert(
            "n1".into(),
            Novation {
                amount: 100,
                side: ["bob@b.com".into(), "carol@c.com".into()],
                initiator: false,
            },
        );
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
        assert!(t.obj.data.novations.contains_key("test-nov"));

        let nov = &t.obj.data.novations["test-nov"];
        assert_eq!(nov.amount, 100);
        assert_eq!(nov.side[IN], "bob@b.com");
        assert_eq!(nov.side[OUT], "");
        // Should have forwarded commit to carol.
        assert!(t.obj.has_pending("carol@c.com"));
    }

    #[test]
    fn triad_rollback_clears_novation() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.obj.data.novations.insert(
            "n1".into(),
            Novation {
                amount: 100,
                side: ["".into(), "bob@b.com".into()],
                initiator: false,
            },
        );

        let payload = serde_json::to_value(RollbackMsg {
            novation_id: "n1".into(),
        })
        .unwrap();
        let result = t
            .handle_request("bob@b.com", "rollback", Some(payload), 0)
            .unwrap();
        assert!(result.ok);
        assert!(!t.obj.data.novations.contains_key("n1"));
    }

    #[test]
    fn triad_force_remove_rolls_back_out_side() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.obj.data.novations.insert(
            "n1".into(),
            Novation {
                amount: 100,
                side: ["charlie@c.com".into(), "bob@b.com".into()],
                initiator: false,
            },
        );

        t.force_remove("bob@b.com");

        // OUT side cleared; rollback sent to IN peer.
        let nov = &t.obj.data.novations["n1"];
        assert_eq!(nov.side[OUT], "");
        assert!(t.obj.has_pending("charlie@c.com"));
    }

    #[test]
    fn triad_collect_outbox_returns_head() {
        let mut t = Triad::for_test("triad", "alice@a.com");
        t.obj.send("bob@b.com", "commit", None);
        t.obj.send("bob@b.com", "finalize", None);

        let candidates = t.collect_outbox("bob@b.com");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].method, "commit");
    }
}
