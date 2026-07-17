//! Client protocol: PUT/GET/DELETE/SCAN/TXN routing and membership proposals.
//!
//! ## Transactions (M17)
//!
//! TXN opcodes 9–12 stage write intents on the **leader** engine only. On
//! `TXN_COMMIT`, intents are taken and proposed as a single atomic
//! [`RaftCommand::TxnCommit`] (type 4) so multi-key materialization is
//! all-or-nothing at the Raft apply layer. In-flight intents remain leader-local
//! (followers do not observe uncommitted intents).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use kaya_engine::{ReadOptions, ScanOptions};
use std::path::PathBuf;

use kaya_engine::CdcOp;
use kaya_net::{
    decode_cdc_checkpoint_request, decode_cdc_poll_request, decode_hello_request,
    decode_key_payload, decode_member_payload, decode_merge_range_request,
    decode_promote_learner_payload, decode_put_payload, decode_remove_member_payload,
    decode_scan_payload, decode_split_range_request, decode_transfer_leader_request,
    decode_txn_id_payload, decode_txn_op_payload, encode_cdc_poll_response, encode_error_payload,
    encode_hello_response, encode_list_ranges_response, encode_range_moved_payload,
    encode_rebalance_plan_response, encode_scan_response, encode_txn_begin_response,
    encode_txn_commit_response, encode_value_payload, read_client_frame, send_envelopes,
    write_client_response, NodeRoster, CDC_CHECKPOINT_OPCODE, CDC_EVENT_DELETE, CDC_EVENT_PUT,
    CDC_POLL_OPCODE, LIST_RANGES_OPCODE, MERGE_RANGE_OPCODE, PROTO_VERSION, REBALANCE_PLAN_OPCODE,
    SPLIT_RANGE_OPCODE, STATUS_ERROR, STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_NOT_LEADER,
    STATUS_OK, STATUS_RANGE_MOVED, STATUS_TXN_CONFLICT, TRANSFER_LEADER_OPCODE, TXN_BEGIN_OPCODE,
    TXN_COMMIT_OPCODE, TXN_OP_DELETE, TXN_OP_GET, TXN_OP_OPCODE, TXN_OP_PUT, TXN_ROLLBACK_OPCODE,
};
use kaya_raft::{
    multi_raft_group_dir, ClusterMember, GroupId, NodeId, RaftConfig, RaftNode, StaticRangeTable,
};
use tokio::sync::RwLock;

use super::{
    SharedApplyIndexes, SharedEngine, SharedPending, SharedPendingReads, SharedPersisters,
    SharedRaftHost,
};
use crate::apply_index::RaftApplyIndex;
use crate::raft_persister::RaftPersister;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::audit::AuditLog;
use crate::client_auth::{decode_client_auth_payload, CLIENT_AUTH_PREFIX};
use crate::command::RaftCommand;
use crate::membership::{members_for_add, members_for_promote, members_for_remove, SharedRoster};
use crate::operator_auth::{
    decode_admin_payload, ADD_MEMBER_OPCODE, ADMIN_AUTH_PREFIX, PROMOTE_LEARNER_OPCODE,
    REMOVE_MEMBER_OPCODE,
};

use super::balancer::{plan_range_count, RebalancePlan};

use super::stats::build_stats_response;

type SharedAuditLog = Option<Arc<AuditLog>>;
/// Mutable range / meta table (M21).
pub(crate) type SharedRangeTable = Arc<RwLock<StaticRangeTable>>;

/// Context needed to create Raft groups at runtime during splits.
#[derive(Clone)]
pub(crate) struct SplitRuntime {
    pub raft: SharedRaftHost,
    pub persisters: SharedPersisters,
    pub apply_indexes: SharedApplyIndexes,
    pub data_dir: PathBuf,
    pub node_id: NodeId,
    pub peers: Vec<NodeId>,
    pub election_timeout_ticks: u64,
    pub heartbeat_interval_ticks: u64,
}

struct DispatchOutcome {
    status: u16,
    body: Vec<u8>,
    auth_kind: &'static str,
    key_len: Option<usize>,
}

fn outcome(
    status: u16,
    body: Vec<u8>,
    auth_kind: &'static str,
    key_len: Option<usize>,
) -> DispatchOutcome {
    DispatchOutcome {
        status,
        body,
        auth_kind,
        key_len,
    }
}

/// Message sent from a client handler to the Raft loop to propose a write.
pub struct ProposeReq {
    /// Target Raft group (0 for single-group / membership).
    pub(crate) group_id: u64,
    pub(crate) command: Vec<u8>,
    pub(crate) reply_tx: oneshot::Sender<Result<(), String>>,
}

pub struct ReadIndexReq {
    pub(crate) group_id: u64,
    pub(crate) request_id: u64,
    pub(crate) reply_tx: oneshot::Sender<Result<(), String>>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn client_accept_loop(
    listener: TcpListener,
    raft: SharedRaftHost,
    engine: SharedEngine,
    pending: SharedPending,
    pending_reads: SharedPendingReads,
    propose_tx: mpsc::Sender<ProposeReq>,
    read_propose_tx: mpsc::Sender<ReadIndexReq>,
    next_read_req_id: Arc<AtomicU64>,
    roster: SharedRoster,
    range_table: SharedRangeTable,
    split_rt: SplitRuntime,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
    client_token: Option<String>,
    audit_log: SharedAuditLog,
    network_partitioned: Option<Arc<AtomicBool>>,
    max_connections: usize,
    drain: bool,
) {
    // Backpressure: stop accepting when `max_connections` handlers are live;
    // further connections queue in the OS backlog until a permit frees up.
    let connection_permits = Arc::new(Semaphore::new(max_connections.max(1)));
    loop {
        let permit = match connection_permits.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let Ok((stream, peer)) = listener.accept().await else {
            return;
        };
        if network_partitioned
            .as_ref()
            .is_some_and(|f| f.load(Ordering::SeqCst))
        {
            drop(stream);
            continue;
        }
        let r = raft.clone();
        let e = engine.clone();
        let p = pending.clone();
        let pr = pending_reads.clone();
        let tx = propose_tx.clone();
        let rtx = read_propose_tx.clone();
        let next_id = next_read_req_id.clone();
        let ros = roster.clone();
        let ranges = range_table.clone();
        let split = split_rt.clone();
        let op_tok = operator_token.clone();
        let cli_tok = client_token.clone();
        let audit = audit_log.clone();
        tokio::spawn(async move {
            let _permit = permit;
            handle_connection(
                stream,
                peer,
                r,
                e,
                p,
                pr,
                tx,
                rtx,
                next_id,
                ros,
                ranges,
                split,
                self_id,
                self_raft,
                self_client,
                op_tok,
                cli_tok,
                audit,
                drain,
            )
            .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection<S>(
    mut stream: S,
    peer: SocketAddr,
    raft: SharedRaftHost,
    engine: SharedEngine,
    _pending: SharedPending,
    _pending_reads: SharedPendingReads,
    propose_tx: mpsc::Sender<ProposeReq>,
    read_propose_tx: mpsc::Sender<ReadIndexReq>,
    next_read_req_id: Arc<AtomicU64>,
    roster: SharedRoster,
    range_table: SharedRangeTable,
    split_rt: SplitRuntime,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
    client_token: Option<String>,
    audit_log: SharedAuditLog,
    drain: bool,
) where
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin,
{
    loop {
        let (opcode, payload) = match read_client_frame(&mut stream).await {
            Ok(f) => f,
            Err(_) => break,
        };
        let result = dispatch(
            &raft,
            &engine,
            &roster,
            &range_table,
            &split_rt,
            &propose_tx,
            &read_propose_tx,
            &next_read_req_id,
            opcode,
            payload,
            self_id,
            self_raft,
            self_client,
            operator_token.clone(),
            client_token.clone(),
            drain,
        )
        .await;
        if let Some(audit) = audit_log.as_ref() {
            audit.record(
                peer,
                opcode,
                result.status,
                result.auth_kind,
                result.key_len,
            );
        }
        if write_client_response(&mut stream, result.status, &result.body)
            .await
            .is_err()
        {
            break;
        }
    }
}

fn get_leader_hint(raft: &SharedRaftHost, roster: &NodeRoster) -> Vec<u8> {
    let leader_id = {
        let host = raft.lock().unwrap();
        host.primary_status()
            .or_else(|| {
                host.sorted_group_ids()
                    .into_iter()
                    .find_map(|g| host.status_of(g))
            })
            .and_then(|s| s.leader_id)
    };
    if let Some(leader_id) = leader_id {
        if let Some(addr) = roster.client_addr(leader_id) {
            return addr.to_string().into_bytes();
        }
    }
    vec![]
}

/// Build an advisory rebalance plan from current range leaders (range-count heuristic).
/// Nodes with no known leadership still appear with an empty range list so they can
/// receive moves. Ranges whose group has no known leader are omitted.
async fn build_rebalance_plan(
    raft: &SharedRaftHost,
    roster: &SharedRoster,
    range_table: &SharedRangeTable,
) -> RebalancePlan {
    use std::collections::HashMap;

    let roster_snap = roster.read().await.clone();
    let mut by_node: HashMap<u64, Vec<u64>> = HashMap::new();
    for id in roster_snap.all_ids() {
        by_node.insert(id.0, Vec::new());
    }

    let table = range_table.read().await;
    {
        let host = raft.lock().unwrap();
        for r in table.ranges() {
            let owner = host
                .status_of(r.group_id)
                .and_then(|s| s.leader_id.map(|n| n.0))
                .or_else(|| {
                    if host.is_leader_of(r.group_id) {
                        host.status_of(r.group_id).map(|s| s.id.0)
                    } else {
                        None
                    }
                });
            if let Some(nid) = owner {
                by_node.entry(nid).or_default().push(r.range_id);
            }
        }
    }

    let nodes: Vec<(u64, Vec<u64>)> = by_node.into_iter().collect();
    plan_range_count(&nodes)
}

fn lookup_group(range_table: &StaticRangeTable, key: &[u8]) -> GroupId {
    range_table.lookup(key).unwrap_or(GroupId::ZERO)
}

fn is_leader_of(raft: &SharedRaftHost, group_id: GroupId) -> bool {
    raft.lock().unwrap().is_leader_of(group_id)
}

/// True when the range table points at a group this process does not host.
fn group_not_hosted(raft: &SharedRaftHost, group_id: GroupId) -> bool {
    raft.lock().unwrap().get(group_id).is_none()
}

/// Build `STATUS_RANGE_MOVED` with a list-ranges body for the key's current owner.
fn range_moved_for_key(
    range_table: &StaticRangeTable,
    key: &[u8],
    client_auth: &'static str,
    key_len: Option<usize>,
) -> Option<DispatchOutcome> {
    let r = range_table.lookup_range(key)?;
    Some(outcome(
        STATUS_RANGE_MOVED,
        encode_range_moved_payload(
            range_table.meta_epoch(),
            r.range_id,
            r.epoch,
            r.group_id.0,
            &r.start_key,
            &r.end_key,
        ),
        client_auth,
        key_len,
    ))
}

/// Ensure a Raft group exists on this host (create empty node + persist paths).
fn ensure_group_hosted(rt: &SplitRuntime, group_id: GroupId) -> Result<(), String> {
    {
        let host = rt.raft.lock().unwrap();
        if host.get(group_id).is_some() {
            return Ok(());
        }
    }
    let group_dir = multi_raft_group_dir(&rt.data_dir, group_id);
    if group_id.0 != 0 {
        std::fs::create_dir_all(&group_dir).map_err(|e| e.to_string())?;
    }
    let raft_cfg = RaftConfig {
        id: rt.node_id,
        peers: rt.peers.clone(),
        election_timeout_ticks: rt.election_timeout_ticks,
        heartbeat_interval_ticks: rt.heartbeat_interval_ticks,
    };
    let mut persister = RaftPersister::open(&group_dir).map_err(|e| e.to_string())?;
    let apply = RaftApplyIndex::open(&group_dir).map_err(|e| e.to_string())?;
    let node = match persister.load_state()? {
        Some(state) => {
            let seed = state.clone();
            let mut n = RaftNode::recover(raft_cfg, state);
            n.set_recovered_apply_floor(kaya_raft::LogIndex(0));
            persister.seed_last_persisted(seed);
            n
        }
        None => RaftNode::new(raft_cfg),
    };
    {
        let mut host = rt.raft.lock().unwrap();
        if host.get(group_id).is_none() {
            host.insert(group_id, node);
        }
    }
    {
        let mut p = rt.persisters.lock().unwrap();
        p.entry(group_id.0).or_insert(persister);
    }
    {
        let mut a = rt.apply_indexes.lock().unwrap();
        a.entry(group_id.0).or_insert(apply);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    raft: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &SharedRoster,
    range_table: &SharedRangeTable,
    split_rt: &SplitRuntime,
    propose_tx: &mpsc::Sender<ProposeReq>,
    read_propose_tx: &mpsc::Sender<ReadIndexReq>,
    next_read_req_id: &Arc<AtomicU64>,
    opcode: u8,
    payload: Vec<u8>,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
    client_token: Option<String>,
    drain: bool,
) -> DispatchOutcome {
    let operator_auth = if operator_token.is_some() {
        "operator"
    } else {
        "none"
    };
    let client_auth = if client_token.is_some() {
        "client"
    } else {
        "none"
    };

    // HELLO (0): optional protocol version handshake; no auth required.
    if opcode == 0 {
        return match decode_hello_request(&payload) {
            Ok(client_version) if client_version > PROTO_VERSION => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&format!(
                    "unsupported protocol version {client_version} (max {PROTO_VERSION})"
                )),
                "none",
                None,
            ),
            Ok(_) => outcome(
                STATUS_OK,
                encode_hello_response(PROTO_VERSION),
                "none",
                None,
            ),
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                "none",
                None,
            ),
        };
    }

    // Handle admin opcodes 7/8/18/19/20 (ADD/REMOVE/TRANSFER/PROMOTE/REBALANCE_PLAN)
    // with optional operator token enforcement. Supports backward-compat raw payloads
    // (no token configured) and ADMIN-prefixed payloads when clients present the
    // credential. If server has token set, must match.
    if opcode == ADD_MEMBER_OPCODE
        || opcode == REMOVE_MEMBER_OPCODE
        || opcode == TRANSFER_LEADER_OPCODE
        || opcode == PROMOTE_LEARNER_OPCODE
        || opcode == REBALANCE_PLAN_OPCODE
    {
        // Peel optional ADMIN prefix + token if present; otherwise treat payload as legacy raw.
        let (clean_payload, presented) =
            if payload.len() >= ADMIN_AUTH_PREFIX.len() && payload.starts_with(ADMIN_AUTH_PREFIX) {
                match decode_admin_payload(&payload) {
                    Ok((got_opcode, inner, tok)) => {
                        if got_opcode != opcode {
                            return outcome(
                                STATUS_INVALID_ARGUMENT,
                                encode_error_payload("admin opcode mismatch"),
                                operator_auth,
                                None,
                            );
                        }
                        (inner, tok)
                    }
                    Err(e) => {
                        return outcome(
                            STATUS_INVALID_ARGUMENT,
                            encode_error_payload(&e),
                            operator_auth,
                            None,
                        );
                    }
                }
            } else {
                (payload, None)
            };

        if let Some(expected) = &operator_token {
            if presented.as_deref() != Some(expected.as_str()) {
                return outcome(
                    STATUS_ERROR,
                    encode_error_payload("operator credential required or invalid"),
                    operator_auth,
                    None,
                );
            }
        }

        if opcode == ADD_MEMBER_OPCODE {
            return match decode_member_payload(&clean_payload) {
                Ok((node_id, raft_addr, client_addr, is_learner)) => {
                    let (status, body) = propose_add_member(
                        raft,
                        roster,
                        self_id,
                        self_raft,
                        self_client,
                        NodeId(node_id),
                        raft_addr,
                        client_addr,
                        is_learner,
                    )
                    .await;
                    outcome(status, body, operator_auth, None)
                }
                Err(e) => outcome(
                    STATUS_INVALID_ARGUMENT,
                    encode_error_payload(&e),
                    operator_auth,
                    None,
                ),
            };
        } else if opcode == REMOVE_MEMBER_OPCODE {
            return match decode_remove_member_payload(&clean_payload) {
                Ok(node_id) => {
                    let (status, body) = propose_remove_member(
                        raft,
                        roster,
                        self_id,
                        self_raft,
                        self_client,
                        NodeId(node_id),
                    )
                    .await;
                    outcome(status, body, operator_auth, None)
                }
                Err(e) => outcome(
                    STATUS_INVALID_ARGUMENT,
                    encode_error_payload(&e),
                    operator_auth,
                    None,
                ),
            };
        } else if opcode == PROMOTE_LEARNER_OPCODE {
            return match decode_promote_learner_payload(&clean_payload) {
                Ok(node_id) => {
                    let (status, body) = propose_promote_learner(
                        raft,
                        roster,
                        self_id,
                        self_raft,
                        self_client,
                        NodeId(node_id),
                    )
                    .await;
                    outcome(status, body, operator_auth, None)
                }
                Err(e) => outcome(
                    STATUS_INVALID_ARGUMENT,
                    encode_error_payload(&e),
                    operator_auth,
                    None,
                ),
            };
        } else if opcode == REBALANCE_PLAN_OPCODE {
            // Advisory only: range-count heuristic over current group leaders.
            // Empty body; does not migrate data or transfer leases.
            let plan = build_rebalance_plan(raft, roster, range_table).await;
            let wire: Vec<(u64, u64, u64)> = plan
                .moves
                .iter()
                .map(|m| (m.range_id, m.from_node, m.to_node))
                .collect();
            return outcome(
                STATUS_OK,
                encode_rebalance_plan_response(&wire),
                operator_auth,
                None,
            );
        } else {
            // TRANSFER_LEADER (18): group_id | target_node_id — leader steps down.
            return match decode_transfer_leader_request(&clean_payload) {
                Ok((group_id, target_node_id)) => {
                    let result = {
                        let mut host = raft.lock().unwrap();
                        host.transfer_leadership(GroupId(group_id), NodeId(target_node_id))
                    };
                    match result {
                        Ok(()) => outcome(STATUS_OK, vec![], operator_auth, None),
                        Err(e) if e == "not leader" => {
                            let roster_snapshot = roster.read().await.clone();
                            let hint = get_leader_hint(raft, &roster_snapshot);
                            outcome(STATUS_NOT_LEADER, hint, operator_auth, None)
                        }
                        Err(e) => outcome(
                            STATUS_INVALID_ARGUMENT,
                            encode_error_payload(&e),
                            operator_auth,
                            None,
                        ),
                    }
                }
                Err(e) => outcome(
                    STATUS_INVALID_ARGUMENT,
                    encode_error_payload(&e),
                    operator_auth,
                    None,
                ),
            };
        }
    }

    // Data-path opcodes 1-4, 6 (STATS), 9-17 (TXN/CDC/ranges) with optional client token
    // enforcement. HEALTH (5) stays open for liveness probes.
    // SPLIT_RANGE (16) / MERGE_RANGE (17) also accept operator token via admin path when configured.
    let payload = if matches!(opcode, 1..=4 | 6 | 9..=17) {
        let (clean_payload, presented) = if payload.len() >= CLIENT_AUTH_PREFIX.len()
            && payload.starts_with(CLIENT_AUTH_PREFIX)
        {
            match decode_client_auth_payload(&payload) {
                Ok((inner, tok)) => (inner, tok),
                Err(e) => {
                    return outcome(
                        STATUS_INVALID_ARGUMENT,
                        encode_error_payload(&e),
                        client_auth,
                        None,
                    );
                }
            }
        } else {
            (payload, None)
        };

        if let Some(expected) = &client_token {
            if presented.as_deref() != Some(expected.as_str()) {
                return outcome(
                    STATUS_ERROR,
                    encode_error_payload("client credential required or invalid"),
                    client_auth,
                    None,
                );
            }
        }
        clean_payload
    } else {
        payload
    };

    let roster_snapshot = roster.read().await.clone();
    match opcode {
        // PUT
        1 => match decode_put_payload(&payload) {
            Ok((key, value)) => {
                let key_len = key.len();
                let group_id = {
                    let t = range_table.read().await;
                    lookup_group(&t, &key)
                };
                // If the group is missing (race after split / not hosted), signal RANGE_MOVED.
                if group_not_hosted(raft, group_id) {
                    let t = range_table.read().await;
                    if let Some(out) = range_moved_for_key(&t, &key, client_auth, Some(key_len)) {
                        return out;
                    }
                }
                let cmd = RaftCommand::Put { key, value }.encode();
                let (status, body) =
                    propose_and_wait(raft, &roster_snapshot, propose_tx, group_id, cmd).await;
                outcome(status, body, client_auth, Some(key_len))
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // GET
        2 => match decode_key_payload(&payload) {
            Ok(key) => {
                let group_id = {
                    let t = range_table.read().await;
                    lookup_group(&t, &key)
                };
                if group_not_hosted(raft, group_id) {
                    let t = range_table.read().await;
                    if let Some(out) = range_moved_for_key(&t, &key, client_auth, None) {
                        return out;
                    }
                }
                let req_id = next_read_req_id.fetch_add(1, Ordering::SeqCst);
                match propose_read_and_wait(raft, read_propose_tx, group_id, req_id).await {
                    Ok(()) => match engine.lock().await.get(&key, ReadOptions::default()).await {
                        Ok(Some(v)) => {
                            outcome(STATUS_OK, encode_value_payload(&v), client_auth, None)
                        }
                        Ok(None) => outcome(STATUS_NOT_FOUND, vec![], client_auth, None),
                        Err(e) => outcome(
                            STATUS_ERROR,
                            encode_error_payload(&e.to_string()),
                            client_auth,
                            None,
                        ),
                    },
                    Err(e) if e == "not_leader" => {
                        let hint = get_leader_hint(raft, &roster_snapshot);
                        outcome(STATUS_NOT_LEADER, hint, client_auth, None)
                    }
                    Err(e) => outcome(STATUS_ERROR, encode_error_payload(&e), client_auth, None),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // DELETE
        3 => match decode_key_payload(&payload) {
            Ok(key) => {
                let key_len = key.len();
                let group_id = {
                    let t = range_table.read().await;
                    lookup_group(&t, &key)
                };
                if group_not_hosted(raft, group_id) {
                    let t = range_table.read().await;
                    if let Some(out) = range_moved_for_key(&t, &key, client_auth, Some(key_len)) {
                        return out;
                    }
                }
                let cmd = RaftCommand::Delete { key }.encode();
                let (status, body) =
                    propose_and_wait(raft, &roster_snapshot, propose_tx, group_id, cmd).await;
                outcome(status, body, client_auth, Some(key_len))
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // SCAN
        4 => match decode_scan_payload(&payload) {
            Ok(prefix) => {
                let group_id = {
                    let t = range_table.read().await;
                    lookup_group(&t, &prefix)
                };
                if group_not_hosted(raft, group_id) {
                    let t = range_table.read().await;
                    if let Some(out) = range_moved_for_key(&t, &prefix, client_auth, None) {
                        return out;
                    }
                }
                let req_id = next_read_req_id.fetch_add(1, Ordering::SeqCst);
                match propose_read_and_wait(raft, read_propose_tx, group_id, req_id).await {
                    Ok(()) => {
                        match engine
                            .lock()
                            .await
                            .scan_prefix(&prefix, ScanOptions::default())
                            .await
                        {
                            Ok(kvs) => {
                                let items: Vec<(Vec<u8>, Vec<u8>)> =
                                    kvs.into_iter().map(|kv| (kv.key, kv.value)).collect();
                                outcome(STATUS_OK, encode_scan_response(&items), client_auth, None)
                            }
                            Err(e @ kaya_core::KayaError::InvalidArgument { .. }) => outcome(
                                STATUS_INVALID_ARGUMENT,
                                encode_error_payload(&e.to_string()),
                                client_auth,
                                None,
                            ),
                            Err(e) => outcome(
                                STATUS_ERROR,
                                encode_error_payload(&e.to_string()),
                                client_auth,
                                None,
                            ),
                        }
                    }
                    Err(e) if e == "not_leader" => {
                        let hint = get_leader_hint(raft, &roster_snapshot);
                        outcome(STATUS_NOT_LEADER, hint, client_auth, None)
                    }
                    Err(e) => outcome(STATUS_ERROR, encode_error_payload(&e), client_auth, None),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // HEALTH
        5 => {
            let is_leader = raft.lock().unwrap().is_leader_any();
            let body = if is_leader {
                b"leader".to_vec()
            } else {
                b"follower".to_vec()
            };
            outcome(STATUS_OK, body, "none", None)
        }

        // STATS
        6 => {
            let (status, body) = build_stats_response(raft, engine, &roster_snapshot, drain).await;
            outcome(status, body, client_auth, None)
        }

        // TXN_BEGIN
        TXN_BEGIN_OPCODE => {
            if !is_leader_of(raft, GroupId::ZERO) {
                let hint = get_leader_hint(raft, &roster_snapshot);
                return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
            }
            let (txn_id, snapshot_ts) = engine.lock().await.begin_txn();
            outcome(
                STATUS_OK,
                encode_txn_begin_response(txn_id, snapshot_ts),
                client_auth,
                None,
            )
        }

        // TXN_OP
        TXN_OP_OPCODE => match decode_txn_op_payload(&payload) {
            Ok((txn_id, op, key, value)) => {
                if !is_leader_of(raft, GroupId::ZERO) {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
                }
                let key_len = key.len();
                let mut eng = engine.lock().await;
                match op {
                    TXN_OP_GET => match eng.txn_get(txn_id, &key) {
                        Ok(Some(v)) => outcome(
                            STATUS_OK,
                            encode_value_payload(&v),
                            client_auth,
                            Some(key_len),
                        ),
                        Ok(None) => outcome(STATUS_NOT_FOUND, vec![], client_auth, Some(key_len)),
                        Err(e) => map_txn_err(e, client_auth, Some(key_len)),
                    },
                    TXN_OP_PUT => {
                        let Some(value) = value else {
                            return outcome(
                                STATUS_INVALID_ARGUMENT,
                                encode_error_payload("TXN_OP put missing value"),
                                client_auth,
                                Some(key_len),
                            );
                        };
                        match eng.txn_put(txn_id, key, value) {
                            Ok(()) => outcome(STATUS_OK, vec![], client_auth, Some(key_len)),
                            Err(e) => map_txn_err(e, client_auth, Some(key_len)),
                        }
                    }
                    TXN_OP_DELETE => match eng.txn_delete(txn_id, key) {
                        Ok(()) => outcome(STATUS_OK, vec![], client_auth, Some(key_len)),
                        Err(e) => map_txn_err(e, client_auth, Some(key_len)),
                    },
                    other => outcome(
                        STATUS_INVALID_ARGUMENT,
                        encode_error_payload(&format!("unknown TXN_OP kind: {other}")),
                        client_auth,
                        Some(key_len),
                    ),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // TXN_COMMIT
        TXN_COMMIT_OPCODE => match decode_txn_id_payload(&payload) {
            Ok(txn_id) => {
                let (status, body) = txn_commit_via_raft(
                    raft,
                    engine,
                    &roster_snapshot,
                    range_table,
                    propose_tx,
                    txn_id,
                )
                .await;
                outcome(status, body, client_auth, None)
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // TXN_ROLLBACK
        TXN_ROLLBACK_OPCODE => match decode_txn_id_payload(&payload) {
            Ok(txn_id) => {
                if !is_leader_of(raft, GroupId::ZERO) {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
                }
                match engine.lock().await.txn_rollback(txn_id) {
                    Ok(()) => outcome(STATUS_OK, vec![], client_auth, None),
                    Err(e) => map_txn_err(e, client_auth, None),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // CDC_POLL (13) — leader-local changefeed poll (events from Raft apply path).
        CDC_POLL_OPCODE => match decode_cdc_poll_request(&payload) {
            Ok((consumer_id, from_seq, limit)) => {
                if !is_leader_of(raft, GroupId::ZERO) {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
                }
                let mut eng = engine.lock().await;
                match eng.cdc_subscribe(&consumer_id, Some(from_seq)) {
                    Ok(mut cursor) => match eng.cdc_poll(&mut cursor, limit as usize) {
                        Ok(events) => {
                            let wire: Vec<_> = events
                                .into_iter()
                                .map(|e| {
                                    let op = match e.op {
                                        CdcOp::Put => CDC_EVENT_PUT,
                                        CdcOp::Delete => CDC_EVENT_DELETE,
                                    };
                                    (e.seq, op, e.key, e.value)
                                })
                                .collect();
                            outcome(
                                STATUS_OK,
                                encode_cdc_poll_response(&wire),
                                client_auth,
                                None,
                            )
                        }
                        Err(e) => outcome(
                            STATUS_ERROR,
                            encode_error_payload(&e.to_string()),
                            client_auth,
                            None,
                        ),
                    },
                    Err(e @ kaya_core::KayaError::InvalidArgument { .. }) => outcome(
                        STATUS_INVALID_ARGUMENT,
                        encode_error_payload(&e.to_string()),
                        client_auth,
                        None,
                    ),
                    Err(e) => outcome(
                        STATUS_ERROR,
                        encode_error_payload(&e.to_string()),
                        client_auth,
                        None,
                    ),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // CDC_CHECKPOINT (14)
        CDC_CHECKPOINT_OPCODE => match decode_cdc_checkpoint_request(&payload) {
            Ok(consumer_id) => {
                if !is_leader_of(raft, GroupId::ZERO) {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
                }
                match engine.lock().await.cdc_checkpoint(&consumer_id).await {
                    Ok(()) => outcome(STATUS_OK, vec![], client_auth, None),
                    Err(e @ kaya_core::KayaError::InvalidArgument { .. }) => outcome(
                        STATUS_INVALID_ARGUMENT,
                        encode_error_payload(&e.to_string()),
                        client_auth,
                        None,
                    ),
                    Err(e) => outcome(
                        STATUS_ERROR,
                        encode_error_payload(&e.to_string()),
                        client_auth,
                        None,
                    ),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // LIST_RANGES (15) — meta table snapshot for client range cache.
        LIST_RANGES_OPCODE => {
            let t = range_table.read().await;
            let wire: Vec<_> = t
                .ranges()
                .iter()
                .map(|r| {
                    (
                        r.range_id,
                        r.epoch,
                        r.group_id.0,
                        r.start_key.clone(),
                        r.end_key.clone(),
                    )
                })
                .collect();
            outcome(
                STATUS_OK,
                encode_list_ranges_response(t.meta_epoch(), &wire),
                client_auth,
                None,
            )
        }

        // SPLIT_RANGE (16) — split range at key; host new group; bump meta epoch.
        SPLIT_RANGE_OPCODE => match decode_split_range_request(&payload) {
            Ok(split_key) => {
                if drain {
                    return outcome(
                        STATUS_ERROR,
                        encode_error_payload("node is draining; refuse new range hosting"),
                        client_auth,
                        None,
                    );
                }
                if !is_leader_of(raft, GroupId::ZERO) {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
                }
                // Hold the table write lock across peek + host + split_at so two
                // concurrent splits cannot host the same peek id while split_at
                // allocates a different one.
                let mut t = range_table.write().await;
                let new_gid = t.peek_next_group_id();
                if let Err(e) = ensure_group_hosted(split_rt, new_gid) {
                    return outcome(STATUS_ERROR, encode_error_payload(&e), client_auth, None);
                }
                match t.split_at(&split_key) {
                    Ok((left, right, gid)) => {
                        debug_assert_eq!(gid, new_gid);
                        let wire = vec![
                            (
                                left.range_id,
                                left.epoch,
                                left.group_id.0,
                                left.start_key,
                                left.end_key,
                            ),
                            (
                                right.range_id,
                                right.epoch,
                                right.group_id.0,
                                right.start_key,
                                right.end_key,
                            ),
                        ];
                        outcome(
                            STATUS_OK,
                            encode_list_ranges_response(t.meta_epoch(), &wire),
                            client_auth,
                            Some(split_key.len()),
                        )
                    }
                    Err(e) => outcome(
                        STATUS_INVALID_ARGUMENT,
                        encode_error_payload(&e),
                        client_auth,
                        None,
                    ),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        // MERGE_RANGE (17) — merge range at left_start with its right neighbor.
        // Orphan right-hand Raft group is left hosted and idle (reclaim is M22 follow-on).
        MERGE_RANGE_OPCODE => match decode_merge_range_request(&payload) {
            Ok(left_start) => {
                if !is_leader_of(raft, GroupId::ZERO) {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    return outcome(STATUS_NOT_LEADER, hint, client_auth, None);
                }
                let mut t = range_table.write().await;
                match t.merge_with_next(&left_start) {
                    Ok(merged) => {
                        let wire = vec![(
                            merged.range_id,
                            merged.epoch,
                            merged.group_id.0,
                            merged.start_key,
                            merged.end_key,
                        )];
                        outcome(
                            STATUS_OK,
                            encode_list_ranges_response(t.meta_epoch(), &wire),
                            client_auth,
                            Some(left_start.len()),
                        )
                    }
                    Err(e) => outcome(
                        STATUS_INVALID_ARGUMENT,
                        encode_error_payload(&e),
                        client_auth,
                        None,
                    ),
                }
            }
            Err(e) => outcome(
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&e),
                client_auth,
                None,
            ),
        },

        other => outcome(
            STATUS_ERROR,
            encode_error_payload(&format!("unknown opcode: {other}")),
            "none",
            None,
        ),
    }
}

fn map_txn_err(
    err: kaya_core::KayaError,
    client_auth: &'static str,
    key_len: Option<usize>,
) -> DispatchOutcome {
    match err {
        kaya_core::KayaError::TxnConflict => outcome(
            STATUS_TXN_CONFLICT,
            encode_error_payload("txn conflict"),
            client_auth,
            key_len,
        ),
        e @ kaya_core::KayaError::InvalidArgument { .. } => outcome(
            STATUS_INVALID_ARGUMENT,
            encode_error_payload(&e.to_string()),
            client_auth,
            key_len,
        ),
        e => outcome(
            STATUS_ERROR,
            encode_error_payload(&e.to_string()),
            client_auth,
            key_len,
        ),
    }
}

/// Atomic multi-key commit: take local intents, propose a single
/// [`RaftCommand::TxnCommit`], apply on all nodes via Raft. Intents are cleared
/// before propose (fail-closed if propose fails — client restarts the txn).
async fn txn_commit_via_raft(
    raft: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &NodeRoster,
    range_table: &SharedRangeTable,
    propose_tx: &mpsc::Sender<ProposeReq>,
    txn_id: u64,
) -> (u16, Vec<u8>) {
    if !is_leader_of(raft, GroupId::ZERO) {
        return (STATUS_NOT_LEADER, get_leader_hint(raft, roster));
    }

    let staged = {
        let mut eng = engine.lock().await;
        match eng.txn_take_commit(txn_id) {
            Ok(writes) => writes,
            Err(kaya_core::KayaError::TxnConflict) => {
                return (STATUS_TXN_CONFLICT, encode_error_payload("txn conflict"));
            }
            Err(e @ kaya_core::KayaError::InvalidArgument { .. }) => {
                return (
                    STATUS_INVALID_ARGUMENT,
                    encode_error_payload(&e.to_string()),
                );
            }
            Err(e) => {
                return (STATUS_ERROR, encode_error_payload(&e.to_string()));
            }
        }
    };

    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> = staged.into_iter().collect();

    // Cross-group atomic txn is not supported in the multi-raft foundation.
    let mut groups = std::collections::BTreeSet::new();
    {
        let t = range_table.read().await;
        for (k, _) in &mutations {
            groups.insert(lookup_group(&t, k).0);
        }
    }
    if groups.len() > 1 {
        return (
            STATUS_INVALID_ARGUMENT,
            encode_error_payload("cross-group transaction not supported"),
        );
    }
    let group_id = groups
        .iter()
        .next()
        .copied()
        .map(GroupId)
        .unwrap_or(GroupId::ZERO);

    let cmd = RaftCommand::TxnCommit { txn_id, mutations }.encode();

    let (status, body) = propose_and_wait(raft, roster, propose_tx, group_id, cmd).await;
    if status != STATUS_OK {
        // Intents already taken; client must restart the transaction.
        return (status, body);
    }

    // Apply path already materialised mutations; report commit_ts from engine.
    let commit_ts = {
        let eng = engine.lock().await;
        eng.stats().last_sequence
    };

    (STATUS_OK, encode_txn_commit_response(commit_ts))
}

/// Snapshot of group-0 voters + full membership for config-change builders.
fn group0_membership_view(raft: &SharedRaftHost) -> (Vec<NodeId>, Vec<ClusterMember>) {
    raft.lock()
        .unwrap()
        .get(GroupId::ZERO)
        .map(|n| {
            let voters: Vec<NodeId> = n
                .effective_config()
                .stable_config()
                .voters
                .iter()
                .copied()
                .collect();
            let members = n.membership().to_vec();
            (voters, members)
        })
        .unwrap_or_default()
}

/// Leader proposes adding a new member (voter or learner) via joint consensus.
#[allow(clippy::too_many_arguments)]
async fn propose_add_member(
    raft: &SharedRaftHost,
    roster: &SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    new_id: NodeId,
    new_raft: String,
    new_client: String,
    is_learner: bool,
) -> (u16, Vec<u8>) {
    if !is_leader_of(raft, GroupId::ZERO) {
        return (
            STATUS_NOT_LEADER,
            get_leader_hint(raft, &*roster.read().await),
        );
    }

    let (current_voters, current_members) = group0_membership_view(raft);

    // Optimistically upsert the new member into our roster so that we can
    // immediately replicate log entries (including the membership change) to it.
    if let (Ok(raft_addr), Ok(client_addr)) = (
        new_raft.clone().parse::<SocketAddr>(),
        new_client.clone().parse::<SocketAddr>(),
    ) {
        roster.write().await.upsert(new_id, raft_addr, client_addr);
    }

    let roster_guard = roster.read().await;

    if current_members.iter().any(|m| m.id == new_id) || current_voters.contains(&new_id) {
        return (
            STATUS_INVALID_ARGUMENT,
            encode_error_payload(&format!("node {} is already a cluster member", new_id.0)),
        );
    }

    let members = members_for_add(
        &roster_guard,
        &current_voters,
        &current_members,
        ClusterMember {
            id: new_id,
            raft_addr: new_raft,
            client_addr: new_client,
            is_learner,
        },
        ClusterMember {
            id: self_id,
            raft_addr: self_raft.to_string(),
            client_addr: self_client.to_string(),
            is_learner: false,
        },
    );

    let (proposed, out) = {
        let mut guard = raft.lock().unwrap();
        let idx = guard
            .get_mut(GroupId::ZERO)
            .and_then(|n| n.propose_membership_change(members));
        let out = if idx.is_some() {
            guard.broadcast_group(GroupId::ZERO)
        } else {
            vec![]
        };
        (idx, out)
    };
    if !out.is_empty() {
        send_envelopes(out, &*roster.read().await).await;
    }
    match proposed {
        Some(idx) => (
            STATUS_OK,
            format!("membership change proposed at index {}", idx.0).into_bytes(),
        ),
        None => (
            STATUS_ERROR,
            encode_error_payload("failed to propose membership change"),
        ),
    }
}

/// Leader proposes removing a voting member or learner (joint-consensus path).
#[allow(clippy::too_many_arguments)]
async fn propose_remove_member(
    raft: &SharedRaftHost,
    roster: &SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    remove_id: NodeId,
) -> (u16, Vec<u8>) {
    if !is_leader_of(raft, GroupId::ZERO) {
        return (
            STATUS_NOT_LEADER,
            get_leader_hint(raft, &*roster.read().await),
        );
    }

    let (current_voters, current_members) = group0_membership_view(raft);

    let roster_guard = roster.read().await;
    let members = match members_for_remove(
        &roster_guard,
        &current_voters,
        &current_members,
        remove_id,
        ClusterMember {
            id: self_id,
            raft_addr: self_raft.to_string(),
            client_addr: self_client.to_string(),
            is_learner: false,
        },
    ) {
        Some(m) => m,
        None => {
            return (
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&format!(
                    "cannot remove node {} (not a member, is self, or would shrink below quorum)",
                    remove_id.0
                )),
            );
        }
    };

    let (proposed, out) = {
        let mut guard = raft.lock().unwrap();
        let idx = guard
            .get_mut(GroupId::ZERO)
            .and_then(|n| n.propose_membership_change(members));
        let out = if idx.is_some() {
            guard.broadcast_group(GroupId::ZERO)
        } else {
            vec![]
        };
        (idx, out)
    };
    if !out.is_empty() {
        send_envelopes(out, &*roster.read().await).await;
    }
    match proposed {
        Some(idx) => (
            STATUS_OK,
            format!("membership removal proposed at index {}", idx.0).into_bytes(),
        ),
        None => (
            STATUS_ERROR,
            encode_error_payload("failed to propose membership removal"),
        ),
    }
}

/// Leader proposes promoting a learner to a full voter (ConfigChange flip).
#[allow(clippy::too_many_arguments)]
async fn propose_promote_learner(
    raft: &SharedRaftHost,
    roster: &SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    promote_id: NodeId,
) -> (u16, Vec<u8>) {
    if !is_leader_of(raft, GroupId::ZERO) {
        return (
            STATUS_NOT_LEADER,
            get_leader_hint(raft, &*roster.read().await),
        );
    }

    let (_voters, current_members) = group0_membership_view(raft);
    let roster_guard = roster.read().await;
    let members = match members_for_promote(
        &roster_guard,
        &current_members,
        promote_id,
        ClusterMember {
            id: self_id,
            raft_addr: self_raft.to_string(),
            client_addr: self_client.to_string(),
            is_learner: false,
        },
    ) {
        Some(m) => m,
        None => {
            return (
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&format!(
                    "cannot promote node {} (not a learner in current membership)",
                    promote_id.0
                )),
            );
        }
    };

    let (proposed, out) = {
        let mut guard = raft.lock().unwrap();
        let idx = guard
            .get_mut(GroupId::ZERO)
            .and_then(|n| n.propose_membership_change(members));
        let out = if idx.is_some() {
            guard.broadcast_group(GroupId::ZERO)
        } else {
            vec![]
        };
        (idx, out)
    };
    if !out.is_empty() {
        send_envelopes(out, &*roster.read().await).await;
    }
    match proposed {
        Some(idx) => (
            STATUS_OK,
            format!("learner promote proposed at index {}", idx.0).into_bytes(),
        ),
        None => (
            STATUS_ERROR,
            encode_error_payload("failed to propose learner promotion"),
        ),
    }
}

/// Send a proposal to the Raft loop and wait for it to be committed+applied.
async fn propose_and_wait(
    raft: &SharedRaftHost,
    roster: &NodeRoster,
    propose_tx: &mpsc::Sender<ProposeReq>,
    group_id: GroupId,
    command: Vec<u8>,
) -> (u16, Vec<u8>) {
    if !is_leader_of(raft, group_id) {
        let hint = get_leader_hint(raft, roster);
        return (STATUS_NOT_LEADER, hint);
    }
    let (reply_tx, reply_rx) = oneshot::channel::<Result<(), String>>();
    if propose_tx
        .send(ProposeReq {
            group_id: group_id.0,
            command,
            reply_tx,
        })
        .await
        .is_err()
    {
        return (STATUS_ERROR, encode_error_payload("raft loop unavailable"));
    }
    match reply_rx.await {
        Ok(Ok(())) => (STATUS_OK, vec![]),
        Ok(Err(e)) if e == "not_leader" => {
            let hint = get_leader_hint(raft, roster);
            (STATUS_NOT_LEADER, hint)
        }
        Ok(Err(e)) => (STATUS_ERROR, encode_error_payload(&e)),
        Err(_) => (STATUS_ERROR, encode_error_payload("reply channel dropped")),
    }
}

/// Send a read proposal to the Raft loop and wait for it to be confirmed by a majority.
async fn propose_read_and_wait(
    raft: &SharedRaftHost,
    read_propose_tx: &mpsc::Sender<ReadIndexReq>,
    group_id: GroupId,
    request_id: u64,
) -> Result<(), String> {
    if !is_leader_of(raft, group_id) {
        return Err("not_leader".to_owned());
    }
    let (reply_tx, reply_rx) = oneshot::channel::<Result<(), String>>();
    if read_propose_tx
        .send(ReadIndexReq {
            group_id: group_id.0,
            request_id,
            reply_tx,
        })
        .await
        .is_err()
    {
        return Err("raft loop unavailable".to_owned());
    }
    match reply_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => Err("reply channel dropped".to_owned()),
    }
}
