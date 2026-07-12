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
use kaya_net::{
    decode_hello_request, decode_key_payload, decode_member_payload, decode_put_payload,
    decode_remove_member_payload, decode_scan_payload, decode_txn_id_payload,
    decode_txn_op_payload, encode_error_payload, encode_hello_response, encode_scan_response,
    encode_txn_begin_response, encode_txn_commit_response, encode_value_payload, read_client_frame,
    send_envelopes, write_client_response, NodeRoster, PROTO_VERSION, STATUS_ERROR,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK, STATUS_TXN_CONFLICT,
    TXN_BEGIN_OPCODE, TXN_COMMIT_OPCODE, TXN_OP_DELETE, TXN_OP_GET, TXN_OP_OPCODE, TXN_OP_PUT,
    TXN_ROLLBACK_OPCODE,
};
use kaya_raft::{ClusterMember, GroupId, NodeId, StaticRangeTable};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Semaphore};

use crate::audit::AuditLog;
use crate::client_auth::{decode_client_auth_payload, CLIENT_AUTH_PREFIX};
use crate::command::RaftCommand;
use crate::membership::{members_for_add, members_for_remove, SharedRoster};
use crate::operator_auth::{
    decode_admin_payload, ADD_MEMBER_OPCODE, ADMIN_AUTH_PREFIX, REMOVE_MEMBER_OPCODE,
};

use super::stats::build_stats_response;
use super::{SharedEngine, SharedPending, SharedPendingReads, SharedRaftHost};

type SharedAuditLog = Option<Arc<AuditLog>>;

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
    range_table: Arc<StaticRangeTable>,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
    client_token: Option<String>,
    audit_log: SharedAuditLog,
    network_partitioned: Option<Arc<AtomicBool>>,
    max_connections: usize,
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
                self_id,
                self_raft,
                self_client,
                op_tok,
                cli_tok,
                audit,
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
    range_table: Arc<StaticRangeTable>,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
    client_token: Option<String>,
    audit_log: SharedAuditLog,
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

fn lookup_group(range_table: &StaticRangeTable, key: &[u8]) -> GroupId {
    range_table.lookup(key).unwrap_or(GroupId::ZERO)
}

fn is_leader_of(raft: &SharedRaftHost, group_id: GroupId) -> bool {
    raft.lock().unwrap().is_leader_of(group_id)
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    raft: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &SharedRoster,
    range_table: &StaticRangeTable,
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

    // Handle admin opcodes 7/8 (ADD/REMOVE) with optional operator token enforcement.
    // Supports backward-compat raw payloads (no token configured) and ADMIN-prefixed
    // payloads when clients present the credential. If server has token set, must match.
    if opcode == ADD_MEMBER_OPCODE || opcode == REMOVE_MEMBER_OPCODE {
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
                Ok((node_id, raft_addr, client_addr)) => {
                    let (status, body) = propose_add_member(
                        raft,
                        roster,
                        self_id,
                        self_raft,
                        self_client,
                        NodeId(node_id),
                        raft_addr,
                        client_addr,
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
        } else {
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
        }
    }

    // Data-path opcodes 1-4, 6 (STATS), and 9-12 (TXN) with optional client token
    // enforcement. HEALTH (5) stays open for liveness probes.
    let payload = if matches!(opcode, 1..=4 | 6 | 9..=12) {
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
                let group_id = lookup_group(range_table, &key);
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
                let group_id = lookup_group(range_table, &key);
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
                let group_id = lookup_group(range_table, &key);
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
                let group_id = lookup_group(range_table, &prefix);
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
            let (status, body) = build_stats_response(raft, engine, &roster_snapshot).await;
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
    range_table: &StaticRangeTable,
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

    let mutations: Vec<(Vec<u8>, Option<Vec<u8>>)> =
        staged.into_iter().map(|(k, v)| (k, v)).collect();

    // Cross-group atomic txn is not supported in the multi-raft foundation.
    let mut groups = std::collections::BTreeSet::new();
    for (k, _) in &mutations {
        groups.insert(lookup_group(range_table, k).0);
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

/// Leader proposes adding a new voting member (joint-consensus path).
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
) -> (u16, Vec<u8>) {
    if !is_leader_of(raft, GroupId::ZERO) {
        return (
            STATUS_NOT_LEADER,
            get_leader_hint(raft, &*roster.read().await),
        );
    }

    let current_voters: Vec<NodeId> = raft
        .lock()
        .unwrap()
        .get(GroupId::ZERO)
        .map(|n| {
            n.effective_config()
                .stable_config()
                .voters
                .iter()
                .copied()
                .collect()
        })
        .unwrap_or_default();

    // Optimistically upsert the new member into our roster so that we can
    // immediately replicate log entries (including the membership change) to it.
    if let (Ok(raft_addr), Ok(client_addr)) = (
        new_raft.clone().parse::<SocketAddr>(),
        new_client.clone().parse::<SocketAddr>(),
    ) {
        roster.write().await.upsert(new_id, raft_addr, client_addr);
    }

    let roster_guard = roster.read().await;

    let members = members_for_add(
        &roster_guard,
        &current_voters,
        ClusterMember {
            id: new_id,
            raft_addr: new_raft,
            client_addr: new_client,
        },
        ClusterMember {
            id: self_id,
            raft_addr: self_raft.to_string(),
            client_addr: self_client.to_string(),
        },
    );

    if current_voters.contains(&new_id) {
        return (
            STATUS_INVALID_ARGUMENT,
            encode_error_payload(&format!("node {} is already a voter", new_id.0)),
        );
    }

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

/// Leader proposes removing a voting member (joint-consensus path).
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

    let current_voters: Vec<NodeId> = raft
        .lock()
        .unwrap()
        .get(GroupId::ZERO)
        .map(|n| {
            n.effective_config()
                .stable_config()
                .voters
                .iter()
                .copied()
                .collect()
        })
        .unwrap_or_default();

    let roster_guard = roster.read().await;
    let members = match members_for_remove(
        &roster_guard,
        &current_voters,
        remove_id,
        ClusterMember {
            id: self_id,
            raft_addr: self_raft.to_string(),
            client_addr: self_client.to_string(),
        },
    ) {
        Some(m) => m,
        None => {
            return (
                STATUS_INVALID_ARGUMENT,
                encode_error_payload(&format!(
                    "cannot remove node {} (not a voter, is self, or would shrink below quorum)",
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
