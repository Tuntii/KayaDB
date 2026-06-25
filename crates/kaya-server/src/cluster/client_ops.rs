//! Client protocol: PUT/GET/DELETE/SCAN routing and membership proposals.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use kaya_engine::{ReadOptions, ScanOptions};
use kaya_net::{
    decode_key_payload, decode_member_payload, decode_put_payload, decode_remove_member_payload,
    decode_scan_payload, encode_error_payload, encode_scan_response, encode_value_payload,
    read_client_frame, send_envelopes, write_client_response, NodeRoster, STATUS_ERROR,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
};
use kaya_raft::{ClusterMember, NodeId};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::command::RaftCommand;
use crate::membership::{members_for_add, members_for_remove, SharedRoster};
use crate::operator_auth::{
    decode_admin_payload, ADD_MEMBER_OPCODE, ADMIN_AUTH_PREFIX, REMOVE_MEMBER_OPCODE,
};

use super::stats::build_stats_response;
use super::{SharedEngine, SharedPending, SharedPendingReads, SharedRaft};

/// Message sent from a client handler to the Raft loop to propose a write.
pub struct ProposeReq {
    pub(crate) command: Vec<u8>,
    pub(crate) reply_tx: oneshot::Sender<Result<(), String>>,
}

pub struct ReadIndexReq {
    pub(crate) request_id: u64,
    pub(crate) reply_tx: oneshot::Sender<Result<(), String>>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn client_accept_loop(
    listener: TcpListener,
    raft: SharedRaft,
    engine: SharedEngine,
    pending: SharedPending,
    pending_reads: SharedPendingReads,
    propose_tx: mpsc::Sender<ProposeReq>,
    read_propose_tx: mpsc::Sender<ReadIndexReq>,
    next_read_req_id: Arc<AtomicU64>,
    roster: SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
    network_partitioned: Option<Arc<AtomicBool>>,
) {
    while let Ok((stream, _peer)) = listener.accept().await {
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
        let tok = operator_token.clone();
        tokio::spawn(async move {
            handle_connection(
                stream,
                r,
                e,
                p,
                pr,
                tx,
                rtx,
                next_id,
                ros,
                self_id,
                self_raft,
                self_client,
                tok,
            )
            .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection<S>(
    mut stream: S,
    raft: SharedRaft,
    engine: SharedEngine,
    _pending: SharedPending,
    _pending_reads: SharedPendingReads,
    propose_tx: mpsc::Sender<ProposeReq>,
    read_propose_tx: mpsc::Sender<ReadIndexReq>,
    next_read_req_id: Arc<AtomicU64>,
    roster: SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
) where
    S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin,
{
    loop {
        let (opcode, payload) = match read_client_frame(&mut stream).await {
            Ok(f) => f,
            Err(_) => break,
        };
        let (status, body) = dispatch(
            &raft,
            &engine,
            &roster,
            &propose_tx,
            &read_propose_tx,
            &next_read_req_id,
            opcode,
            payload,
            self_id,
            self_raft,
            self_client,
            operator_token.clone(),
        )
        .await;
        if write_client_response(&mut stream, status, &body)
            .await
            .is_err()
        {
            break;
        }
    }
}

fn get_leader_hint(raft: &SharedRaft, roster: &NodeRoster) -> Vec<u8> {
    if let Some(leader_id) = raft.lock().unwrap().status().leader_id {
        if let Some(addr) = roster.client_addr(leader_id) {
            return addr.to_string().into_bytes();
        }
    }
    vec![]
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    raft: &SharedRaft,
    engine: &SharedEngine,
    roster: &SharedRoster,
    propose_tx: &mpsc::Sender<ProposeReq>,
    read_propose_tx: &mpsc::Sender<ReadIndexReq>,
    next_read_req_id: &Arc<AtomicU64>,
    opcode: u8,
    payload: Vec<u8>,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    operator_token: Option<String>,
) -> (u16, Vec<u8>) {
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
                            return (
                                STATUS_INVALID_ARGUMENT,
                                encode_error_payload("admin opcode mismatch"),
                            );
                        }
                        (inner, tok)
                    }
                    Err(e) => {
                        return (STATUS_INVALID_ARGUMENT, encode_error_payload(&e));
                    }
                }
            } else {
                (payload, None)
            };

        if let Some(expected) = &operator_token {
            if presented.as_deref() != Some(expected.as_str()) {
                return (
                    STATUS_ERROR,
                    encode_error_payload("operator credential required or invalid"),
                );
            }
        }

        if opcode == ADD_MEMBER_OPCODE {
            return match decode_member_payload(&clean_payload) {
                Ok((node_id, raft_addr, client_addr)) => {
                    propose_add_member(
                        raft,
                        roster,
                        self_id,
                        self_raft,
                        self_client,
                        NodeId(node_id),
                        raft_addr,
                        client_addr,
                    )
                    .await
                }
                Err(e) => (STATUS_INVALID_ARGUMENT, encode_error_payload(&e)),
            };
        } else {
            return match decode_remove_member_payload(&clean_payload) {
                Ok(node_id) => {
                    propose_remove_member(
                        raft,
                        roster,
                        self_id,
                        self_raft,
                        self_client,
                        NodeId(node_id),
                    )
                    .await
                }
                Err(e) => (STATUS_INVALID_ARGUMENT, encode_error_payload(&e)),
            };
        }
    }

    let roster_snapshot = roster.read().await.clone();
    match opcode {
        // PUT
        1 => match decode_put_payload(&payload) {
            Ok((key, value)) => {
                let cmd = RaftCommand::Put { key, value }.encode();
                propose_and_wait(raft, &roster_snapshot, propose_tx, cmd).await
            }
            Err(e) => (STATUS_INVALID_ARGUMENT, encode_error_payload(&e)),
        },

        // GET
        2 => {
            let req_id = next_read_req_id.fetch_add(1, Ordering::SeqCst);
            match propose_read_and_wait(raft, read_propose_tx, req_id).await {
                Ok(()) => match decode_key_payload(&payload) {
                    Ok(key) => match engine.lock().await.get(&key, ReadOptions::default()).await {
                        Ok(Some(v)) => (STATUS_OK, encode_value_payload(&v)),
                        Ok(None) => (STATUS_NOT_FOUND, vec![]),
                        Err(e) => (STATUS_ERROR, encode_error_payload(&e.to_string())),
                    },
                    Err(e) => (STATUS_INVALID_ARGUMENT, encode_error_payload(&e)),
                },
                Err(e) if e == "not_leader" => {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    (STATUS_NOT_LEADER, hint)
                }
                Err(e) => (STATUS_ERROR, encode_error_payload(&e)),
            }
        }

        // DELETE
        3 => match decode_key_payload(&payload) {
            Ok(key) => {
                let cmd = RaftCommand::Delete { key }.encode();
                propose_and_wait(raft, &roster_snapshot, propose_tx, cmd).await
            }
            Err(e) => (STATUS_INVALID_ARGUMENT, encode_error_payload(&e)),
        },

        // SCAN
        4 => {
            let req_id = next_read_req_id.fetch_add(1, Ordering::SeqCst);
            match propose_read_and_wait(raft, read_propose_tx, req_id).await {
                Ok(()) => match decode_scan_payload(&payload) {
                    Ok(prefix) => {
                        match engine
                            .lock()
                            .await
                            .scan_prefix(&prefix, ScanOptions::default())
                            .await
                        {
                            Ok(kvs) => {
                                let items: Vec<(Vec<u8>, Vec<u8>)> =
                                    kvs.into_iter().map(|kv| (kv.key, kv.value)).collect();
                                (STATUS_OK, encode_scan_response(&items))
                            }
                            Err(e) => (STATUS_ERROR, encode_error_payload(&e.to_string())),
                        }
                    }
                    Err(e) => (STATUS_INVALID_ARGUMENT, encode_error_payload(&e)),
                },
                Err(e) if e == "not_leader" => {
                    let hint = get_leader_hint(raft, &roster_snapshot);
                    (STATUS_NOT_LEADER, hint)
                }
                Err(e) => (STATUS_ERROR, encode_error_payload(&e)),
            }
        }

        // HEALTH
        5 => {
            let is_leader = raft.lock().unwrap().is_leader();
            let body = if is_leader {
                b"leader".to_vec()
            } else {
                b"follower".to_vec()
            };
            (STATUS_OK, body)
        }

        // STATS
        6 => build_stats_response(raft, engine, &roster_snapshot).await,

        other => (
            STATUS_ERROR,
            encode_error_payload(&format!("unknown opcode: {other}")),
        ),
    }
}

/// Leader proposes adding a new voting member (joint-consensus path).
#[allow(clippy::too_many_arguments)]
async fn propose_add_member(
    raft: &SharedRaft,
    roster: &SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    new_id: NodeId,
    new_raft: String,
    new_client: String,
) -> (u16, Vec<u8>) {
    if !raft.lock().unwrap().is_leader() {
        return (
            STATUS_NOT_LEADER,
            get_leader_hint(raft, &*roster.read().await),
        );
    }

    let current_voters: Vec<NodeId> = raft
        .lock()
        .unwrap()
        .effective_config()
        .stable_config()
        .voters
        .iter()
        .copied()
        .collect();

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
        let idx = guard.propose_membership_change(members);
        let out = if idx.is_some() {
            guard.broadcast()
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
    raft: &SharedRaft,
    roster: &SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    remove_id: NodeId,
) -> (u16, Vec<u8>) {
    if !raft.lock().unwrap().is_leader() {
        return (
            STATUS_NOT_LEADER,
            get_leader_hint(raft, &*roster.read().await),
        );
    }

    let current_voters: Vec<NodeId> = raft
        .lock()
        .unwrap()
        .effective_config()
        .stable_config()
        .voters
        .iter()
        .copied()
        .collect();

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
        let idx = guard.propose_membership_change(members);
        let out = if idx.is_some() {
            guard.broadcast()
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
    raft: &SharedRaft,
    roster: &NodeRoster,
    propose_tx: &mpsc::Sender<ProposeReq>,
    command: Vec<u8>,
) -> (u16, Vec<u8>) {
    if !raft.lock().unwrap().is_leader() {
        let hint = get_leader_hint(raft, roster);
        return (STATUS_NOT_LEADER, hint);
    }
    let (reply_tx, reply_rx) = oneshot::channel::<Result<(), String>>();
    if propose_tx
        .send(ProposeReq { command, reply_tx })
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
    raft: &SharedRaft,
    read_propose_tx: &mpsc::Sender<ReadIndexReq>,
    request_id: u64,
) -> Result<(), String> {
    if !raft.lock().unwrap().is_leader() {
        return Err("not_leader".to_owned());
    }
    let (reply_tx, reply_rx) = oneshot::channel::<Result<(), String>>();
    if read_propose_tx
        .send(ReadIndexReq {
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
