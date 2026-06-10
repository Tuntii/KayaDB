//! [`ClusterNode`]: wires together Raft, engine, TCP transport, and the client
//! protocol into a running cluster member.
//!
//! ## How it works
//!
//! 1. The engine is opened from `config.data_dir`.
//! 2. A TCP listener is bound for Raft peer messages.
//! 3. A TCP listener is bound for client connections.
//! 4. A *Raft loop* task drives [`RaftNode::tick`] on a timer and reacts to
//!    incoming peer messages and client proposals.
//! 5. A *client accept loop* task handles incoming `kayactl`/protocol
//!    connections: GET/SCAN are served directly from the engine; PUT/DELETE
//!    are routed through Raft and acknowledged once committed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use kaya_core::{DurabilityConfig, DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, ScanOptions, WriteOptions};
use kaya_io::FileDisk;
use kaya_net::{
    decode_key_payload, decode_put_payload, decode_scan_payload, encode_error_payload,
    encode_scan_response, encode_value_payload, read_client_frame, send_envelopes,
    start_raft_listener, write_client_response, NodeRoster, STATUS_ERROR, STATUS_INVALID_ARGUMENT,
    STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
};
use kaya_raft::{Envelope, LogIndex, NodeId, RaftConfig, RaftNode};

use crate::command::RaftCommand;

// ── public API ────────────────────────────────────────────────────────────────

/// Static configuration for a single cluster member.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's numeric identity (must be unique in the cluster).
    pub node_id: NodeId,
    /// Directory for the local engine data.
    pub data_dir: PathBuf,
    /// Address on which this node listens for **Raft peer** messages.
    pub raft_addr: SocketAddr,
    /// Address on which this node listens for **client** connections.
    pub client_addr: SocketAddr,
    /// Full cluster roster: maps every node (including self) to its Raft addr.
    pub roster: NodeRoster,
    /// Milliseconds between Raft ticks.
    pub tick_interval_ms: u64,
    /// Ticks before a follower starts an election.
    pub election_timeout_ticks: u64,
    /// Ticks between leader heartbeats.
    pub heartbeat_interval_ticks: u64,
}

impl ClusterConfig {
    /// Build a config from raw values.
    ///
    /// `peers` — list of `(peer_id, peer_raft_addr, peer_client_addr)` for every other node.
    /// Do **not** include the local node in `peers`; it is added automatically.
    pub fn new(
        node_id: u64,
        data_dir: impl Into<PathBuf>,
        raft_addr: SocketAddr,
        client_addr: SocketAddr,
        peers: Vec<(u64, SocketAddr, SocketAddr)>,
    ) -> Self {
        let cluster_size = peers.len() + 1;
        // Stagger election timeouts to avoid tied elections.
        let offset = (node_id.saturating_sub(1) % cluster_size as u64) * 5;
        let mut roster_entries: Vec<(NodeId, SocketAddr, SocketAddr)> = peers
            .iter()
            .map(|(id, raft_addr, client_addr)| (NodeId(*id), *raft_addr, *client_addr))
            .collect();
        roster_entries.push((NodeId(node_id), raft_addr, client_addr));
        Self {
            node_id: NodeId(node_id),
            data_dir: data_dir.into(),
            raft_addr,
            client_addr,
            roster: NodeRoster::new_with_client(roster_entries),
            tick_interval_ms: 10,
            election_timeout_ticks: 15 + offset,
            heartbeat_interval_ticks: 3,
        }
    }
}

/// A running cluster node.
pub struct ClusterNode {
    config: ClusterConfig,
}

impl ClusterNode {
    pub fn new(config: ClusterConfig) -> Self {
        Self { config }
    }

    /// Start the node and run until an unrecoverable I/O error occurs.
    ///
    /// Blocks the current async task.
    pub async fn run(self) -> std::io::Result<()> {
        run_cluster_node(self.config).await
    }
}

// ── internal types ────────────────────────────────────────────────────────────

type SharedRaft = Arc<Mutex<RaftNode>>;
type SharedEngine = Arc<tokio::sync::Mutex<Engine<FileDisk>>>;
// LogIndex → oneshot channel for the client waiting on that proposal.
type PendingMap = HashMap<LogIndex, oneshot::Sender<Result<(), String>>>;
type SharedPending = Arc<Mutex<PendingMap>>;

type PendingReadsMap = HashMap<u64, oneshot::Sender<Result<(), String>>>;
type SharedPendingReads = Arc<Mutex<PendingReadsMap>>;

/// Message sent from a client handler to the Raft loop to propose a write.
struct ProposeReq {
    command: Vec<u8>,
    reply_tx: oneshot::Sender<Result<(), String>>,
}

struct ReadIndexReq {
    request_id: u64,
    reply_tx: oneshot::Sender<Result<(), String>>,
}

// ── startup ───────────────────────────────────────────────────────────────────

async fn run_cluster_node(config: ClusterConfig) -> std::io::Result<()> {
    // ── engine ────────────────────────────────────────────────────────────────
    let engine_cfg = EngineConfig {
        data_dir: config.data_dir.clone(),
        durability: DurabilityConfig {
            mode: DurabilityMode::Relaxed,
            ..DurabilityConfig::default()
        },
        ..EngineConfig::default()
    };
    let disk = Arc::new(FileDisk::new(engine_cfg.data_dir.clone()));
    let engine = Engine::open(engine_cfg, disk)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let shared_engine: SharedEngine = Arc::new(tokio::sync::Mutex::new(engine));

    // ── raft node ─────────────────────────────────────────────────────────────
    let peers: Vec<NodeId> = config
        .roster
        .all_ids()
        .into_iter()
        .filter(|&id| id != config.node_id)
        .collect();
    let raft_cfg = RaftConfig {
        id: config.node_id,
        peers,
        election_timeout_ticks: config.election_timeout_ticks,
        heartbeat_interval_ticks: config.heartbeat_interval_ticks,
    };
    let shared_raft: SharedRaft = Arc::new(Mutex::new(RaftNode::new(raft_cfg)));
    let shared_pending: SharedPending = Arc::new(Mutex::new(HashMap::new()));
    let shared_pending_reads: SharedPendingReads = Arc::new(Mutex::new(HashMap::new()));

    // ── raft listener ─────────────────────────────────────────────────────────
    let (incoming_tx, incoming_rx) = mpsc::channel::<Envelope>(512);
    let raft_bound = start_raft_listener(config.raft_addr, incoming_tx)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::AddrInUse, e))?;
    eprintln!(
        "[node {}] raft  listening on {raft_bound}",
        config.node_id.0
    );

    // ── client listener ───────────────────────────────────────────────────────
    let client_listener = TcpListener::bind(config.client_addr).await?;
    let client_bound = client_listener.local_addr()?;
    eprintln!(
        "[node {}] client listening on {client_bound}",
        config.node_id.0
    );

    // ── proposal channels ─────────────────────────────────────────────────────
    let (propose_tx, propose_rx) = mpsc::channel::<ProposeReq>(256);
    let (read_propose_tx, read_propose_rx) = mpsc::channel::<ReadIndexReq>(256);
    let next_read_req_id = Arc::new(AtomicU64::new(1));

    // ── client accept and raft loops ──────────────────────────────────────────
    let r = shared_raft.clone();
    let e = shared_engine.clone();
    let p = shared_pending.clone();
    let pr = shared_pending_reads.clone();
    let tx = propose_tx.clone();
    let rtx = read_propose_tx.clone();
    let next_id = next_read_req_id.clone();
    let ros = config.roster.clone();

    let accept_fut = client_accept_loop(client_listener, r, e, p, pr, tx, rtx, next_id, ros);
    let raft_fut = raft_event_loop(
        shared_raft,
        shared_engine,
        config.roster,
        incoming_rx,
        propose_rx,
        read_propose_rx,
        shared_pending,
        shared_pending_reads,
        config.tick_interval_ms,
    );

    tokio::select! {
        _ = accept_fut => {}
        _ = raft_fut => {}
    }

    Ok(())
}

// ── raft event loop ───────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn raft_event_loop(
    raft: SharedRaft,
    engine: SharedEngine,
    roster: NodeRoster,
    mut incoming_rx: mpsc::Receiver<Envelope>,
    mut propose_rx: mpsc::Receiver<ProposeReq>,
    mut read_propose_rx: mpsc::Receiver<ReadIndexReq>,
    pending: SharedPending,
    pending_reads: SharedPendingReads,
    tick_interval_ms: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(tick_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let was_leader = raft.lock().unwrap().is_leader();

        tokio::select! {
            // ── periodic tick ─────────────────────────────────────────────────
            _ = interval.tick() => {
                let out = raft.lock().unwrap().tick();
                send_envelopes(out, &roster).await;
                drain_and_apply(&raft, &engine, &pending, &pending_reads).await;
            }

            // ── incoming raft message ─────────────────────────────────────────
            Some(env) = incoming_rx.recv() => {
                if !roster.contains(env.from) {
                    eprintln!("[server] warning: received Raft message from unrecognized node id={:?}. Message ignored.", env.from);
                    continue;
                }
                let out = raft.lock().unwrap().handle(env);
                send_envelopes(out, &roster).await;
                drain_and_apply(&raft, &engine, &pending, &pending_reads).await;
            }

            // ── client write proposal ─────────────────────────────────────────
            Some(req) = propose_rx.recv() => {
                let idx_opt = raft.lock().unwrap().propose(req.command);
                match idx_opt {
                    Some(idx) => {
                        pending.lock().unwrap().insert(idx, req.reply_tx);
                        // Immediately replicate the new entry instead of
                        // waiting for the next heartbeat.
                        let out = raft.lock().unwrap().broadcast();
                        send_envelopes(out, &roster).await;
                        drain_and_apply(&raft, &engine, &pending, &pending_reads).await;
                    }
                    None => {
                        let _ = req.reply_tx.send(Err("not_leader".to_owned()));
                    }
                }
            }

            // ── client read proposal ─────────────────────────────────────────
            Some(req) = read_propose_rx.recv() => {
                let commit_idx_opt = raft.lock().unwrap().propose_read(req.request_id);
                match commit_idx_opt {
                    Some(_idx) => {
                        pending_reads.lock().unwrap().insert(req.request_id, req.reply_tx);
                        // Immediately broadcast heartbeats to confirm leadership
                        let out = raft.lock().unwrap().broadcast();
                        send_envelopes(out, &roster).await;
                        drain_and_apply(&raft, &engine, &pending, &pending_reads).await;
                    }
                    None => {
                        let _ = req.reply_tx.send(Err("not_leader".to_owned()));
                    }
                }
            }
        }

        let is_leader = raft.lock().unwrap().is_leader();
        if was_leader && !is_leader {
            // Cleared leader role: abort all pending writes and reads
            for (_idx, tx) in pending.lock().unwrap().drain() {
                let _ = tx.send(Err("not_leader".to_owned()));
            }
            for (_req_id, tx) in pending_reads.lock().unwrap().drain() {
                let _ = tx.send(Err("not_leader".to_owned()));
            }
        }
    }
}

/// Drain freshly-applied Raft entries and execute them against the engine.
async fn drain_and_apply(
    raft: &SharedRaft,
    engine: &SharedEngine,
    pending: &SharedPending,
    pending_reads: &SharedPendingReads,
) {
    let applied = raft.lock().unwrap().drain_applied();
    for (idx, _term, command) in applied {
        let result = if command.is_empty() {
            // No-op entry appended by the new leader to establish a commit
            // barrier.  Nothing to apply.
            Ok(())
        } else {
            apply_command(engine, &command).await
        };
        if let Some(tx) = pending.lock().unwrap().remove(&idx) {
            let _ = tx.send(result);
        }
    }

    let ready_ids = raft.lock().unwrap().drain_ready_reads();
    for req_id in ready_ids {
        if let Some(tx) = pending_reads.lock().unwrap().remove(&req_id) {
            let _ = tx.send(Ok(()));
        }
    }
}

/// Decode and execute a single [`RaftCommand`] against the engine.
async fn apply_command(engine: &SharedEngine, command: &[u8]) -> Result<(), String> {
    match RaftCommand::decode(command) {
        Ok(RaftCommand::Put { key, value }) => engine
            .lock()
            .await
            .put(key, value, WriteOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Ok(RaftCommand::Delete { key }) => engine
            .lock()
            .await
            .delete(key, WriteOptions::default())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        Err(e) => Err(format!("corrupt command in log: {e}")),
    }
}

// ── client accept loop ────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn client_accept_loop(
    listener: TcpListener,
    raft: SharedRaft,
    engine: SharedEngine,
    pending: SharedPending,
    pending_reads: SharedPendingReads,
    propose_tx: mpsc::Sender<ProposeReq>,
    read_propose_tx: mpsc::Sender<ReadIndexReq>,
    next_read_req_id: Arc<AtomicU64>,
    roster: NodeRoster,
) {
    while let Ok((stream, _peer)) = listener.accept().await {
        let r = raft.clone();
        let e = engine.clone();
        let p = pending.clone();
        let pr = pending_reads.clone();
        let tx = propose_tx.clone();
        let rtx = read_propose_tx.clone();
        let next_id = next_read_req_id.clone();
        let ros = roster.clone();
        tokio::spawn(async move {
            handle_connection(stream, r, e, p, pr, tx, rtx, next_id, ros).await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: TcpStream,
    raft: SharedRaft,
    engine: SharedEngine,
    _pending: SharedPending,
    _pending_reads: SharedPendingReads,
    propose_tx: mpsc::Sender<ProposeReq>,
    read_propose_tx: mpsc::Sender<ReadIndexReq>,
    next_read_req_id: Arc<AtomicU64>,
    roster: NodeRoster,
) {
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

// ── request dispatch ──────────────────────────────────────────────────────────

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
    roster: &NodeRoster,
    propose_tx: &mpsc::Sender<ProposeReq>,
    read_propose_tx: &mpsc::Sender<ReadIndexReq>,
    next_read_req_id: &Arc<AtomicU64>,
    opcode: u8,
    payload: Vec<u8>,
) -> (u16, Vec<u8>) {
    match opcode {
        // PUT
        1 => match decode_put_payload(&payload) {
            Ok((key, value)) => {
                let cmd = RaftCommand::Put { key, value }.encode();
                propose_and_wait(raft, roster, propose_tx, cmd).await
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
                    let hint = get_leader_hint(raft, roster);
                    (STATUS_NOT_LEADER, hint)
                }
                Err(e) => (STATUS_ERROR, encode_error_payload(&e)),
            }
        }

        // DELETE
        3 => match decode_key_payload(&payload) {
            Ok(key) => {
                let cmd = RaftCommand::Delete { key }.encode();
                propose_and_wait(raft, roster, propose_tx, cmd).await
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
                    let hint = get_leader_hint(raft, roster);
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
        6 => {
            let (role, term, commit_idx, applied_idx, peer_count) = {
                let r = raft.lock().unwrap();
                let status = r.status();
                let peer_cnt = roster.all_ids().len().saturating_sub(1);
                (
                    format!("{:?}", status.role).to_lowercase(),
                    status.current_term.0,
                    status.commit_index.0,
                    status.last_applied.0,
                    peer_cnt,
                )
            };

            let engine_stats = engine.lock().await.stats();

            let stats_json = format!(
                "{{\"role\":\"{}\",\"term\":{},\"commit_index\":{},\"applied_index\":{},\"peer_count\":{},\"engine\":{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{}}}}}",
                role, term, commit_idx, applied_idx, peer_count,
                engine_stats.put_count, engine_stats.get_count, engine_stats.delete_count, engine_stats.scan_count,
                engine_stats.wal_bytes_written, engine_stats.wal_fsync_count, engine_stats.memtable_entries, engine_stats.sstable_count, engine_stats.last_sequence
            );

            (STATUS_OK, stats_json.into_bytes())
        }

        other => (
            STATUS_ERROR,
            encode_error_payload(&format!("unknown opcode: {other}")),
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
