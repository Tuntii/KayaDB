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
    decode_key_payload, decode_member_payload, decode_put_payload, decode_remove_member_payload,
    decode_scan_payload, encode_error_payload, encode_scan_response, encode_value_payload,
    read_client_frame, send_envelopes, start_raft_listener, write_client_response, NodeRoster,
    STATUS_ERROR, STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
};
use kaya_raft::{
    ClusterMember, ConfigChangePhase, Envelope, LogIndex, NodeId, RaftApplyCommand, RaftConfig,
    RaftNode,
};

use crate::apply_index::RaftApplyIndex;
use crate::command::RaftCommand;
use crate::raft_persister::RaftPersister;
use crate::membership::{
    apply_config_change_to_roster, build_raft_snapshot_payload, decode_config_change,
    load_persisted_roster, members_for_add, members_for_remove, parse_raft_snapshot_payload,
    persist_roster, shared_roster, SharedRoster,
};

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
    /// When true, this node is joining an existing cluster via seed peers only.
    pub join_cluster: bool,
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
            join_cluster: false,
        }
    }

    /// Mark this node as a join-cluster participant (seed `--peer` entries required).
    pub fn with_join_cluster(mut self) -> Self {
        self.join_cluster = true;
        self
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
type SharedPersister = Arc<Mutex<RaftPersister>>;
type SharedEngine = Arc<tokio::sync::Mutex<Engine<FileDisk>>>;
// LogIndex → oneshot channel for the client waiting on that proposal.
type PendingMap = HashMap<LogIndex, oneshot::Sender<Result<(), String>>>;
type SharedPending = Arc<Mutex<PendingMap>>;

type PendingReadsMap = HashMap<u64, oneshot::Sender<Result<(), String>>>;
type SharedPendingReads = Arc<Mutex<PendingReadsMap>>;
type SharedApplyIndex = Arc<Mutex<RaftApplyIndex>>;

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

    let mut persister = RaftPersister::open(&config.data_dir)
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let apply_path = config.data_dir.join("raft-apply-index.jsonl");
    let apply_floor = RaftApplyIndex::load_all(&apply_path)
        .map(|recs| {
            recs.into_iter()
                .map(|r| r.index)
                .max()
                .unwrap_or(LogIndex(0))
        })
        .unwrap_or(LogIndex(0));

    let raft_node = match persister
        .load_state()
        .map_err(|e| std::io::Error::other(e))?
    {
        Some(state) => {
            let seed = state.clone();
            let mut node = RaftNode::recover(raft_cfg, state);
            node.set_recovered_apply_floor(apply_floor);
            persister.seed_last_persisted(seed);
            node
        }
        None => RaftNode::new(raft_cfg),
    };
    let shared_raft: SharedRaft = Arc::new(Mutex::new(raft_node));
    let shared_persister: SharedPersister = Arc::new(Mutex::new(persister));

    let mut roster = config.roster.clone();
    load_persisted_roster(&config.data_dir, &mut roster);
    roster.upsert(config.node_id, config.raft_addr, config.client_addr);
    if let Err(e) = persist_roster(&config.data_dir, &roster) {
        eprintln!("warning: failed to persist initial cluster roster: {e}");
    }
    let shared_roster: SharedRoster = shared_roster(roster);

    if config.join_cluster {
        let seed_count = config.roster.all_ids().len().saturating_sub(1);
        eprintln!(
            "[node {}] join-cluster mode: connected to {seed_count} seed peer(s); awaiting voter inclusion",
            config.node_id.0,
        );
    }

    let shared_pending: SharedPending = Arc::new(Mutex::new(HashMap::new()));
    let shared_pending_reads: SharedPendingReads = Arc::new(Mutex::new(HashMap::new()));
    let apply_index = Arc::new(Mutex::new(
        RaftApplyIndex::open(&config.data_dir).map_err(|e| std::io::Error::other(e.to_string()))?,
    ));

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
    let ros = shared_roster.clone();
    let self_id = config.node_id;
    let self_raft = config.raft_addr;
    let self_client = config.client_addr;

    let accept_fut = client_accept_loop(
        client_listener,
        r,
        e,
        p,
        pr,
        tx,
        rtx,
        next_id,
        ros.clone(),
        self_id,
        self_raft,
        self_client,
    );
    // Load persisted Raft snapshot once at startup (before the event loop applies entries).
    // This brings the engine LSM state to the last Raft snapshot point.
    // If the persisted payload contains embedded membership (from dynamic cluster
    // snapshot), restore roster + Raft effective config too.
    {
        let snap_path = config.data_dir.join("raft-snapshot.bin");
        if snap_path.exists() {
            if let Ok(raw) = std::fs::read(&snap_path) {
                match parse_raft_snapshot_payload(&raw) {
                    Ok((eng, mems)) => {
                        if !eng.is_empty() {
                            if let Err(e) = shared_engine.lock().await.install_snapshot(&eng).await
                            {
                                eprintln!(
                                    "warning: failed to install persisted engine snapshot: {e}"
                                );
                            }
                        }
                        if !mems.is_empty() {
                            apply_config_change_to_roster(
                                &config.data_dir,
                                &shared_roster,
                                ConfigChangePhase::Final,
                                &mems,
                                config.node_id,
                                config.raft_addr,
                                config.client_addr,
                            )
                            .await;
                            let mut rg = shared_raft.lock().unwrap();
                            rg.restore_config_from_snapshot(mems);
                        }
                    }
                    Err(_) => {
                        // legacy pure engine data
                        if let Err(e) = shared_engine.lock().await.install_snapshot(&raw).await {
                            eprintln!("warning: failed to install legacy persisted snapshot: {e}");
                        }
                    }
                }
            }
        }
    }

    let raft_fut = raft_event_loop(
        shared_raft,
        shared_persister,
        shared_engine,
        shared_roster,
        config.data_dir.clone(),
        apply_index,
        incoming_rx,
        propose_rx,
        read_propose_rx,
        shared_pending,
        shared_pending_reads,
        config.tick_interval_ms,
        self_id,
        self_raft,
        self_client,
    );

    tokio::select! {
        _ = accept_fut => {}
        _ = raft_fut => {}
    }

    Ok(())
}

// ── raft event loop ───────────────────────────────────────────────────────────

fn persist_raft_state(raft: &SharedRaft, persister: &SharedPersister) {
    let view = raft.lock().unwrap().persist_view();
    if let Err(e) = persister.lock().unwrap().flush_view(view) {
        eprintln!("[server] warning: raft persist failed: {e}");
    }
}

#[allow(clippy::too_many_arguments)]
async fn raft_event_loop(
    raft: SharedRaft,
    persister: SharedPersister,
    engine: SharedEngine,
    roster: SharedRoster,
    data_dir: PathBuf,
    apply_index: SharedApplyIndex,
    mut incoming_rx: mpsc::Receiver<Envelope>,
    mut propose_rx: mpsc::Receiver<ProposeReq>,
    mut read_propose_rx: mpsc::Receiver<ReadIndexReq>,
    pending: SharedPending,
    pending_reads: SharedPendingReads,
    tick_interval_ms: u64,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(tick_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let was_leader = raft.lock().unwrap().is_leader();

        tokio::select! {
            // ── periodic tick ─────────────────────────────────────────────────
            _ = interval.tick() => {
                let out = raft.lock().unwrap().tick();
                send_envelopes(out, &*roster.read().await).await;
                drain_and_apply(
                    &raft,
                    &engine,
                    &roster,
                    &data_dir,
                    &apply_index,
                    &pending,
                    &pending_reads,
                    self_id,
                    self_raft,
                    self_client,
                )
                .await;
                persist_raft_state(&raft, &persister);
            }

            // ── incoming raft message ─────────────────────────────────────────
            Some(env) = incoming_rx.recv() => {
                if !is_known_raft_peer(&raft, &roster, env.from).await {
                    eprintln!("[server] warning: received Raft message from unrecognized node id={:?}. Message ignored.", env.from);
                    continue;
                }
                let out = raft.lock().unwrap().handle(env);
                send_envelopes(out, &*roster.read().await).await;
                drain_and_apply(
                    &raft,
                    &engine,
                    &roster,
                    &data_dir,
                    &apply_index,
                    &pending,
                    &pending_reads,
                    self_id,
                    self_raft,
                    self_client,
                )
                .await;
                persist_raft_state(&raft, &persister);
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
                        send_envelopes(out, &*roster.read().await).await;
                        drain_and_apply(
                            &raft,
                            &engine,
                            &roster,
                            &data_dir,
                            &apply_index,
                            &pending,
                            &pending_reads,
                            self_id,
                            self_raft,
                            self_client,
                        )
                        .await;
                        persist_raft_state(&raft, &persister);
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
                        send_envelopes(out, &*roster.read().await).await;
                        drain_and_apply(
                            &raft,
                            &engine,
                            &roster,
                            &data_dir,
                            &apply_index,
                            &pending,
                            &pending_reads,
                            self_id,
                            self_raft,
                            self_client,
                        )
                        .await;
                        persist_raft_state(&raft, &persister);
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

async fn is_known_raft_peer(raft: &SharedRaft, roster: &SharedRoster, from: NodeId) -> bool {
    if roster.read().await.contains(from) {
        return true;
    }
    let guard = raft.lock().unwrap();
    guard.effective_config().all_voters().contains(&from)
}

/// Drain freshly-applied Raft entries and execute them against the engine.
#[allow(clippy::too_many_arguments)]
async fn drain_and_apply(
    raft: &SharedRaft,
    engine: &SharedEngine,
    roster: &SharedRoster,
    data_dir: &std::path::Path,
    apply_index: &SharedApplyIndex,
    pending: &SharedPending,
    pending_reads: &SharedPendingReads,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    // First, handle any snapshot that was just installed on us (via InstallSnapshot).
    // This brings our engine state to the snapshot point.
    // If the snapshot payload includes embedded membership (for dynamic cluster),
    // also restore roster and Raft effective config so the node has correct
    // membership even if it never replayed the original config-change entries.
    let installed_snapshot = {
        let mut guard = raft.lock().unwrap();
        guard.drain_installed_snapshot()
    };
    if let Some((_idx, _term, data)) = installed_snapshot {
        match parse_raft_snapshot_payload(&data) {
            Ok((eng, mems)) => {
                if !eng.is_empty() {
                    if let Err(e) = engine.lock().await.install_snapshot(&eng).await {
                        eprintln!("error installing Raft snapshot into engine: {e}");
                    }
                }
                if !mems.is_empty() {
                    apply_config_change_to_roster(
                        data_dir,
                        roster,
                        ConfigChangePhase::Final,
                        &mems,
                        self_id,
                        self_raft,
                        self_client,
                    )
                    .await;
                    let mut rg = raft.lock().unwrap();
                    rg.restore_config_from_snapshot(mems);
                }
            }
            Err(e) => {
                // Fallback: try as pure engine data (legacy)
                if let Err(e2) = engine.lock().await.install_snapshot(&data).await {
                    eprintln!("error installing (legacy) snapshot: {e2} (parse err was {e})");
                }
            }
        }
    }

    let applied = {
        let mut guard = raft.lock().unwrap();
        guard.drain_applied()
    };
    for (idx, term, command) in applied {
        if let Some((phase, members)) = decode_config_change(&command) {
            apply_config_change_to_roster(
                data_dir,
                roster,
                phase,
                &members,
                self_id,
                self_raft,
                self_client,
            )
            .await;
            if phase == ConfigChangePhase::Final {
                eprintln!(
                    "[node {}] membership applied: {} voters",
                    self_id.0,
                    members.len()
                );
            }
        }

        let apply_meta = RaftApplyCommand {
            term,
            index: idx,
            engine_lsn_hint: None,
        };

        let meta = if command.is_empty() {
            apply_meta
        } else {
            match apply_command(engine, &command).await {
                Ok(lsn) => RaftApplyCommand {
                    engine_lsn_hint: lsn,
                    ..apply_meta
                },
                Err(e) => {
                    if let Some(tx) = pending.lock().unwrap().remove(&idx) {
                        let _ = tx.send(Err(e.clone()));
                    }
                    continue;
                }
            }
        };

        if let Err(e) = apply_index.lock().unwrap().append(&meta) {
            eprintln!("warning: failed to persist raft↔lsn correlation: {e}");
        }

        let result = Ok(());
        if let Some(tx) = pending.lock().unwrap().remove(&idx) {
            let _ = tx.send(result);
        }
    }

    let ready_ids = {
        let mut guard = raft.lock().unwrap();
        guard.drain_ready_reads()
    };
    for req_id in ready_ids {
        if let Some(tx) = pending_reads.lock().unwrap().remove(&req_id) {
            let _ = tx.send(Ok(()));
        }
    }

    // Periodic Raft log compaction using real pinned manifest-anchored MVCC snapshot.
    let compaction_target = {
        let guard = raft.lock().unwrap();
        let status = guard.status();
        if status.last_applied.0 > 0 && status.last_applied.0 % 64 == 0 {
            Some((status.last_applied, status.current_term))
        } else {
            None
        }
    };
    if let Some((last, term)) = compaction_target {
        // Before replacing the Raft snapshot with a newer one, release pins held
        // by the *previous* snapshot view on this node. Only the latest snapshot
        // that Raft can send needs its tables protected.
        {
            let old_data = {
                let guard = raft.lock().unwrap();
                guard.snapshot().and_then(
                    |(_idx, _term, d)| {
                        if d.is_empty() {
                            None
                        } else {
                            Some(d)
                        }
                    },
                )
            };
            if let Some(data) = old_data {
                let _ = engine.lock().await.release_snapshot(&data).await;
            }
        }

        let engine_data = match engine.lock().await.create_snapshot().await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("warning: engine snapshot failed for compaction: {e}");
                vec![]
            }
        };

        // Capture current membership so snapshot receivers (new nodes or lagging
        // followers) can restore the correct effective config and roster even if
        // they jump over config-change log entries.
        let members_snapshot: Vec<ClusterMember> = {
            let roster_guard = roster.read().await;
            let voters: Vec<NodeId> = raft
                .lock()
                .unwrap()
                .effective_config()
                .stable_config()
                .voters
                .iter()
                .copied()
                .collect();
            voters
                .into_iter()
                .filter_map(|id| {
                    if let (Some(r), Some(c)) =
                        (roster_guard.addr(id), roster_guard.client_addr(id))
                    {
                        Some(ClusterMember {
                            id,
                            raft_addr: r.to_string(),
                            client_addr: c.to_string(),
                        })
                    } else {
                        None
                    }
                })
                .collect()
        };

        let snap_data = build_raft_snapshot_payload(&engine_data, &members_snapshot);
        raft.lock().unwrap().compact(last, term, snap_data.clone());

        // Persisted snapshot for fast restart. Written atomically (tmp + rename + fsync)
        // so that a crash leaves either the old complete snapshot or the new one.
        if !snap_data.is_empty() {
            let snap_path = data_dir.join("raft-snapshot.bin");
            let tmp_path = data_dir.join("raft-snapshot.bin.tmp");
            let write_ok = (|| -> std::io::Result<()> {
                use std::fs::File;
                use std::io::Write;
                let mut f = File::create(&tmp_path)?;
                f.write_all(&snap_data)?;
                f.sync_all()?;
                std::fs::rename(&tmp_path, &snap_path)?;
                if let Ok(dirf) = File::open(data_dir) {
                    let _ = dirf.sync_all();
                }
                Ok(())
            })();
            if let Err(e) = write_ok {
                eprintln!("warning: failed to persist raft snapshot atomically: {e}");
                // best effort cleanup
                let _ = std::fs::remove_file(&tmp_path);
            }
        }
    }
}

/// Decode and execute a single [`RaftCommand`] against the engine.
/// Returns the engine LSN when the command performed a durable write.
async fn apply_command(
    engine: &SharedEngine,
    command: &[u8],
) -> Result<Option<kaya_core::Lsn>, String> {
    match RaftCommand::decode(command) {
        Ok(RaftCommand::Put { key, value }) => engine
            .lock()
            .await
            .put(key, value, WriteOptions::default())
            .await
            .map(|r| Some(r.lsn))
            .map_err(|e| e.to_string()),
        Ok(RaftCommand::Delete { key }) => engine
            .lock()
            .await
            .delete(key, WriteOptions::default())
            .await
            .map(|r| Some(r.lsn))
            .map_err(|e| e.to_string()),
        Ok(RaftCommand::ConfigChange { .. }) => Ok(None),
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
    roster: SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
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
            )
            .await;
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
    roster: SharedRoster,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
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
            self_id,
            self_raft,
            self_client,
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
    roster: &SharedRoster,
    propose_tx: &mpsc::Sender<ProposeReq>,
    read_propose_tx: &mpsc::Sender<ReadIndexReq>,
    next_read_req_id: &Arc<AtomicU64>,
    opcode: u8,
    payload: Vec<u8>,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) -> (u16, Vec<u8>) {
    if opcode == 7 {
        return match decode_member_payload(&payload) {
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
    }
    if opcode == 8 {
        return match decode_remove_member_payload(&payload) {
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
        6 => {
            let (role, term, commit_idx, applied_idx, peer_count) = {
                let r = raft.lock().unwrap();
                let status = r.status();
                let peer_cnt = roster_snapshot.all_ids().len().saturating_sub(1);
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
                "{{\"role\":\"{}\",\"term\":{},\"commit_index\":{},\"applied_index\":{},\"peer_count\":{},\"engine\":{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"wal_fsync_total_us\":{},\"wal_fsync_max_us\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{},\"flush_total_us\":{},\"flush_max_us\":{},\"flush_count\":{},\"compaction_total_us\":{},\"compaction_max_us\":{},\"compaction_count\":{}}}}}",
                role, term, commit_idx, applied_idx, peer_count,
                engine_stats.put_count, engine_stats.get_count, engine_stats.delete_count, engine_stats.scan_count,
                engine_stats.wal_bytes_written, engine_stats.wal_fsync_count, engine_stats.wal_fsync_total_us, engine_stats.wal_fsync_max_us, engine_stats.memtable_entries, engine_stats.sstable_count, engine_stats.last_sequence,
                engine_stats.flush_total_us, engine_stats.flush_max_us, engine_stats.flush_count,
                engine_stats.compaction_total_us, engine_stats.compaction_max_us, engine_stats.compaction_count
            );

            (STATUS_OK, stats_json.into_bytes())
        }

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

    let roster_guard = roster.read().await;

    // Optimistically upsert the new member into our roster so that we can
    // immediately replicate log entries (including the membership change) to it.
    if let (Ok(raft_addr), Ok(client_addr)) = (
        new_raft.clone().parse::<SocketAddr>(),
        new_client.clone().parse::<SocketAddr>(),
    ) {
        roster.write().await.upsert(new_id, raft_addr, client_addr);
    }

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
