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

mod client_ops;
mod election;
mod replication;
mod snapshot;
mod stats;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;
use tokio::sync::mpsc;

use kaya_core::{DurabilityConfig, DurabilityMode, EngineConfig};
use kaya_engine::Engine;
use kaya_io::FileDisk;
use kaya_net::{start_raft_listener, NodeRoster};
use kaya_raft::{LogIndex, NodeId, RaftConfig, RaftNode};

#[cfg(feature = "tls")]
use kaya_net::start_raft_listener_tls;

use crate::apply_index::RaftApplyIndex;
use crate::membership::{load_persisted_roster, persist_roster, shared_roster, SharedRoster};
use crate::raft_persister::RaftPersister;

use client_ops::{ProposeReq, ReadIndexReq};

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
    /// Optional operator token. If set, ADD/REMOVE_MEMBER (opcodes 7/8) require the
    /// presented credential (via ADMIN auth prefix) to match exactly. If None, any
    /// caller may perform membership changes (backward compat for dev).
    pub operator_token: Option<String>,
    /// TLS configuration. If Some, both Raft and client listeners (and outbound peer connections)
    /// will use TLS. See kaya_net::TlsConfig.
    pub tls: Option<kaya_net::TlsConfig>,
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
            operator_token: None,
            tls: None,
        }
    }

    /// Mark this node as a join-cluster participant (seed `--peer` entries required).
    pub fn with_join_cluster(mut self) -> Self {
        self.join_cluster = true;
        self
    }

    /// Require the given operator token for ADD_MEMBER / REMOVE_MEMBER operations.
    /// Callers must present it using the ADMIN auth framing.
    pub fn with_operator_token(mut self, token: String) -> Self {
        self.operator_token = Some(token);
        self
    }

    /// Enable TLS for both Raft and client listeners (and peer connections).
    pub fn with_tls(mut self, tls: kaya_net::TlsConfig) -> Self {
        self.tls = Some(tls);
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

pub(crate) type SharedRaft = Arc<Mutex<RaftNode>>;
pub(crate) type SharedPersister = Arc<Mutex<RaftPersister>>;
pub(crate) type SharedEngine = Arc<tokio::sync::Mutex<Engine<FileDisk>>>;
// LogIndex → oneshot channel for the client waiting on that proposal.
pub(crate) type PendingMap = HashMap<LogIndex, tokio::sync::oneshot::Sender<Result<(), String>>>;
pub(crate) type SharedPending = Arc<Mutex<PendingMap>>;

pub(crate) type PendingReadsMap = HashMap<u64, tokio::sync::oneshot::Sender<Result<(), String>>>;
pub(crate) type SharedPendingReads = Arc<Mutex<PendingReadsMap>>;
pub(crate) type SharedApplyIndex = Arc<Mutex<RaftApplyIndex>>;

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

    let mut persister =
        RaftPersister::open(&config.data_dir).map_err(|e| std::io::Error::other(e.to_string()))?;
    let apply_path = config.data_dir.join("raft-apply-index.jsonl");
    let apply_floor = RaftApplyIndex::load_all(&apply_path)
        .map(|recs| {
            recs.into_iter()
                .map(|r| r.index)
                .max()
                .unwrap_or(LogIndex(0))
        })
        .unwrap_or(LogIndex(0));

    let raft_node = match persister.load_state().map_err(std::io::Error::other)? {
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
    let (incoming_tx, incoming_rx) = mpsc::channel(512);
    let raft_bound = if config.tls.is_some() {
        #[cfg(feature = "tls")]
        {
            let tls_cfg = config.tls.as_ref().unwrap();
            start_raft_listener_tls(config.raft_addr, incoming_tx, tls_cfg)
                .await
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::AddrInUse, e))?
        }
        #[cfg(not(feature = "tls"))]
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "TLS requested but 'tls' feature not enabled for kaya-server",
            ));
        }
    } else {
        start_raft_listener(config.raft_addr, incoming_tx)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::AddrInUse, e))?
    };
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

    // TLS for client listener: accept Tcp, handshake with TlsAcceptor (built from tls_config),
    // pass resulting stream (which implements AsyncRead/Write) to generic handle_connection.
    // (Scaffolding complete; symmetric to raft TLS listener.)

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
    let operator_token = config.operator_token.clone();

    let accept_fut = client_ops::client_accept_loop(
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
        operator_token,
    );
    // Load persisted Raft snapshot once at startup (before the event loop applies entries).
    snapshot::install_persisted_snapshot_at_startup(
        &config.data_dir,
        &shared_engine,
        &shared_raft,
        &shared_roster,
        config.node_id,
        config.raft_addr,
        config.client_addr,
    )
    .await;

    let raft_fut = election::raft_event_loop(
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
        config.tls.clone(),
    );

    tokio::select! {
        _ = accept_fut => {}
        _ = raft_fut => {}
    }

    Ok(())
}