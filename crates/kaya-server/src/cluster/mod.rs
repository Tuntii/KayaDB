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
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use kaya_core::{DurabilityConfig, DurabilityMode, EngineConfig};
use kaya_engine::Engine;
use kaya_io::FileDisk;
use kaya_net::{start_raft_listener, NodeRoster};
use kaya_raft::{LogIndex, NodeId, RaftConfig, RaftNode};

#[cfg(feature = "tls")]
use kaya_net::start_raft_listener_tls;

use crate::apply_index::RaftApplyIndex;
use crate::audit::AuditLog;
use crate::membership::{load_persisted_roster, persist_roster, shared_roster, SharedRoster};
use crate::raft_persister::RaftPersister;

#[cfg(feature = "ebpf")]
use kaya_ebpf::{
    clear_usdt_marker_sink, install_usdt_marker_sink, shared_probe_manager, ProbeConfig,
    SharedProbeManager,
};

use client_ops::{ProposeReq, ReadIndexReq};

// ── public API ────────────────────────────────────────────────────────────────

/// Default cap on concurrent client connections per node.
pub const DEFAULT_MAX_CLIENT_CONNECTIONS: usize = 1024;

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
    /// Optional client token. If set, PUT/GET/DELETE/SCAN/STATS (opcodes 1-4, 6) require the
    /// presented credential (via CLIENT auth prefix) to match exactly. HEALTH (5) stays open.
    pub client_token: Option<String>,
    /// TLS configuration. If Some, both Raft and client listeners (and outbound peer connections)
    /// will use TLS. See kaya_net::TlsConfig.
    pub tls: Option<kaya_net::TlsConfig>,
    /// When set and true, inbound client/raft connections are dropped (Jepsen partition fallback).
    pub network_partitioned: Option<Arc<AtomicBool>>,
    /// When true, append structured JSONL audit events to `{data_dir}/audit.jsonl`.
    pub audit_log: bool,
    /// When `Some`, also forward each audit record to this syslog collector over
    /// UDP (RFC 5424) for SIEM ingestion.
    pub audit_syslog: Option<SocketAddr>,
    /// When `Some`, expose Prometheus metrics at this listen address (`GET /metrics`).
    pub metrics_addr: Option<SocketAddr>,
    /// Maximum concurrent client connections. Further connections are not
    /// accepted until an active one closes (TCP backlog backpressure).
    pub max_client_connections: usize,
    /// When true, start the in-process eBPF probe runtime (`ebpf` feature).
    #[cfg(feature = "ebpf")]
    pub ebpf_enabled: bool,
    /// Deterministic seed for eBPF trace artifacts.
    #[cfg(feature = "ebpf")]
    pub ebpf_seed: u64,
    /// When true, emit OpenTelemetry spans at durability boundaries (`otel` feature).
    #[cfg(feature = "otel")]
    pub otel_enabled: bool,
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
            client_token: None,
            tls: None,
            network_partitioned: None,
            audit_log: false,
            audit_syslog: None,
            metrics_addr: None,
            max_client_connections: DEFAULT_MAX_CLIENT_CONNECTIONS,
            #[cfg(feature = "ebpf")]
            ebpf_enabled: false,
            #[cfg(feature = "ebpf")]
            ebpf_seed: 0,
            #[cfg(feature = "otel")]
            otel_enabled: false,
        }
    }

    /// Drop inbound client/raft TCP when the flag is true (in-process partition nemesis).
    pub fn with_network_partitioned(mut self, flag: Arc<AtomicBool>) -> Self {
        self.network_partitioned = Some(flag);
        self
    }

    /// Mark this node as a join-cluster participant (seed `--peer` entries required).
    pub fn with_join_cluster(mut self) -> Self {
        self.join_cluster = true;
        self
    }

    /// Cap concurrent client connections (must be at least 1).
    pub fn with_max_client_connections(mut self, max: usize) -> Self {
        self.max_client_connections = max.max(1);
        self
    }

    /// Require the given operator token for ADD_MEMBER / REMOVE_MEMBER operations.
    /// Callers must present it using the ADMIN auth framing.
    pub fn with_operator_token(mut self, token: String) -> Self {
        self.operator_token = Some(token);
        self
    }

    /// Require the given client token for PUT/GET/DELETE/SCAN/STATS operations.
    /// Callers must present it using the CLIENT auth framing.
    pub fn with_client_token(mut self, token: String) -> Self {
        self.client_token = Some(token);
        self
    }

    /// Enable TLS for both Raft and client listeners (and peer connections).
    pub fn with_tls(mut self, tls: kaya_net::TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Enable or disable structured audit logging to `{data_dir}/audit.jsonl`.
    pub fn with_audit_log(mut self, enabled: bool) -> Self {
        self.audit_log = enabled;
        self
    }

    /// Forward audit records to a remote syslog collector (UDP, RFC 5424).
    pub fn with_audit_syslog(mut self, addr: Option<SocketAddr>) -> Self {
        self.audit_syslog = addr;
        self
    }

    /// Enable the Prometheus `/metrics` HTTP listener on `addr`.
    pub fn with_metrics_addr(mut self, addr: SocketAddr) -> Self {
        self.metrics_addr = Some(addr);
        self
    }

    /// Disable the Prometheus `/metrics` HTTP listener.
    pub fn without_metrics(mut self) -> Self {
        self.metrics_addr = None;
        self
    }

    /// Enable in-process eBPF observability with a deterministic trace seed.
    #[cfg(feature = "ebpf")]
    pub fn with_ebpf(mut self, seed: u64) -> Self {
        self.ebpf_enabled = true;
        self.ebpf_seed = seed;
        self
    }

    /// Enable OpenTelemetry durability spans (`wal_fsync`, `flush`).
    #[cfg(feature = "otel")]
    pub fn with_otel(mut self) -> Self {
        self.otel_enabled = true;
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
    #[cfg(feature = "ebpf")]
    let durability_mode = if config.ebpf_enabled {
        DurabilityMode::Strict
    } else {
        DurabilityMode::Relaxed
    };
    #[cfg(not(feature = "ebpf"))]
    let durability_mode = DurabilityMode::Relaxed;

    let engine_cfg = {
        // `mut` is only exercised when the ebpf feature shrinks the memtable below.
        #[cfg_attr(not(feature = "ebpf"), allow(unused_mut))]
        let mut cfg = EngineConfig {
            data_dir: config.data_dir.clone(),
            durability: DurabilityConfig {
                mode: durability_mode,
                ..DurabilityConfig::default()
            },
            ..EngineConfig::default()
        };
        #[cfg(feature = "ebpf")]
        if config.ebpf_enabled {
            // Small memtable cap so PUT traffic produces flush USDT markers in trace.jsonl.
            cfg.memtable.max_bytes = 16;
        }
        cfg
    };

    #[cfg(feature = "otel")]
    if config.otel_enabled && crate::otel_spans::provider_slot_is_empty() {
        crate::otel_spans::install_default_durability_spans();
    }

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
            start_raft_listener_tls(
                config.raft_addr,
                incoming_tx,
                tls_cfg,
                config.network_partitioned.clone(),
            )
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
        start_raft_listener(
            config.raft_addr,
            incoming_tx,
            config.network_partitioned.clone(),
        )
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

    #[cfg(feature = "ebpf")]
    let shared_ebpf: Option<SharedProbeManager> = if config.ebpf_enabled {
        let config_hash = format!(
            "node={}:durability=strict:ebpf_seed={}",
            config.node_id.0, config.ebpf_seed
        );
        let probe_cfg = ProbeConfig::for_server(&config.data_dir, config.ebpf_seed, config_hash);
        let mgr = shared_probe_manager(probe_cfg);
        {
            let mut guard = mgr.lock();
            if let Err(e) = guard.attach() {
                eprintln!(
                    "[node {}] warning: eBPF attach failed: {e}",
                    config.node_id.0
                );
            } else {
                eprintln!(
                    "[node {}] eBPF probes attached (seed={})",
                    config.node_id.0, config.ebpf_seed
                );
                let _ = guard.write_status();
            }
        }
        install_usdt_marker_sink(mgr.clone());
        Some(mgr)
    } else {
        None
    };

    #[cfg(feature = "ebpf")]
    let ebpf_pump_fut = if let Some(ref ebpf) = shared_ebpf {
        let ebpf = ebpf.clone();
        let engine = shared_engine.clone();
        Some(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let stats = engine.lock().await.stats();
                let mut guard = ebpf.lock();
                guard.sync_from_engine_stats(
                    stats.wal_fsync_total_us,
                    stats.wal_fsync_max_us,
                    stats.flush_total_us,
                );
                let _ = guard.write_status();
                if !guard.events().is_empty() {
                    let _ = guard.flush_trace();
                }
            }
        })
    } else {
        None
    };

    let metrics_fut = if let Some(metrics_addr) = config.metrics_addr {
        let metrics_listener = TcpListener::bind(metrics_addr).await?;
        let metrics_bound = metrics_listener.local_addr()?;
        eprintln!(
            "[node {}] metrics listening on {metrics_bound}",
            config.node_id.0
        );
        let r = shared_raft.clone();
        let e = shared_engine.clone();
        #[cfg(feature = "ebpf")]
        {
            let ebpf = shared_ebpf.clone();
            Some(metrics_accept_loop(metrics_listener, r, e, ebpf))
        }
        #[cfg(not(feature = "ebpf"))]
        {
            Some(metrics_accept_loop(metrics_listener, r, e))
        }
    } else {
        None
    };

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
    let client_token = config.client_token.clone();

    let shared_audit = if config.audit_log {
        let opened = AuditLog::open(&config.data_dir, config.node_id).and_then(|log| match config
            .audit_syslog
        {
            Some(addr) => log.with_syslog(addr),
            None => Ok(log),
        });
        match opened {
            Ok(log) => {
                eprintln!(
                    "[node {}] audit log enabled at {}{}",
                    config.node_id.0,
                    config.data_dir.join("audit.jsonl").display(),
                    match config.audit_syslog {
                        Some(addr) => format!(" (syslog → {addr})"),
                        None => String::new(),
                    }
                );
                Some(Arc::new(log))
            }
            Err(e) => {
                eprintln!(
                    "[node {}] warning: audit log disabled: {e}",
                    config.node_id.0
                );
                None
            }
        }
    } else {
        None
    };

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
        client_token,
        shared_audit,
        config.network_partitioned.clone(),
        config.max_client_connections,
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

    let shutdown_fut = shutdown_signal(config.node_id.0);

    #[cfg(feature = "ebpf")]
    match (metrics_fut, ebpf_pump_fut) {
        (Some(metrics_fut), Some(ebpf_pump_fut)) => {
            tokio::select! {
                _ = accept_fut => {}
                _ = raft_fut => {}
                _ = metrics_fut => {}
                _ = ebpf_pump_fut => {}
                _ = shutdown_fut => {}
            }
        }
        (Some(metrics_fut), None) => {
            tokio::select! {
                _ = accept_fut => {}
                _ = raft_fut => {}
                _ = metrics_fut => {}
                _ = shutdown_fut => {}
            }
        }
        (None, Some(ebpf_pump_fut)) => {
            tokio::select! {
                _ = accept_fut => {}
                _ = raft_fut => {}
                _ = ebpf_pump_fut => {}
                _ = shutdown_fut => {}
            }
        }
        (None, None) => {
            tokio::select! {
                _ = accept_fut => {}
                _ = raft_fut => {}
                _ = shutdown_fut => {}
            }
        }
    }

    #[cfg(not(feature = "ebpf"))]
    match metrics_fut {
        Some(metrics_fut) => {
            tokio::select! {
                _ = accept_fut => {}
                _ = raft_fut => {}
                _ = metrics_fut => {}
                _ = shutdown_fut => {}
            }
        }
        None => {
            tokio::select! {
                _ = accept_fut => {}
                _ = raft_fut => {}
                _ = shutdown_fut => {}
            }
        }
    }

    #[cfg(feature = "ebpf")]
    if let Some(ebpf) = shared_ebpf {
        let mut guard = ebpf.lock();
        guard.pump_events();
        let _ = guard.flush_trace();
        let _ = guard.write_status();
        guard.detach();
        clear_usdt_marker_sink();
        eprintln!(
            "[node {}] eBPF probes detached ({} events)",
            config.node_id.0,
            guard.events().len()
        );
    }

    #[cfg(feature = "otel")]
    if config.otel_enabled {
        crate::otel_spans::shutdown_durability_spans();
    }

    eprintln!("[node {}] shut down cleanly", config.node_id.0);
    Ok(())
}

/// Resolves when the process receives Ctrl-C (all platforms) or SIGTERM (Unix),
/// letting the run loop fall through to its cleanup path instead of being killed
/// mid-flight.
async fn shutdown_signal(node_id: u64) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
    eprintln!("[node {node_id}] shutdown signal received; closing");
}

#[cfg(feature = "ebpf")]
async fn metrics_accept_loop(
    listener: TcpListener,
    raft: SharedRaft,
    engine: SharedEngine,
    ebpf: Option<SharedProbeManager>,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let engine = engine.clone();
        let ebpf = ebpf.clone();
        tokio::spawn(async move {
            let _ = handle_metrics_connection(stream, raft, engine, ebpf).await;
        });
    }
}

#[cfg(not(feature = "ebpf"))]
async fn metrics_accept_loop(
    listener: TcpListener,
    raft: SharedRaft,
    engine: SharedEngine,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            let _ = handle_metrics_connection(stream, raft, engine).await;
        });
    }
}

#[cfg(feature = "ebpf")]
async fn handle_metrics_connection(
    mut stream: TcpStream,
    raft: SharedRaft,
    engine: SharedEngine,
    ebpf: Option<SharedProbeManager>,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");

    if request.starts_with("GET /metrics") {
        let snapshot = {
            let (status, is_leader) = {
                let guard = raft.lock().unwrap();
                (guard.status(), guard.is_leader())
            };
            let engine_stats = engine.lock().await.stats();
            if let Some(ref mgr) = ebpf {
                let mut guard = mgr.lock();
                guard.sync_from_engine_stats(
                    engine_stats.wal_fsync_total_us,
                    engine_stats.wal_fsync_max_us,
                    engine_stats.flush_total_us,
                );
                let _ = guard.write_status();
            }
            crate::metrics::MetricsSnapshot::from_engine_and_raft(engine_stats, &status, is_leader)
        };
        let ebpf_hist = ebpf.as_ref().map(|mgr| mgr.lock().histogram().clone());
        let body = crate::metrics::render_prometheus_with_ebpf(&snapshot, ebpf_hist.as_ref());
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n{body}");
        stream.write_all(response.as_bytes()).await?;
    } else {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\n";
        stream.write_all(response.as_bytes()).await?;
    }
    stream.shutdown().await?;
    Ok(())
}

#[cfg(not(feature = "ebpf"))]
async fn handle_metrics_connection(
    mut stream: TcpStream,
    raft: SharedRaft,
    engine: SharedEngine,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");

    if request.starts_with("GET /metrics") {
        let snapshot = {
            let (status, is_leader) = {
                let guard = raft.lock().unwrap();
                (guard.status(), guard.is_leader())
            };
            let engine_stats = engine.lock().await.stats();
            crate::metrics::MetricsSnapshot::from_engine_and_raft(engine_stats, &status, is_leader)
        };
        let body = crate::metrics::render_prometheus(&snapshot);
        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n{body}");
        stream.write_all(response.as_bytes()).await?;
    } else {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\n";
        stream.write_all(response.as_bytes()).await?;
    }
    stream.shutdown().await?;
    Ok(())
}
