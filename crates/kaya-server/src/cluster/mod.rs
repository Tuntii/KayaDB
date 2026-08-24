//! [`ClusterNode`]: wires together Raft, engine, TCP transport, and the client
//! protocol into a running cluster member.
//!
//! ## How it works
//!
//! 1. The engine is opened from `config.data_dir`.
//! 2. A TCP listener is bound for Raft peer messages.
//! 3. A TCP listener is bound for client connections.
//! 4. A *Raft loop* task drives [`MultiRaftHost::tick_all`] on a timer and
//!    reacts to incoming peer messages (demux by `group_id`) and client proposals.
//! 5. A *client accept loop* task handles incoming `kayactl`/protocol
//!    connections: GET/SCAN/PUT/DELETE route via the static range table to the
//!    owning Raft group; writes are acknowledged once committed on that group.

mod balancer;
mod client_ops;
mod election;
mod replication;
mod snapshot;
mod stats;
mod txn_coord;

pub use balancer::{plan_range_count, RangeMove, RebalancePlan};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use kaya_core::{
    DurabilityConfig, DurabilityMode, EngineConfig, Result as KayaResult,
    DEFAULT_MAX_CLOCK_OFFSET_MICROS,
};
use kaya_engine::Engine;
use kaya_io::{DirEntry, Disk, EncryptedDisk, FileDisk, RelativePath};
use kaya_net::{start_raft_listener, NodeRoster};
use kaya_raft::{
    multi_raft_group_dir, GroupId, LogIndex, MultiRaftHost, NodeId, RaftConfig, RaftNode,
    StaticRange, StaticRangeTable,
};

#[cfg(feature = "tls")]
use kaya_net::start_raft_listener_tls;

use crate::apply_index::RaftApplyIndex;
use crate::audit::AuditLog;
use crate::membership::{load_persisted_roster, persist_roster, shared_roster, SharedRoster};
use crate::raft_persister::RaftPersister;
use crate::range_meta::load_persisted_range_table;

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
    /// Optional operator token. If set, ADD/REMOVE_MEMBER (opcodes 7/8),
    /// TRANSFER_LEADER (18), PROMOTE_LEARNER (19), and REBALANCE_PLAN (20) require the
    /// presented credential to match exactly. If None, any caller may perform those
    /// admin ops (dev default).
    pub operator_token: Option<String>,
    /// Optional client token. If set, PUT/GET/DELETE/SCAN/STATS (opcodes 1-4, 6) require the
    /// presented credential (via CLIENT auth prefix) to match exactly. HEALTH (5) stays open.
    pub client_token: Option<String>,
    /// Optional per-prefix ACL. When set, PUT/GET/DELETE/SCAN/TXN_* authorize the
    /// presented client token via longest-prefix match. Empty ACL denies all data ops.
    pub acl: Option<crate::acl::PrefixAcl>,
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
    /// When `Some`, expose the read-only JSON dashboard (`GET /health`,
    /// `/v1/ranges`, `/v1/raft`) at this listen address.
    pub dashboard_addr: Option<SocketAddr>,
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
    /// When true, engine commit sequences are assigned from a hybrid logical clock.
    /// Default false for back-compat; enabled automatically when multi-group ranges are configured.
    pub use_hlc: bool,
    /// HLC uncertainty bound in microseconds, forwarded to `EngineConfig::max_clock_offset_micros`
    /// (only meaningful when `use_hlc` is true). See `spec/docs/transactions-spec.md` §17.7 and
    /// `docs/runbooks/hlc-clock-skew.md`. Default 500ms.
    pub max_clock_offset_micros: u64,
    /// Static key-range -> Raft group routing. Default is single group 0 (whole keyspace).
    pub range_table: StaticRangeTable,
    /// When true, this node is draining for decommission: status JSON reports
    /// `"drain": true`. Existing leadership still works until the operator
    /// transfers it away; operators must transfer leaders before removal
    /// (see `docs/runbooks/decommission-node.md`). New range hosting via
    /// SPLIT_RANGE is rejected on a draining node.
    pub drain: bool,
    /// Optional AES-256-GCM encryption-at-rest keyring. When set, the engine
    /// opens over [`EncryptedDisk`] wrapping [`FileDisk`]. A single-key ring
    /// (id 0) reproduces the original v1 on-disk format unchanged; a rotated
    /// ring (#28) adds a key id to new envelopes and keeps old keys available
    /// to decrypt files not yet rewritten under the active key.
    pub encryption_key: Option<kaya_io::Keyring>,
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
            acl: None,
            tls: None,
            network_partitioned: None,
            audit_log: false,
            audit_syslog: None,
            metrics_addr: None,
            dashboard_addr: None,
            max_client_connections: DEFAULT_MAX_CLIENT_CONNECTIONS,
            #[cfg(feature = "ebpf")]
            ebpf_enabled: false,
            #[cfg(feature = "ebpf")]
            ebpf_seed: 0,
            #[cfg(feature = "otel")]
            otel_enabled: false,
            use_hlc: false,
            max_clock_offset_micros: DEFAULT_MAX_CLOCK_OFFSET_MICROS,
            range_table: StaticRangeTable::single_group(GroupId::ZERO),
            drain: false,
            encryption_key: None,
        }
    }

    /// Enable AES-256-GCM encryption-at-rest for engine files (WAL/SST/manifest
    /// via the Disk layer) with a single non-rotating key (id 0).
    pub fn with_encryption_key(mut self, key: [u8; 32]) -> Self {
        self.encryption_key = Some(kaya_io::Keyring::new(0, key));
        self
    }

    /// Enable AES-256-GCM encryption-at-rest with a full keyring (#28 rotation:
    /// active key seals writes, previous keys remain readable).
    pub fn with_encryption_keyring(mut self, keyring: kaya_io::Keyring) -> Self {
        self.encryption_key = Some(keyring);
        self
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

    /// Require the given operator token for ADD/REMOVE_MEMBER, TRANSFER_LEADER,
    /// PROMOTE_LEARNER, REBALANCE_PLAN. Callers must present it using the ADMIN auth framing.
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

    /// Install a per-prefix ACL (M24). When set, PUT/GET/DELETE/SCAN/TXN_* require
    /// a CLIENT-framed token that matches the longest prefix rule for the key.
    pub fn with_acl(mut self, acl: crate::acl::PrefixAcl) -> Self {
        self.acl = Some(acl);
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

    /// Enable the read-only JSON dashboard HTTP listener on `addr`.
    pub fn with_dashboard_addr(mut self, addr: SocketAddr) -> Self {
        self.dashboard_addr = Some(addr);
        self
    }

    /// Disable the read-only JSON dashboard HTTP listener.
    pub fn without_dashboard(mut self) -> Self {
        self.dashboard_addr = None;
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

    /// Enable hybrid-logical-clock commit timestamps on the local engine.
    pub fn with_use_hlc(mut self) -> Self {
        self.use_hlc = true;
        self
    }

    /// Set the HLC uncertainty bound (microseconds). A remote clock sample
    /// more than this far ahead of local wall-clock time is rejected rather
    /// than merged; see `spec/docs/transactions-spec.md` §17.7.
    pub fn with_max_clock_offset_micros(mut self, micros: u64) -> Self {
        self.max_clock_offset_micros = micros;
        self
    }

    /// Configure static key-range -> Raft group routing (multi-raft production path).
    pub fn with_static_ranges(mut self, ranges: Vec<StaticRange>) -> Self {
        self.range_table = StaticRangeTable::from_ranges(ranges);
        self
    }

    /// Mark this node as draining for decommission (status reports `"drain": true`).
    pub fn with_drain(mut self) -> Self {
        self.drain = true;
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

/// Always a MultiRaftHost (at least group 0).
pub(crate) type SharedRaftHost = Arc<Mutex<MultiRaftHost>>;
pub(crate) type SharedPersisters = Arc<Mutex<HashMap<u64, RaftPersister>>>;
/// Engine disk backend: plain [`FileDisk`] or AES-GCM [`EncryptedDisk`].
pub(crate) enum EngineDisk {
    Plain(FileDisk),
    Encrypted(EncryptedDisk<FileDisk>),
}

impl Disk for EngineDisk {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> KayaResult<usize> {
        match self {
            Self::Plain(d) => d.read_at(path, offset, buf).await,
            Self::Encrypted(d) => d.read_at(path, offset, buf).await,
        }
    }

    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> KayaResult<usize> {
        match self {
            Self::Plain(d) => d.write_at(path, offset, buf).await,
            Self::Encrypted(d) => d.write_at(path, offset, buf).await,
        }
    }

    async fn append(&self, path: &RelativePath, buf: &[u8]) -> KayaResult<u64> {
        match self {
            Self::Plain(d) => d.append(path, buf).await,
            Self::Encrypted(d) => d.append(path, buf).await,
        }
    }

    async fn fsync_file(&self, path: &RelativePath) -> KayaResult<()> {
        match self {
            Self::Plain(d) => d.fsync_file(path).await,
            Self::Encrypted(d) => d.fsync_file(path).await,
        }
    }

    async fn fsync_dir(&self, path: &RelativePath) -> KayaResult<()> {
        match self {
            Self::Plain(d) => d.fsync_dir(path).await,
            Self::Encrypted(d) => d.fsync_dir(path).await,
        }
    }

    async fn truncate(&self, path: &RelativePath, len: u64) -> KayaResult<()> {
        match self {
            Self::Plain(d) => d.truncate(path, len).await,
            Self::Encrypted(d) => d.truncate(path, len).await,
        }
    }

    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> KayaResult<()> {
        match self {
            Self::Plain(d) => d.rename(from, to).await,
            Self::Encrypted(d) => d.rename(from, to).await,
        }
    }

    async fn remove_file(&self, path: &RelativePath) -> KayaResult<()> {
        match self {
            Self::Plain(d) => d.remove_file(path).await,
            Self::Encrypted(d) => d.remove_file(path).await,
        }
    }

    async fn list_dir(&self, path: &RelativePath) -> KayaResult<Vec<DirEntry>> {
        match self {
            Self::Plain(d) => d.list_dir(path).await,
            Self::Encrypted(d) => d.list_dir(path).await,
        }
    }

    async fn file_len(&self, path: &RelativePath) -> KayaResult<u64> {
        match self {
            Self::Plain(d) => d.file_len(path).await,
            Self::Encrypted(d) => d.file_len(path).await,
        }
    }
}

pub(crate) type SharedEngine = Arc<tokio::sync::Mutex<Engine<EngineDisk>>>;
// (group_id, LogIndex) → oneshot channel for the client waiting on that proposal.
pub(crate) type PendingKey = (u64, LogIndex);
pub(crate) type PendingMap = HashMap<PendingKey, tokio::sync::oneshot::Sender<Result<(), String>>>;
pub(crate) type SharedPending = Arc<Mutex<PendingMap>>;

pub(crate) type PendingReadsMap = HashMap<u64, tokio::sync::oneshot::Sender<Result<(), String>>>;
pub(crate) type SharedPendingReads = Arc<Mutex<PendingReadsMap>>;
pub(crate) type SharedApplyIndexes = Arc<Mutex<HashMap<u64, RaftApplyIndex>>>;
/// Cumulative count of orphan Raft groups reclaimed since process start (issue #30).
pub(crate) type SharedReclaimStats = Arc<std::sync::atomic::AtomicU64>;

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

    // Durable range layout (#25): prefer last committed snapshot over config default.
    let mut boot_range_table = config.range_table.clone();
    if let Some(persisted) = load_persisted_range_table(&config.data_dir) {
        eprintln!(
            "[node {}] restored range table from disk (meta_epoch={}, ranges={})",
            config.node_id.0,
            persisted.meta_epoch(),
            persisted.ranges().len()
        );
        boot_range_table = persisted;
    }

    let multi_group = {
        let ids: std::collections::BTreeSet<u64> = boot_range_table
            .ranges()
            .iter()
            .map(|r| r.group_id.0)
            .collect();
        ids.len() > 1
    };

    let engine_cfg = {
        // `mut` is only exercised when the ebpf feature shrinks the memtable below.
        #[cfg_attr(not(feature = "ebpf"), allow(unused_mut))]
        let mut cfg = EngineConfig {
            data_dir: config.data_dir.clone(),
            durability: DurabilityConfig {
                mode: durability_mode,
                ..DurabilityConfig::default()
            },
            use_hlc: config.use_hlc || multi_group,
            max_clock_offset_micros: config.max_clock_offset_micros,
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

    let file_disk = FileDisk::new(engine_cfg.data_dir.clone());
    let disk = Arc::new(match config.encryption_key {
        Some(keyring) => EngineDisk::Encrypted(EncryptedDisk::with_keyring(file_disk, keyring)),
        None => EngineDisk::Plain(file_disk),
    });
    let mut engine = Engine::open(engine_cfg, disk)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    // 2PC crash recovery: abort Preparing/Prepared; finish Committing.
    match txn_coord::recover_incomplete_2pc(&mut engine).await {
        Ok((0, 0)) => {}
        Ok((aborted, finished)) => eprintln!(
            "[node {}] 2PC recovery: aborted {aborted} in-doubt record(s), finished {finished} Committing",
            config.node_id.0
        ),
        Err(e) => eprintln!(
            "[node {}] warning: 2PC recovery scan failed: {e}",
            config.node_id.0
        ),
    }
    let shared_engine: SharedEngine = Arc::new(tokio::sync::Mutex::new(engine));

    // ── multi-raft host (always ≥ group 0) ────────────────────────────────────
    let peers: Vec<NodeId> = config
        .roster
        .all_ids()
        .into_iter()
        .filter(|&id| id != config.node_id)
        .collect();

    let mut group_ids: std::collections::BTreeSet<u64> = boot_range_table
        .ranges()
        .iter()
        .map(|r| r.group_id.0)
        .collect();
    if group_ids.is_empty() {
        group_ids.insert(0);
    }
    // Always host group 0 for membership / legacy clients / range meta.
    group_ids.insert(0);

    let mut host = MultiRaftHost::new();
    let mut persister_map: HashMap<u64, RaftPersister> = HashMap::new();
    let mut apply_map: HashMap<u64, RaftApplyIndex> = HashMap::new();

    for gid in group_ids {
        let group_dir = multi_raft_group_dir(&config.data_dir, GroupId(gid));
        if gid != 0 {
            std::fs::create_dir_all(&group_dir).map_err(|e| {
                std::io::Error::other(format!("create group dir {}: {e}", group_dir.display()))
            })?;
        }
        let raft_cfg = RaftConfig {
            id: config.node_id,
            peers: peers.clone(),
            election_timeout_ticks: config.election_timeout_ticks,
            heartbeat_interval_ticks: config.heartbeat_interval_ticks,
        };
        let mut persister =
            RaftPersister::open(&group_dir).map_err(|e| std::io::Error::other(e.to_string()))?;
        let apply_path = group_dir.join("raft-apply-index.jsonl");
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
        host.insert(GroupId(gid), raft_node);
        persister_map.insert(gid, persister);
        apply_map.insert(
            gid,
            RaftApplyIndex::open(&group_dir).map_err(|e| std::io::Error::other(e.to_string()))?,
        );
    }

    let shared_raft: SharedRaftHost = Arc::new(Mutex::new(host));
    let shared_persisters: SharedPersisters = Arc::new(Mutex::new(persister_map));
    let apply_indexes: SharedApplyIndexes = Arc::new(Mutex::new(apply_map));
    let reclaimed_total: SharedReclaimStats = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let shared_range_table: client_ops::SharedRangeTable =
        Arc::new(tokio::sync::RwLock::new(boot_range_table));
    let split_rt = client_ops::SplitRuntime {
        raft: shared_raft.clone(),
        persisters: shared_persisters.clone(),
        apply_indexes: apply_indexes.clone(),
        data_dir: config.data_dir.clone(),
        node_id: config.node_id,
        peers: peers.clone(),
        election_timeout_ticks: config.election_timeout_ticks,
        heartbeat_interval_ticks: config.heartbeat_interval_ticks,
    };

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
        let ranges = shared_range_table.clone();
        let reclaimed = reclaimed_total.clone();
        #[cfg(feature = "ebpf")]
        {
            let ebpf = shared_ebpf.clone();
            Some(metrics_accept_loop(
                metrics_listener,
                r,
                e,
                ranges,
                reclaimed,
                ebpf,
            ))
        }
        #[cfg(not(feature = "ebpf"))]
        {
            Some(metrics_accept_loop(
                metrics_listener,
                r,
                e,
                ranges,
                reclaimed,
            ))
        }
    } else {
        None
    };

    // Read-only dashboard: spawned so it does not explode the select! matrix.
    // Dropped when the process shuts down via the runtime.
    if let Some(dashboard_addr) = config.dashboard_addr {
        let raft = shared_raft.clone();
        let ranges = shared_range_table.clone();
        let node_id = config.node_id.0;
        tokio::spawn(async move {
            if let Err(e) = crate::dashboard::serve(dashboard_addr, node_id, raft, ranges).await {
                eprintln!("[node {node_id}] dashboard listener error: {e}");
            }
        });
    }

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
    let acl = config.acl.clone();
    let drain = config.drain;
    if drain {
        eprintln!(
            "[node {}] drain mode: status will report drain=true; transfer leaders before remove",
            config.node_id.0
        );
    }

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
        shared_range_table.clone(),
        split_rt.clone(),
        self_id,
        self_raft,
        self_client,
        operator_token,
        client_token,
        acl,
        shared_audit,
        config.network_partitioned.clone(),
        config.max_client_connections,
        drain,
    );
    // Load persisted Raft snapshot once at startup (before the event loop applies entries).
    snapshot::install_persisted_snapshot_at_startup(
        &config.data_dir,
        &shared_engine,
        &shared_raft,
        &shared_roster,
        &shared_range_table,
        &split_rt,
        config.node_id,
        config.raft_addr,
        config.client_addr,
    )
    .await;

    let raft_fut = election::raft_event_loop(
        shared_raft,
        shared_persisters,
        shared_engine,
        shared_roster,
        shared_range_table.clone(),
        split_rt.clone(),
        config.data_dir.clone(),
        apply_indexes,
        reclaimed_total,
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
    raft: SharedRaftHost,
    engine: SharedEngine,
    range_table: client_ops::SharedRangeTable,
    reclaimed_total: SharedReclaimStats,
    ebpf: Option<SharedProbeManager>,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let engine = engine.clone();
        let range_table = range_table.clone();
        let reclaimed_total = reclaimed_total.clone();
        let ebpf = ebpf.clone();
        tokio::spawn(async move {
            let _ =
                handle_metrics_connection(stream, raft, engine, range_table, reclaimed_total, ebpf)
                    .await;
        });
    }
}

#[cfg(not(feature = "ebpf"))]
async fn metrics_accept_loop(
    listener: TcpListener,
    raft: SharedRaftHost,
    engine: SharedEngine,
    range_table: client_ops::SharedRangeTable,
    reclaimed_total: SharedReclaimStats,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let engine = engine.clone();
        let range_table = range_table.clone();
        let reclaimed_total = reclaimed_total.clone();
        tokio::spawn(async move {
            let _ =
                handle_metrics_connection(stream, raft, engine, range_table, reclaimed_total).await;
        });
    }
}

#[cfg(feature = "ebpf")]
async fn handle_metrics_connection(
    mut stream: TcpStream,
    raft: SharedRaftHost,
    engine: SharedEngine,
    range_table: client_ops::SharedRangeTable,
    reclaimed_total: SharedReclaimStats,
    ebpf: Option<SharedProbeManager>,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");

    if request.starts_with("GET /metrics") {
        let snapshot = {
            let (status, is_leader) = {
                let guard = raft.lock().unwrap();
                let status = guard
                    .primary_status()
                    .or_else(|| {
                        guard
                            .sorted_group_ids()
                            .into_iter()
                            .find_map(|g| guard.status_of(g))
                    })
                    .unwrap_or(kaya_raft::RaftStatus {
                        id: kaya_raft::NodeId(0),
                        role: kaya_raft::Role::Follower,
                        current_term: kaya_raft::Term(0),
                        commit_index: kaya_raft::LogIndex(0),
                        last_applied: kaya_raft::LogIndex(0),
                        leader_id: None,
                    });
                let is_leader = guard.is_leader_any();
                (status, is_leader)
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
            // Orphan group reclaim (#30): live gauge + cumulative counter.
            let orphan_group_count = replication::orphan_group_count(&raft, &range_table).await;
            let reclaim_total = reclaimed_total.load(std::sync::atomic::Ordering::Relaxed);
            crate::metrics::MetricsSnapshot::from_engine_and_raft(
                engine_stats,
                &status,
                is_leader,
                orphan_group_count,
                reclaim_total,
            )
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
    raft: SharedRaftHost,
    engine: SharedEngine,
    range_table: client_ops::SharedRangeTable,
    reclaimed_total: SharedReclaimStats,
) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");

    if request.starts_with("GET /metrics") {
        let snapshot = {
            let (status, is_leader) = {
                let guard = raft.lock().unwrap();
                let status = guard
                    .primary_status()
                    .or_else(|| {
                        guard
                            .sorted_group_ids()
                            .into_iter()
                            .find_map(|g| guard.status_of(g))
                    })
                    .unwrap_or(kaya_raft::RaftStatus {
                        id: kaya_raft::NodeId(0),
                        role: kaya_raft::Role::Follower,
                        current_term: kaya_raft::Term(0),
                        commit_index: kaya_raft::LogIndex(0),
                        last_applied: kaya_raft::LogIndex(0),
                        leader_id: None,
                    });
                let is_leader = guard.is_leader_any();
                (status, is_leader)
            };
            let engine_stats = engine.lock().await.stats();
            // Orphan group reclaim (#30): live gauge + cumulative counter.
            let orphan_group_count = replication::orphan_group_count(&raft, &range_table).await;
            let reclaim_total = reclaimed_total.load(std::sync::atomic::Ordering::Relaxed);
            crate::metrics::MetricsSnapshot::from_engine_and_raft(
                engine_stats,
                &status,
                is_leader,
                orphan_group_count,
                reclaim_total,
            )
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
