//! Prometheus text exposition for engine and Raft observability.

use kaya_ebpf::FsyncHistogram;
use kaya_engine::EngineStats;
use kaya_raft::{RaftStatus, Role};

/// Point-in-time metrics built from engine stats and Raft status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub wal_fsync_total_us: u64,
    pub wal_fsync_max_us: u64,
    pub live_sstables: u64,
    pub raft_term: u64,
    pub raft_is_leader: u8,
    pub raft_role: String,
    pub raft_leader_id: Option<u64>,
}

impl MetricsSnapshot {
    /// Build a snapshot from engine stats and an observed Raft status.
    pub fn from_engine_and_raft(
        engine_stats: EngineStats,
        raft_status: &RaftStatus,
        is_leader: bool,
    ) -> Self {
        Self {
            wal_fsync_total_us: engine_stats.wal_fsync_total_us,
            wal_fsync_max_us: engine_stats.wal_fsync_max_us,
            live_sstables: engine_stats.sstable_count,
            raft_term: raft_status.current_term.0,
            raft_is_leader: u8::from(is_leader),
            raft_role: role_label(raft_status.role),
            raft_leader_id: raft_status.leader_id.map(|id| id.0),
        }
    }
}

fn role_label(role: Role) -> String {
    format!("{role:?}").to_lowercase()
}

/// Render Prometheus text exposition format (0.0.4) for the given snapshot.
pub fn render_prometheus(snapshot: &MetricsSnapshot) -> String {
    render_prometheus_with_ebpf(snapshot, None)
}

/// Render engine/Raft metrics plus optional eBPF-derived fsync histograms.
pub fn render_prometheus_with_ebpf(
    snapshot: &MetricsSnapshot,
    ebpf: Option<&FsyncHistogram>,
) -> String {
    let mut body = format!(
        concat!(
            "# HELP kaya_wal_fsync_total_us Cumulative microseconds spent in WAL fsync calls.\n",
            "# TYPE kaya_wal_fsync_total_us counter\n",
            "kaya_wal_fsync_total_us {}\n",
            "# HELP kaya_wal_fsync_max_us Maximum single WAL fsync duration observed in microseconds.\n",
            "# TYPE kaya_wal_fsync_max_us gauge\n",
            "kaya_wal_fsync_max_us {}\n",
            "# HELP kaya_engine_live_sstables Number of live SSTables in the engine.\n",
            "# TYPE kaya_engine_live_sstables gauge\n",
            "kaya_engine_live_sstables {}\n",
            "# HELP kaya_raft_term Current Raft term.\n",
            "# TYPE kaya_raft_term gauge\n",
            "kaya_raft_term {}\n",
            "# HELP kaya_raft_is_leader 1 if this node is the Raft leader, 0 otherwise.\n",
            "# TYPE kaya_raft_is_leader gauge\n",
            "kaya_raft_is_leader {}\n",
        ),
        snapshot.wal_fsync_total_us,
        snapshot.wal_fsync_max_us,
        snapshot.live_sstables,
        snapshot.raft_term,
        snapshot.raft_is_leader,
    );
    if let Some(hist) = ebpf {
        body.push_str(&hist.render_prometheus());
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_engine::EngineStats;
    use kaya_raft::{LogIndex, NodeId, RaftStatus, Role, Term};

    fn sample_snapshot() -> MetricsSnapshot {
        let status = RaftStatus {
            id: NodeId(1),
            role: Role::Leader,
            current_term: Term(7),
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            leader_id: Some(NodeId(1)),
        };
        MetricsSnapshot::from_engine_and_raft(
            EngineStats {
                wal_fsync_total_us: 12_345,
                wal_fsync_max_us: 678,
                sstable_count: 3,
                ..EngineStats::default()
            },
            &status,
            true,
        )
    }

    #[test]
    fn render_prometheus_contains_expected_metrics() {
        let body = render_prometheus(&sample_snapshot());

        for expected in [
            "# HELP kaya_wal_fsync_total_us",
            "# TYPE kaya_wal_fsync_total_us counter",
            "kaya_wal_fsync_total_us 12345",
            "# HELP kaya_wal_fsync_max_us",
            "# TYPE kaya_wal_fsync_max_us gauge",
            "kaya_wal_fsync_max_us 678",
            "# HELP kaya_engine_live_sstables",
            "# TYPE kaya_engine_live_sstables gauge",
            "kaya_engine_live_sstables 3",
            "# HELP kaya_raft_term",
            "# TYPE kaya_raft_term gauge",
            "kaya_raft_term 7",
            "# HELP kaya_raft_is_leader",
            "# TYPE kaya_raft_is_leader gauge",
            "kaya_raft_is_leader 1",
        ] {
            assert!(
                body.contains(expected),
                "expected metric fragment missing: {expected}\nbody:\n{body}"
            );
        }
    }

    #[test]
    fn metrics_snapshot_captures_role_and_leader_id() {
        let snapshot = sample_snapshot();
        assert_eq!(snapshot.raft_role, "leader");
        assert_eq!(snapshot.raft_leader_id, Some(1));
    }

    #[test]
    fn render_prometheus_includes_ebpf_fsync_histogram_when_present() {
        let snapshot = sample_snapshot();
        let mut hist = FsyncHistogram::new();
        hist.observe(kaya_ebpf::SyscallKind::Fsync, 120);
        let body = render_prometheus_with_ebpf(&snapshot, Some(&hist));
        assert!(body.contains("kaya_ebpf_fsync_latency_us_bucket"));
        assert!(body.contains("kaya_ebpf_fsync_latency_us_count{syscall=\"fsync\"} 1"));
        assert!(body.contains("kaya_wal_fsync_total_us 12345"));
    }
}
