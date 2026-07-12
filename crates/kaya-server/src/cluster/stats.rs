//! Cluster status and engine stats reporting.

use kaya_net::{NodeRoster, STATUS_OK};
use kaya_raft::GroupId;

use super::{SharedEngine, SharedRaftHost};

pub(crate) async fn build_stats_response(
    host: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &NodeRoster,
) -> (u16, Vec<u8>) {
    let (role, term, commit_idx, applied_idx, peer_count, group_count) = {
        let r = host.lock().unwrap();
        let status = r
            .primary_status()
            .or_else(|| {
                r.sorted_group_ids()
                    .into_iter()
                    .find_map(|g| r.status_of(g))
            })
            .unwrap_or(kaya_raft::RaftStatus {
                id: kaya_raft::NodeId(0),
                role: kaya_raft::Role::Follower,
                current_term: kaya_raft::Term(0),
                commit_index: kaya_raft::LogIndex(0),
                last_applied: kaya_raft::LogIndex(0),
                leader_id: None,
            });
        let peer_cnt = roster.all_ids().len().saturating_sub(1);
        let groups = r.len();
        let _ = GroupId::ZERO;
        (
            format!("{:?}", status.role).to_lowercase(),
            status.current_term.0,
            status.commit_index.0,
            status.last_applied.0,
            peer_cnt,
            groups,
        )
    };

    let engine_stats = engine.lock().await.stats();

    let stats_json = format!(
        "{{\"role\":\"{}\",\"term\":{},\"commit_index\":{},\"applied_index\":{},\"peer_count\":{},\"raft_groups\":{},\"engine\":{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"wal_fsync_total_us\":{},\"wal_fsync_max_us\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{},\"flush_total_us\":{},\"flush_max_us\":{},\"flush_count\":{},\"compaction_total_us\":{},\"compaction_max_us\":{},\"compaction_count\":{},\"block_cache_hits\":{},\"block_cache_misses\":{},\"recovery_duration_us\":{}}}}}",
        role, term, commit_idx, applied_idx, peer_count, group_count,
        engine_stats.put_count, engine_stats.get_count, engine_stats.delete_count, engine_stats.scan_count,
        engine_stats.wal_bytes_written, engine_stats.wal_fsync_count, engine_stats.wal_fsync_total_us, engine_stats.wal_fsync_max_us, engine_stats.memtable_entries, engine_stats.sstable_count, engine_stats.last_sequence,
        engine_stats.flush_total_us, engine_stats.flush_max_us, engine_stats.flush_count,
        engine_stats.compaction_total_us, engine_stats.compaction_max_us, engine_stats.compaction_count,
        engine_stats.block_cache_hits, engine_stats.block_cache_misses, engine_stats.recovery_duration_us
    );

    (STATUS_OK, stats_json.into_bytes())
}
