//! Cluster status and engine stats reporting.

use kaya_net::{NodeRoster, STATUS_OK};

use super::{SharedEngine, SharedRaft};

pub(crate) async fn build_stats_response(
    raft: &SharedRaft,
    engine: &SharedEngine,
    roster: &NodeRoster,
) -> (u16, Vec<u8>) {
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
        "{{\"role\":\"{}\",\"term\":{},\"commit_index\":{},\"applied_index\":{},\"peer_count\":{},\"engine\":{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"wal_fsync_total_us\":{},\"wal_fsync_max_us\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{},\"flush_total_us\":{},\"flush_max_us\":{},\"flush_count\":{},\"compaction_total_us\":{},\"compaction_max_us\":{},\"compaction_count\":{}}}}}",
        role, term, commit_idx, applied_idx, peer_count,
        engine_stats.put_count, engine_stats.get_count, engine_stats.delete_count, engine_stats.scan_count,
        engine_stats.wal_bytes_written, engine_stats.wal_fsync_count, engine_stats.wal_fsync_total_us, engine_stats.wal_fsync_max_us, engine_stats.memtable_entries, engine_stats.sstable_count, engine_stats.last_sequence,
        engine_stats.flush_total_us, engine_stats.flush_max_us, engine_stats.flush_count,
        engine_stats.compaction_total_us, engine_stats.compaction_max_us, engine_stats.compaction_count
    );

    (STATUS_OK, stats_json.into_bytes())
}