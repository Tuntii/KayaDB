use std::path::PathBuf;
use std::sync::Arc;

use kaya_core::{DurabilityConfig, DurabilityMode, EngineConfig, Result};
use kaya_engine::{recover as engine_recover, EngineStats, RecoveryReport};
use kaya_io::FileDisk;

use crate::cli::{block_on, json_string};

pub(crate) fn run_local_stats(
    data_dir: String,
    durability: DurabilityMode,
    json: bool,
    latency_view: bool,
) -> Result<()> {
    let engine = block_on(crate::local::open_engine(data_dir, durability))?;
    let stats = engine.stats();
    let recovery = engine.last_recovery().clone();
    if json {
        print_stats_json(&stats, &recovery);
    } else if latency_view {
        print_latency_human(&stats);
    } else {
        print_stats_human(&stats, &recovery);
    }
    Ok(())
}

pub(crate) fn run_recover_dry_run(
    data_dir: String,
    durability: DurabilityMode,
    json: bool,
) -> Result<()> {
    let config = EngineConfig {
        data_dir: PathBuf::from(&data_dir),
        durability: DurabilityConfig {
            mode: durability,
            ..DurabilityConfig::default()
        },
        ..EngineConfig::default()
    };
    let disk = Arc::new(FileDisk::new(config.data_dir.clone()));
    let recovery = block_on(engine_recover(config, disk))?;
    if json {
        print_recovery_json(&recovery);
    } else {
        print_recovery_human(&recovery);
    }
    Ok(())
}

pub(crate) fn print_human_stats_from_json(json: &str) {
    println!("=== KayaDB Cluster Node Status ===");
    let extract = |key: &str| -> Option<String> {
        let pattern = format!("\"{}\":", key);
        if let Some(pos) = json.find(&pattern) {
            let start = pos + pattern.len();
            let mut end = start;
            let bytes = json.as_bytes();
            let mut in_quotes = false;
            while end < bytes.len() {
                let c = bytes[end] as char;
                if c == '"' {
                    in_quotes = !in_quotes;
                } else if !in_quotes && (c == ',' || c == '}' || c == '{') {
                    break;
                }
                end += 1;
            }
            let val = &json[start..end];
            return Some(val.replace("\"", "").trim().to_string());
        }
        None
    };

    if let Some(role) = extract("role") {
        println!("Role:           {}", role);
    }
    if let Some(term) = extract("term") {
        println!("Term:           {}", term);
    }
    if let Some(commit) = extract("commit_index") {
        println!("Commit Index:   {}", commit);
    }
    if let Some(applied) = extract("applied_index") {
        println!("Applied Index:  {}", applied);
    }
    if let Some(peers) = extract("peer_count") {
        println!("Peer Count:     {}", peers);
    }

    println!("\n--- LSM Storage Engine Metrics ---");
    if let Some(put) = extract("put_count") {
        println!("PUT Operations:       {}", put);
    }
    if let Some(get) = extract("get_count") {
        println!("GET Operations:       {}", get);
    }
    if let Some(delete) = extract("delete_count") {
        println!("DELETE Operations:    {}", delete);
    }
    if let Some(scan) = extract("scan_count") {
        println!("SCAN Operations:      {}", scan);
    }
    if let Some(wal_bytes) = extract("wal_bytes_written") {
        println!("WAL Bytes Written:    {} bytes", wal_bytes);
    }
    if let Some(wal_fsync) = extract("wal_fsync_count") {
        println!("WAL Fsync Count:      {}", wal_fsync);
    }
    if let Some(total_us) = extract("wal_fsync_total_us") {
        println!("WAL Fsync Total Us:   {}", total_us);
    }
    if let Some(max_us) = extract("wal_fsync_max_us") {
        println!("WAL Fsync Max Us:     {}", max_us);
    }
    if let Some(mem_entries) = extract("memtable_entries") {
        println!("Memtable Entries:     {}", mem_entries);
    }
    if let Some(sst_count) = extract("sstable_count") {
        println!("SSTable Count:        {}", sst_count);
    }
    if let Some(last_seq) = extract("last_sequence") {
        println!("Last Sequence Number: {}", last_seq);
    }
    if let Some(f_cnt) = extract("flush_count") {
        println!("Flush Count:            {}", f_cnt);
    }
    if let Some(f_tot) = extract("flush_total_us") {
        println!("Flush Total Us:         {}", f_tot);
    }
    if let Some(f_max) = extract("flush_max_us") {
        println!("Flush Max Us:           {}", f_max);
    }
    if let Some(f_avg) = extract("flush_avg_us") {
        println!("Flush Avg Us:           {}", f_avg);
    }
    if let Some(c_cnt) = extract("compaction_count") {
        println!("Compaction Count:       {}", c_cnt);
    }
    if let Some(c_tot) = extract("compaction_total_us") {
        println!("Compaction Total Us:    {}", c_tot);
    }
    if let Some(c_max) = extract("compaction_max_us") {
        println!("Compaction Max Us:      {}", c_max);
    }
    if let Some(c_avg) = extract("compaction_avg_us") {
        println!("Compaction Avg Us:      {}", c_avg);
    }
    if let Some(hits) = extract("block_cache_hits") {
        println!("Block Cache Hits:       {}", hits);
    }
    if let Some(misses) = extract("block_cache_misses") {
        println!("Block Cache Misses:     {}", misses);
    }
    if let Some(recovery_us) = extract("recovery_duration_us") {
        println!("Recovery Duration Us:   {}", recovery_us);
    }
    println!("==================================");
}

fn print_stats_human(stats: &EngineStats, recovery: &RecoveryReport) {
    println!("put_count:         {}", stats.put_count);
    println!("get_count:         {}", stats.get_count);
    println!("delete_count:      {}", stats.delete_count);
    println!("scan_count:        {}", stats.scan_count);
    println!("wal_bytes_written: {}", stats.wal_bytes_written);
    println!("wal_fsync_count:   {}", stats.wal_fsync_count);
    println!("wal_fsync_total_us:{}", stats.wal_fsync_total_us);
    println!("wal_fsync_max_us:  {}", stats.wal_fsync_max_us);
    if let Some(avg) = stats.wal_fsync_total_us.checked_div(stats.wal_fsync_count) {
        println!("wal_fsync_avg_us:  {} (total/count)", avg);
    }
    println!("memtable_entries:  {}", stats.memtable_entries);
    println!("sstable_count:     {}", stats.sstable_count);
    println!("last_sequence:     {}", stats.last_sequence);
    // Track A latency metrics
    println!("flush_count:       {}", stats.flush_count);
    println!("flush_total_us:    {}", stats.flush_total_us);
    println!("flush_max_us:      {}", stats.flush_max_us);
    if let Some(avg) = stats.flush_total_us.checked_div(stats.flush_count) {
        println!("flush_avg_us:      {} (total/count)", avg);
    }
    println!("compaction_count:  {}", stats.compaction_count);
    println!("compaction_total_us: {}", stats.compaction_total_us);
    println!("compaction_max_us:   {}", stats.compaction_max_us);
    if let Some(avg) = stats
        .compaction_total_us
        .checked_div(stats.compaction_count)
    {
        println!("compaction_avg_us: {} (total/count)", avg);
    }
    println!("block_cache_hits:  {}", stats.block_cache_hits);
    println!("block_cache_misses:{}", stats.block_cache_misses);
    println!("recovery_duration_us: {}", stats.recovery_duration_us);
    println!();
    println!(
        "recovery.manifest_records_replayed: {}",
        recovery.manifest_records_replayed
    );
    println!(
        "recovery.live_sstable_count:        {}",
        recovery.live_sstable_count
    );
    println!(
        "recovery.wal_records_replayed:      {}",
        recovery.wal_records_replayed
    );
    println!(
        "recovery.wal_truncated_bytes:       {}",
        recovery.wal_truncated_bytes
    );
    println!(
        "recovery.tmp_files_removed:         {}",
        recovery.tmp_files_removed
    );
    println!(
        "recovery.last_lsn:                  {}",
        recovery.last_lsn.map_or(0, |l| l.get())
    );
    println!(
        "recovery.last_sequence:             {}",
        recovery.last_sequence.map_or(0, |s| s.get())
    );
    println!(
        "recovery.records_replayed:          {}",
        recovery.records_replayed
    );
    for w in &recovery.warnings {
        println!("recovery.warning: {w}");
    }
}

fn print_stats_json(stats: &EngineStats, recovery: &RecoveryReport) {
    print!(
        "{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},",
        stats.put_count, stats.get_count, stats.delete_count, stats.scan_count
    );
    print!(
        "\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"wal_fsync_total_us\":{},\"wal_fsync_max_us\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{},\"flush_total_us\":{},\"flush_max_us\":{},\"flush_count\":{},\"compaction_total_us\":{},\"compaction_max_us\":{},\"compaction_count\":{},\"block_cache_hits\":{},\"block_cache_misses\":{},\"recovery_duration_us\":{},",
        stats.wal_bytes_written, stats.wal_fsync_count, stats.wal_fsync_total_us, stats.wal_fsync_max_us, stats.memtable_entries, stats.sstable_count, stats.last_sequence,
        stats.flush_total_us, stats.flush_max_us, stats.flush_count,
        stats.compaction_total_us, stats.compaction_max_us, stats.compaction_count,
        stats.block_cache_hits, stats.block_cache_misses, stats.recovery_duration_us
    );
    print!(
        "\"recovery\":{{\"manifest_records_replayed\":{},\"live_sstable_count\":{},\"wal_records_replayed\":{},\"wal_truncated_bytes\":{},\"tmp_files_removed\":{},\"last_lsn\":{},\"last_sequence\":{},\"records_replayed\":{},\"warnings\":[",
        recovery.manifest_records_replayed,
        recovery.live_sstable_count,
        recovery.wal_records_replayed,
        recovery.wal_truncated_bytes,
        recovery.tmp_files_removed,
        recovery.last_lsn.map_or(0, |l| l.get()),
        recovery.last_sequence.map_or(0, |s| s.get()),
        recovery.records_replayed
    );
    for (i, w) in recovery.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}}}");
}

/// Focused latency view (Track A). Use with `kayactl [--data] stats --latency` or pipe server status JSON.
pub(crate) fn print_latency_human(stats: &EngineStats) {
    println!("=== KayaDB Latency / Durability (Track A) ===");
    println!("WAL fsyncs (strict durability cost):");
    println!("  count:   {}", stats.wal_fsync_count);
    println!("  total_us:{}", stats.wal_fsync_total_us);
    println!("  max_us:  {}", stats.wal_fsync_max_us);
    if let Some(avg) = stats.wal_fsync_total_us.checked_div(stats.wal_fsync_count) {
        println!("  avg_us:  {avg}");
    }
    println!();
    println!("Flush (memtable -> SSTable publish, manifest, dir fsyncs):");
    println!("  count:   {}", stats.flush_count);
    println!("  total_us:{}", stats.flush_total_us);
    println!("  max_us:  {}", stats.flush_max_us);
    if let Some(avg) = stats.flush_total_us.checked_div(stats.flush_count) {
        println!("  avg_us:  {avg}");
    }
    println!();
    println!("Compaction (L0 merge + publish):");
    println!("  count:   {}", stats.compaction_count);
    println!("  total_us:{}", stats.compaction_total_us);
    println!("  max_us:  {}", stats.compaction_max_us);
    if let Some(avg) = stats
        .compaction_total_us
        .checked_div(stats.compaction_count)
    {
        println!("  avg_us:  {avg}");
    }
    println!();
    println!("Cross-reference with scripts/ebpf/ (Linux bpftrace probes; see README.md)");
    println!("========================================");
}

fn print_recovery_human(recovery: &RecoveryReport) {
    println!(
        "manifest_records_replayed: {}",
        recovery.manifest_records_replayed
    );
    println!("live_sstable_count:        {}", recovery.live_sstable_count);
    println!(
        "wal_records_replayed:      {}",
        recovery.wal_records_replayed
    );
    println!(
        "wal_truncated_bytes:       {}",
        recovery.wal_truncated_bytes
    );
    println!("tmp_files_removed:         {}", recovery.tmp_files_removed);
    println!(
        "last_lsn:                  {}",
        recovery.last_lsn.map_or(0, |l| l.get())
    );
    println!(
        "last_sequence:             {}",
        recovery.last_sequence.map_or(0, |s| s.get())
    );
    println!("records_replayed:          {}", recovery.records_replayed);
    for w in &recovery.warnings {
        println!("warning: {w}");
    }
    if recovery.warnings.is_empty() {
        println!("warnings:            none");
    }
}

fn print_recovery_json(recovery: &RecoveryReport) {
    print!(
        "{{\"manifest_records_replayed\":{},\"live_sstable_count\":{},\"wal_records_replayed\":{},\"wal_truncated_bytes\":{},\"tmp_files_removed\":{},\"last_lsn\":{},\"last_sequence\":{},\"records_replayed\":{},\"warnings\":[",
        recovery.manifest_records_replayed,
        recovery.live_sstable_count,
        recovery.wal_records_replayed,
        recovery.wal_truncated_bytes,
        recovery.tmp_files_removed,
        recovery.last_lsn.map_or(0, |l| l.get()),
        recovery.last_sequence.map_or(0, |s| s.get()),
        recovery.records_replayed
    );
    for (i, w) in recovery.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}");
}
