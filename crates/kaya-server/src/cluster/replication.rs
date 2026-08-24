//! Log replication: drain applied Raft entries and execute them against the engine.

use std::net::SocketAddr;

use kaya_engine::WriteOptions;
use kaya_raft::{ConfigChangePhase, NodeId, RaftApplyCommand, StaticRangeTable};

use crate::command::RaftCommand;
use crate::membership::{apply_config_change_to_roster, decode_config_change, SharedRoster};
use crate::range_meta::{decode_range_meta, persist_range_table};

use super::client_ops::{ensure_group_hosted, SharedRangeTable, SplitRuntime};
use super::snapshot::{apply_installed_raft_snapshot, maybe_compact_raft_log};
use super::{
    SharedApplyIndexes, SharedEngine, SharedPending, SharedPendingReads, SharedRaftHost,
    SharedReclaimStats,
};

/// Drain freshly-applied Raft entries from all groups and execute them against the engine.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_and_apply(
    host: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &SharedRoster,
    range_table: &SharedRangeTable,
    split_rt: &SplitRuntime,
    data_dir: &std::path::Path,
    apply_indexes: &SharedApplyIndexes,
    pending: &SharedPending,
    pending_reads: &SharedPendingReads,
    reclaimed_total: &SharedReclaimStats,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    apply_installed_raft_snapshot(
        host,
        engine,
        roster,
        range_table,
        split_rt,
        data_dir,
        self_id,
        self_raft,
        self_client,
    )
    .await;

    let applied = {
        let mut guard = host.lock().unwrap();
        guard.drain_all_applied()
    };
    for (gid, idx, term, command) in applied {
        if let Some((phase, members)) = decode_config_change(&command) {
            // Membership changes are applied cluster-wide (group 0 convention).
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
                    "[node {}] membership applied (group {}): {} voters",
                    self_id.0,
                    gid.0,
                    members.len()
                );
            }
        }

        // Range meta (#25): CAS-apply snapshot, persist, host groups.
        if let Some((base_epoch, snapshot)) = decode_range_meta(&command) {
            match apply_range_meta_entry(data_dir, range_table, split_rt, base_epoch, &snapshot)
                .await
            {
                Ok(()) => {
                    eprintln!(
                        "[node {}] range meta applied (group {}): base_epoch={base_epoch}",
                        self_id.0, gid.0
                    );
                }
                Err(e) => {
                    if let Some(tx) = pending.lock().unwrap().remove(&(gid.0, idx)) {
                        let _ = tx.send(Err(e));
                    }
                    continue;
                }
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
                    if let Some(tx) = pending.lock().unwrap().remove(&(gid.0, idx)) {
                        let _ = tx.send(Err(e.clone()));
                    }
                    continue;
                }
            }
        };

        if let Err(e) = {
            let mut map = apply_indexes.lock().unwrap();
            if let Some(ai) = map.get_mut(&gid.0) {
                ai.append(&meta)
            } else if let Some(ai) = map.get_mut(&0) {
                ai.append(&meta)
            } else {
                Ok(())
            }
        } {
            eprintln!(
                "warning: failed to persist raft↔lsn correlation for group {}: {e}",
                gid.0
            );
        }

        if let Some(tx) = pending.lock().unwrap().remove(&(gid.0, idx)) {
            let _ = tx.send(Ok(()));
        }
    }

    let ready_ids = {
        let mut guard = host.lock().unwrap();
        guard.drain_all_ready_reads()
    };
    for (_gid, req_id) in ready_ids {
        if let Some(tx) = pending_reads.lock().unwrap().remove(&req_id) {
            let _ = tx.send(Ok(()));
        }
    }

    maybe_compact_raft_log(host, engine, roster, range_table, data_dir).await;
    reclaim_orphan_groups(split_rt, range_table, reclaimed_total).await;
}

/// Unhost and delete the data dir of every Raft group no longer referenced by the
/// range table (orphaned by `merge_with_next`; issue #30).
///
/// ## Invariants
/// - Never touches group 0 (meta / membership group is always live).
/// - Never reclaims a group the range table still references — the candidate
///   set is `hosted \ referenced`, so a group is only ever a candidate once a
///   committed `RangeMeta` snapshot has already dropped it (see
///   [`apply_range_meta_entry`]).
/// - **Never reclaims `gid >= range_table.peek_next_group_id()`.** SPLIT_RANGE
///   calls `ensure_group_hosted` for its new group *before* the `RangeMeta`
///   command commits (so the leader can serve it the instant split succeeds).
///   In that window the group is hosted, unreferenced (the old table is still
///   live), and trivially "drained" (a fresh node has `commit_index ==
///   last_applied == 0`) — exactly what an orphan looks like. `next_group_id`
///   only advances when a `RangeMeta` commits, so a not-yet-committed split's
///   group id always equals the table's current `peek_next_group_id()`; this
///   bound is read under the same lock as `referenced` so the two agree.
///   Known bounded leak: if that split's propose never commits (leader steps
///   down, channel drop), its pre-hosted group stays hosted — un-reclaimable
///   by this rule, since nothing ever advances `next_group_id` past it — until
///   the next *successful* split on this range bumps the counter past it, at
///   which point it becomes a normal (referenced-check) candidate.
/// - Only reclaims a group once it is drained (`commit_index == last_applied`):
///   no unapplied entries left in flight for that group.
/// - Idempotent / crash-safe: a group already removed from the host (previous
///   pass, or never rehosted after a restart — startup only hosts groups the
///   persisted range table still references) is silently skipped, and a
///   missing data dir on `remove_dir_all` is not an error. So a crash between
///   unhosting and deleting the dir just leaves the dir removal to run again
///   on the next drain pass with no observable difference.
async fn reclaim_orphan_groups(
    split_rt: &SplitRuntime,
    range_table: &SharedRangeTable,
    reclaimed_total: &SharedReclaimStats,
) {
    let (referenced, next_group_id) = referenced_group_ids(range_table).await;

    let candidates: Vec<kaya_raft::GroupId> = {
        let guard = split_rt.raft.lock().unwrap();
        guard
            .group_ids()
            .filter(|gid| gid.0 != 0 && gid.0 < next_group_id && !referenced.contains(&gid.0))
            .collect()
    };

    for gid in candidates {
        let drained = {
            let guard = split_rt.raft.lock().unwrap();
            match guard.status_of(gid) {
                Some(status) => status.commit_index == status.last_applied,
                None => continue, // reclaimed by a concurrent pass already
            }
        };
        if !drained {
            continue;
        }
        {
            let mut guard = split_rt.raft.lock().unwrap();
            if guard.remove(gid).is_none() {
                continue; // raced with another reclaim pass
            }
        }
        split_rt.persisters.lock().unwrap().remove(&gid.0);
        split_rt.apply_indexes.lock().unwrap().remove(&gid.0);

        let group_dir = kaya_raft::multi_raft_group_dir(&split_rt.data_dir, gid);
        if let Err(e) = std::fs::remove_dir_all(&group_dir) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "warning: reclaimed group {} but failed to remove data dir {}: {e}",
                    gid.0,
                    group_dir.display()
                );
            }
        }
        reclaimed_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "[node] reclaimed orphan Raft group {} (merge cleanup)",
            gid.0
        );
    }
}

/// Group ids the range table currently routes to, plus the next id it would
/// assign to a not-yet-committed split. Read together under one lock so a
/// caller filtering on both sees a consistent snapshot (issue #30).
async fn referenced_group_ids(
    range_table: &SharedRangeTable,
) -> (std::collections::HashSet<u64>, u64) {
    let t = range_table.read().await;
    (
        t.group_ids().into_iter().map(|g| g.0).collect(),
        t.peek_next_group_id().0,
    )
}

/// Live count of hosted groups no longer referenced by the range table
/// (metrics gauge; issue #30). Not authoritative for the drain gate — a
/// group can appear here briefly before it is safe to reclaim. Excludes a
/// pre-hosted, not-yet-committed split's group (see `reclaim_orphan_groups`)
/// so the gauge does not flap on an in-flight split.
pub(crate) async fn orphan_group_count(
    host: &SharedRaftHost,
    range_table: &SharedRangeTable,
) -> u64 {
    let (referenced, next_group_id) = referenced_group_ids(range_table).await;
    let guard = host.lock().unwrap();
    guard
        .group_ids()
        .filter(|gid| gid.0 != 0 && gid.0 < next_group_id && !referenced.contains(&gid.0))
        .count() as u64
}

/// Apply a committed RangeMeta snapshot with optimistic concurrency on meta_epoch.
async fn apply_range_meta_entry(
    data_dir: &std::path::Path,
    range_table: &SharedRangeTable,
    split_rt: &SplitRuntime,
    base_epoch: u64,
    snapshot: &[u8],
) -> Result<(), String> {
    let new_table = StaticRangeTable::decode(snapshot)?;
    let group_ids = new_table.group_ids();

    {
        let mut guard = range_table.write().await;
        let current = guard.meta_epoch();
        if current != base_epoch {
            // Idempotent re-apply after durable restore, or concurrent stale proposal.
            if guard.encode() == snapshot {
                return Ok(());
            }
            return Err(format!(
                "range meta CAS failed: base_epoch={base_epoch} current={current}"
            ));
        }
        // Disk first: a crash after this reopen restores the new layout.
        persist_range_table(data_dir, &new_table)?;
        guard.restore(new_table);
    }

    for gid in group_ids {
        if let Err(e) = ensure_group_hosted(split_rt, gid) {
            eprintln!(
                "warning: failed to host group {} after range meta apply: {e}",
                gid.0
            );
        }
    }
    Ok(())
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
        Ok(RaftCommand::RangeMeta { .. }) => Ok(None), // handled above
        Ok(RaftCommand::TxnCommit { mutations, .. }) => {
            if mutations.is_empty() {
                return Ok(None);
            }
            // Atomic w.r.t. other Raft applies: single log entry, single apply.
            // Index + CDC fire per put/delete inside apply_mutations.
            engine
                .lock()
                .await
                .apply_mutations(mutations.into_iter().collect(), WriteOptions::default())
                .await
                .map(|_| None) // LSN correlation is optional for batch commits
                .map_err(|e| e.to_string())
        }
        Ok(RaftCommand::TxnPrepare {
            txn_id, mutations, ..
        }) => {
            let mutations: Vec<_> = mutations.into_iter().collect();
            engine
                .lock()
                .await
                .apply_txn_prepare(txn_id, &mutations)
                .await
                .map(|_| None)
                .map_err(|e| e.to_string())
        }
        Ok(RaftCommand::TxnCommit2pc { txn_id }) => engine
            .lock()
            .await
            .apply_txn_commit_2pc(txn_id)
            .await
            .map(|_| None)
            .map_err(|e| e.to_string()),
        Ok(RaftCommand::TxnAbort2pc { txn_id }) => engine
            .lock()
            .await
            .apply_txn_abort_2pc(txn_id)
            .await
            .map(|_| None)
            .map_err(|e| e.to_string()),
        Err(e) => Err(format!("corrupt command in log: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_raft::{GroupId, MultiRaftHost, NodeId, StaticRangeTable};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::sync::RwLock;

    fn test_split_runtime(host: MultiRaftHost) -> SplitRuntime {
        SplitRuntime {
            raft: Arc::new(Mutex::new(host)),
            persisters: Arc::new(Mutex::new(HashMap::new())),
            apply_indexes: Arc::new(Mutex::new(HashMap::new())),
            data_dir: std::env::temp_dir().join(format!(
                "kaya-reclaim-test-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )),
            node_id: NodeId(1),
            peers: vec![],
            election_timeout_ticks: 10,
            heartbeat_interval_ticks: 2,
        }
    }

    /// Review fix (#51): SPLIT_RANGE pre-hosts its new group before the
    /// RangeMeta command commits. That group's id always equals the table's
    /// `peek_next_group_id()` until the split lands, so it must survive a
    /// reclaim pass even though it looks unreferenced + trivially drained
    /// (fresh node: commit_index == last_applied == 0) — reclaiming it would
    /// strand the in-flight split.
    #[tokio::test]
    async fn reclaim_skips_group_pre_hosted_for_in_flight_split() {
        let mut host = MultiRaftHost::new();
        host.insert_single_node(GroupId::ZERO, NodeId(1));
        let next_id = GroupId(1);
        host.insert_single_node(next_id, NodeId(1)); // pre-hosted; split not committed

        let split_rt = test_split_runtime(host);
        let range_table: SharedRangeTable =
            Arc::new(RwLock::new(StaticRangeTable::single_group(GroupId::ZERO)));
        assert_eq!(
            range_table.read().await.peek_next_group_id(),
            next_id,
            "test setup: pre-hosted group must equal the table's next id"
        );
        let reclaimed_total: SharedReclaimStats = Arc::new(AtomicU64::new(0));

        reclaim_orphan_groups(&split_rt, &range_table, &reclaimed_total).await;

        assert!(
            split_rt.raft.lock().unwrap().get(next_id).is_some(),
            "in-flight split's pre-hosted group must not be reclaimed"
        );
        assert_eq!(reclaimed_total.load(Ordering::Relaxed), 0);
    }

    /// Sanity check that the fix didn't disable reclaim: a group below
    /// `next_group_id` and unreferenced (a real post-merge orphan) still goes.
    #[tokio::test]
    async fn reclaim_still_removes_a_real_orphan_below_next_id() {
        let mut host = MultiRaftHost::new();
        host.insert_single_node(GroupId::ZERO, NodeId(1));
        host.insert_single_node(GroupId(1), NodeId(1)); // orphaned by a prior merge

        let split_rt = test_split_runtime(host);
        // next_group_id = 2 here, so group 1 (< 2, unreferenced) is a real orphan.
        let mut table = StaticRangeTable::single_group(GroupId::ZERO);
        table.split_at(b"m").unwrap();
        table.merge_with_next(b"").unwrap(); // drops group 1 from the table again
        assert_eq!(table.peek_next_group_id(), GroupId(2));
        let range_table: SharedRangeTable = Arc::new(RwLock::new(table));
        let reclaimed_total: SharedReclaimStats = Arc::new(AtomicU64::new(0));

        reclaim_orphan_groups(&split_rt, &range_table, &reclaimed_total).await;

        assert!(split_rt.raft.lock().unwrap().get(GroupId(1)).is_none());
        assert_eq!(reclaimed_total.load(Ordering::Relaxed), 1);
    }
}
