//! Raft snapshot install and log compaction (group-aware).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use kaya_raft::{
    build_snapshot_payload_v2, parse_snapshot_payload_v2, ClusterMember, ConfigChangePhase,
    GroupId, NodeId, StaticRangeTable,
};

use crate::membership::{apply_config_change_to_roster, SharedRoster};
use crate::range_meta::persist_range_table;

use super::client_ops::{ensure_group_hosted, SharedRangeTable, SplitRuntime};
use super::{SharedEngine, SharedRaftHost};

/// Load persisted Raft snapshot once at startup (before the event loop applies entries).
///
/// Engine + membership snapshots live at the data-dir root (shared across groups).
/// Group 0's Raft node receives membership restore.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn install_persisted_snapshot_at_startup(
    data_dir: &Path,
    shared_engine: &SharedEngine,
    shared_host: &SharedRaftHost,
    shared_roster: &SharedRoster,
    range_table: &SharedRangeTable,
    split_rt: &SplitRuntime,
    node_id: NodeId,
    raft_addr: SocketAddr,
    client_addr: SocketAddr,
) {
    let snap_path = data_dir.join("raft-snapshot.bin");
    if snap_path.exists() {
        if let Ok(raw) = std::fs::read(&snap_path) {
            match parse_snapshot_payload_v2(&raw) {
                Ok((eng, mems, ranges)) => {
                    if !eng.is_empty() {
                        if let Err(e) = shared_engine.lock().await.install_snapshot(&eng).await {
                            eprintln!("warning: failed to install persisted engine snapshot: {e}");
                        }
                    }
                    if !mems.is_empty() {
                        apply_config_change_to_roster(
                            data_dir,
                            shared_roster,
                            ConfigChangePhase::Final,
                            &mems,
                            node_id,
                            raft_addr,
                            client_addr,
                        )
                        .await;
                        let mut host = shared_host.lock().unwrap();
                        // Restore membership on every group so effective configs stay aligned.
                        for gid in host.sorted_group_ids() {
                            if let Some(node) = host.get_mut(gid) {
                                node.restore_config_from_snapshot(mems.clone());
                            }
                        }
                    }
                    restore_range_table_from_snapshot(data_dir, range_table, split_rt, &ranges)
                        .await;
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

/// Handle any snapshot that was just installed on us (via InstallSnapshot).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn apply_installed_raft_snapshot(
    host: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &SharedRoster,
    range_table: &SharedRangeTable,
    split_rt: &SplitRuntime,
    data_dir: &Path,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    // Drain installed snapshots from every group.
    let installed: Vec<(GroupId, _, _, Vec<u8>)> = {
        let mut guard = host.lock().unwrap();
        let mut out = Vec::new();
        for gid in guard.sorted_group_ids() {
            if let Some(node) = guard.get_mut(gid) {
                if let Some((idx, term, data)) = node.drain_installed_snapshot() {
                    out.push((gid, idx, term, data));
                }
            }
        }
        out
    };

    for (_gid, _idx, _term, data) in installed {
        match parse_snapshot_payload_v2(&data) {
            Ok((eng, mems, ranges)) => {
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
                    let mut guard = host.lock().unwrap();
                    for gid in guard.sorted_group_ids() {
                        if let Some(node) = guard.get_mut(gid) {
                            node.restore_config_from_snapshot(mems.clone());
                        }
                    }
                }
                restore_range_table_from_snapshot(data_dir, range_table, split_rt, &ranges).await;
            }
            Err(e) => {
                // Fallback: try as pure engine data (legacy)
                if let Err(e2) = engine.lock().await.install_snapshot(&data).await {
                    eprintln!("error installing (legacy) snapshot: {e2} (parse err was {e})");
                }
            }
        }
    }
}

/// Periodic Raft log compaction using real pinned manifest-anchored MVCC snapshot.
///
/// Compacts each group independently when its last_applied crosses a threshold.
pub(crate) async fn maybe_compact_raft_log(
    host: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &SharedRoster,
    range_table: &SharedRangeTable,
    data_dir: &Path,
) {
    let compaction_targets: Vec<(GroupId, _, _)> = {
        let guard = host.lock().unwrap();
        guard
            .sorted_group_ids()
            .into_iter()
            .filter_map(|gid| {
                let status = guard.status_of(gid)?;
                if status.last_applied.0 > 0 && status.last_applied.0 % 64 == 0 {
                    Some((gid, status.last_applied, status.current_term))
                } else {
                    None
                }
            })
            .collect()
    };

    if compaction_targets.is_empty() {
        return;
    }

    // One engine snapshot for all groups (shared state machine).
    // Release previous pins once (using group 0 snapshot if present).
    {
        let old_data = {
            let guard = host.lock().unwrap();
            guard
                .get(GroupId::ZERO)
                .and_then(|n| n.snapshot())
                .and_then(|(_idx, _term, d)| if d.is_empty() { None } else { Some(d) })
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

    // Capture current membership (voters + learners) so snapshot receivers restore config.
    let members_snapshot: Vec<ClusterMember> = {
        let roster_guard = roster.read().await;
        let known: Vec<ClusterMember> = {
            let guard = host.lock().unwrap();
            guard
                .get(GroupId::ZERO)
                .or_else(|| {
                    guard
                        .sorted_group_ids()
                        .into_iter()
                        .find_map(|g| guard.get(g))
                })
                .map(|n| {
                    let members = n.membership().to_vec();
                    if !members.is_empty() {
                        members
                    } else {
                        n.effective_config()
                            .stable_config()
                            .voters
                            .iter()
                            .map(|&id| ClusterMember {
                                id,
                                raft_addr: String::new(),
                                client_addr: String::new(),
                                is_learner: false,
                            })
                            .collect()
                    }
                })
                .unwrap_or_default()
        };
        known
            .into_iter()
            .filter_map(|mut m| {
                if m.raft_addr.is_empty() {
                    if let Some(r) = roster_guard.addr(m.id) {
                        m.raft_addr = r.to_string();
                    }
                }
                if m.client_addr.is_empty() {
                    if let Some(c) = roster_guard.client_addr(m.id) {
                        m.client_addr = c.to_string();
                    }
                }
                if m.raft_addr.is_empty() || m.client_addr.is_empty() {
                    None
                } else {
                    Some(m)
                }
            })
            .collect()
    };

    let range_bytes = range_table.read().await.encode();
    let snap_data = build_snapshot_payload_v2(&engine_data, &members_snapshot, &range_bytes);

    for (gid, last, term) in compaction_targets {
        if let Some(n) = host.lock().unwrap().get_mut(gid) {
            n.compact(last, term, snap_data.clone())
        }
    }

    // Persisted snapshot for fast restart (shared engine+membership at root).
    if !snap_data.is_empty() {
        persist_raft_snapshot_atomically(data_dir, &snap_data);
    }
}

/// Install a snapshot-embedded range table when it is at least as new as the live one.
async fn restore_range_table_from_snapshot(
    data_dir: &Path,
    range_table: &SharedRangeTable,
    split_rt: &SplitRuntime,
    snapshot: &[u8],
) {
    if snapshot.is_empty() {
        return;
    }
    let new_table = match StaticRangeTable::decode(snapshot) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("warning: snapshot range table decode failed: {e}");
            return;
        }
    };
    let group_ids = new_table.group_ids();
    {
        let mut guard = range_table.write().await;
        if new_table.meta_epoch() < guard.meta_epoch() {
            return;
        }
        if let Err(e) = persist_range_table(data_dir, &new_table) {
            eprintln!("warning: failed to persist range table from snapshot: {e}");
            return;
        }
        guard.restore(new_table);
    }
    for gid in group_ids {
        if let Err(e) = ensure_group_hosted(split_rt, gid) {
            eprintln!(
                "warning: failed to host group {} after snapshot range restore: {e}",
                gid.0
            );
        }
    }
}

fn persist_raft_snapshot_atomically(data_dir: &Path, snap_data: &[u8]) {
    let snap_path = data_dir.join("raft-snapshot.bin");
    let tmp_path: PathBuf = data_dir.join("raft-snapshot.bin.tmp");
    let write_ok = (|| -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;
        let mut f = File::create(&tmp_path)?;
        f.write_all(snap_data)?;
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
