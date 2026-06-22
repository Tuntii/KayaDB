//! Raft snapshot install and log compaction.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use kaya_raft::{ClusterMember, ConfigChangePhase, NodeId};

use crate::membership::{
    apply_config_change_to_roster, build_raft_snapshot_payload, parse_raft_snapshot_payload,
    SharedRoster,
};

use super::{SharedEngine, SharedRaft};

/// Load persisted Raft snapshot once at startup (before the event loop applies entries).
pub(crate) async fn install_persisted_snapshot_at_startup(
    data_dir: &Path,
    shared_engine: &SharedEngine,
    shared_raft: &SharedRaft,
    shared_roster: &SharedRoster,
    node_id: NodeId,
    raft_addr: SocketAddr,
    client_addr: SocketAddr,
) {
    let snap_path = data_dir.join("raft-snapshot.bin");
    if snap_path.exists() {
        if let Ok(raw) = std::fs::read(&snap_path) {
            match parse_raft_snapshot_payload(&raw) {
                Ok((eng, mems)) => {
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

/// Handle any snapshot that was just installed on us (via InstallSnapshot).
pub(crate) async fn apply_installed_raft_snapshot(
    raft: &SharedRaft,
    engine: &SharedEngine,
    roster: &SharedRoster,
    data_dir: &Path,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
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
}

/// Periodic Raft log compaction using real pinned manifest-anchored MVCC snapshot.
pub(crate) async fn maybe_compact_raft_log(
    raft: &SharedRaft,
    engine: &SharedEngine,
    roster: &SharedRoster,
    data_dir: &Path,
) {
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
            persist_raft_snapshot_atomically(data_dir, &snap_data);
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