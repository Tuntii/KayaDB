//! Log replication: drain applied Raft entries and execute them against the engine.

use std::net::SocketAddr;

use kaya_engine::WriteOptions;
use kaya_raft::{ConfigChangePhase, NodeId, RaftApplyCommand};

use crate::command::RaftCommand;
use crate::membership::{apply_config_change_to_roster, decode_config_change, SharedRoster};

use super::snapshot::{apply_installed_raft_snapshot, maybe_compact_raft_log};
use super::{SharedApplyIndexes, SharedEngine, SharedPending, SharedPendingReads, SharedRaftHost};

/// Drain freshly-applied Raft entries from all groups and execute them against the engine.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_and_apply(
    host: &SharedRaftHost,
    engine: &SharedEngine,
    roster: &SharedRoster,
    data_dir: &std::path::Path,
    apply_indexes: &SharedApplyIndexes,
    pending: &SharedPending,
    pending_reads: &SharedPendingReads,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    apply_installed_raft_snapshot(
        host,
        engine,
        roster,
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

    maybe_compact_raft_log(host, engine, roster, data_dir).await;
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
