//! Log replication: drain applied Raft entries and execute them against the engine.

use std::net::SocketAddr;

use kaya_engine::WriteOptions;
use kaya_raft::{ConfigChangePhase, NodeId, RaftApplyCommand};

use crate::command::RaftCommand;
use crate::membership::{apply_config_change_to_roster, decode_config_change, SharedRoster};

use super::snapshot::{apply_installed_raft_snapshot, maybe_compact_raft_log};
use super::{SharedApplyIndex, SharedEngine, SharedPending, SharedPendingReads, SharedRaft};

/// Drain freshly-applied Raft entries and execute them against the engine.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_and_apply(
    raft: &SharedRaft,
    engine: &SharedEngine,
    roster: &SharedRoster,
    data_dir: &std::path::Path,
    apply_index: &SharedApplyIndex,
    pending: &SharedPending,
    pending_reads: &SharedPendingReads,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
) {
    apply_installed_raft_snapshot(
        raft,
        engine,
        roster,
        data_dir,
        self_id,
        self_raft,
        self_client,
    )
    .await;

    let applied = {
        let mut guard = raft.lock().unwrap();
        guard.drain_applied()
    };
    for (idx, term, command) in applied {
        if let Some((phase, members)) = decode_config_change(&command) {
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
                    "[node {}] membership applied: {} voters",
                    self_id.0,
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
                    if let Some(tx) = pending.lock().unwrap().remove(&idx) {
                        let _ = tx.send(Err(e.clone()));
                    }
                    continue;
                }
            }
        };

        if let Err(e) = apply_index.lock().unwrap().append(&meta) {
            eprintln!("warning: failed to persist raft↔lsn correlation: {e}");
        }

        let result = Ok(());
        if let Some(tx) = pending.lock().unwrap().remove(&idx) {
            let _ = tx.send(result);
        }
    }

    let ready_ids = {
        let mut guard = raft.lock().unwrap();
        guard.drain_ready_reads()
    };
    for req_id in ready_ids {
        if let Some(tx) = pending_reads.lock().unwrap().remove(&req_id) {
            let _ = tx.send(Ok(()));
        }
    }

    maybe_compact_raft_log(raft, engine, roster, data_dir).await;
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
        Err(e) => Err(format!("corrupt command in log: {e}")),
    }
}
