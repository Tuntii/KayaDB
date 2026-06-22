//! Raft event loop: ticks, elections, heartbeats, and leader state transitions.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use kaya_net::send_envelopes;
use kaya_raft::{Envelope, NodeId};
use tokio::sync::mpsc;

use crate::membership::SharedRoster;

use super::client_ops::{ProposeReq, ReadIndexReq};
use super::replication::drain_and_apply;
use super::{
    SharedApplyIndex, SharedEngine, SharedPending, SharedPendingReads, SharedPersister, SharedRaft,
};

fn persist_raft_state(raft: &SharedRaft, persister: &SharedPersister) {
    let view = raft.lock().unwrap().persist_view();
    if let Err(e) = persister.lock().unwrap().flush_view(view) {
        eprintln!("[server] warning: raft persist failed: {e}");
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn raft_event_loop(
    raft: SharedRaft,
    persister: SharedPersister,
    engine: SharedEngine,
    roster: SharedRoster,
    data_dir: PathBuf,
    apply_index: SharedApplyIndex,
    mut incoming_rx: mpsc::Receiver<Envelope>,
    mut propose_rx: mpsc::Receiver<ProposeReq>,
    mut read_propose_rx: mpsc::Receiver<ReadIndexReq>,
    pending: SharedPending,
    pending_reads: SharedPendingReads,
    tick_interval_ms: u64,
    self_id: NodeId,
    self_raft: SocketAddr,
    self_client: SocketAddr,
    _tls: Option<kaya_net::TlsConfig>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(tick_interval_ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let was_leader = raft.lock().unwrap().is_leader();

        tokio::select! {
            // ── periodic tick ─────────────────────────────────────────────────
            _ = interval.tick() => {
                let out = raft.lock().unwrap().tick();
                send_envelopes(out, &*roster.read().await).await;
                drain_and_apply(
                    &raft,
                    &engine,
                    &roster,
                    &data_dir,
                    &apply_index,
                    &pending,
                    &pending_reads,
                    self_id,
                    self_raft,
                    self_client,
                )
                .await;
                persist_raft_state(&raft, &persister);
            }

            // ── incoming raft message ─────────────────────────────────────────
            Some(env) = incoming_rx.recv() => {
                if !is_known_raft_peer(&raft, &roster, env.from).await {
                    eprintln!("[server] warning: received Raft message from unrecognized node id={:?}. Message ignored.", env.from);
                    continue;
                }
                let out = raft.lock().unwrap().handle(env);
                send_envelopes(out, &*roster.read().await).await;
                drain_and_apply(
                    &raft,
                    &engine,
                    &roster,
                    &data_dir,
                    &apply_index,
                    &pending,
                    &pending_reads,
                    self_id,
                    self_raft,
                    self_client,
                )
                .await;
                persist_raft_state(&raft, &persister);
            }

            // ── client write proposal ─────────────────────────────────────────
            Some(req) = propose_rx.recv() => {
                let idx_opt = raft.lock().unwrap().propose(req.command);
                match idx_opt {
                    Some(idx) => {
                        pending.lock().unwrap().insert(idx, req.reply_tx);
                        // Immediately replicate the new entry instead of
                        // waiting for the next heartbeat.
                        let out = raft.lock().unwrap().broadcast();
                        send_envelopes(out, &*roster.read().await).await;
                        drain_and_apply(
                            &raft,
                            &engine,
                            &roster,
                            &data_dir,
                            &apply_index,
                            &pending,
                            &pending_reads,
                            self_id,
                            self_raft,
                            self_client,
                        )
                        .await;
                        persist_raft_state(&raft, &persister);
                    }
                    None => {
                        let _ = req.reply_tx.send(Err("not_leader".to_owned()));
                    }
                }
            }

            // ── client read proposal ─────────────────────────────────────────
            Some(req) = read_propose_rx.recv() => {
                let commit_idx_opt = raft.lock().unwrap().propose_read(req.request_id);
                match commit_idx_opt {
                    Some(_idx) => {
                        pending_reads.lock().unwrap().insert(req.request_id, req.reply_tx);
                        // Immediately broadcast heartbeats to confirm leadership
                        let out = raft.lock().unwrap().broadcast();
                        send_envelopes(out, &*roster.read().await).await;
                        drain_and_apply(
                            &raft,
                            &engine,
                            &roster,
                            &data_dir,
                            &apply_index,
                            &pending,
                            &pending_reads,
                            self_id,
                            self_raft,
                            self_client,
                        )
                        .await;
                        persist_raft_state(&raft, &persister);
                    }
                    None => {
                        let _ = req.reply_tx.send(Err("not_leader".to_owned()));
                    }
                }
            }
        }

        let is_leader = raft.lock().unwrap().is_leader();
        if was_leader && !is_leader {
            // Cleared leader role: abort all pending writes and reads
            for (_idx, tx) in pending.lock().unwrap().drain() {
                let _ = tx.send(Err("not_leader".to_owned()));
            }
            for (_req_id, tx) in pending_reads.lock().unwrap().drain() {
                let _ = tx.send(Err("not_leader".to_owned()));
            }
        }
    }
}

async fn is_known_raft_peer(raft: &SharedRaft, roster: &SharedRoster, from: NodeId) -> bool {
    if roster.read().await.contains(from) {
        return true;
    }
    let guard = raft.lock().unwrap();
    guard.effective_config().all_voters().contains(&from)
}
