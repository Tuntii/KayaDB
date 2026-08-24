//! Raft event loop: ticks, elections, heartbeats, and leader state transitions.
//!
//! Hosts a [`MultiRaftHost`]: ticks are coalesced, inbound envelopes are demuxed
//! by `group_id`, and client proposals target a specific group.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use kaya_net::send_envelopes;
use kaya_raft::{GroupId, NodeId};
use tokio::sync::mpsc;

use crate::membership::SharedRoster;
use crate::raft_persister::RaftPersister;

use super::client_ops::{ProposeReq, ReadIndexReq, SharedRangeTable, SplitRuntime};
use super::replication::drain_and_apply;
use super::{
    SharedApplyIndexes, SharedEngine, SharedPending, SharedPendingReads, SharedRaftHost,
    SharedReclaimStats,
};

fn persist_raft_state(
    host: &SharedRaftHost,
    persisters: &std::sync::Arc<std::sync::Mutex<HashMap<u64, RaftPersister>>>,
) {
    let views: Vec<(u64, _)> = {
        let guard = host.lock().unwrap();
        guard
            .sorted_group_ids()
            .into_iter()
            .filter_map(|gid| guard.get(gid).map(|n| (gid.0, n.persist_view())))
            .collect()
    };
    let mut map = persisters.lock().unwrap();
    for (gid, view) in views {
        if let Some(p) = map.get_mut(&gid) {
            if let Err(e) = p.flush_view(view) {
                eprintln!("[server] warning: raft persist failed for group {gid}: {e}");
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn raft_event_loop(
    host: SharedRaftHost,
    persisters: std::sync::Arc<std::sync::Mutex<HashMap<u64, RaftPersister>>>,
    engine: SharedEngine,
    roster: SharedRoster,
    range_table: SharedRangeTable,
    split_rt: SplitRuntime,
    data_dir: PathBuf,
    apply_indexes: SharedApplyIndexes,
    reclaimed_total: SharedReclaimStats,
    mut incoming_rx: mpsc::Receiver<kaya_raft::Envelope>,
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

    // Track leadership per group for demotion abort of pending client ops.
    let mut leader_groups: std::collections::HashSet<u64> = std::collections::HashSet::new();
    {
        let guard = host.lock().unwrap();
        for gid in guard.sorted_group_ids() {
            if guard.is_leader_of(gid) {
                leader_groups.insert(gid.0);
            }
        }
    }

    loop {
        tokio::select! {
            // ── periodic tick ─────────────────────────────────────────────────
            _ = interval.tick() => {
                let out = host.lock().unwrap().tick_all();
                send_envelopes(out, &*roster.read().await).await;
                drain_and_apply(
                    &host,
                    &engine,
                    &roster,
                    &range_table,
                    &split_rt,
                    &data_dir,
                    &apply_indexes,
                    &pending,
                    &pending_reads,
                    &reclaimed_total,
                    self_id,
                    self_raft,
                    self_client,
                )
                .await;
                persist_raft_state(&host, &persisters);
            }

            // ── incoming raft message ─────────────────────────────────────────
            Some(env) = incoming_rx.recv() => {
                if !is_known_raft_peer(&host, &roster, env.from).await {
                    eprintln!("[server] warning: received Raft message from unrecognized node id={:?}. Message ignored.", env.from);
                    continue;
                }
                let out = host.lock().unwrap().handle(env);
                send_envelopes(out, &*roster.read().await).await;
                drain_and_apply(
                    &host,
                    &engine,
                    &roster,
                    &range_table,
                    &split_rt,
                    &data_dir,
                    &apply_indexes,
                    &pending,
                    &pending_reads,
                    &reclaimed_total,
                    self_id,
                    self_raft,
                    self_client,
                )
                .await;
                persist_raft_state(&host, &persisters);
            }

            // ── client write proposal ─────────────────────────────────────────
            Some(req) = propose_rx.recv() => {
                let gid = GroupId(req.group_id);
                let idx_opt = host.lock().unwrap().propose(gid, req.command);
                match idx_opt {
                    Some(idx) => {
                        pending.lock().unwrap().insert((req.group_id, idx), req.reply_tx);
                        // Immediately replicate the new entry instead of
                        // waiting for the next heartbeat.
                        let out = host.lock().unwrap().broadcast_group(gid);
                        send_envelopes(out, &*roster.read().await).await;
                        drain_and_apply(
                            &host,
                            &engine,
                            &roster,
                            &range_table,
                            &split_rt,
                            &data_dir,
                            &apply_indexes,
                            &pending,
                            &pending_reads,
                            &reclaimed_total,
                            self_id,
                            self_raft,
                            self_client,
                        )
                        .await;
                        persist_raft_state(&host, &persisters);
                    }
                    None => {
                        let _ = req.reply_tx.send(Err("not_leader".to_owned()));
                    }
                }
            }

            // ── client read proposal ─────────────────────────────────────────
            Some(req) = read_propose_rx.recv() => {
                let gid = GroupId(req.group_id);
                let commit_idx_opt = host.lock().unwrap().propose_read_group(gid, req.request_id);
                match commit_idx_opt {
                    Some(_idx) => {
                        pending_reads.lock().unwrap().insert(req.request_id, req.reply_tx);
                        let out = host.lock().unwrap().broadcast_group(gid);
                        send_envelopes(out, &*roster.read().await).await;
                        drain_and_apply(
                            &host,
                            &engine,
                            &roster,
                            &range_table,
                            &split_rt,
                            &data_dir,
                            &apply_indexes,
                            &pending,
                            &pending_reads,
                            &reclaimed_total,
                            self_id,
                            self_raft,
                            self_client,
                        )
                        .await;
                        persist_raft_state(&host, &persisters);
                    }
                    None => {
                        let _ = req.reply_tx.send(Err("not_leader".to_owned()));
                    }
                }
            }
        }

        // Abort pending ops for groups that lost leadership.
        let current_leaders: std::collections::HashSet<u64> = {
            let guard = host.lock().unwrap();
            guard
                .sorted_group_ids()
                .into_iter()
                .filter(|g| guard.is_leader_of(*g))
                .map(|g| g.0)
                .collect()
        };
        let lost: Vec<u64> = leader_groups
            .difference(&current_leaders)
            .copied()
            .collect();
        if !lost.is_empty() {
            {
                let mut pend = pending.lock().unwrap();
                let keys: Vec<_> = pend
                    .keys()
                    .filter(|(gid, _)| lost.contains(gid))
                    .copied()
                    .collect();
                for k in keys {
                    if let Some(tx) = pend.remove(&k) {
                        let _ = tx.send(Err("not_leader".to_owned()));
                    }
                }
            }
            // Read-index requests are not keyed by group; abort all if any group lost leadership.
            // (Read request_ids are unique; orphaned waits are rare on demotion.)
            for (_req_id, tx) in pending_reads.lock().unwrap().drain() {
                let _ = tx.send(Err("not_leader".to_owned()));
            }
        }
        leader_groups = current_leaders;
    }
}

async fn is_known_raft_peer(host: &SharedRaftHost, roster: &SharedRoster, from: NodeId) -> bool {
    if roster.read().await.contains(from) {
        return true;
    }
    host.lock().unwrap().is_voter_anywhere(from)
}
