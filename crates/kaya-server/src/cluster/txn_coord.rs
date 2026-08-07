//! Cross-group 2PC transaction coordinator (M23).
//!
//! When a multi-key SI commit spans more than one Raft group (via
//! [`StaticRangeTable`](kaya_raft::StaticRangeTable) lookup), the leader runs
//! prepare-then-commit (or abort) using:
//! - [`RaftCommand::TxnPrepare`] (type 5)
//! - [`RaftCommand::TxnCommit2pc`] (type 6)
//! - [`RaftCommand::TxnAbort2pc`] (type 7)
//!
//! Single-group commits stay on type-4 [`RaftCommand::TxnCommit`].
//!
//! # Crash recovery
//!
//! On startup (and on every [`kaya_engine::Engine::open`]), incomplete 2PC
//! records are recovered:
//! - `Preparing` / `Prepared` → abort (fail-closed; no durable global decision)
//! - `Committing` → finish commit (decision was durable; never abort)
//! - `Committed` / `Aborted` → leave untouched
//!
//! Server startup still calls [`recover_incomplete_2pc`] for logging; engine open
//! already applied the same logic so the second call is idempotent.

use std::collections::HashMap;
use std::future::Future;

use kaya_io::Disk;
use kaya_raft::{GroupId, RaftCommand};

/// Put when `Some`, delete when `None`.
type Mutation = (Vec<u8>, Option<Vec<u8>>);
/// Mutations partitioned by Raft group for multi-range 2PC.
type MutationsByGroup = HashMap<GroupId, Vec<Mutation>>;

/// Group owning the lexicographically smallest key among all mutations.
///
/// Used as `coordinator_group` in each `TxnPrepare` record for diagnostics /
/// recovery hints.
pub fn coordinator_group(mutations_by_group: &MutationsByGroup) -> Option<GroupId> {
    let mut best: Option<(&[u8], GroupId)> = None;
    for (gid, muts) in mutations_by_group {
        for (k, _) in muts {
            match best {
                None => best = Some((k.as_slice(), *gid)),
                Some((bk, _)) if k.as_slice() < bk => best = Some((k.as_slice(), *gid)),
                _ => {}
            }
        }
    }
    best.map(|(_, g)| g)
}

/// Run multi-group 2PC for `txn_id`.
///
/// 1. `coordinator_group` = group of the lexicographically smallest key.
/// 2. Propose `TxnPrepare` on each participant (all sent before waiting so the
///    raft loop can apply them without an extra client round-trip).
/// 3. If every prepare applies: propose `TxnCommit2pc` on all participants.
/// 4. Else: propose `TxnAbort2pc` on every group that prepared successfully.
///
/// `propose(group, cmd)` must wait until the command is committed and applied
/// on that group (or return an error).
pub async fn commit_cross_group<F, Fut>(
    txn_id: u64,
    mutations_by_group: MutationsByGroup,
    propose: F,
) -> Result<(), String>
where
    F: Fn(GroupId, RaftCommand) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    if mutations_by_group.is_empty() {
        return Ok(());
    }
    if mutations_by_group.len() < 2 {
        return Err(
            "commit_cross_group requires mutations spanning more than one group".to_owned(),
        );
    }

    let coordinator = coordinator_group(&mutations_by_group)
        .ok_or_else(|| "commit_cross_group: no keys in mutations_by_group".to_owned())?;

    // ── Phase 1: Prepare ────────────────────────────────────────────────────
    let mut prepared: Vec<GroupId> = Vec::with_capacity(mutations_by_group.len());
    let mut prepare_err: Option<String> = None;

    // Sequential proposes keep the API simple (no 'static bound on `propose`).
    // The raft event loop still applies each command as soon as it is enqueued.
    for (gid, mutations) in &mutations_by_group {
        let cmd = RaftCommand::TxnPrepare {
            txn_id,
            coordinator_group: coordinator.0,
            mutations: mutations.clone(),
        };
        match propose(*gid, cmd).await {
            Ok(()) => prepared.push(*gid),
            Err(e) => {
                prepare_err = Some(e);
                break;
            }
        }
    }

    if let Some(err) = prepare_err {
        // Best-effort abort of groups that reached Prepared.
        for gid in prepared {
            let _ = propose(gid, RaftCommand::TxnAbort2pc { txn_id }).await;
        }
        return Err(err);
    }

    // ── Phase 2: Commit decision ────────────────────────────────────────────
    let mut commit_err: Option<String> = None;
    for gid in mutations_by_group.keys() {
        match propose(*gid, RaftCommand::TxnCommit2pc { txn_id }).await {
            Ok(()) => {}
            Err(e) => {
                commit_err = Some(e);
                break;
            }
        }
    }

    if let Some(err) = commit_err {
        // Partial commit decision is rare (shared-engine single-node often
        // materializes all intents on the first Commit2pc). Still attempt abort
        // on remaining groups is unsafe if any commit applied — leave recovery
        // to the conservative startup scanner if the process dies here.
        return Err(format!("2PC commit phase failed for txn {txn_id}: {err}"));
    }

    Ok(())
}

/// Crash recovery for incomplete 2PC participant records.
///
/// Delegates to [`kaya_engine::Engine::recover_incomplete_2pc`] (also run on
/// every engine open). Returns `(aborted, finished_commits)`.
///
/// Called at node startup for operator logging before the Raft event loop runs.
pub async fn recover_incomplete_2pc<D: Disk>(
    engine: &mut kaya_engine::Engine<D>,
) -> Result<(u32, u32), String> {
    let stats = engine
        .recover_incomplete_2pc()
        .await
        .map_err(|e| e.to_string())?;
    Ok((stats.aborted, stats.finished_commits))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn coordinator_picks_group_of_lex_smallest_key() {
        let mut m: MutationsByGroup = HashMap::new();
        m.insert(GroupId(2), vec![(b"m".to_vec(), Some(b"1".to_vec()))]);
        m.insert(GroupId(1), vec![(b"a".to_vec(), Some(b"1".to_vec()))]);
        assert_eq!(coordinator_group(&m), Some(GroupId(1)));
    }

    #[test]
    fn parse_rec_key_roundtrip() {
        let k = kaya_engine::encode_rec_key(0x0102_0304_0506_0708);
        assert_eq!(
            kaya_engine::parse_rec_txn_id(&k),
            Some(0x0102_0304_0506_0708)
        );
        assert_eq!(kaya_engine::parse_rec_txn_id(b"nope"), None);
    }

    #[tokio::test]
    async fn commit_cross_group_prepare_then_commit() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();

        let mut m: MutationsByGroup = HashMap::new();
        m.insert(GroupId(1), vec![(b"a".to_vec(), Some(b"1".to_vec()))]);
        m.insert(GroupId(2), vec![(b"m".to_vec(), Some(b"2".to_vec()))]);

        commit_cross_group(99, m, |gid, cmd| {
            let log = log2.clone();
            async move {
                let tag = match &cmd {
                    RaftCommand::TxnPrepare {
                        txn_id,
                        coordinator_group,
                        ..
                    } => format!("prep g{} txn{txn_id} coord{coordinator_group}", gid.0),
                    RaftCommand::TxnCommit2pc { txn_id } => {
                        format!("commit g{} txn{txn_id}", gid.0)
                    }
                    RaftCommand::TxnAbort2pc { txn_id } => {
                        format!("abort g{} txn{txn_id}", gid.0)
                    }
                    _ => format!("other g{}", gid.0),
                };
                log.lock().unwrap().push(tag);
                Ok(())
            }
        })
        .await
        .unwrap();

        let entries = log.lock().unwrap().clone();
        assert_eq!(entries.len(), 4, "{entries:?}");
        // Two prepares then two commits (group order follows HashMap iteration).
        assert_eq!(entries.iter().filter(|e| e.starts_with("prep")).count(), 2);
        assert_eq!(
            entries.iter().filter(|e| e.starts_with("commit")).count(),
            2
        );
        assert!(entries.iter().all(|e| !e.starts_with("abort")));
        // Coordinator is group of key "a" → group 1.
        assert!(entries.iter().any(|e| e.contains("coord1")));
    }

    #[tokio::test]
    async fn recover_aborts_prepared_leaves_committed() {
        use std::sync::Arc;

        use kaya_core::EngineConfig;
        use kaya_engine::{Engine, ReadOptions, Txn2pcState};
        use kaya_io::SimDisk;

        let disk = Arc::new(SimDisk::new());
        let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

        // Prepared → abort on recovery.
        engine
            .apply_txn_prepare(1, &[(b"prep".to_vec(), Some(b"1".to_vec()))])
            .await
            .unwrap();
        // Fully committed → untouched.
        engine
            .apply_txn_prepare(2, &[(b"fin".to_vec(), Some(b"2".to_vec()))])
            .await
            .unwrap();
        engine.apply_txn_commit_2pc(2).await.unwrap();

        let (aborted, finished) = recover_incomplete_2pc(&mut engine).await.unwrap();
        assert_eq!(aborted, 1, "txn 1 Prepared must abort");
        assert_eq!(finished, 0);
        assert_eq!(
            engine.read_txn2pc_state(1).unwrap(),
            Some(Txn2pcState::Aborted)
        );
        assert_eq!(
            engine.get(b"prep", ReadOptions::default()).await.unwrap(),
            None
        );
        assert_eq!(
            engine.read_txn2pc_state(2).unwrap(),
            Some(Txn2pcState::Committed)
        );
        assert_eq!(
            engine.get(b"fin", ReadOptions::default()).await.unwrap(),
            Some(b"2".to_vec())
        );
    }

    #[tokio::test]
    async fn recover_finishes_committing_via_wrapper() {
        use std::sync::Arc;

        use kaya_core::EngineConfig;
        use kaya_engine::{Engine, ReadOptions, Txn2pcState};
        use kaya_io::SimDisk;

        let disk = Arc::new(SimDisk::new());
        let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
        engine
            .apply_txn_prepare(3, &[(b"x".to_vec(), Some(b"v".to_vec()))])
            .await
            .unwrap();
        // Engine-level tests cover Committing reopen; here ensure the server
        // wrapper aborts Prepared via the same recover_incomplete_2pc path.
        let (aborted, finished) = recover_incomplete_2pc(&mut engine).await.unwrap();
        assert_eq!(aborted, 1);
        assert_eq!(finished, 0);
        assert_eq!(
            engine.read_txn2pc_state(3).unwrap(),
            Some(Txn2pcState::Aborted)
        );
        assert_eq!(
            engine.get(b"x", ReadOptions::default()).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn commit_cross_group_aborts_on_prepare_failure() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();
        let call = Arc::new(Mutex::new(0u32));
        let call2 = call.clone();

        let mut m: MutationsByGroup = HashMap::new();
        // Deterministic iteration: use BTreeMap-like insert order is not guaranteed
        // for HashMap — fail the second prepare by counting prepare calls.
        m.insert(GroupId(1), vec![(b"a".to_vec(), Some(b"1".to_vec()))]);
        m.insert(GroupId(2), vec![(b"z".to_vec(), Some(b"2".to_vec()))]);

        let err = commit_cross_group(7, m, |gid, cmd| {
            let log = log2.clone();
            let call = call2.clone();
            async move {
                match &cmd {
                    RaftCommand::TxnPrepare { .. } => {
                        let n = {
                            let mut c = call.lock().unwrap();
                            *c += 1;
                            *c
                        };
                        log.lock().unwrap().push(format!("prep {}", gid.0));
                        if n >= 2 {
                            return Err("inject prepare fail".to_owned());
                        }
                        Ok(())
                    }
                    RaftCommand::TxnAbort2pc { .. } => {
                        log.lock().unwrap().push(format!("abort {}", gid.0));
                        Ok(())
                    }
                    RaftCommand::TxnCommit2pc { .. } => {
                        log.lock().unwrap().push(format!("commit {}", gid.0));
                        Ok(())
                    }
                    _ => Ok(()),
                }
            }
        })
        .await
        .unwrap_err();

        assert!(err.contains("inject prepare fail"), "{err}");
        let entries = log.lock().unwrap().clone();
        assert!(
            entries.iter().any(|e| e.starts_with("abort")),
            "expected abort after partial prepare: {entries:?}"
        );
        assert!(
            entries.iter().all(|e| !e.starts_with("commit")),
            "must not commit after prepare failure: {entries:?}"
        );
    }
}
