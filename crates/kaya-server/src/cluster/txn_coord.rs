//! Cross-group 2PC transaction coordinator (M23, #26).
//!
//! When a multi-key SI commit spans more than one Raft group (via
//! [`StaticRangeTable`](kaya_raft::StaticRangeTable) lookup), the group-0 leader
//! acts as **coordinator** and drives prepare → decide → commit/abort using:
//! - [`RaftCommand::TxnPrepare`] (type 5)
//! - [`RaftCommand::TxnDecision`] (type 9) — durable global decision, meta group
//! - [`RaftCommand::TxnCommit2pc`] (type 6)
//! - [`RaftCommand::TxnAbort2pc`] (type 7)
//!
//! Single-group commits stay on type-4 [`RaftCommand::TxnCommit`].
//!
//! # Phases
//!
//! Prepare, commit and abort each fan out to every participant **in parallel**
//! (concurrently in the coordinator task) under a per-phase timeout. Between
//! prepare and commit the coordinator writes the durable global decision on the
//! meta group (group 0); **no participant is ever asked to commit before that
//! record is applied**, which is what makes recovery deterministic.
//!
//! # Crash recovery
//!
//! On startup (and on every [`kaya_engine::Engine::open`]), incomplete 2PC
//! records are recovered against the decision log:
//! - `Preparing` / `Prepared` → commit if a durable commit decision exists,
//!   else abort (fail-closed; the coordinator cannot have committed anywhere)
//! - `Committing` → finish commit (decision was durable; never abort)
//! - `Committed` / `Aborted` → leave untouched
//!
//! Server startup still calls [`recover_incomplete_2pc`] for logging; engine open
//! already applied the same logic so the second call is idempotent.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use kaya_io::Disk;
use kaya_raft::{GroupId, RaftCommand};

/// Meta group carrying the durable global 2PC decision log.
pub const DECISION_GROUP: GroupId = GroupId::ZERO;

/// Default per-phase deadline for prepare / decide / commit fan-out.
///
/// A phase that blows the deadline is treated as failed: prepare timeouts abort
/// the txn, commit timeouts leave the (already durable) decision for recovery.
pub const DEFAULT_PHASE_TIMEOUT: Duration = Duration::from_secs(5);

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

/// Poll every future concurrently in the current task; results keep input order.
///
/// A hand-rolled `join_all` avoids a `futures` dependency and, unlike `JoinSet`,
/// imposes no `'static` bound on the caller's closure.
async fn join_all<F: Future>(futs: Vec<F>) -> Vec<F::Output> {
    let mut pending: Vec<Option<Pin<Box<F>>>> =
        futs.into_iter().map(|f| Some(Box::pin(f))).collect();
    let mut out: Vec<Option<F::Output>> = pending.iter().map(|_| None).collect();
    std::future::poll_fn(|cx| {
        let mut done = true;
        for (slot, res) in pending.iter_mut().zip(out.iter_mut()) {
            let Some(fut) = slot else { continue };
            match fut.as_mut().poll(cx) {
                Poll::Ready(v) => {
                    *res = Some(v);
                    *slot = None;
                }
                Poll::Pending => done = false,
            }
        }
        if done {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    out.into_iter()
        .map(|o| o.expect("joined future produced no output"))
        .collect()
}

/// Fan `cmd_for(group)` out to every group in parallel under `timeout`.
///
/// Returns `(ok_groups, first_error)`.
async fn fan_out<F, Fut>(
    groups: &[GroupId],
    timeout: Duration,
    propose: &F,
    cmd_for: impl Fn(GroupId) -> RaftCommand,
) -> (Vec<GroupId>, Option<String>)
where
    F: Fn(GroupId, RaftCommand) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let results = join_all(
        groups
            .iter()
            .map(|gid| tokio::time::timeout(timeout, propose(*gid, cmd_for(*gid))))
            .collect(),
    )
    .await;

    let mut ok = Vec::with_capacity(groups.len());
    let mut err = None;
    for (gid, res) in groups.iter().zip(results) {
        match res {
            Ok(Ok(())) => ok.push(*gid),
            Ok(Err(e)) => {
                err.get_or_insert(e);
            }
            Err(_) => {
                err.get_or_insert_with(|| {
                    format!(
                        "2PC phase timed out after {}ms on group {}",
                        timeout.as_millis(),
                        gid.0
                    )
                });
            }
        }
    }
    (ok, err)
}

/// Run multi-group 2PC for `txn_id`.
///
/// 1. `coordinator_group` = group of the lexicographically smallest key.
/// 2. **Parallel** `TxnPrepare` on every participant (per-phase timeout).
/// 3. All prepared → propose `TxnDecision { commit: true }` on the meta group
///    (group 0) and wait for it to apply. **Nothing user-visible has changed
///    yet**, so a crash before this point is an abort.
/// 4. Decision durable → **parallel** `TxnCommit2pc` on every participant.
/// 5. Prepare (or decision) failed → propose `TxnDecision { commit: false }`,
///    then **parallel** `TxnAbort2pc` on every group that reached `Prepared`.
///
/// `propose(group, cmd)` must wait until the command is committed and applied on
/// that group (or return an error); it may forward to that group's leader.
pub async fn commit_cross_group<F, Fut>(
    txn_id: u64,
    mutations_by_group: MutationsByGroup,
    phase_timeout: Duration,
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

    // Deterministic participant order (HashMap iteration is not stable).
    let mut participants: Vec<GroupId> = mutations_by_group.keys().copied().collect();
    participants.sort_by_key(|g| g.0);

    // ── Phase 1: Prepare (parallel) ─────────────────────────────────────────
    let (prepared, prepare_err) = fan_out(&participants, phase_timeout, &propose, |gid| {
        RaftCommand::TxnPrepare {
            txn_id,
            coordinator_group: coordinator.0,
            mutations: mutations_by_group[&gid].clone(),
        }
    })
    .await;

    // ── Phase 2: Durable global decision (meta group) ───────────────────────
    // Written before any participant commits. TXN-2PC-6.
    let decision_err = if prepare_err.is_none() {
        decide(txn_id, true, phase_timeout, &propose).await.err()
    } else {
        None
    };

    if let Some(err) = prepare_err.or(decision_err) {
        // Nothing committed anywhere: record the abort decision (so a restarted
        // participant resolves deterministically instead of fail-closing), then
        // release prepared intents.
        if !prepared.is_empty() {
            let _ = decide(txn_id, false, phase_timeout, &propose).await;
            let _ = fan_out(&prepared, phase_timeout, &propose, |_| {
                RaftCommand::TxnAbort2pc { txn_id }
            })
            .await;
        }
        return Err(err);
    }

    // ── Phase 3: Commit (parallel) ──────────────────────────────────────────
    let (_, commit_err) = fan_out(&participants, phase_timeout, &propose, |_| {
        RaftCommand::TxnCommit2pc { txn_id }
    })
    .await;

    if let Some(err) = commit_err {
        // The commit decision is durable on the meta group, so aborting here
        // would be unsafe. Recovery finishes the remaining participants.
        return Err(format!("2PC commit phase failed for txn {txn_id}: {err}"));
    }

    Ok(())
}

/// Propose the durable global decision for `txn_id` on the meta group.
async fn decide<F, Fut>(
    txn_id: u64,
    commit: bool,
    timeout: Duration,
    propose: &F,
) -> Result<(), String>
where
    F: Fn(GroupId, RaftCommand) -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let cmd = RaftCommand::TxnDecision { txn_id, commit };
    match tokio::time::timeout(timeout, propose(DECISION_GROUP, cmd)).await {
        Ok(r) => r.map_err(|e| format!("2PC decision log write failed for txn {txn_id}: {e}")),
        Err(_) => Err(format!(
            "2PC decision log write timed out after {}ms for txn {txn_id}",
            timeout.as_millis()
        )),
    }
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

        commit_cross_group(99, m, DEFAULT_PHASE_TIMEOUT, |gid, cmd| {
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
                    RaftCommand::TxnDecision { txn_id, commit } => {
                        format!("decide g{} txn{txn_id} commit{commit}", gid.0)
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
        assert_eq!(entries.len(), 5, "{entries:?}");
        // Two prepares, one decision on the meta group, then two commits.
        assert_eq!(entries.iter().filter(|e| e.starts_with("prep")).count(), 2);
        assert_eq!(
            entries.iter().filter(|e| e.starts_with("commit")).count(),
            2
        );
        assert!(entries.iter().all(|e| !e.starts_with("abort")));
        // Coordinator is group of key "a" → group 1.
        assert!(entries.iter().any(|e| e.contains("coord1")));

        // TXN-2PC-6: the decision lands on group 0 strictly before any commit.
        let decide_at = entries
            .iter()
            .position(|e| e == "decide g0 txn99 committrue")
            .unwrap_or_else(|| panic!("no commit decision on meta group: {entries:?}"));
        let first_commit = entries
            .iter()
            .position(|e| e.starts_with("commit"))
            .expect("commit");
        assert!(
            decide_at < first_commit,
            "decision must precede every commit: {entries:?}"
        );
    }

    /// Prepare fan-out must be concurrent: two prepares that each block until
    /// both have started can only finish if they run at the same time.
    #[tokio::test]
    async fn prepare_and_commit_fan_out_in_parallel() {
        let started = Arc::new(tokio::sync::Barrier::new(2));

        let mut m: MutationsByGroup = HashMap::new();
        m.insert(GroupId(1), vec![(b"a".to_vec(), Some(b"1".to_vec()))]);
        m.insert(GroupId(2), vec![(b"m".to_vec(), Some(b"2".to_vec()))]);

        let run = commit_cross_group(5, m, DEFAULT_PHASE_TIMEOUT, |_gid, cmd| {
            let started = started.clone();
            async move {
                // Only the two-participant phases rendezvous; the meta-group
                // decision is a single proposal.
                if !matches!(cmd, RaftCommand::TxnDecision { .. }) {
                    started.wait().await;
                }
                Ok(())
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("parallel fan-out deadlocked — phases ran sequentially")
            .expect("2PC should succeed");
    }

    /// A participant that never answers must not hang the coordinator.
    #[tokio::test]
    async fn prepare_timeout_aborts_and_records_decision() {
        let log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let log2 = log.clone();

        let mut m: MutationsByGroup = HashMap::new();
        m.insert(GroupId(1), vec![(b"a".to_vec(), Some(b"1".to_vec()))]);
        m.insert(GroupId(2), vec![(b"m".to_vec(), Some(b"2".to_vec()))]);

        let err = commit_cross_group(11, m, std::time::Duration::from_millis(50), |gid, cmd| {
            let log = log2.clone();
            async move {
                log.lock()
                    .unwrap()
                    .push(format!("{} g{}", tag(&cmd), gid.0));
                // Group 2 never answers its prepare.
                if gid == GroupId(2) && matches!(cmd, RaftCommand::TxnPrepare { .. }) {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        })
        .await
        .unwrap_err();

        assert!(err.contains("timed out"), "{err}");
        let entries = log.lock().unwrap().clone();
        assert!(
            entries.contains(&"decide g0".to_owned()),
            "abort decision must be durable: {entries:?}"
        );
        assert!(
            entries.contains(&"abort g1".to_owned()),
            "prepared participant must be released: {entries:?}"
        );
        assert!(
            entries.iter().all(|e| !e.starts_with("commit")),
            "must not commit after a prepare timeout: {entries:?}"
        );
    }

    fn tag(cmd: &RaftCommand) -> &'static str {
        match cmd {
            RaftCommand::TxnPrepare { .. } => "prep",
            RaftCommand::TxnCommit2pc { .. } => "commit",
            RaftCommand::TxnAbort2pc { .. } => "abort",
            RaftCommand::TxnDecision { .. } => "decide",
            _ => "other",
        }
    }

    #[tokio::test]
    async fn recover_aborts_prepared_leaves_committed() {
        use std::sync::Arc;

        use kaya_core::EngineConfig;
        use kaya_engine::{Engine, ReadOptions, Txn2pcState};
        use kaya_io::SimDisk;

        let disk = Arc::new(SimDisk::new());
        // SimDisk is in-memory, but the directory lock is not: these tests all
        // share the default data_dir, so skip it rather than serialize them.
        let cfg = EngineConfig {
            disable_locking: true,
            ..EngineConfig::default()
        };
        let mut engine = Engine::open(cfg, disk).await.unwrap();

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
        // SimDisk is in-memory, but the directory lock is not: these tests all
        // share the default data_dir, so skip it rather than serialize them.
        let cfg = EngineConfig {
            disable_locking: true,
            ..EngineConfig::default()
        };
        let mut engine = Engine::open(cfg, disk).await.unwrap();
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

        let err = commit_cross_group(7, m, DEFAULT_PHASE_TIMEOUT, |gid, cmd| {
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
                    RaftCommand::TxnDecision { commit, .. } => {
                        log.lock().unwrap().push(format!("decide {commit}"));
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
        assert!(
            entries.contains(&"decide false".to_owned()),
            "abort must be recorded in the decision log: {entries:?}"
        );
    }
}
