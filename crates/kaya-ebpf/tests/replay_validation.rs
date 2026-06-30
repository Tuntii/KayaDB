use std::path::PathBuf;

use kaya_ebpf::{replay_validate, seeded_fsync_events, write_trace, TraceReplayError};
use tempfile::tempdir;

#[test]
fn replay_accepts_fixture_and_rejects_perturbed_seed() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("trace.jsonl");
    let events = seeded_fsync_events(99, 6);
    write_trace(&path, 99, "chaos-fixture", &events).unwrap();

    let ok = replay_validate(&path, 99).unwrap();
    assert_eq!(ok.len(), 6);

    let wrong_seed = replay_validate(&path, 100);
    assert_eq!(
        wrong_seed,
        Err(TraceReplayError::SeedMismatch {
            expected: 100,
            actual: 99
        })
    );
}

#[test]
fn replay_rejects_sequence_gap_in_committed_fixture() {
    let dir = tempdir().unwrap();
    let path: PathBuf = dir.path().join("perturbed.jsonl");
    let mut events = seeded_fsync_events(11, 3);
    let kaya_ebpf::ProbeEvent::FsyncLatency { seq, .. } = &mut events[1];
    *seq = 5;
    write_trace(&path, 11, "perturbed", &events).unwrap();
    assert!(matches!(
        replay_validate(&path, 11),
        Err(TraceReplayError::SequenceGap { .. })
    ));
}