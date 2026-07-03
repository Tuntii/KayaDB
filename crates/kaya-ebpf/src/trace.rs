use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::event::{MarkerPhase, MarkerSite, ProbeEvent, PublishSyscallKind, SyscallKind};

/// Header record written once at the top of every trace artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceHeader {
    pub seed: u64,
    pub config_hash: String,
    pub artifact: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TraceReplayError {
    #[error("missing trace header")]
    MissingHeader,
    #[error("expected seed {expected}, got {actual}")]
    SeedMismatch { expected: u64, actual: u64 },
    #[error("event sequence gap: expected {expected}, got {actual}")]
    SequenceGap { expected: u64, actual: u64 },
    #[error("no durability events in trace")]
    NoDurabilityEvents,
    #[error("invalid json line: {0}")]
    InvalidJson(String),
}

pub fn write_trace(
    path: &Path,
    seed: u64,
    config_hash: &str,
    events: &[ProbeEvent],
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    let header = TraceHeader {
        seed,
        config_hash: config_hash.to_owned(),
        artifact: "kaya-ebpf-trace-v1".to_owned(),
    };
    writeln!(file, "{}", serde_json::to_string(&header).unwrap())?;
    for event in events {
        writeln!(file, "{}", serde_json::to_string(event).unwrap())?;
    }
    Ok(())
}

pub fn replay_validate(
    path: &Path,
    expected_seed: u64,
) -> Result<Vec<ProbeEvent>, TraceReplayError> {
    let file = File::open(path).map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let first = lines
        .next()
        .transpose()
        .map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?
        .ok_or(TraceReplayError::MissingHeader)?;

    let header: TraceHeader =
        serde_json::from_str(&first).map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?;
    if header.seed != expected_seed {
        return Err(TraceReplayError::SeedMismatch {
            expected: expected_seed,
            actual: header.seed,
        });
    }

    let mut events = Vec::new();
    let mut expected_seq = 1u64;
    for line in lines {
        let line = line.map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let event: ProbeEvent = serde_json::from_str(&line)
            .map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?;
        if event.seq() != expected_seq {
            return Err(TraceReplayError::SequenceGap {
                expected: expected_seq,
                actual: event.seq(),
            });
        }
        expected_seq += 1;
        events.push(event);
    }

    if events.iter().all(|e| !e.is_durability_event()) {
        return Err(TraceReplayError::NoDurabilityEvents);
    }
    Ok(events)
}

pub fn filter_wal_events(events: &[ProbeEvent]) -> Vec<&ProbeEvent> {
    events.iter().filter(|e| e.is_wal_relevant()).collect()
}

pub fn filter_publish_events(events: &[ProbeEvent]) -> Vec<&ProbeEvent> {
    events.iter().filter(|e| e.is_publish_relevant()).collect()
}

/// Deterministic seeded fsync events for tests and simulated backends.
pub fn seeded_fsync_events(seed: u64, count: usize) -> Vec<ProbeEvent> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut events = Vec::with_capacity(count);
    for seq in 1..=count as u64 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let latency_us = 50 + (state % 900);
        let syscall = if state & 1 == 0 {
            SyscallKind::Fsync
        } else {
            SyscallKind::Fdatasync
        };
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let ts_ns = state.wrapping_mul(1_000);
        events.push(ProbeEvent::FsyncLatency {
            seq,
            syscall,
            latency_us,
            ts_ns,
        });
    }
    events
}

/// Mixed durability fixture: fsync latency + USDT markers + publish syscalls (schema drift tests).
pub fn seeded_mixed_durability_events(seed: u64) -> Vec<ProbeEvent> {
    let fsync = seeded_fsync_events(seed, 2);
    let mut events = Vec::with_capacity(8);
    events.extend(fsync);
    let ts_base = seed.wrapping_mul(1_000);
    events.push(ProbeEvent::UsdtMarker {
        seq: 3,
        site: MarkerSite::WalFsync,
        phase: MarkerPhase::Enter,
        duration_us: None,
        ts_ns: ts_base.wrapping_add(10),
    });
    events.push(ProbeEvent::UsdtMarker {
        seq: 4,
        site: MarkerSite::WalFsync,
        phase: MarkerPhase::Exit,
        duration_us: Some(120),
        ts_ns: ts_base.wrapping_add(11),
    });
    events.push(ProbeEvent::UsdtMarker {
        seq: 5,
        site: MarkerSite::Flush,
        phase: MarkerPhase::Enter,
        duration_us: None,
        ts_ns: ts_base.wrapping_add(20),
    });
    events.push(ProbeEvent::PublishSyscall {
        seq: 6,
        syscall: PublishSyscallKind::Rename,
        latency_us: Some(55),
        ts_ns: ts_base.wrapping_add(21),
    });
    events.push(ProbeEvent::UsdtMarker {
        seq: 7,
        site: MarkerSite::Flush,
        phase: MarkerPhase::Exit,
        duration_us: Some(45_000),
        ts_ns: ts_base.wrapping_add(22),
    });
    events.push(ProbeEvent::PublishSyscall {
        seq: 8,
        syscall: PublishSyscallKind::FsyncDir,
        latency_us: Some(200),
        ts_ns: ts_base.wrapping_add(23),
    });
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn replay_accepts_fixed_seed_fixture_and_rejects_perturbed_sequence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("trace.jsonl");
        let events = seeded_fsync_events(42, 4);
        write_trace(&path, 42, "test-config", &events).unwrap();

        let replayed = replay_validate(&path, 42).unwrap();
        assert_eq!(replayed.len(), 4);

        let perturbed = dir.path().join("bad.jsonl");
        let mut bad = events.clone();
        if let ProbeEvent::FsyncLatency { seq, .. } = &mut bad[2] {
            *seq = 99;
        }
        write_trace(&perturbed, 42, "test-config", &bad).unwrap();
        assert_eq!(
            replay_validate(&perturbed, 42),
            Err(TraceReplayError::SequenceGap {
                expected: 3,
                actual: 99
            })
        );
    }

    #[test]
    fn replay_accepts_mixed_durability_fixture() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed.jsonl");
        let events = seeded_mixed_durability_events(77);
        write_trace(&path, 77, "mixed-fixture", &events).unwrap();
        let replayed = replay_validate(&path, 77).unwrap();
        assert_eq!(replayed.len(), 8);
        assert!(replayed
            .iter()
            .any(|e| matches!(e, ProbeEvent::UsdtMarker { .. })));
        assert!(replayed
            .iter()
            .any(|e| matches!(e, ProbeEvent::PublishSyscall { .. })));
    }

    #[test]
    fn replay_rejects_unknown_event_kind_on_schema_drift() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("drift.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        use std::io::Write;
        writeln!(
            file,
            r#"{{"seed":1,"config_hash":"x","artifact":"kaya-ebpf-trace-v1"}}"#
        )
        .unwrap();
        writeln!(
            file,
            r#"{{"kind":"unknown_future_kind","seq":1,"ts_ns":1}}"#
        )
        .unwrap();
        assert!(matches!(
            replay_validate(&path, 1),
            Err(TraceReplayError::InvalidJson(_))
        ));
    }
}
