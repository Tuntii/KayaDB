use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::event::{ProbeEvent, SyscallKind};

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

pub fn write_trace(path: &Path, seed: u64, config_hash: &str, events: &[ProbeEvent]) -> std::io::Result<()> {
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

pub fn replay_validate(path: &Path, expected_seed: u64) -> Result<Vec<ProbeEvent>, TraceReplayError> {
    let file = File::open(path).map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let first = lines
        .next()
        .transpose()
        .map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?
        .ok_or(TraceReplayError::MissingHeader)?;

    let header: TraceHeader = serde_json::from_str(&first)
        .map_err(|e| TraceReplayError::InvalidJson(e.to_string()))?;
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

    if events.iter().all(|e| !e.is_wal_relevant()) {
        return Err(TraceReplayError::NoDurabilityEvents);
    }
    Ok(events)
}

pub fn filter_wal_events(events: &[ProbeEvent]) -> Vec<&ProbeEvent> {
    events.iter().filter(|e| e.is_wal_relevant()).collect()
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
}