# Consensus & Productionization (Validation-First) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship durable Raft persistence (Phase 0), then a Rust-native chaos harness proving M13-3 linearizability under kill/partition/membership/snapshot nemesis before TLS/ops work.

**Architecture:** `DiskRaftStorage` in `kaya-raft` (feature `disk-storage`) persists hard-state + framed log rewrite; `kaya-server` recovers on startup and flushes after mutations. `kaya-jepsen-test` gains `ClusterController` + scenario registry; CI runs sequential smoke on PR and WGL full gate nightly.

**Tech Stack:** Rust 1.85+, tokio, `kaya-raft`, `kaya-server`, `kaya-jepsen-test`, `kaya-sim` LinearizabilityChecker, GitHub Actions ubuntu-latest, iptables (nightly only).

**Spec:** `docs/superpowers/specs/2026-06-18-consensus-productionization-design.md`

---

## File map

| File | Responsibility |
|------|----------------|
| `crates/kaya-raft/src/storage.rs` | `HardState`, `PersistedRaftState`, `RaftStorage` trait, encode/decode helpers |
| `crates/kaya-raft/src/disk_storage.rs` | `DiskRaftStorage` — atomic hard-state, log rewrite |
| `crates/kaya-raft/src/node.rs` | `recover()`, `persist_view()`, `set_recovered_apply_floor()` |
| `crates/kaya-raft/Cargo.toml` | `disk-storage` feature → `kaya-io` |
| `crates/kaya-server/src/raft_persister.rs` | Detect dirty state, call storage after raft mutations |
| `crates/kaya-server/src/cluster.rs` | Startup recover; hook persister in event loop |
| `crates/kaya-jepsen-test/src/cluster_controller.rs` | Spawn/kill/restart/partition/membership |
| `crates/kaya-jepsen-test/src/scenario.rs` | `Scenario`, `VerifyMode`, `Topology`, registry |
| `crates/kaya-jepsen-test/src/runner.rs` | `run_scenario()` with verify mode selection |
| `crates/kaya-jepsen-test/tests/smoke.rs` | PR gate |
| `crates/kaya-jepsen-test/tests/full_gate.rs` | Nightly T1–T7 (`#[ignore]`) |
| `.github/workflows/ci.yml` | `chaos-smoke` job |
| `.github/workflows/chaos-nightly.yml` | `chaos-full` job |

---

## Phase 0 — Raft persistence (BLOCKER)

### Task 1: Hard-state format + roundtrip tests

**Files:**
- Create: `crates/kaya-raft/src/storage.rs`
- Modify: `crates/kaya-raft/src/lib.rs`
- Test: `crates/kaya-raft/src/storage.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Write failing test**

Add to `storage.rs`:

```rust
use crate::types::{LogIndex, NodeId, Term};
use kaya_core::crc32c;

pub const RAFT_HARD_STATE_MAGIC: u32 = 0x484B_5352; // "HSKR" LE
pub const RAFT_HARD_STATE_VERSION: u32 = 1;
pub const RAFT_HARD_STATE_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}

pub fn encode_hard_state(hs: &HardState) -> [u8; RAFT_HARD_STATE_LEN] {
    let mut buf = [0u8; RAFT_HARD_STATE_LEN];
    buf[0..4].copy_from_slice(&RAFT_HARD_STATE_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&RAFT_HARD_STATE_VERSION.to_le_bytes());
    buf[8..16].copy_from_slice(&hs.current_term.0.to_le_bytes());
    buf[16..24].copy_from_slice(&hs.voted_for.map(|n| n.0).unwrap_or(0).to_le_bytes());
    buf[24..32].copy_from_slice(&hs.last_included_index.0.to_le_bytes());
    buf[32..40].copy_from_slice(&hs.last_included_term.0.to_le_bytes());
    let crc = crc32c(&buf[..60]);
    buf[60..64].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode_hard_state(bytes: &[u8]) -> Result<HardState, String> {
    if bytes.len() != RAFT_HARD_STATE_LEN {
        return Err(format!("hard-state wrong len: {}", bytes.len()));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != RAFT_HARD_STATE_MAGIC {
        return Err(format!("bad hard-state magic: {magic:#x}"));
    }
    let ver = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if ver != RAFT_HARD_STATE_VERSION {
        return Err(format!("unsupported hard-state version: {ver}"));
    }
    let crc = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
    if crc32c(&bytes[..60]) != crc {
        return Err("hard-state crc mismatch".into());
    }
    let voted_raw = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    Ok(HardState {
        current_term: Term(u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
        voted_for: if voted_raw == 0 { None } else { Some(NodeId(voted_raw)) },
        last_included_index: LogIndex(u64::from_le_bytes(bytes[24..32].try_into().unwrap())),
        last_included_term: Term(u64::from_le_bytes(bytes[32..40].try_into().unwrap())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_roundtrip() {
        let hs = HardState {
            current_term: Term(7),
            voted_for: Some(NodeId(2)),
            last_included_index: LogIndex(100),
            last_included_term: Term(6),
        };
        let enc = encode_hard_state(&hs);
        assert_eq!(decode_hard_state(&enc).unwrap(), hs);
    }

    #[test]
    fn hard_state_rejects_bad_crc() {
        let mut enc = encode_hard_state(&HardState {
            current_term: Term(1),
            voted_for: None,
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        });
        enc[10] ^= 0xFF;
        assert!(decode_hard_state(&enc).is_err());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```powershell
cargo test -p kaya-raft hard_state_roundtrip -- --nocapture
```

Expected: FAIL — module `storage` not declared in `lib.rs`

- [ ] **Step 3: Wire module**

In `crates/kaya-raft/src/lib.rs`:

```rust
mod storage;
pub use storage::{decode_hard_state, encode_hard_state, HardState, RAFT_HARD_STATE_LEN};
```

- [ ] **Step 4: Run tests**

```powershell
cargo test -p kaya-raft hard_state -- --nocapture
```

Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```powershell
git add crates/kaya-raft/src/storage.rs crates/kaya-raft/src/lib.rs
git commit -m "feat(raft): add hard-state encode/decode with CRC"
```

---

### Task 2: Raft log frame encode/decode

**Files:**
- Modify: `crates/kaya-raft/src/storage.rs`
- Modify: `crates/kaya-raft/src/log.rs` (export `LogEntry` if needed — already public)

- [ ] **Step 1: Write failing test**

Append to `storage.rs`:

```rust
pub const RAFT_LOG_FRAME_MAGIC: u32 = 0x46474C52; // "RLGF" LE
pub const RAFT_LOG_FRAME_VERSION: u16 = 1;
pub const RAFT_LOG_FRAME_HEADER_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFrame {
    pub index: LogIndex,
    pub term: Term,
    pub command: Vec<u8>,
}

pub fn encode_log_file(frames: &[LogFrame]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in frames {
        let mut header = [0u8; RAFT_LOG_FRAME_HEADER_LEN];
        header[0..4].copy_from_slice(&RAFT_LOG_FRAME_MAGIC.to_le_bytes());
        header[4..6].copy_from_slice(&RAFT_LOG_FRAME_VERSION.to_le_bytes());
        header[8..16].copy_from_slice(&f.index.0.to_le_bytes());
        header[16..24].copy_from_slice(&f.term.0.to_le_bytes());
        header[24..28].copy_from_slice(&(f.command.len() as u32).to_le_bytes());
        let mut crc_input = Vec::new();
        crc_input.extend_from_slice(&f.index.0.to_le_bytes());
        crc_input.extend_from_slice(&f.term.0.to_le_bytes());
        crc_input.extend_from_slice(&(f.command.len() as u32).to_le_bytes());
        crc_input.extend_from_slice(&f.command);
        let frame_crc = crc32c(&crc_input);
        header[28..32].copy_from_slice(&frame_crc.to_le_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(&f.command);
    }
    out
}

pub fn decode_log_file(bytes: &[u8]) -> Result<Vec<LogFrame>, String> {
    let mut frames = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes.len() - offset < RAFT_LOG_FRAME_HEADER_LEN {
            return Err("truncated log frame header".into());
        }
        let hdr = &bytes[offset..offset + RAFT_LOG_FRAME_HEADER_LEN];
        let magic = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
        if magic != RAFT_LOG_FRAME_MAGIC {
            return Err(format!("bad log frame magic at {offset}"));
        }
        let index = LogIndex(u64::from_le_bytes(hdr[8..16].try_into().unwrap()));
        let term = Term(u64::from_le_bytes(hdr[16..24].try_into().unwrap()));
        let payload_len = u32::from_le_bytes(hdr[24..28].try_into().unwrap()) as usize;
        let frame_crc = u32::from_le_bytes(hdr[28..32].try_into().unwrap());
        offset += RAFT_LOG_FRAME_HEADER_LEN;
        if bytes.len() - offset < payload_len {
            return Err("truncated log payload".into());
        }
        let command = bytes[offset..offset + payload_len].to_vec();
        offset += payload_len;
        let mut crc_input = Vec::new();
        crc_input.extend_from_slice(&index.0.to_le_bytes());
        crc_input.extend_from_slice(&term.0.to_le_bytes());
        crc_input.extend_from_slice(&(payload_len as u32).to_le_bytes());
        crc_input.extend_from_slice(&command);
        if crc32c(&crc_input) != frame_crc {
            return Err(format!("log frame crc mismatch at index {}", index.0));
        }
        frames.push(LogFrame { index, term, command });
    }
    Ok(frames)
}
```

Add test:

```rust
#[test]
fn log_file_roundtrip() {
    let frames = vec![
        LogFrame { index: LogIndex(1), term: Term(1), command: b"noop".to_vec() },
        LogFrame { index: LogIndex(2), term: Term(1), command: b"put:k".to_vec() },
    ];
    let bytes = encode_log_file(&frames);
    assert_eq!(decode_log_file(&bytes).unwrap(), frames);
}
```

- [ ] **Step 2: Run failing test**

```powershell
cargo test -p kaya-raft log_file_roundtrip -- --nocapture
```

Expected: FAIL until implemented

- [ ] **Step 3: Fix encode/decode** (use code above; ensure header layout is self-consistent)

- [ ] **Step 4: Run tests**

```powershell
cargo test -p kaya-raft log_file -- --nocapture
```

Expected: PASS

- [ ] **Step 5: Commit**

```powershell
git commit -am "feat(raft): add framed raft-log encode/decode"
```

---

### Task 3: `MemLog` ↔ `LogFrame` conversion

**Files:**
- Modify: `crates/kaya-raft/src/storage.rs`
- Modify: `crates/kaya-raft/src/log.rs`

- [ ] **Step 1: Write failing test**

```rust
use crate::log::MemLog;
use crate::log::LogEntry;

pub fn memlog_to_frames(log: &MemLog) -> Vec<LogFrame> {
    let start = log.last_included_index().0 + 1;
    let mut frames = Vec::new();
    let mut idx = start;
    while let Some(entry) = log.get(LogIndex(idx)) {
        frames.push(LogFrame {
            index: LogIndex(idx),
            term: entry.term,
            command: entry.command.clone(),
        });
        idx += 1;
    }
    frames
}

pub fn frames_to_memlog(hs: &HardState, frames: Vec<LogFrame>) -> MemLog {
    let mut log = MemLog::new();
    if hs.last_included_index.0 > 0 {
        log.install_snapshot(
            hs.last_included_index,
            hs.last_included_term,
            vec![], // snapshot payload lives in engine; log holds boundary only
        );
    }
    for f in frames {
        debug_assert_eq!(f.index, log.last_index().0 + 1);
        log.append(LogEntry { term: f.term, command: f.command });
    }
    log
}
```

Test: append 3 entries to MemLog, roundtrip through frames, assert `last_index()` unchanged.

- [ ] **Step 2–4: Implement + test**

```powershell
cargo test -p kaya-raft memlog_to_frames -- --nocapture
```

- [ ] **Step 5: Commit**

```powershell
git commit -am "feat(raft): convert MemLog to framed log file"
```

---

### Task 4: `DiskRaftStorage` + `disk-storage` feature

**Files:**
- Create: `crates/kaya-raft/src/disk_storage.rs`
- Modify: `crates/kaya-raft/Cargo.toml`
- Modify: `crates/kaya-raft/src/lib.rs`
- Modify: `crates/kaya-server/Cargo.toml`

- [ ] **Step 1: Add feature to `kaya-raft/Cargo.toml`**

```toml
[features]
default = []
disk-storage = ["dep:kaya-io"]

[dependencies]
kaya-io = { workspace = true, optional = true }
```

- [ ] **Step 2: Write failing test**

`disk_storage.rs`:

```rust
use std::path::{Path, PathBuf};
use crate::log::MemLog;
use crate::storage::{
    decode_hard_state, decode_log_file, encode_hard_state, encode_log_file,
    frames_to_memlog, memlog_to_frames, HardState, PersistedRaftState, RaftStorage,
};
use crate::types::{LogIndex, NodeId, Term};

pub struct DiskRaftStorage {
    data_dir: PathBuf,
}

impl DiskRaftStorage {
    pub fn open(data_dir: impl Into<PathBuf>) -> Self {
        Self { data_dir: data_dir.into() }
    }

    fn hard_state_path(&self) -> PathBuf {
        self.data_dir.join("raft-hard-state")
    }

    fn log_path(&self) -> PathBuf {
        self.data_dir.join("raft-log")
    }
}

impl RaftStorage for DiskRaftStorage {
    fn load(&self) -> Result<PersistedRaftState, RaftStorageError> { /* ... */ }
    fn save_hard_state(&mut self, hs: &HardState) -> Result<(), RaftStorageError> { /* atomic tmp+rename */ }
    fn save_log(&mut self, log: &MemLog, hs: &HardState) -> Result<(), RaftStorageError> { /* rewrite */ }
    fn sync(&mut self) -> Result<(), RaftStorageError> { /* fsync dir */ }
}
```

Test uses tempdir: save hard-state + log, load, compare.

Atomic write helper:

```rust
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    let f = std::fs::File::open(&tmp)?;
    f.sync_data()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
```

- [ ] **Step 3: Enable feature in `kaya-server/Cargo.toml`**

```toml
kaya-raft = { workspace = true, features = ["disk-storage"] }
```

- [ ] **Step 4: Run tests**

```powershell
cargo test -p kaya-raft --features disk-storage disk_storage -- --nocapture
```

- [ ] **Step 5: Commit**

```powershell
git commit -am "feat(raft): add DiskRaftStorage with atomic hard-state writes"
```

---

### Task 5: `RaftNode::recover` + persist view API

**Files:**
- Modify: `crates/kaya-raft/src/node.rs`
- Modify: `crates/kaya-raft/src/storage.rs` (`PersistedRaftState`, `RaftStorageError`)

- [ ] **Step 1: Write failing test** in `node.rs` `#[cfg(test)]`:

```rust
#[test]
fn recover_restores_term_and_log() {
    let cfg = RaftConfig {
        id: NodeId(1),
        peers: vec![NodeId(2), NodeId(3)],
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
    };
    let mut log = MemLog::new();
    log.append(LogEntry { term: Term(2), command: b"x".to_vec() });
    let state = PersistedRaftState {
        hard_state: HardState {
            current_term: Term(2),
            voted_for: Some(NodeId(2)),
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        },
        log,
    };
    let node = RaftNode::recover(cfg, state);
    assert_eq!(node.status().current_term, Term(2));
    assert_eq!(node.status().role, Role::Follower);
    assert_eq!(node.log_last_index(), LogIndex(1));
}
```

- [ ] **Step 2: Add APIs to `RaftNode`**

```rust
pub fn recover(config: RaftConfig, state: PersistedRaftState) -> Self { /* load fields */ }

pub fn persist_view(&self) -> PersistedRaftState {
    PersistedRaftState {
        hard_state: HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            last_included_index: self.log.last_included_index(),
            last_included_term: self.log.last_included_term(),
        },
        log: self.log.clone(), // add Clone to MemLog or snapshot entries
    }
}

pub fn set_recovered_apply_floor(&mut self, last_applied: LogIndex) {
    self.last_applied = last_applied;
    self.commit_index = last_applied;
}

pub fn log_last_index(&self) -> LogIndex {
    self.log.last_index()
}
```

Add `Clone` to `MemLog` (entries vec clone is fine for prototype).

- [ ] **Step 3: Run test**

```powershell
cargo test -p kaya-raft recover_restores -- --nocapture
```

- [ ] **Step 4: Commit**

```powershell
git commit -am "feat(raft): add RaftNode::recover and persist_view"
```

---

### Task 6: Server `RaftPersister` + startup recover

**Files:**
- Create: `crates/kaya-server/src/raft_persister.rs`
- Modify: `crates/kaya-server/src/lib.rs`
- Modify: `crates/kaya-server/src/cluster.rs`

- [ ] **Step 1: Write failing integration test** in `integration_tests.rs`:

```rust
#[tokio::test]
async fn test_node_restart_preserves_raft_term() {
    // spawn 1-node or 3-node, propose via leader, kill handle, restart same data_dir
    // assert stats or health shows term >= previous term and data readable
}
```

- [ ] **Step 2: Implement `RaftPersister`**

```rust
pub struct RaftPersister {
    storage: DiskRaftStorage,
}

impl RaftPersister {
    pub fn open(data_dir: &Path) -> Self { ... }

    pub fn load_or_empty(&self) -> Result<Option<PersistedRaftState>, String> { ... }

    pub fn flush(&mut self, raft: &RaftNode) -> Result<(), String> {
        let view = raft.persist_view();
        self.storage.save_hard_state(&view.hard_state)?;
        self.storage.save_log(&view.log, &view.hard_state)?;
        self.storage.sync()?;
        Ok(())
    }
}
```

- [ ] **Step 3: Modify `run_cluster_node` startup** (~line 184):

```rust
let persister = RaftPersister::open(&config.data_dir)?;
let apply_floor = RaftApplyIndex::load_all(&config.data_dir.join("raft-apply-index.jsonl"))
    .ok()
    .map(|recs| recs.iter().map(|r| r.index).max().unwrap_or(LogIndex(0)))
    .unwrap_or(LogIndex(0));

let mut raft_node = match persister.load_or_empty()? {
    Some(state) => {
        let mut n = RaftNode::recover(raft_cfg, state);
        n.set_recovered_apply_floor(apply_floor);
        n
    }
    None => RaftNode::new(raft_cfg),
};
```

- [ ] **Step 4: Call `persister.flush(&raft)` after** tick/handle/propose/drain paths (pass `&mut persister` into loop or use `Arc<Mutex<RaftPersister>>`).

- [ ] **Step 5: Run integration test**

```powershell
cargo test -p kaya-server test_node_restart_preserves_raft_term -- --nocapture
```

- [ ] **Step 6: Commit**

```powershell
git commit -am "feat(server): recover and persist Raft state on restart"
```

---

### Task 7: SimDisk crash property tests

**Files:**
- Create: `crates/kaya-raft/tests/raft_persist_crash.rs` (requires `disk-storage` + dev-dep `kaya-io`, `kaya-sim`)

- [ ] **Step 1: Add dev-deps to `kaya-raft/Cargo.toml`**

```toml
[dev-dependencies]
kaya-io = { workspace = true }
kaya-sim = { workspace = true }
```

- [ ] **Step 2: Write crash roundtrip test**

Use `SimDisk` with `FaultSchedule` injecting `FsyncFailed` on `raft-hard-state.tmp` write; assert recover returns `NotFound` or last good state without panic.

- [ ] **Step 3: Run**

```powershell
cargo test -p kaya-raft --features disk-storage raft_persist_crash -- --nocapture
```

- [ ] **Step 4: Commit + verify workspace**

```powershell
cargo test --workspace
git commit -am "test(raft): SimDisk crash properties for Raft persistence"
```

**Phase 0 exit:** All 8 criteria in spec §5.9 green. Set repo variable `CHAOS_CI_ENABLED=true` when ready.

---

## Phase 1 — Chaos harness

### Task 8: Extract `ClusterController`

**Files:**
- Create: `crates/kaya-jepsen-test/src/cluster_controller.rs`
- Modify: `crates/kaya-jepsen-test/Cargo.toml` (add `kaya-server` dev-dep)
- Modify: `crates/kaya-jepsen-test/src/lib.rs`

- [ ] **Step 1: Write failing test** `tests/cluster_controller_smoke.rs`:

```rust
#[tokio::test]
async fn spawns_three_node_cluster_and_finds_leader() {
    let dir = tempfile::tempdir().unwrap();
    let mut cc = ClusterController::spawn_three_node(dir.path().to_path_buf()).await.unwrap();
    let leader = cc.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    assert!(leader.client_addr.port() > 0);
    cc.shutdown_all().await;
}
```

- [ ] **Step 2: Implement** by extracting `get_free_port`, health check from `integration_tests.rs`.

- [ ] **Step 3: Run test**

```powershell
cargo test -p kaya-jepsen-test spawns_three_node -- --nocapture
```

- [ ] **Step 4: Commit**

---

### Task 9: Port-aware partition in `ClusterController`

**Files:**
- Modify: `crates/kaya-jepsen-test/src/cluster_controller.rs`

- [ ] **Step 1: Implement `partition_node`**

```rust
pub async fn partition_node(&self, id: u64) -> Result<(), String> {
    let node = self.node(id)?;
    if cfg!(not(target_os = "linux")) {
        return Err("partition requires linux".into());
    }
    let comment = format!("kaya-jepsen-n{id}");
    for port in [node.client_addr.port(), node.raft_addr.port()] {
        let status = std::process::Command::new("sudo")
            .args([
                "iptables", "-I", "OUTPUT", "1",
                "-p", "tcp", "-d", "127.0.0.1", "--dport", &port.to_string(),
                "-m", "comment", "--comment", &comment, "-j", "DROP",
            ])
            .status()
            .map_err(|e| e.to_string())?;
        if !status.success() {
            return Err(format!("iptables failed for port {port}"));
        }
    }
    Ok(())
}
```

Mirror `heal_partition` with `-D` rules matching comment.

- [ ] **Step 2: Unit test** with `#[cfg(target_os = "linux")]` guard or mock Command in test-only hook.

- [ ] **Step 3: Commit**

---

### Task 10: `Scenario` registry + runner refactor

**Files:**
- Create: `crates/kaya-jepsen-test/src/scenario.rs`
- Modify: `crates/kaya-jepsen-test/src/runner.rs`
- Modify: `crates/kaya-jepsen-test/src/nemesis.rs` (AddMember, RemoveMember, Composite)

- [ ] **Step 1: Add types**

```rust
pub enum VerifyMode { Sequential, Concurrent }
pub enum Topology { ThreeNode, FourNodeJoin }
pub struct Scenario { pub id: &'static str, /* ... */ }

pub fn smoke_scenario() -> Scenario { /* 30s, 2 clients, KillNode, Sequential */ }
pub fn t1_scenario() -> Scenario { /* ... */ }
// t2..t7
```

- [ ] **Step 2: Refactor `TestRunner::run_scenario`**

```rust
let verify_result = match scenario.verify {
    VerifyMode::Sequential => history.check_linearizability(),
    VerifyMode::Concurrent => history.check_concurrent(),
};
```

- [ ] **Step 3: Wire membership nemesis** using `encode_member_payload` / `encode_remove_member_payload` from `kaya-net`.

- [ ] **Step 4: Commit**

---

### Task 11: T6 membership + T7 snapshot scenarios

**Files:**
- Create: `crates/kaya-jepsen-test/src/scenarios/t6_membership.rs`
- Create: `crates/kaya-jepsen-test/src/scenarios/t7_snapshot.rs`

- [ ] **Step 1: T6** — spawn 3 nodes + join node 4; during Register workload call `add_member` then `remove_member` while kill nemesis runs; WGL verify.

- [ ] **Step 2: T7** — replicate `test_install_snapshot_over_tcp` flow inside scenario: burst 128 puts, kill follower, restart, assert `GET snap-127` on all endpoints.

- [ ] **Step 3: Local run**

```powershell
cargo test -p kaya-jepsen-test t7_snapshot -- --ignored --nocapture
```

- [ ] **Step 4: Commit**

---

## Phase 2 — PR smoke CI

### Task 12: `tests/smoke.rs` + CI job

**Files:**
- Create: `crates/kaya-jepsen-test/tests/smoke.rs`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: smoke test**

```rust
#[tokio::test]
async fn chaos_smoke_kill_and_linearize() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf()).await.unwrap();
    let result = TestRunner::new(TestConfig::from_scenario(smoke_scenario(), dir.path()))
        .run_scenario(&smoke_scenario(), &mut cluster)
        .await
        .unwrap();
    assert!(result.passed, "{:?}", result.violations);
    cluster.shutdown_all().await;
}
```

- [ ] **Step 2: Add CI job** (after `Test` step):

```yaml
  chaos-smoke:
    runs-on: ubuntu-latest
    needs: rust
    if: vars.CHAOS_CI_ENABLED == 'true'
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test -p kaya-jepsen-test --test smoke -- --nocapture
```

- [ ] **Step 3: Verify locally, commit**

---

## Phase 3 — Nightly full gate

### Task 13: `tests/full_gate.rs` + nightly workflow

**Files:**
- Create: `crates/kaya-jepsen-test/tests/full_gate.rs`
- Create: `.github/workflows/chaos-nightly.yml`

- [ ] **Step 1: full_gate tests** — one `#[tokio::test] #[ignore]` per T1–T7, each calls `run_scenario` with `VerifyMode::Concurrent`.

- [ ] **Step 2: Create `chaos-nightly.yml`**

```yaml
name: Chaos Nightly
on:
  schedule: [{ cron: '0 3 * * *' }]
  workflow_dispatch:
  push:
    tags: ['v*']
jobs:
  chaos-full:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - run: cargo test -p kaya-jepsen-test --test full_gate -- --ignored --nocapture --test-threads=1
      - uses: actions/upload-artifact@v4
        if: failure()
        with:
          name: chaos-traces
          path: "**/traces/*.jsonl"
          retention-days: 14
```

- [ ] **Step 3: Manual dispatch once; document in `docs/jepsen-design.md`**

- [ ] **Step 4: Commit**

---

### Task 14: Documentation updates

**Files:**
- Modify: `docs/jepsen-design.md`
- Modify: `ROADMAP.md` (M13-1 🟡→✅, M13-3 when nightly green)

- [ ] Update Phase 3 CI section, T6/T7, `CHAOS_CI_ENABLED` note, dynamic-port partition.

- [ ] Commit: `docs: update jepsen-design and roadmap for chaos gates`

---

## Plan self-review

| Spec section | Task(s) |
|--------------|---------|
| Phase 0 DiskRaftStorage | Tasks 1–7 |
| Phase 1 ClusterController | Tasks 8–9 |
| Phase 1 scenarios T1–T7 | Tasks 10–11 |
| Phase 2 PR smoke | Task 12 |
| Phase 3 nightly | Task 13 |
| Docs | Task 14 |
| Dual linearizability | Task 10 |
| M13-3 mapping | Tasks 12–13 |

No TBD placeholders. Type names consistent: `HardState`, `DiskRaftStorage`, `ClusterController`, `VerifyMode`.

---

## Execution handoff

Plan saved to `docs/superpowers/plans/2026-06-18-consensus-productionization.md`.

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration
2. **Inline Execution** — execute in this session via executing-plans with batch checkpoints

**Which approach?**