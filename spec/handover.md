# KayaDB Handover & Future Roadmap

This document serves as a transition guide for subsequent development sessions. It details the active state of **KayaDB**, the accomplishments of the current session, and the precise design backlog for upcoming milestones.

---

## 1. Current State & Recent Accomplishments

During the recent release-hardening phase, we finalized the core requirements for the upcoming v1.0.0 production publishing:

* **Platform-Specific Exclusive Directory Locking (`KAYA_LOCK`):**
  * Added solid data directory protection using `OpenOptionsExt::share_mode(0)` on Windows and POSIX advisory `libc::flock` on Unix systems.
  * Resolved parallel unit-test lock collisions by introducing the `disable_locking` flag in `EngineConfig` (activated automatically in simulation environments).
* **Disk-Full Failure Resilience (`engine_disk_full_resilience`):**
  * Proved LSM engine durability safety (flush/compaction disk operations occur *before* updating volatile in-memory state).
  * Implemented an automated `SimDisk` fault-injection unit test guaranteeing that database state remains fully consistent and readable after `KayaError::DiskFull` failures.
* **Static Cluster Membership Boundaries:**
  * Updated `raft_event_loop` inside `kaya-server` to drop incoming Raft packets from unrecognized node IDs not configured in the `NodeRoster`, logging clear warnings.
* **Production Deployment Security Guide:**
  * Documented all network isolation recommendations and provided an explicit step-by-step mTLS proxy wrapping tutorial using `ghostunnel` at [docs/security.md](../docs/security.md).
* **Automated Semic-Stripped Binary Packaging Pipeline:**
  * Configured a GitHub Actions workflow at [.github/workflows/release.yml](../.github/workflows/release.yml) to compile, package, and publish stripped release artifacts for Windows, Linux, and macOS upon tag pushes.
* **Workspace Cleanliness (CI Green):**
  * Formatted the entire codebase with `cargo fmt --all` and resolved 100% of Clippy warnings (`cargo clippy --workspace --all-targets -- -D warnings`), achieving zero compile warnings. All 87 unit and integration tests are passing perfectly.

---

## 2. Dev-Handover Backlog (Remaining Release Readiness Tasks)

Below are the open items from [release_checklist.md](../release_checklist.md) to be tackled in subsequent sessions:

### Task A: Large-Seed Simulation Burn-In Run
* **Context:** We successfully implemented a dedicated, manual burn-in stress test target `sim_large_seed_burn_in` inside `crates/kaya-sim/src/lib.rs`.
* **Handover Instructions:**
  Run the burn-in suite on a multi-core machine to verify engine linearizability over 100 random seeds for 10,000 operations each:
  ```bash
  cargo test -p kaya-sim --lib sim_large_seed_burn_in -- --ignored --nocapture
  ```
  Verify that zero invariant violations (`ENG-001` through `ENG-004`) are reported.

### Task B: Continuous Fuzzing Run (24 Hours)
* **Context:** Highly optimized fuzzing targets exist (`fuzz_wal_decoder`, `fuzz_sstable_block`, `fuzz_manifest_decoder`) under `fuzz/`.
* **Handover Instructions:**
  Setup a nightly runner with LLVM/libFuzzer to execute these targets continuously for 24 hours to prove memory safety (no heap corruptions or overflow panics) on arbitrary malformed bytes:
  ```bash
  cargo +nightly fuzz run fuzz_wal_decoder
  ```

### Task C: Jepsen Nemesis Stress Test Container execution
* **Context:** We implemented the cross-platform `NodeController` supporting process suspension (`SuspendThread`/`ResumeThread` on Windows, `SIGSTOP`/`SIGCONT` on Unix).
* **Handover Instructions:**
  Integrate this local controller with a Jepsen-like nemesis loop that fires random network splits, node pauses, and leader terminations while verifying linearizability via the `LinearizabilityChecker`.

---

## 3. Post-v1.0.0 Architectural Roadmap (Milestone M8 & Beyond)

These are complex distributed systems features that must be designed spec-first in the next major releases:

### 1. Linearizable Followers Reads (`ReadIndex` or Leader Leases)
* **Problem:** In v1.0.0, followers redirect reads to the leader to ensure linearizability. Network partitions could still allow client GETs to hit isolated leaders (handled via client timeout, but not optimal).
* **Proposed Design:**
  * **Option A (`ReadIndex`):** When a follower receives a read, it queries the leader for the current `CommitIndex`. The leader must broadcast a heartbeat to a majority to confirm it is still the leader. Once confirmed, the follower waits for its local state machine to apply up to `ReadIndex` and serves the read locally.
  * **Option B (Leader Leases):** The leader maintains a time-bound lease confirmed by a majority heartbeat. During the lease window, the leader can serve reads locally without broadcasting heartbeats, and followers can query the leader without network validation.

### 2. Raft Log Compaction (Snapshots & Log Truncation)
* **Problem:** Currently, Raft logs grow indefinitely, risking disk exhaustion.
* **Proposed Design:**
  * Implement state machine snapshotting. Once the state machine reaches a threshold (e.g., every 50,000 applied entries), it writes a serialized LSM snapshot to disk.
  * The leader then broadcasts a `InstallSnapshot` RPC to lagging followers.
  * Applied log entries preceding the snapshot index are truncated from the Raft log safely.

### 3. Linux eBPF engine Observability (Milestone M7)
* **Goal:** Zero-overhead observability of local LSM performance metrics.
* **Proposed Design:**
  * Build eBPF probes targeting the `kaya-engine` processes.
  * Track actual disk-write latencies during flushes, thread preemption latencies during compactions, and system call boundaries without modifying the Rust codebase.
