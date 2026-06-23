# Changelog

All notable changes to KayaDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Changed
- Documentation refresh: database-style landing page, `installation.md`, `releases.md` (v0.1.43), synced sidebar/SUMMARY, updated status in `KayaDB_Explained.md` and `security.md`

---

## [0.1.43] — 2026-06-23

M14 correctness + algorithms milestone prep: storage algorithm upgrades, module splits, and expanded CI correctness gates.

### Added
- `CompactionPolicy` trait in `kaya-lsm` with L0 merge, leveled, and size-tiered strategies; wired through `EngineConfig.compaction`
- SSTable v2 bloom filter (configurable `bloom_bits_per_key`) with read-path negative lookup pruning
- WAL group-commit batching via `WalBatchWriter` and `WalBatchConfig` (record count, byte limit, time flush)
- Chaos matrix CI (`.github/workflows/chaos-matrix.yml`): DiskFull, NetworkPartition, ClockSkew axes
- Jepsen CI (`.github/workflows/jepsen.yml`): PR smoke scenario + nightly/tag full T1–T7 WGL gate
- Security audit CI (`audit.yml`: `cargo audit` + `cargo deny`) and `deny.toml`
- crates.io badges for `kaya-engine` and `kayactl` in README

### Changed
- Split `kaya-engine` god-file into `memtable`, `flush`, `snapshot`, and `stats` modules
- Split `kaya-server` cluster god-file into `client_ops`, `election`, `replication`, `snapshot`, and `stats`
- Split `kayactl` god-file into `cli`, `local`, `server`, `inspect`, `stats_cmd`, and `ebpf` modules
- ROADMAP and README status badge updated to M14 correctness+algorithm (in progress)

---

## [M13] — 2026-06-21

M13 productization milestone: operators can run KayaDB with documented security controls and day-2 procedures. Experimental status label removed.

### Added
- Native TLS transport (`tls` feature): rustls listeners for Raft + client (`kaya-net`, `kaya-server`, `kaya-client`, `kayactl`)
- Operator token auth for `ADD_MEMBER` / `REMOVE_MEMBER` (opcodes 7/8) via `--operator-token` / `KAYA_OPERATOR_TOKEN`
- Day-2 runbooks: `docs/runbooks/` (add-remove-node, rolling-restart, backup-restore, detecting-split-brain, mtls-sidecar)
- Durable Raft hard state + log persistence across `kayadb-server` restart
- Security enforcement table with code cross-references in `docs/security.md`
- Performance regression gate in CI (`kaya-bench/tests/perf_gate.rs`)

### Changed
- Experimental badge removed; README and ROADMAP reflect M13 exit
- `docs/security.md` §7: post-M13 gaps documented as accepted deployment risks with mitigations
- Runbooks aligned to current CLI (`--node-id`, `--data`, `--join-cluster` + `--peer`)

### Security
- Accepted risks (not correctness bugs): full client authZ, data-at-rest encryption, multi-tenant isolation, compliance audit logging — see `docs/security.md` §7

---

## Historical

See [ROADMAP.md](ROADMAP.md) for the detailed development history (M0–M13 productization tracks).

Major milestones completed before formal changelog adoption:
- Complete LSM + WAL engine with SimDisk deterministic fault injection
- Raft consensus + dynamic membership (joint consensus)
- TCP cluster + async client with leader redirection
- `kayactl` operator tooling + inspect for all on-disk formats
- Simulation testing, Jepsen-style harness, fuzz targets
- Multi-platform release binaries via GitHub Actions

For older notes see git history and the archived sections of ROADMAP.md.
