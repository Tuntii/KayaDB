# Changelog

All notable changes to KayaDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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
