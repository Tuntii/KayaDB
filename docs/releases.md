# Releases & Versioning

KayaDB uses [Semantic Versioning](https://semver.org/) while in `0.1.x` pre-1.0 development. Breaking storage-format or wire-protocol changes bump the minor segment; patch releases add features and fixes within a milestone.

**Changelog:** [CHANGELOG.md](CHANGELOG.md)  
**Roadmap:** [ROADMAP.md](ROADMAP.md)

---

## Current release — v0.1.113

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.113`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.113) |
| **Date** | 2026-07-17 |
| **Milestone** | M21 polish + M22–M25 production path ✅ · **v0.2.0 candidate** |
| **Workspace version** | `0.1.113` in root `Cargo.toml` |
| **crates.io** | Core crates + `kaya-ebpf`, `kaya-server`, `kayactl` at `0.1.113` |

### Highlights

- **Range routing:** `LIST_RANGES` / `SPLIT_RANGE` / `MERGE_RANGE`, advisory rebalance plan, drain/decommission
- **Cross-shard TXN:** sequential 2PC over multi-raft groups; SI single-group atomic commit
- **Hardening:** AES-GCM encryption-at-rest, per-prefix ACL, Dashboard v1
- **Ecosystem:** Go TXN + retries, TypeScript client, Python client, conformance v3, deployment guide v2
- **Honest residuals:** no live `MOVE_RANGE`, process-local range meta, no key rotation — see [ROADMAP](ROADMAP.md) north-star re-eval

### Install this version

```bash
cargo install kayactl --version 0.1.113
cargo install kaya-server --bin kayadb-server --version 0.1.113
```

Or build from `main` / download binaries from the latest [GitHub Release](https://github.com/Tuntii/KayaDB/releases).

---

## Previous release — v0.1.110

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.110`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.110) |
| **Date** | 2026-07-12 |
| **Milestone** | M16–M20 Distributed Transactional KV foundation |

Highlights: MVCC, single-group SI, secondary indexes, CDC, multi-raft host + static ranges.

```bash
cargo install kayactl --version 0.1.110
cargo install kaya-server --bin kayadb-server --version 0.1.110
```

---

## Earlier: v0.1.46 (M15)

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.46`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.46) |
| **Date** | 2026-06-30 |
| **Milestone** | M15 remaining tracks (auth, ops, clients, deploy) |

See [release-notes/v0.1.46.md](release-notes/v0.1.46.md).

---

## Release artifacts

Each `v*` tag triggers the [Release Binaries workflow](https://github.com/Tuntii/KayaDB/blob/main/.github/workflows/release.yml):

| Artifact | Contents |
|---|---|
| `kayadb-<tag>-<target>.tar.gz` / `.zip` | `kayadb-server`, `kayactl`, README, CHANGELOG, security guide |
| [crates.io](https://crates.io) packages | Workspace crates via [Publish Crates](https://github.com/Tuntii/KayaDB/actions/workflows/publish.yml) / `scripts/ci_publish_crates.sh` |

Jepsen full gate (`T1–T7`) runs on tag pushes and nightly — see [jepsen-design](jepsen-design.md).

---

## Version numbering

| Source | Rule |
|---|---|
| **Tagged releases** | Explicit `v0.1.N` tags (e.g. `v0.1.113`) |
| **crates.io publish helper** | `scripts/smart_publish.ps1` / `ci_publish_crates.sh` can skip versions already on the registry |
| **Development** | `main` may be ahead of the latest tag; see [CHANGELOG](CHANGELOG.md) |

---

## Release history (recent)

| Tag | Summary |
|---|---|
| `v0.1.113` | M21–M25 production path; v0.2.0 candidate (**current**) |
| `v0.1.110` | M16–M20 transactional KV foundation |
| `v0.1.46` | M15: client auth, audit, Go client, Prometheus, Docker/K8s, watch |
| `v0.1.45` | Post-M14: ZSTD/prefix/cache stats, rich nemesis, eBPF stub, manifest TLA+ |
| `v0.1.44` | M14 closure: Jepsen full gate, honest partition, io_uring prototype |
| `v0.1.43` | M14 storage algorithms + CI correctness gates |

---

## Cutting a release

1. Finalize [CHANGELOG.md](CHANGELOG.md) and add `docs/release-notes/v0.1.N.md` for the GitHub Release body
2. Update this page and [installation.md](installation.md) version pins
3. Align `workspace.package.version` and `workspace.dependencies` path crate versions in root `Cargo.toml`
4. Commit on `main`, then tag: `git tag v0.1.N && git push origin v0.1.N`
5. GitHub Actions builds multi-platform binaries and creates the GitHub Release
6. Publish crates: Actions **Publish Crates** (needs `CARGO_REGISTRY_TOKEN`) or `bash scripts/ci_publish_crates.sh`

Prefer merging publish/CI fixes to `main` so docs and workflows stay aligned.
