# Releases & Versioning

KayaDB uses [Semantic Versioning](https://semver.org/) while in `0.1.x` pre-1.0 development. Breaking storage-format or wire-protocol changes bump the minor segment; patch releases add features and fixes within a milestone.

**Changelog:** [CHANGELOG.md](../CHANGELOG.md)  
**Roadmap:** [ROADMAP.md](../ROADMAP.md)

---

## Current release — v0.1.44

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.44`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.44) |
| **Date** | 2026-06-25 |
| **Milestone** | M14 — correctness + algorithms ✅ |
| **Workspace version** | `0.1.44` in root `Cargo.toml` |

### Highlights

- **Jepsen full gate T1–T7** — WGL concurrent linearizability verify; shared-key register workload; `KAYA_JEPSEN_FAST=1` local shortcut
- **Honest partition observability** — `PartitionTracker`; OS iptables/firewall success drives `applied`; linux CI owns `applied>0` proof
- **Linux `io_uring` Disk prototype** — `IoUringDisk` behind `io_uring` feature; shared Disk contract tests
- **Compaction + bloom + WAL batching** (from M14 track) — policy trait, SSTable v2 bloom, `WalBatchWriter`

### Install this version

```bash
cargo install kayactl --version 0.1.44
cargo install kaya-server --bin kayadb-server --version 0.1.44
```

Or download binaries from the [v0.1.44 GitHub Release](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.44).

See [Installation](installation.md) for full options.

---

## Release artifacts

Each `v*` tag triggers [`.github/workflows/release.yml`](../.github/workflows/release.yml):

| Artifact | Contents |
|---|---|
| `kayadb-<tag>-<target>.tar.gz` / `.zip` | `kayadb-server`, `kayactl`, README, CHANGELOG, security guide |
| [crates.io](https://crates.io) packages | Workspace crates published via `scripts/smart_publish.ps1` after the tag |

Jepsen full gate (`T1–T7`) runs on tag pushes and nightly — see [jepsen-design](jepsen-design.md).

---

## Version numbering

| Source | Rule |
|---|---|
| **Tagged releases** | Explicit `v0.1.N` tags (e.g. `v0.1.44`) |
| **crates.io publish helper** | `scripts/smart_publish.ps1` can auto-bump to `0.1.<git-commit-count>` unless `-SkipVersionUpdate` |
| **Development** | `main` may be ahead of the latest tag; see [CHANGELOG Unreleased](../CHANGELOG.md) |

---

## Release history (recent)

| Tag | Summary |
|---|---|
| `v0.1.44` | M14 closure: Jepsen full gate, honest partition, io_uring prototype (current) |
| `v0.1.43` | M14 storage algorithms + CI correctness gates |
| `v0.1.4` | Early public prototype |

---

## Cutting a release

1. Finalize [CHANGELOG.md](../CHANGELOG.md) for the target version
2. Update this page and [installation.md](installation.md) version pins
3. Commit on `main`, then tag: `git tag v0.1.N && git push origin v0.1.N`
4. GitHub Actions builds multi-platform binaries and creates the GitHub Release
5. Publish crates: `powershell -File scripts/smart_publish.ps1 -SkipVersionUpdate`

Release-prep branches like `tag/v0.1.43` hold CI and publish fixes that land on `main` before or after the tag. Prefer merging those fixes to `main` so docs and workflows stay aligned.