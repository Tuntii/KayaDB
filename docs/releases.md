# Releases & Versioning

KayaDB uses [Semantic Versioning](https://semver.org/). Pre-1.0, the minor segment (`0.2.0`) marks a coordinated feature cut; patch releases add fixes within that line. Breaking storage-format or wire-protocol changes still bump the minor segment.

**Changelog:** [CHANGELOG.md](CHANGELOG.md)  
**Roadmap:** [ROADMAP.md](ROADMAP.md)

---

## Current release — v0.2.0

| Item | Detail |
|---|---|
| **Tag** | [`v0.2.0`](https://github.com/Tuntii/KayaDB/releases/tag/v0.2.0) |
| **Date** | 2026-09-05 |
| **Milestone** | First 0.2 line · M16–M25 production path + post-candidate residuals |
| **Workspace version** | `0.2.0` in root `Cargo.toml` |
| **crates.io** | Core crates + `kaya-ebpf`, `kaya-server`, `kayactl` at `0.2.0` |
| **Notes** | [release-notes/v0.2.0.md](release-notes/v0.2.0.md) |

### Highlights

- **Range migrate:** live `MOVE_RANGE` (21), durable range meta, orphan group reclaim
- **Transactions:** parallel 2PC + durable decision log, HLC uncertainty clamp
- **Security:** online key rotation, named tenant isolation (`--tenant-file`)
- **Ops:** Dashboard v2 Phase A (`/v1/cluster`, `/v1/leadership`, `/v1/errors`)
- **Clients / correctness:** TypeScript TXN + retries; `kaya-wgl` MUS explorer
- **Honest residuals:** physical key copy on migrate, 2PC TLS forwarding, quotas/RBAC, Dashboard B/C, Zig client. See [ROADMAP](ROADMAP.md)

### Install this version

```bash
cargo install kayactl --version 0.2.0
cargo install kaya-server --bin kayadb-server --version 0.2.0
```

Or build from `main` / download binaries from the latest [GitHub Release](https://github.com/Tuntii/KayaDB/releases).

---

## Previous release — v0.1.113

| Item | Detail |
|---|---|
| **Tag** | [`v0.1.113`](https://github.com/Tuntii/KayaDB/releases/tag/v0.1.113) |
| **Date** | 2026-07-17 |
| **Milestone** | M21 polish + M22–M25 production path · v0.2.0 candidate |

```bash
cargo install kayactl --version 0.1.113
cargo install kaya-server --bin kayadb-server --version 0.1.113
```

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
| **Tagged releases** | Explicit `v0.Y.Z` tags (e.g. `v0.2.0`) |
| **crates.io publish helper** | `scripts/smart_publish.ps1` / `ci_publish_crates.sh` can skip versions already on the registry |
| **Development** | `main` may be ahead of the latest tag; see [CHANGELOG](CHANGELOG.md) |

---

## Release history (recent)

| Tag | Summary |
|---|---|
| `v0.2.0` | First 0.2 line: M16–M25 path + post-candidate residuals (**current**) |
| `v0.1.113` | M21–M25 production path; v0.2.0 candidate |
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
