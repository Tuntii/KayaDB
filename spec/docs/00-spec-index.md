# KayaDB Spec Index

**Status:** Draft v0.2  
**Scope:** Product-to-implementation specification map  
**Language:** Rust  
**Primary platform:** Linux-first  

---

## 1. Design thesis

KayaDB'nin ana tezi:

> Correctness bugs must be reproducible, inspectable, and eventually impossible to reintroduce silently.

İlk sürüm performans yarışına değil, şu üç temel özelliğe odaklanır:

1. **Crash consistency** — strict durability modunda ACK edilmiş veri process crash sonrası kaybolmamalıdır.
2. **Deterministic failure testing** — disk, zaman, scheduler ve ileride network deterministik simüle edilmelidir.
3. **Inspectability** — WAL, SSTable, manifest ve trace formatları CLI ile incelenebilir olmalıdır.

---

## 2. Spec document map

| Dosya | Sorumluluk | Öncelik |
|---|---|---|
| `architecture-spec.md` | Crate sınırları, lifecycle, data flow | P0 |
| `disk-and-io-spec.md` | `Disk`, `FileDisk`, `SimDisk`, fsync semantics | P0 |
| `wal-spec.md` | WAL binary format, append/recover, tail truncation | P0 |
| `recovery-spec.md` | Cross-layer recovery lifecycle and idempotence | P0 |
| `testing-and-invariants-spec.md` | Invariant catalog, unit/property/fuzz/sim tests | P0 |
| `engine-api-spec.md` | Embedded API, command semantics, errors | P1 |
| `lsm-storage-format-spec.md` | Memtable, SSTable, manifest, compaction | P1 |
| `mvcc-spec.md` | Multi-version keys, visibility, GC watermark | P1 |
| `transactions-spec.md` | Snapshot Isolation, write intents, txn lifecycle (M17) | P1 |
| secondary-index-spec.md | Secondary indexes over primary KV (M18 foundation) | P1 |
| `manifest-spec.md` | Manifest record format and publication rules | P1 |
| `simulation-spec.md` | Seeded simulator, nemesis, trace replay | P1 |
| `cli-ux-spec.md` | `kayactl` command UX and inspect outputs | P1 |
| `server-and-protocol-spec.md` | Local server and binary protocol boundary | P2 |
| `configuration-spec.md` | Config files, defaults, validation | P2 |
| `observability-spec.md` | Logs, metrics, traces, future eBPF | P2 |
| `security-and-safety-spec.md` | Parser safety, unsafe policy, threat limits | P2 |
| `benchmarking-spec.md` | Benchmarks and performance reporting policy | P2 |
| `format-versioning-spec.md` | Persistent format compatibility and migration policy | P2 |
| `raft-and-distributed-roadmap-spec.md` | Future Raft and network simulation boundaries | P3 |
| `contributor-workflow-spec.md` | Contribution rules and definition of done | P2 |

---

## 3. Terminology

| Term | Meaning |
|---|---|
| ACK | Bir write operasyonunun client'a başarılı olarak dönmesi |
| LSN | Log Sequence Number; WAL içindeki monoton artan kayıt numarası |
| SequenceNumber | Engine visibility ordering numarası |
| WAL | Write-Ahead Log; crash recovery için append-only log |
| Segment | WAL'in parça dosyası |
| Durable prefix | Crash sonrası recover edilebilir WAL prefix'i |
| Memtable | Son yazılan key/value kayıtlarının memory'deki ordered yapısı |
| Immutable memtable | Flush için dondurulmuş memtable snapshot'ı |
| SSTable | Immutable sorted string table dosyası |
| Manifest | Canlı SSTable dosyalarını ve metadata'yı takip eden log |
| Tombstone | Delete operasyonunu temsil eden kayıt |
| SimDisk | Deterministic fault injection yapan disk implementasyonu |
| Nemesis | Simülasyonda failure üreten bileşen |
| Trace | Simülasyon olaylarının replay edilebilir kaydı |
| Invariant | Sistem boyunca doğru kalması gereken özellik |
| Strict durability | ACK öncesi WAL fsync zorunluluğu |
| Relaxed durability | ACK'in fsync öncesi dönebildiği mod |
| Salvage mode | Normal recovery dışında, manual repair amaçlı prefix kurtarma modu |

---

## 4. Decision records

| ID | Decision | Rationale |
|---|---|---|
| D-001 | İlk implementasyon single-node olacak | Distributed complexity storage correctness'ten sonra gelmeli |
| D-002 | Disk abstraction ilk milestone'da yazılacak | SimDisk olmadan crash testing ciddiye alınamaz |
| D-003 | WAL, SSTable'dan önce tamamlanacak | Durability base layer |
| D-004 | `strict` durability default olacak | Correctness-first positioning |
| D-005 | `io_uring` ilk MVP'de şart değil | Low-level I/O semantics stable olduktan sonra eklenmeli |
| D-006 | WAL decoder malformed data'da panic etmeyecek | Corruption normal failure mode sayılır |
| D-007 | Trace replay açık formatla başlayacak | Debuggability binary compactness'ten önemli |
| D-008 | MVP segmentleri record-only olabilir | Segment header sonraki format version'a bırakılabilir |
| D-009 | Manifest publication atomicity source-of-truth olacak | File existence tek başına live state değildir |
| D-010 | CLI strings presentation layer'dır | Engine keys/values raw bytes kalmalı |
| D-011 | Recovery idempotence P0 invariant'tır | Aynı data dir üzerinde recovery tekrarı state değiştirmemeli |
| D-012 | `unsafe` MVP'de yasak-varsayılan kabul edilir | Gerekirse izole ve belgeli safety invariant gerekir |
| D-013 | eBPF v2+ scope'tur | MVP correctness path'i engellememeli |
| D-014 | Raft storage stable olduktan sonra başlayacak | Replication, bozuk local durability'yi iyileştirmez |
| D-015 | Config defaults güvenli olacak | Production-ready iddiası yok ama data-loss default olmamalı |
| D-016 | Inspector çıktıları testlenebilir olmalı | Debug UX regressions yakalanmalı |
| D-017 | Corrupt middle segment normal modda fail-open değil fail-fast olmalı | Silent data loss/salvage riski azaltılır |

---

## 5. Ownership matrix

| Area | Primary crate | Owns | Must not own |
|---|---|---|---|
| Core types | `kaya-core` | errors, config, typed IDs, byte wrappers | file I/O, WAL parser |
| Disk | `kaya-io` | disk trait, FileDisk, SimDisk | WAL/LSM semantics |
| WAL | `kaya-wal` | record format, append, recover, inspect | memtable visibility |
| LSM | `kaya-lsm` | memtable, SSTable, manifest, compaction | server protocol |
| Engine | `kaya-engine` | write/read orchestration, recovery | physical disk details |
| Simulation | `kaya-sim` | RNG, scheduler, nemesis, trace | production server |
| CLI | `kayactl` | UX, inspection, command mapping | storage invariants |
| Server | `kaya-server` | connection handling, protocol | storage format decisions |
| Raft future | `kaya-raft` | consensus state machine | local file layout |

---

## 6. Milestone gates

| Gate | Required proof |
|---|---|
| M0 Skeleton | workspace compiles, CI exists, docs linked |
| M1 Disk/WAL | FileDisk and SimDisk run same WAL tests |
| M2 Recovery | partial/corrupt tail recovery and idempotence tests pass |
| M3 Engine | PUT/GET/DELETE/SCAN over WAL+memtable survives restart |
| M4 LSM | SSTable flush + manifest replay works after crash points |
| M5 Compaction | visible state preserved under generated overlap cases |
| M6 Simulator | failing seed writes replayable trace |
| M7 Raft prototype | simulated cluster satisfies basic Raft safety invariants |

---

## 7. Spec status policy

Each spec should declare one of:

- **Draft** — design can still change freely.
- **Accepted** — implementation should follow unless a new decision record updates it.
- **Implemented** — code and tests exist.
- **Deprecated** — superseded by another spec.

Current state: all docs are **Draft**.
