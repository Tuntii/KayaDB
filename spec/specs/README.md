# KayaDB Formal Specs

**Status:** Draft  

Bu klasör küçük ama kritik invariant'lar için executable specification artefaktlarını tutar.

İlk hedef tüm sistemi formel olarak modellemek değildir. Hedef, en riskli correctness çekirdeklerini küçük modellerle görünür hale getirmektir.

---

## Current specs

| Path | Purpose |
|---|---|
| `wal/WalCrash.tla` | Strict ACK + durable prefix recovery model |
| `wal/WalCrash.cfg` | TLC model checker config |

---

## Policy

Formal specs should be treated as executable documentation.

A formal spec is useful when it:

- maps directly to a named invariant,
- is small enough to understand,
- can be run by contributors,
- influences tests or design decisions.

Formal specs are not a replacement for unit/property/simulation tests. They are a flashlight, not a force field.
