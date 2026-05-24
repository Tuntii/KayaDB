# KayaDB Spec Area

**Status:** Draft v0.2  
**Last expanded:** 2026-05-18  
**Primary source documents:** `KayaDB_PRD.md`, `KayaDB_Technical_Spec.md`

Bu klasör KayaDB'nin ürün, teknik tasarım, test, formal specification ve implementation planning belgelerini bir arada tutar.

KayaDB için spec alanının amacı yalnızca “doküman yazmak” değildir. Bu alan, kod başlamadan önce hangi davranışların doğru kabul edildiğini, hangi crash/failure senaryolarının destekleneceğini ve her milestone'un hangi invariant'ları kanıtlaması gerektiğini tarif eder.

---

## 1. Nasıl okunmalı?

Önerilen okuma sırası:

1. `KayaDB_PRD.md` — ürün vizyonu, kapsam, hedef kullanıcılar, milestone'lar.
2. `docs/00-spec-index.md` — spec haritası, kararlar, terminology, status tablosu.
3. `docs/architecture-spec.md` — crate sınırları, process lifecycle, veri akışı.
4. `docs/disk-and-io-spec.md` — gerçek/simüle disk sözleşmesi.
5. `docs/wal-spec.md` — WAL formatı, append/recovery/durability.
6. `docs/recovery-spec.md` — WAL + manifest + SSTable recovery bütünlüğü.
7. `docs/testing-and-invariants-spec.md` — test stratejisi ve invariant katalogu.
8. `issues/expanded-implementation-roadmap.md` — uygulanabilir issue/milestone planı.
9. `../ROADMAP.md` — repo kökündeki insan-okunur geliştirme roadmap'i.
10. `handover.md` — gelecek oturumlar için aktif durum, kalan işler ve mimari yol haritası.

---

## 2. Spec katmanları

| Katman | Amaç | Örnek belge |
|---|---|---|
| Product | Neden, kim için, hangi kapsam? | `KayaDB_PRD.md` |
| Architecture | Modüller, sınırlar, yaşam döngüsü | `docs/architecture-spec.md` |
| Storage format | Diskteki binary/log formatlar | `docs/wal-spec.md`, `docs/lsm-storage-format-spec.md` |
| Failure model | Crash, partial write, fsync, corruption | `docs/disk-and-io-spec.md`, `docs/recovery-spec.md` |
| API/UX | Embedded API, CLI, server protocol | `docs/engine-api-spec.md`, `docs/cli-ux-spec.md` |
| Compatibility | Persistent format versioning ve migration politikası | `docs/format-versioning-spec.md` |
| Verification | Tests, invariants, simulation, TLA+ | `docs/testing-and-invariants-spec.md`, `specs/wal/WalCrash.tla` |
| Planning | Issue breakdown, milestones, release gates | `../ROADMAP.md`, `issues/expanded-implementation-roadmap.md` |

---

## 3. Kaynak-of-truth politikası

Bu repo şu anda iki büyük kaynak belge içeriyor:

- `KayaDB_PRD.md`
- `KayaDB_Technical_Spec.md`

Yeni `docs/`, `specs/` ve `issues/` altındaki dosyalar bu kaynakları daha küçük, uygulanabilir ve genişletilmiş spesifikasyonlara böler. Zamanla asıl çalışma bu bölümlenmiş dosyalara taşınmalıdır.

Kural:

> Kod, test ve issue yazarken mümkün olduğunca bölümlenmiş spec dosyalarına link ver; bundle belgeleri tarihsel/kapsayıcı referans olarak tut.

---

## 4. Spec değişiklik süreci

Bir spec değişikliği şu alanları güncellemelidir:

- İlgili `docs/*-spec.md` dosyası.
- Davranış değiştiyse invariant ID veya acceptance criteria.
- Persistent format değiştiyse magic/version ve migration/recovery davranışı.
- Test beklentisi değiştiyse `docs/testing-and-invariants-spec.md`.
- Uygulama sırası değiştiyse `issues/expanded-implementation-roadmap.md`.
- Açık karar etkileniyorsa `docs/00-spec-index.md` decision table.

---

## 5. v0.2 genişletme notları

Bu genişletme şunları ekler:

- Bölümlenmiş spec navigasyonu.
- Recovery, manifest, CLI, server protocol, configuration, observability, security, benchmarking, format versioning ve Raft roadmap belgeleri.
- WAL durable-prefix için ilk TLA+ model iskeleti.
- Daha uygulanabilir, milestone bazlı issue yol haritası.

Henüz kod yok; bu kasıtlıdır. İlk hedef, kod başladığında yapılacak işlerin crash-consistency ve deterministic testing açısından net olmasıdır.
