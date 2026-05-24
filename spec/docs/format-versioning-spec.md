# Format Versioning Spec

**Status:** Draft v0.1  
**Scope:** Persistent format versions, compatibility policy, migration/recovery behavior  

---

## 1. Purpose

KayaDB has multiple persistent formats:

- WAL records,
- manifest records,
- SSTable blocks/footer,
- simulation traces,
- future Raft log entries.

Every persistent binary format must have explicit magic and version fields.

Rule:

> If bytes are meant to survive a process, they need a version story.

---

## 2. Required fields

For binary persistent records:

```text
magic
version
header_len or fixed-size guarantee
flags or reserved field
payload_len where variable payload exists
checksum where corruption matters
```

For text/JSON artifacts:

```json
{"format":"kayadb-trace","version":1}
```

---

## 3. Version policy

| Change type | Version action |
|---|---|
| add optional field guarded by header_len/flags | same major version may be acceptable |
| change field meaning | bump version |
| change checksum coverage | bump version |
| remove field | bump version |
| change endian/layout | bump version |
| add new record type | no bump if old decoders reject unknown safely |
| make old data unreadable | bump version and document migration/failure |

MVP may use simple integer versions rather than semantic versions.

---

## 4. Compatibility behavior

Default behavior for unsupported versions:

| Format | Behavior |
|---|---|
| WAL | fail recovery with `UnsupportedVersion` unless salvage explicitly requested |
| Manifest | fail open |
| SSTable | fail open if live table; inspector reports unsupported |
| Trace | replay fails with clear message |
| Server protocol | reject request with unsupported version/status if versioned later |

---

## 5. Migration policy

Migrations are not MVP.

When introduced, migration must define:

- source version,
- target version,
- online/offline behavior,
- rollback behavior,
- backup recommendation,
- tests and fixtures.

No automatic destructive migration should happen silently.

---

## 6. Fixtures

For each persistent format, keep small fixtures:

```text
valid-v1
bad-magic
unsupported-version
bad-checksum
partial-tail where relevant
```

Fixtures must include text descriptions.

---

## 7. Inspector policy

Inspectors should print:

- magic,
- version,
- checksum status,
- record count when possible,
- first corruption offset when possible,
- unsupported fields/flags warning.

Inspector behavior is part of debuggability and should have snapshot-style tests once stable.

---

## 8. Invariants

| ID | Invariant |
|---|---|
| FMT-001 | Every persistent binary format has magic + version |
| FMT-002 | Unsupported versions return typed errors |
| FMT-003 | Format changes update spec and fixtures |
| FMT-004 | Inspectors report version/checksum status |
| FMT-005 | Migration is explicit, never silent destructive behavior |

---

## 9. Acceptance criteria

Format versioning baseline is ready when:

- WAL, manifest and SSTable specs include magic/version,
- unsupported version tests exist,
- fixture policy is documented,
- inspector output includes version/checksum,
- incompatible changes require spec update in review checklist.
