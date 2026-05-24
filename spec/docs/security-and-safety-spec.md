# Security and Safety Spec

**Status:** Draft v0.1  
**Scope:** Parser safety, filesystem safety, unsafe Rust policy, MVP threat boundaries  

---

## 1. Scope and non-goals

KayaDB is not initially a security-focused database. However, it must avoid obvious unsafe behavior.

MVP security scope:

- malformed persistent files must not panic unexpectedly,
- parsers must validate lengths before allocation,
- path traversal must be impossible through `RelativePath`,
- server binds to localhost by default,
- no production-ready security claims.

Non-goals for MVP:

- authentication,
- encryption at rest,
- TLS,
- multi-tenant isolation,
- hardened remote deployment.

---

## 2. Parser safety rules

All binary decoders must:

- check minimum header length before reading fields,
- check magic/version before deeper parsing,
- reject unsupported versions,
- reject unknown flags unless explicitly allowed,
- validate payload length against configured and hard limits,
- validate checksums before trusting payload content,
- avoid panics on malformed input,
- avoid unbounded allocation.

Applies to:

- WAL decoder,
- manifest decoder,
- SSTable footer/block decoder,
- server command frame decoder,
- simulation trace parser.

---

## 3. Filesystem safety

Rules:

- storage code must use `RelativePath` for database-internal paths,
- no `..`, absolute path, drive prefix or path traversal,
- temp file cleanup must stay inside data directory,
- error messages should avoid unnecessarily leaking unrelated absolute paths,
- symlink policy must be decided before production claims.

MVP symlink policy:

> Do not intentionally follow symlinks created inside the DB directory for internal files. If platform APIs make this hard, document behavior and avoid production claims.

---

## 4. `unsafe` Rust policy

Default policy:

> No `unsafe` in MVP storage correctness path unless there is a measured, documented need.

If `unsafe` is introduced:

- isolate it in a small module,
- write a `SAFETY:` comment explaining invariants,
- add tests around safe wrapper behavior,
- ensure fuzz targets cover parser-facing code,
- prefer established crates for tricky low-level/crypto/protocol logic.

---

## 5. Resource limits

Recommended defaults:

| Limit | Default |
|---|---:|
| max key length | 4096 bytes |
| max value length | 16 MiB |
| max WAL payload | 32 MiB |
| max server frame | 64 MiB |
| max scan result without explicit limit | TBD; prefer bounded |

Unbounded `SCAN` can be acceptable for early CLI but server mode should support limits.

---

## 6. Corruption handling

Corruption is a normal failure mode, not a panic condition.

Rules:

- bad checksum returns typed corruption error,
- recoverable tail corruption returns warning/report,
- non-tail corruption in required structures fails open,
- salvage mode must be explicit and clearly labeled if introduced.

---

## 7. Network exposure

Server defaults:

```text
host = 127.0.0.1
```

If user binds to public interface:

- log a warning,
- docs must state authentication is not implemented,
- do not call it production-ready.

---

## 8. Security/safety invariants

| ID | Invariant |
|---|---|
| SEC-001 | Decoders reject oversized lengths before allocation |
| SEC-002 | Malformed persistent files do not cause expected-path panic |
| SEC-003 | `RelativePath` prevents data-dir escape |
| SEC-004 | Server binds to localhost by default |
| SEC-005 | `unsafe` code requires documented safety invariant |
| SEC-006 | Corruption is reported as typed error/warning |

---

## 9. Acceptance criteria

Security/safety baseline is ready when:

- path traversal tests exist,
- WAL decoder fuzz target exists,
- server frame decoder rejects oversized frames,
- malformed WAL/SSTable/manifest inputs return typed errors,
- `unsafe` usage is absent or documented,
- README states experimental/non-production status.
