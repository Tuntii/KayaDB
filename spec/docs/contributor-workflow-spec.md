# Contributor Workflow Spec

**Status:** Draft v0.1  
**Scope:** Contribution rules, issue shape, definition of done, review checklist  

---

## 1. Purpose

KayaDB should be friendly to contributors without lowering correctness expectations.

Every meaningful storage change should answer:

- Which invariant does this affect?
- Which failure mode is covered?
- How can a reviewer reproduce a bug or verify behavior?

---

## 2. Issue template

Recommended issue body:

```text
## Goal

## Relevant specs
- spec/docs/...

## Scope

## Non-goals

## Acceptance criteria

## Suggested tests

## Invariants

## Notes
```

Good issues should be small enough to review but meaningful enough to improve the system.

---

## 3. Pull request checklist

```text
- [ ] Relevant spec linked
- [ ] Acceptance criteria covered
- [ ] Invariant IDs mentioned in tests or failure messages
- [ ] Unit tests added/updated
- [ ] Crash/property/simulation test added where relevant
- [ ] Persistent format version updated if needed
- [ ] Inspector output updated if format changed
- [ ] cargo fmt passes
- [ ] cargo clippy passes or warnings justified
- [ ] cargo test passes
```

---

## 4. Definition of done by change type

### 4.1 Parser/format change

Done when:

- spec layout updated,
- magic/version policy considered,
- malformed input tests added,
- oversized length rejection tested,
- inspector output updated,
- fixtures updated if used.

### 4.2 WAL change

Done when:

- roundtrip tests pass,
- checksum failure tests pass,
- recovery prefix behavior tested,
- SimDisk path considered,
- strict ACK invariant preserved.

### 4.3 Disk change

Done when:

- FileDisk and SimDisk contract tests pass,
- path traversal tests pass,
- short write/fsync failure semantics documented,
- trace event updated if behavior changed.

### 4.4 Engine change

Done when:

- command semantics tests pass,
- recovery tests pass if persistent behavior changes,
- reference model or simulator updated if needed,
- stats/diagnostics updated if relevant.

### 4.5 CLI/server change

Done when:

- command maps to engine API,
- exit code/status code tested,
- JSON output updated if stable contract changes,
- malformed input handled.

---

## 5. Labels

Recommended labels:

```text
good-first-issue
storage
wal
lsm
manifest
engine
simulation
fuzzing
docs
testing
raft
ebpf
unsafe
performance
cli
server
```

---

## 6. Review priorities

Review order:

1. Correctness and invariants.
2. Crash/recovery behavior.
3. Parser/resource safety.
4. API boundary cleanliness.
5. Test quality.
6. Performance.
7. Style.

A fast but subtly wrong storage engine is just a bug delivery mechanism with flair.

---

## 7. Regression policy

Every correctness bug should produce one of:

- unit regression test,
- property test seed,
- simulation seed + trace,
- fuzz corpus input,
- formal spec update.

Regression tests should mention the related invariant ID where possible.

---

## 8. Acceptance criteria

Contributor workflow is ready when:

- issue templates exist,
- PR checklist exists,
- labels are documented,
- initial implementation roadmap is split into scoped issues,
- contributor docs link to spec index and testing spec.
