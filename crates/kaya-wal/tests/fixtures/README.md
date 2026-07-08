# WAL format fixtures

Golden fixtures for the WAL v1 record format, per the fixture policy in
`spec/docs/format-versioning-spec.md` section 6. The committed binaries are
the source of truth for the on-disk byte layout; `tests/format_fixtures.rs`
must fail if the encoder or decoder drifts from them.

Regenerate only for an intentional, spec-reviewed format change:

```text
cargo test -p kaya-wal --test format_fixtures -- --ignored regenerate_wal_fixtures
```

| File | Category | Contents |
|---|---|---|
| `wal_v1_valid.bin` | valid-v1 | Three v1 records: `PUT(lsn=1, seq=1, key="user:1", value="Ada")`, `DELETE(lsn=2, seq=2, key="user:2")`, `NOOP(lsn=3, seq=3)` |
| `wal_v1_bad_magic.bin` | bad-magic | First record of the valid fixture with byte 0 of the magic zeroed; decoder must report `BadMagic` |
| `wal_v1_unsupported_version.bin` | unsupported-version | First record with version field set to 9 and the header CRC recomputed (only the version is wrong); decoder must report `UnsupportedVersion` |
| `wal_v1_bad_checksum.bin` | bad-checksum | First record with the last payload byte flipped; decoder must report `BadPayloadChecksum` |
| `wal_v1_partial_tail.bin` | partial-tail | Complete first record followed by the second record truncated 4 bytes into its payload; decoder must return the first record then `Incomplete`/`PartialPayload` |

All records are encoded by `kaya_wal::encode_record`, which is deterministic
(pure function of the record), so tests assert byte-for-byte equality with the
current encoder in addition to decode results.
