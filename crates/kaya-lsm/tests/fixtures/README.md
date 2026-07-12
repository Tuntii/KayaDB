# SSTable and manifest format fixtures

Golden fixtures for the SSTable (v2/v3/v4) and manifest (v1) formats, per the
fixture policy in `spec/docs/format-versioning-spec.md` section 6. The
committed binaries are the source of truth for the on-disk byte layout;
`tests/format_fixtures.rs` must fail if the encoders or decoders drift from
them.

Regenerate only for an intentional, spec-reviewed format change:

```text
cargo test -p kaya-lsm --test format_fixtures -- --ignored regenerate_lsm_fixtures
```

## SSTable

| File | Category | Contents |
|---|---|---|
| `sstable_v2_valid.sst` | valid (v2) | No bloom, no compression. Entries: `aaa=v1@1`, `bbb=v2@2`, tombstone `ccc@3` |
| `sstable_v2_valid_bloom.sst` | valid (v2, bloom) | Same entries built with `bloom_bits_per_key=10`; footer carries bloom offset/len/hash_count |
| `sstable_v3_valid_prefix.sst` | valid (v3, prefix) | 20 entries `shared:prefix:key:NN` built with prefix compression (v3 footer, codec NONE) |
| `sstable_v3_valid_lz4.sst` | valid (v3, LZ4) | 8 compressible entries built with LZ4 block compression. Decode-only: compressed bytes depend on the `lz4_flex` version, so no byte-for-byte re-encode assertion |
| `sstable_v4_valid.sst` | valid (v4, mvcc) | Multi-version: `aaa=v2@2`, `aaa=v1@1`, `bbb=vb@3` (InternalKey order); footer format_version=4, physical layout same as v3 |
| `sstable_v2_bad_magic.sst` | bad-magic | Valid v2 fixture with the trailing 4 magic bytes zeroed; open must fail with a bad-magic corruption error |
| `sstable_v2_unsupported_version.sst` | unsupported-version | Valid v2 fixture with footer `format_version` set to 9 and the footer CRC recomputed (only the version is wrong); open must fail with an unsupported-version error |
| `sstable_v2_bad_checksum.sst` | bad-checksum | Valid v2 fixture with a CRC-covered footer byte flipped; open must fail with a footer CRC mismatch |

A partial-tail fixture is omitted for SSTables: they are written whole and
truncation surfaces as footer corruption, which the bad-magic/bad-checksum
fixtures and the fuzz tests already cover.

## Manifest

| File | Category | Contents |
|---|---|---|
| `manifest_v1_valid.bin` | valid-v1 | Four frames: `CREATE_TABLE(id=1)`, `CREATE_TABLE(id=2)`, `SET_LAST_SEQUENCE(20)`, `DELETE_TABLE(id=1)` with edit_seq 1..4. Replay yields one live table (id=2), last_sequence=20 |
| `manifest_v1_bad_magic.bin` | bad-magic | First frame with byte 0 of the magic zeroed; replay must stop with an `Invalid` warning |
| `manifest_v1_unsupported_version.bin` | unsupported-version | First frame with version set to 9 and the header CRC recomputed (only the version is wrong); replay must stop with an `Invalid` warning |
| `manifest_v1_bad_checksum.bin` | bad-checksum | First frame with the last payload byte flipped; replay must stop with a payload-CRC `Invalid` warning |
| `manifest_v1_partial_tail.bin` | partial-tail | Complete first frame followed by 16 bytes of the second frame's header; replay must keep the first edit and report `Truncated` |

`SstableBuilder::finish` and `encode_manifest_edit` are deterministic (pure
functions of their inputs; the bloom filter hashes with crc32c), so all
fixtures except the LZ4 one are also asserted byte-for-byte against the
current encoders.
