//! Golden persistent-format fixtures for the SSTable and manifest formats.
//!
//! Policy: spec/docs/format-versioning-spec.md section 6 (fixture categories:
//! valid-v1, bad-magic, unsupported-version, bad-checksum, partial-tail).
//!
//! The committed files under `tests/fixtures/` are the source of truth for the
//! on-disk byte layout. If any test here fails, an on-disk format changed:
//! either revert the accidental change, or (for an intentional change) bump
//! the format version, update the spec, and regenerate the fixtures with:
//!
//! ```text
//! cargo test -p kaya-lsm --test format_fixtures -- --ignored regenerate_lsm_fixtures
//! ```
//!
//! Determinism: `SstableBuilder::finish` and `encode_manifest_edit` are pure
//! functions of their inputs (no timestamps or randomness; the bloom filter
//! hashes with crc32c), so tests assert both byte-for-byte equality with the
//! committed fixtures and logical decode results. The one exception is the
//! LZ4 fixture: compressed bytes depend on the `lz4_flex` version, so that
//! fixture is decode-only (the LZ4 frame format itself is stable).

use std::fs;
use std::path::PathBuf;

use kaya_core::{crc32c, SequenceNumber};
use kaya_lsm::{
    encode_manifest_edit, replay_manifest, ManifestEdit, ManifestWarning, SstEntry, SstFooter,
    SstableBuildOptions, SstableBuilder, SstableReader, TableMetadata, COMPRESSION_CODEC_LZ4,
    COMPRESSION_CODEC_NONE, MANIFEST_HEADER_LEN, MANIFEST_MAGIC, MANIFEST_VERSION,
    SST_FOOTER_LEN_V2, SST_VERSION, SST_VERSION_V2, SST_VERSION_V4,
};

const UNSUPPORTED_VERSION: u16 = 9;

const FIXTURE_SST_V2_VALID: &str = "sstable_v2_valid.sst";
const FIXTURE_SST_V2_VALID_BLOOM: &str = "sstable_v2_valid_bloom.sst";
const FIXTURE_SST_V3_VALID_PREFIX: &str = "sstable_v3_valid_prefix.sst";
const FIXTURE_SST_V3_VALID_LZ4: &str = "sstable_v3_valid_lz4.sst";
const FIXTURE_SST_V4_VALID: &str = "sstable_v4_valid.sst";
const FIXTURE_SST_BAD_MAGIC: &str = "sstable_v2_bad_magic.sst";
const FIXTURE_SST_UNSUPPORTED_VERSION: &str = "sstable_v2_unsupported_version.sst";
const FIXTURE_SST_BAD_CHECKSUM: &str = "sstable_v2_bad_checksum.sst";

const FIXTURE_MANIFEST_VALID: &str = "manifest_v1_valid.bin";
const FIXTURE_MANIFEST_BAD_MAGIC: &str = "manifest_v1_bad_magic.bin";
const FIXTURE_MANIFEST_UNSUPPORTED_VERSION: &str = "manifest_v1_unsupported_version.bin";
const FIXTURE_MANIFEST_BAD_CHECKSUM: &str = "manifest_v1_bad_checksum.bin";
const FIXTURE_MANIFEST_PARTIAL_TAIL: &str = "manifest_v1_partial_tail.bin";

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn read_fixture(name: &str) -> Vec<u8> {
    let path = fixtures_dir().join(name);
    fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "missing fixture {} ({e}); regenerate with \
             `cargo test -p kaya-lsm --test format_fixtures -- --ignored regenerate_lsm_fixtures`",
            path.display()
        )
    })
}

// ====================================================================
// SSTable fixtures
// ====================================================================

fn sst_basic_entries() -> Vec<SstEntry> {
    vec![
        SstEntry {
            key: b"aaa".to_vec(),
            value: Some(b"v1".to_vec()),
            sequence: SequenceNumber::new(1),
        },
        SstEntry {
            key: b"bbb".to_vec(),
            value: Some(b"v2".to_vec()),
            sequence: SequenceNumber::new(2),
        },
        SstEntry {
            key: b"ccc".to_vec(),
            value: None, // tombstone
            sequence: SequenceNumber::new(3),
        },
    ]
}

fn sst_prefix_entries() -> Vec<SstEntry> {
    (0_u8..20)
        .map(|i| SstEntry {
            key: format!("shared:prefix:key:{i:02}").into_bytes(),
            value: Some(vec![i]),
            sequence: SequenceNumber::new(u64::from(i) + 1),
        })
        .collect()
}

fn sst_lz4_entries() -> Vec<SstEntry> {
    (0_u8..8)
        .map(|i| SstEntry {
            key: vec![b'k', i],
            value: Some(vec![i; 128]),
            sequence: SequenceNumber::new(u64::from(i) + 1),
        })
        .collect()
}

fn build_sstable(options: SstableBuildOptions, entries: &[SstEntry]) -> Vec<u8> {
    let mut builder = SstableBuilder::with_options(options);
    for entry in entries {
        builder.add(entry.clone());
    }
    builder.finish().expect("fixture SSTable builds")
}

fn build_sst_v2_valid() -> Vec<u8> {
    build_sstable(
        SstableBuildOptions {
            target_block_bytes: 64 * 1024,
            bloom_bits_per_key: 0,
            ..Default::default()
        },
        &sst_basic_entries(),
    )
}

fn build_sst_v2_valid_bloom() -> Vec<u8> {
    build_sstable(
        SstableBuildOptions {
            target_block_bytes: 64 * 1024,
            bloom_bits_per_key: 10,
            ..Default::default()
        },
        &sst_basic_entries(),
    )
}

fn build_sst_v3_valid_prefix() -> Vec<u8> {
    build_sstable(
        SstableBuildOptions {
            target_block_bytes: 4096,
            bloom_bits_per_key: 0,
            prefix_compression: true,
            ..Default::default()
        },
        &sst_prefix_entries(),
    )
}

fn build_sst_v3_valid_lz4() -> Vec<u8> {
    build_sstable(
        SstableBuildOptions {
            target_block_bytes: 256,
            bloom_bits_per_key: 0,
            compression_lz4: true,
            ..Default::default()
        },
        &sst_lz4_entries(),
    )
}

/// Multi-version v4 fixture: two versions of `aaa` (seq 2 then 1, InternalKey order)
/// plus a second key `bbb`.
fn sst_v4_entries() -> Vec<SstEntry> {
    vec![
        SstEntry {
            key: b"aaa".to_vec(),
            value: Some(b"v2".to_vec()),
            sequence: SequenceNumber::new(2),
        },
        SstEntry {
            key: b"aaa".to_vec(),
            value: Some(b"v1".to_vec()),
            sequence: SequenceNumber::new(1),
        },
        SstEntry {
            key: b"bbb".to_vec(),
            value: Some(b"vb".to_vec()),
            sequence: SequenceNumber::new(3),
        },
    ]
}

fn build_sst_v4_valid() -> Vec<u8> {
    build_sstable(
        SstableBuildOptions {
            target_block_bytes: 64 * 1024,
            bloom_bits_per_key: 0,
            mvcc: true,
            ..Default::default()
        },
        &sst_v4_entries(),
    )
}

fn build_sst_bad_magic() -> Vec<u8> {
    let mut bytes = build_sst_v2_valid();
    let len = bytes.len();
    bytes[len - 4..].fill(0);
    bytes
}

fn build_sst_unsupported_version() -> Vec<u8> {
    // Only the footer version differs; footer CRC is recomputed so the
    // failure is isolated to version handling.
    let mut bytes = build_sst_v2_valid();
    let start = bytes.len() - SST_FOOTER_LEN_V2;
    bytes[start + 36..start + 38].copy_from_slice(&UNSUPPORTED_VERSION.to_le_bytes());
    let crc = crc32c(&bytes[start..start + SST_FOOTER_LEN_V2 - 8]);
    bytes[start + 56..start + 60].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_sst_bad_checksum() -> Vec<u8> {
    // Flip a CRC-covered footer byte (index_block_offset) without updating
    // the footer CRC.
    let mut bytes = build_sst_v2_valid();
    let start = bytes.len() - SST_FOOTER_LEN_V2;
    bytes[start] ^= 0xff;
    bytes
}

fn assert_footer_v2_no_bloom(footer: &SstFooter) {
    assert_eq!(footer.format_version, SST_VERSION_V2);
    assert_eq!(footer.entry_count, 3);
    assert_eq!(footer.bloom_offset, 0);
    assert_eq!(footer.bloom_len, 0);
    assert_eq!(footer.bloom_hash_count, 0);
    assert_eq!(footer.compression_codec, COMPRESSION_CODEC_NONE);
}

#[test]
fn sstable_v2_valid_fixture_decodes_to_expected_contents() {
    let reader = SstableReader::open(read_fixture(FIXTURE_SST_V2_VALID)).expect("fixture opens");
    assert_footer_v2_no_bloom(reader.footer());
    assert_eq!(reader.all_entries().unwrap(), sst_basic_entries());
    assert_eq!(
        reader.get(b"aaa").unwrap().unwrap().value,
        Some(b"v1".to_vec())
    );
    assert_eq!(reader.get(b"ccc").unwrap().unwrap().value, None);
    assert!(reader.get(b"zzz").unwrap().is_none());
}

#[test]
fn sstable_v2_valid_fixture_matches_encoder_byte_for_byte() {
    assert_eq!(
        read_fixture(FIXTURE_SST_V2_VALID),
        build_sst_v2_valid(),
        "SSTable v2 encoder output drifted from the committed golden fixture; \
         this is an on-disk format change (see format-versioning-spec.md)"
    );
}

#[test]
fn sstable_v2_bloom_fixture_decodes_to_expected_contents() {
    let reader =
        SstableReader::open(read_fixture(FIXTURE_SST_V2_VALID_BLOOM)).expect("fixture opens");
    let footer = reader.footer();
    assert_eq!(footer.format_version, SST_VERSION_V2);
    assert_eq!(footer.entry_count, 3);
    assert!(footer.bloom_len > 0);
    assert!(footer.bloom_hash_count > 0);
    assert_eq!(footer.compression_codec, COMPRESSION_CODEC_NONE);
    assert_eq!(reader.all_entries().unwrap(), sst_basic_entries());
    assert_eq!(
        reader.get(b"bbb").unwrap().unwrap().value,
        Some(b"v2".to_vec())
    );
    assert!(reader.get(b"missing-key").unwrap().is_none());
}

#[test]
fn sstable_v2_bloom_fixture_matches_encoder_byte_for_byte() {
    assert_eq!(
        read_fixture(FIXTURE_SST_V2_VALID_BLOOM),
        build_sst_v2_valid_bloom(),
        "SSTable v2 (bloom) encoder output drifted from the committed golden fixture"
    );
}

#[test]
fn sstable_v3_prefix_fixture_decodes_to_expected_contents() {
    let reader =
        SstableReader::open(read_fixture(FIXTURE_SST_V3_VALID_PREFIX)).expect("fixture opens");
    let footer = reader.footer();
    assert_eq!(footer.format_version, SST_VERSION);
    assert_eq!(footer.entry_count, 20);
    assert_eq!(footer.compression_codec, COMPRESSION_CODEC_NONE);
    assert_eq!(reader.all_entries().unwrap(), sst_prefix_entries());
}

#[test]
fn sstable_v3_prefix_fixture_matches_encoder_byte_for_byte() {
    assert_eq!(
        read_fixture(FIXTURE_SST_V3_VALID_PREFIX),
        build_sst_v3_valid_prefix(),
        "SSTable v3 (prefix compression) encoder output drifted from the committed golden fixture"
    );
}

// Decode-only: compressed block bytes depend on the `lz4_flex` version, so
// byte-for-byte equality with the current encoder is not asserted.
#[test]
fn sstable_v3_lz4_fixture_decodes_to_expected_contents() {
    let reader =
        SstableReader::open(read_fixture(FIXTURE_SST_V3_VALID_LZ4)).expect("fixture opens");
    let footer = reader.footer();
    assert_eq!(footer.format_version, SST_VERSION);
    assert_eq!(footer.entry_count, 8);
    assert_eq!(footer.compression_codec, COMPRESSION_CODEC_LZ4);
    assert_eq!(reader.all_entries().unwrap(), sst_lz4_entries());
}

#[test]
fn sstable_v4_valid_fixture_decodes_to_expected_contents() {
    let reader = SstableReader::open(read_fixture(FIXTURE_SST_V4_VALID)).expect("fixture opens");
    let footer = reader.footer();
    assert_eq!(footer.format_version, SST_VERSION_V4);
    assert_eq!(footer.entry_count, 3);
    assert_eq!(footer.compression_codec, COMPRESSION_CODEC_NONE);
    assert_eq!(reader.all_entries().unwrap(), sst_v4_entries());
    assert_eq!(
        reader.get_at(b"aaa", 1).unwrap().unwrap().value,
        Some(b"v1".to_vec())
    );
    assert_eq!(
        reader.get_at(b"aaa", 2).unwrap().unwrap().value,
        Some(b"v2".to_vec())
    );
    assert_eq!(
        reader.get(b"bbb").unwrap().unwrap().value,
        Some(b"vb".to_vec())
    );
}

#[test]
fn sstable_v4_valid_fixture_matches_encoder_byte_for_byte() {
    assert_eq!(
        read_fixture(FIXTURE_SST_V4_VALID),
        build_sst_v4_valid(),
        "SSTable v4 encoder output drifted from the committed golden fixture"
    );
}

#[test]
fn sstable_encoding_is_deterministic() {
    assert_eq!(build_sst_v2_valid(), build_sst_v2_valid());
    assert_eq!(build_sst_v2_valid_bloom(), build_sst_v2_valid_bloom());
    assert_eq!(build_sst_v3_valid_prefix(), build_sst_v3_valid_prefix());
    assert_eq!(build_sst_v4_valid(), build_sst_v4_valid());
}

#[test]
fn sstable_bad_magic_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_SST_BAD_MAGIC);
    assert_eq!(bytes, build_sst_bad_magic());
    let err = SstableReader::open(bytes).expect_err("bad magic must fail open");
    assert!(
        err.to_string().contains("bad SSTable magic"),
        "unexpected error: {err}"
    );
}

#[test]
fn sstable_unsupported_version_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_SST_UNSUPPORTED_VERSION);
    assert_eq!(bytes, build_sst_unsupported_version());
    let err = SstableReader::open(bytes).expect_err("unsupported version must fail open");
    assert!(
        err.to_string().contains(&format!(
            "unsupported SSTable version: {UNSUPPORTED_VERSION}"
        )),
        "unexpected error: {err}"
    );
}

#[test]
fn sstable_bad_checksum_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_SST_BAD_CHECKSUM);
    assert_eq!(bytes, build_sst_bad_checksum());
    let err = SstableReader::open(bytes).expect_err("bad footer CRC must fail open");
    assert!(
        err.to_string().contains("SSTable footer CRC mismatch"),
        "unexpected error: {err}"
    );
}

// ====================================================================
// Manifest fixtures
// ====================================================================

fn manifest_meta_1() -> TableMetadata {
    TableMetadata {
        table_id: 1,
        level: 0,
        path: "sst/0000000000000001.sst".to_string(),
        smallest_key: b"aaa".to_vec(),
        largest_key: b"zzz".to_vec(),
        min_sequence: SequenceNumber::new(1),
        max_sequence: SequenceNumber::new(10),
        entry_count: 5,
        file_size: 1024,
        footer_checksum: 0xdead_beef,
    }
}

fn manifest_meta_2() -> TableMetadata {
    TableMetadata {
        table_id: 2,
        level: 1,
        path: "sst/0000000000000002.sst".to_string(),
        smallest_key: b"aaa".to_vec(),
        largest_key: b"mmm".to_vec(),
        min_sequence: SequenceNumber::new(11),
        max_sequence: SequenceNumber::new(20),
        entry_count: 7,
        file_size: 2048,
        footer_checksum: 0xcafe_babe,
    }
}

fn manifest_edits() -> Vec<(ManifestEdit, u64)> {
    vec![
        (ManifestEdit::CreateTable(manifest_meta_1()), 1),
        (ManifestEdit::CreateTable(manifest_meta_2()), 2),
        (
            ManifestEdit::SetLastSequence {
                sequence: SequenceNumber::new(20),
            },
            3,
        ),
        (ManifestEdit::DeleteTable { table_id: 1 }, 4),
    ]
}

fn build_manifest_valid() -> Vec<u8> {
    let mut bytes = Vec::new();
    for (edit, edit_seq) in manifest_edits() {
        bytes.extend(encode_manifest_edit(&edit, edit_seq));
    }
    bytes
}

fn manifest_first_frame() -> Vec<u8> {
    let (edit, edit_seq) = &manifest_edits()[0];
    encode_manifest_edit(edit, *edit_seq)
}

/// Recompute the manifest header CRC32C (bytes 24..28, covering bytes 0..24)
/// after mutating header bytes.
fn rewrite_manifest_header_crc(frame: &mut [u8]) {
    let crc = crc32c(&frame[..24]);
    frame[24..28].copy_from_slice(&crc.to_le_bytes());
}

fn build_manifest_bad_magic() -> Vec<u8> {
    let mut bytes = manifest_first_frame();
    bytes[0] = 0x00;
    bytes
}

fn build_manifest_unsupported_version() -> Vec<u8> {
    let mut bytes = manifest_first_frame();
    bytes[4..6].copy_from_slice(&UNSUPPORTED_VERSION.to_le_bytes());
    rewrite_manifest_header_crc(&mut bytes);
    bytes
}

fn build_manifest_bad_checksum() -> Vec<u8> {
    // Flip the last payload byte; header stays valid, payload CRC fails.
    let mut bytes = manifest_first_frame();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    bytes
}

fn build_manifest_partial_tail() -> Vec<u8> {
    // One complete frame followed by a second frame cut mid-header.
    let mut bytes = manifest_first_frame();
    let second = {
        let (edit, edit_seq) = &manifest_edits()[1];
        encode_manifest_edit(edit, *edit_seq)
    };
    bytes.extend_from_slice(&second[..MANIFEST_HEADER_LEN / 2]);
    bytes
}

#[test]
fn manifest_valid_fixture_replays_to_expected_state() {
    let bytes = read_fixture(FIXTURE_MANIFEST_VALID);
    let (state, replayed, warnings) = replay_manifest(&bytes);
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
    assert_eq!(replayed, 4);
    assert_eq!(state.live_tables, vec![manifest_meta_2()]);
    assert_eq!(state.last_sequence, SequenceNumber::new(20));
    assert_eq!(state.last_edit_seq, 4);
}

#[test]
fn manifest_valid_fixture_matches_encoder_byte_for_byte() {
    assert_eq!(
        read_fixture(FIXTURE_MANIFEST_VALID),
        build_manifest_valid(),
        "manifest v1 encoder output drifted from the committed golden fixture; \
         this is an on-disk format change (see format-versioning-spec.md)"
    );
}

#[test]
fn manifest_valid_fixture_has_documented_header_layout() {
    // Lock the absolute wire layout of the first frame header, independent of
    // the encoder implementation.
    let bytes = read_fixture(FIXTURE_MANIFEST_VALID);
    assert_eq!(bytes[0..4], MANIFEST_MAGIC.to_le_bytes(), "magic at 0");
    assert_eq!(bytes[4..6], MANIFEST_VERSION.to_le_bytes(), "version at 4");
    assert_eq!(
        bytes[6..8],
        (MANIFEST_HEADER_LEN as u16).to_le_bytes(),
        "header_len at 6"
    );
    assert_eq!(bytes[8..10], 1_u16.to_le_bytes(), "CREATE_TABLE type at 8");
    assert_eq!(bytes[10..12], 0_u16.to_le_bytes(), "flags at 10");
    assert_eq!(bytes[12..20], 1_u64.to_le_bytes(), "edit_seq at 12");
}

#[test]
fn manifest_encoding_is_deterministic() {
    assert_eq!(build_manifest_valid(), build_manifest_valid());
}

#[test]
fn manifest_bad_magic_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_MANIFEST_BAD_MAGIC);
    assert_eq!(bytes, build_manifest_bad_magic());
    let (state, replayed, warnings) = replay_manifest(&bytes);
    assert_eq!(replayed, 0);
    assert!(state.live_tables.is_empty());
    match warnings.as_slice() {
        [ManifestWarning::Invalid { offset: 0, message }] => {
            assert!(
                message.contains("bad manifest magic"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected one Invalid warning, got {other:?}"),
    }
}

#[test]
fn manifest_unsupported_version_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_MANIFEST_UNSUPPORTED_VERSION);
    assert_eq!(bytes, build_manifest_unsupported_version());
    let (_, replayed, warnings) = replay_manifest(&bytes);
    assert_eq!(replayed, 0);
    match warnings.as_slice() {
        [ManifestWarning::Invalid { offset: 0, message }] => {
            assert!(
                message.contains(&format!(
                    "unsupported manifest version: {UNSUPPORTED_VERSION}"
                )),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected one Invalid warning, got {other:?}"),
    }
}

#[test]
fn manifest_bad_checksum_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_MANIFEST_BAD_CHECKSUM);
    assert_eq!(bytes, build_manifest_bad_checksum());
    let (_, replayed, warnings) = replay_manifest(&bytes);
    assert_eq!(replayed, 0);
    match warnings.as_slice() {
        [ManifestWarning::Invalid { offset: 0, message }] => {
            assert!(
                message.contains("payload CRC mismatch"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected one Invalid warning, got {other:?}"),
    }
}

#[test]
fn manifest_partial_tail_fixture_keeps_durable_prefix() {
    let bytes = read_fixture(FIXTURE_MANIFEST_PARTIAL_TAIL);
    assert_eq!(bytes, build_manifest_partial_tail());
    let (state, replayed, warnings) = replay_manifest(&bytes);
    assert_eq!(replayed, 1, "the complete first edit must replay");
    assert_eq!(state.live_tables, vec![manifest_meta_1()]);
    let first_frame_len = manifest_first_frame().len() as u64;
    assert_eq!(
        warnings,
        vec![ManifestWarning::Truncated {
            offset: first_frame_len,
            trailing_bytes: MANIFEST_HEADER_LEN / 2,
        }]
    );
}

// ====================================================================
// Regeneration
// ====================================================================

/// Regenerates all committed SSTable and manifest fixtures. Run only for an
/// intentional, spec-reviewed format change; the committed files are the
/// source of truth.
#[test]
#[ignore = "writes committed golden fixtures; run only on intentional format change"]
fn regenerate_lsm_fixtures() {
    let dir = fixtures_dir();
    fs::create_dir_all(&dir).expect("create fixtures dir");
    let fixtures: &[(&str, Vec<u8>)] = &[
        (FIXTURE_SST_V2_VALID, build_sst_v2_valid()),
        (FIXTURE_SST_V2_VALID_BLOOM, build_sst_v2_valid_bloom()),
        (FIXTURE_SST_V3_VALID_PREFIX, build_sst_v3_valid_prefix()),
        (FIXTURE_SST_V3_VALID_LZ4, build_sst_v3_valid_lz4()),
        (FIXTURE_SST_V4_VALID, build_sst_v4_valid()),
        (FIXTURE_SST_BAD_MAGIC, build_sst_bad_magic()),
        (
            FIXTURE_SST_UNSUPPORTED_VERSION,
            build_sst_unsupported_version(),
        ),
        (FIXTURE_SST_BAD_CHECKSUM, build_sst_bad_checksum()),
        (FIXTURE_MANIFEST_VALID, build_manifest_valid()),
        (FIXTURE_MANIFEST_BAD_MAGIC, build_manifest_bad_magic()),
        (
            FIXTURE_MANIFEST_UNSUPPORTED_VERSION,
            build_manifest_unsupported_version(),
        ),
        (FIXTURE_MANIFEST_BAD_CHECKSUM, build_manifest_bad_checksum()),
        (FIXTURE_MANIFEST_PARTIAL_TAIL, build_manifest_partial_tail()),
    ];
    for (name, bytes) in fixtures {
        fs::write(dir.join(name), bytes).expect("write fixture");
    }
}
