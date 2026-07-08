//! Golden persistent-format fixtures for the WAL v1 record format.
//!
//! Policy: spec/docs/format-versioning-spec.md section 6 (fixture categories:
//! valid-v1, bad-magic, unsupported-version, bad-checksum, partial-tail).
//!
//! The committed files under `tests/fixtures/` are the source of truth for the
//! on-disk byte layout. If any test here fails, the WAL wire format changed:
//! either revert the accidental format change, or (for an intentional change)
//! bump `WAL_VERSION`, update spec/docs/wal-spec + format-versioning-spec, and
//! regenerate the fixtures with:
//!
//! ```text
//! cargo test -p kaya-wal --test format_fixtures -- --ignored regenerate_wal_fixtures
//! ```
//!
//! Determinism: `encode_record` is a pure function of the record (no
//! timestamps or randomness), so tests assert both byte-for-byte equality with
//! the committed fixture and logical decode results.

use std::fs;
use std::path::PathBuf;

use kaya_core::{crc32c, Lsn, SequenceNumber};
use kaya_wal::{
    decode_record, encode_record, DecodeRecordResult, WalPayload, WalRecord, WalWarning,
    WAL_HEADER_LEN, WAL_MAGIC, WAL_VERSION,
};

const MAX_PAYLOAD_LEN: u32 = 1 << 20;
const UNSUPPORTED_VERSION: u16 = 9;

const FIXTURE_VALID: &str = "wal_v1_valid.bin";
const FIXTURE_BAD_MAGIC: &str = "wal_v1_bad_magic.bin";
const FIXTURE_UNSUPPORTED_VERSION: &str = "wal_v1_unsupported_version.bin";
const FIXTURE_BAD_CHECKSUM: &str = "wal_v1_bad_checksum.bin";
const FIXTURE_PARTIAL_TAIL: &str = "wal_v1_partial_tail.bin";

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
             `cargo test -p kaya-wal --test format_fixtures -- --ignored regenerate_wal_fixtures`",
            path.display()
        )
    })
}

// ---- Fixed logical inputs ----

fn expected_records() -> Vec<WalRecord> {
    vec![
        WalRecord::new(
            Lsn::new(1),
            SequenceNumber::new(1),
            WalPayload::Put {
                key: b"user:1".to_vec(),
                value: b"Ada".to_vec(),
            },
        ),
        WalRecord::new(
            Lsn::new(2),
            SequenceNumber::new(2),
            WalPayload::Delete {
                key: b"user:2".to_vec(),
            },
        ),
        WalRecord::new(Lsn::new(3), SequenceNumber::new(3), WalPayload::Noop),
    ]
}

// ---- Deterministic fixture builders ----

fn build_valid_fixture() -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in expected_records() {
        bytes.extend(encode_record(&record).expect("fixture record encodes"));
    }
    bytes
}

fn first_record_bytes() -> Vec<u8> {
    encode_record(&expected_records()[0]).expect("fixture record encodes")
}

/// Recompute the header CRC32C (bytes 32..36, computed with the CRC field
/// zeroed) after mutating header bytes.
fn rewrite_header_crc(record: &mut [u8]) {
    record[32..36].copy_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c(&record[..WAL_HEADER_LEN]);
    record[32..36].copy_from_slice(&crc.to_le_bytes());
}

fn build_bad_magic_fixture() -> Vec<u8> {
    let mut bytes = first_record_bytes();
    bytes[0] = 0x00;
    bytes
}

fn build_unsupported_version_fixture() -> Vec<u8> {
    // Only the version differs; header CRC is valid so the failure is
    // isolated to version handling.
    let mut bytes = first_record_bytes();
    bytes[4..6].copy_from_slice(&UNSUPPORTED_VERSION.to_le_bytes());
    rewrite_header_crc(&mut bytes);
    bytes
}

fn build_bad_checksum_fixture() -> Vec<u8> {
    // Flip the last payload byte; header stays valid, payload CRC fails.
    let mut bytes = first_record_bytes();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    bytes
}

fn build_partial_tail_fixture() -> Vec<u8> {
    // One complete record followed by a second record cut mid-payload
    // (header complete, payload truncated).
    let mut bytes = first_record_bytes();
    let second = encode_record(&expected_records()[1]).expect("fixture record encodes");
    assert!(second.len() > WAL_HEADER_LEN + 4);
    bytes.extend_from_slice(&second[..WAL_HEADER_LEN + 4]);
    bytes
}

fn decode_all(bytes: &[u8]) -> Vec<WalRecord> {
    let mut records = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        match decode_record(&bytes[offset..], offset as u64, MAX_PAYLOAD_LEN) {
            DecodeRecordResult::Complete { record, bytes_read } => {
                records.push(record);
                offset += bytes_read;
            }
            other => panic!("unexpected decode result at offset {offset}: {other:?}"),
        }
    }
    records
}

// ---- Tests ----

#[test]
fn wal_valid_fixture_decodes_to_expected_records() {
    let bytes = read_fixture(FIXTURE_VALID);
    let records = decode_all(&bytes);
    assert_eq!(records, expected_records());
}

#[test]
fn wal_valid_fixture_matches_encoder_byte_for_byte() {
    let committed = read_fixture(FIXTURE_VALID);
    let regenerated = build_valid_fixture();
    assert_eq!(
        committed, regenerated,
        "WAL v1 encoder output drifted from the committed golden fixture; \
         this is an on-disk format change (see format-versioning-spec.md)"
    );
}

#[test]
fn wal_valid_fixture_has_documented_header_layout() {
    // Lock the absolute wire layout of the first header, independent of the
    // encoder implementation.
    let bytes = read_fixture(FIXTURE_VALID);
    assert_eq!(bytes[0..4], WAL_MAGIC.to_le_bytes(), "magic at offset 0");
    assert_eq!(
        bytes[4..6],
        WAL_VERSION.to_le_bytes(),
        "version at offset 4"
    );
    assert_eq!(
        bytes[6..8],
        (WAL_HEADER_LEN as u16).to_le_bytes(),
        "header_len at offset 6"
    );
    assert_eq!(bytes[8..10], 0_u16.to_le_bytes(), "flags at offset 8");
    assert_eq!(bytes[10..12], 1_u16.to_le_bytes(), "PUT record_type");
    assert_eq!(bytes[12..20], 1_u64.to_le_bytes(), "lsn at offset 12");
    assert_eq!(bytes[20..28], 1_u64.to_le_bytes(), "sequence at offset 20");
    assert_eq!(
        bytes[28..32],
        17_u32.to_le_bytes(),
        "PUT payload_len (8 + key 6 + value 3)"
    );
}

#[test]
fn wal_encoding_is_deterministic() {
    assert_eq!(build_valid_fixture(), build_valid_fixture());
}

#[test]
fn wal_bad_magic_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_BAD_MAGIC);
    assert_eq!(bytes, build_bad_magic_fixture());
    match decode_record(&bytes, 0, MAX_PAYLOAD_LEN) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::BadMagic { offset: 0, .. },
        } => {}
        other => panic!("expected BadMagic, got {other:?}"),
    }
}

#[test]
fn wal_unsupported_version_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_UNSUPPORTED_VERSION);
    assert_eq!(bytes, build_unsupported_version_fixture());
    match decode_record(&bytes, 0, MAX_PAYLOAD_LEN) {
        DecodeRecordResult::Invalid {
            warning:
                WalWarning::UnsupportedVersion {
                    offset: 0,
                    found: UNSUPPORTED_VERSION,
                },
        } => {}
        other => panic!("expected UnsupportedVersion, got {other:?}"),
    }
}

#[test]
fn wal_bad_checksum_fixture_is_rejected() {
    let bytes = read_fixture(FIXTURE_BAD_CHECKSUM);
    assert_eq!(bytes, build_bad_checksum_fixture());
    match decode_record(&bytes, 0, MAX_PAYLOAD_LEN) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::BadPayloadChecksum { offset: 0, .. },
        } => {}
        other => panic!("expected BadPayloadChecksum, got {other:?}"),
    }
}

#[test]
fn wal_partial_tail_fixture_yields_durable_prefix_then_incomplete() {
    let bytes = read_fixture(FIXTURE_PARTIAL_TAIL);
    assert_eq!(bytes, build_partial_tail_fixture());

    let first = match decode_record(&bytes, 0, MAX_PAYLOAD_LEN) {
        DecodeRecordResult::Complete { record, bytes_read } => {
            assert_eq!(record, expected_records()[0]);
            bytes_read
        }
        other => panic!("expected first record Complete, got {other:?}"),
    };
    match decode_record(&bytes[first..], first as u64, MAX_PAYLOAD_LEN) {
        DecodeRecordResult::Incomplete {
            warning: WalWarning::PartialPayload { .. },
        } => {}
        other => panic!("expected PartialPayload for truncated tail, got {other:?}"),
    }
}

/// Regenerates all committed WAL fixtures. Run only for an intentional,
/// spec-reviewed format change; the committed files are the source of truth.
#[test]
#[ignore = "writes committed golden fixtures; run only on intentional format change"]
fn regenerate_wal_fixtures() {
    let dir = fixtures_dir();
    fs::create_dir_all(&dir).expect("create fixtures dir");
    let fixtures: &[(&str, Vec<u8>)] = &[
        (FIXTURE_VALID, build_valid_fixture()),
        (FIXTURE_BAD_MAGIC, build_bad_magic_fixture()),
        (
            FIXTURE_UNSUPPORTED_VERSION,
            build_unsupported_version_fixture(),
        ),
        (FIXTURE_BAD_CHECKSUM, build_bad_checksum_fixture()),
        (FIXTURE_PARTIAL_TAIL, build_partial_tail_fixture()),
    ];
    for (name, bytes) in fixtures {
        fs::write(dir.join(name), bytes).expect("write fixture");
    }
}
