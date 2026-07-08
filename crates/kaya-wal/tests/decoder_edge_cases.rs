//! Targeted WAL decoder edge-case tests (ROADMAP M1 debt item).
//!
//! Each test crafts a valid record with the real encoder, corrupts the
//! specific wire bytes for one failure mode, and asserts the decoder returns
//! exactly the expected `WalWarning` variant without panicking.
//!
//! Header layout (little-endian, `WAL_HEADER_LEN` = 40 bytes):
//! ```text
//! [0..4)   magic        [4..6)   version      [6..8)   header_len
//! [8..10)  flags        [10..12) record_type  [12..20) lsn
//! [20..28) sequence     [28..32) payload_len  [32..36) header_crc
//! [36..40) payload_crc  [40..)   payload
//! ```

use std::sync::Arc;

use kaya_core::{crc32c, DurabilityMode, Lsn, SequenceNumber, WalConfig};
use kaya_io::{Disk, RelativePath, SimDisk};
use kaya_wal::{
    decode_record, encode_record, recover_wal, DecodeRecordResult, WalPayload, WalRecord,
    WalWarning, WalWriter, WAL_HEADER_LEN,
};

const MAX_PAYLOAD: u32 = 1024;

fn sample_put() -> WalRecord {
    WalRecord::new(
        Lsn::new(1),
        SequenceNumber::new(1),
        WalPayload::Put {
            key: b"user:1".to_vec(),
            value: b"Ada".to_vec(),
        },
    )
}

fn encoded_put() -> Vec<u8> {
    encode_record(&sample_put()).expect("record encodes")
}

/// Recompute the header CRC after a deliberate header mutation so that only
/// the intended corruption trips the decoder (proves the specific check
/// fires even when the checksum is self-consistent).
fn rewrite_header_crc(bytes: &mut [u8]) {
    bytes[32..36].copy_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c(&bytes[..WAL_HEADER_LEN]);
    bytes[32..36].copy_from_slice(&crc.to_le_bytes());
}

/// Recompute the payload CRC after a deliberate payload mutation.
fn rewrite_payload_crc(bytes: &mut [u8]) {
    let crc = crc32c(&bytes[WAL_HEADER_LEN..]);
    bytes[36..40].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn partial_header_is_incomplete() {
    let encoded = encoded_put();
    for cut in [0, 1, WAL_HEADER_LEN - 1] {
        match decode_record(&encoded[..cut], 7, MAX_PAYLOAD) {
            DecodeRecordResult::Incomplete {
                warning: WalWarning::PartialHeader { offset },
            } => assert_eq!(offset, 7),
            other => panic!("cut={cut}: unexpected decode result: {other:?}"),
        }
    }
}

#[test]
fn partial_payload_is_incomplete() {
    let encoded = encoded_put();
    let cut = encoded.len() - 1;
    match decode_record(&encoded[..cut], 0, MAX_PAYLOAD) {
        DecodeRecordResult::Incomplete {
            warning:
                WalWarning::PartialPayload {
                    offset,
                    expected,
                    actual,
                },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(expected, encoded.len());
            assert_eq!(actual, cut);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn unsupported_version_is_invalid() {
    let mut encoded = encoded_put();
    encoded[4..6].copy_from_slice(&2_u16.to_le_bytes());
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::UnsupportedVersion { offset, found },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(found, 2);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn bad_header_length_is_invalid() {
    let mut encoded = encoded_put();
    encoded[6..8].copy_from_slice(&32_u16.to_le_bytes());
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::BadHeaderLength { offset, found },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(found, 32);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn unknown_flags_are_invalid() {
    let mut encoded = encoded_put();
    encoded[8..10].copy_from_slice(&0x0004_u16.to_le_bytes());
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::UnknownFlags { offset, found },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(found, 0x0004);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn unknown_record_type_is_invalid() {
    let mut encoded = encoded_put();
    encoded[10..12].copy_from_slice(&99_u16.to_le_bytes());
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::UnknownRecordType { offset, found },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(found, 99);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn oversized_length_field_is_invalid() {
    let mut encoded = encoded_put();
    encoded[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::OversizedPayload { offset, found, max },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(found, u32::MAX);
            assert_eq!(max, MAX_PAYLOAD);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn payload_over_decoder_limit_is_invalid() {
    // The record itself is untouched; the decoder's max_payload_len contract
    // must still reject it.
    let encoded = encoded_put();
    let payload_len = (encoded.len() - WAL_HEADER_LEN) as u32;
    let max = payload_len - 1;
    match decode_record(&encoded, 0, max) {
        DecodeRecordResult::Invalid {
            warning:
                WalWarning::OversizedPayload {
                    offset,
                    found,
                    max: reported_max,
                },
        } => {
            assert_eq!(offset, 0);
            assert_eq!(found, payload_len);
            assert_eq!(reported_max, max);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn corrupted_lsn_bytes_fail_header_checksum() {
    let mut encoded = encoded_put();
    encoded[12] ^= 0xff;
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning:
                WalWarning::BadHeaderChecksum {
                    offset,
                    expected,
                    actual,
                },
        } => {
            assert_eq!(offset, 0);
            assert_ne!(expected, actual);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn corrupted_header_crc_field_fails_header_checksum() {
    let mut encoded = encoded_put();
    encoded[33] ^= 0xff;
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::BadHeaderChecksum { .. },
        } => {}
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn corrupted_payload_byte_fails_payload_checksum() {
    let mut encoded = encoded_put();
    let last = encoded.len() - 1;
    encoded[last] ^= 0xff;
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning:
                WalWarning::BadPayloadChecksum {
                    offset,
                    expected,
                    actual,
                },
        } => {
            assert_eq!(offset, 0);
            assert_ne!(expected, actual);
        }
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn noop_with_payload_is_malformed() {
    // NOOP records must carry an empty payload; splice one byte in with
    // self-consistent CRCs so only the payload-shape check can fire.
    let noop = WalRecord::new(Lsn::new(1), SequenceNumber::new(1), WalPayload::Noop);
    let mut encoded = encode_record(&noop).expect("record encodes");
    encoded.push(0xAB);
    encoded[28..32].copy_from_slice(&1_u32.to_le_bytes());
    rewrite_payload_crc(&mut encoded);
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::MalformedPayload { offset, .. },
        } => assert_eq!(offset, 0),
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn put_with_truncated_payload_body_is_malformed() {
    // Shrink the payload below the 8-byte PUT length prelude, keeping the
    // header length field and both CRCs consistent with the shrunken bytes.
    let mut encoded = encoded_put();
    encoded.truncate(WAL_HEADER_LEN + 4);
    encoded[28..32].copy_from_slice(&4_u32.to_le_bytes());
    rewrite_payload_crc(&mut encoded);
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::MalformedPayload { offset, .. },
        } => assert_eq!(offset, 0),
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn put_with_lying_key_length_is_malformed() {
    // The embedded key_len claims more bytes than the payload holds.
    let mut encoded = encoded_put();
    encoded[WAL_HEADER_LEN..WAL_HEADER_LEN + 4].copy_from_slice(&100_u32.to_le_bytes());
    rewrite_payload_crc(&mut encoded);
    rewrite_header_crc(&mut encoded);
    match decode_record(&encoded, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::MalformedPayload { offset, .. },
        } => assert_eq!(offset, 0),
        other => panic!("unexpected decode result: {other:?}"),
    }
}

#[test]
fn decoder_is_stateless_across_a_corrupt_record() {
    // decode_record is per-record: an Invalid result at one offset must not
    // affect decoding a well-formed record that follows it in the buffer.
    let first = encoded_put();
    let mut second = encoded_put();
    let last = second.len() - 1;
    second[last] ^= 0xff; // BadPayloadChecksum
    let third_record = WalRecord::new(
        Lsn::new(3),
        SequenceNumber::new(3),
        WalPayload::Delete {
            key: b"user:1".to_vec(),
        },
    );
    let third = encode_record(&third_record).expect("record encodes");

    let mut buffer = first.clone();
    buffer.extend_from_slice(&second);
    buffer.extend_from_slice(&third);

    match decode_record(&buffer, 0, MAX_PAYLOAD) {
        DecodeRecordResult::Complete { bytes_read, .. } => assert_eq!(bytes_read, first.len()),
        other => panic!("unexpected decode result for first record: {other:?}"),
    }
    let second_offset = first.len();
    match decode_record(&buffer[second_offset..], second_offset as u64, MAX_PAYLOAD) {
        DecodeRecordResult::Invalid {
            warning: WalWarning::BadPayloadChecksum { offset, .. },
        } => assert_eq!(offset, second_offset as u64),
        other => panic!("unexpected decode result for corrupt record: {other:?}"),
    }
    let third_offset = first.len() + second.len();
    match decode_record(&buffer[third_offset..], third_offset as u64, MAX_PAYLOAD) {
        DecodeRecordResult::Complete { record, bytes_read } => {
            assert_eq!(record, third_record);
            assert_eq!(bytes_read, third.len());
        }
        other => panic!("unexpected decode result for third record: {other:?}"),
    }
}

// Recovery semantics: recover_wal stops at the first warning, keeps only the
// durable prefix, truncates the corrupt tail, and reports the warning. Valid
// records located after the corruption are intentionally discarded.
#[test]
fn recovery_stops_at_corruption_and_truncates_tail() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime builds");
    runtime.block_on(async {
        let disk = Arc::new(SimDisk::new());
        let config = WalConfig::default();
        let writer = WalWriter::open(config.clone(), disk.clone())
            .await
            .expect("writer opens");

        for key in 0_u8..3 {
            writer
                .append(
                    WalPayload::Put {
                        key: vec![key],
                        value: vec![key, key],
                    },
                    DurabilityMode::Strict,
                )
                .await
                .expect("append succeeds");
        }

        // Locate the single WAL segment and the second record inside it.
        let wal_dir = RelativePath::new("wal").expect("valid path");
        let entries = disk.list_dir(&wal_dir).await.expect("list_dir succeeds");
        assert_eq!(entries.len(), 1, "expected exactly one segment");
        let segment = entries[0].path.clone();
        let len = disk.file_len(&segment).await.expect("file_len succeeds");
        let mut bytes = vec![0_u8; len as usize];
        let read = disk
            .read_at(&segment, 0, &mut bytes)
            .await
            .expect("read_at succeeds");
        assert_eq!(read as u64, len);

        let first_len = match decode_record(&bytes, 0, config.max_record_bytes) {
            DecodeRecordResult::Complete { bytes_read, .. } => bytes_read,
            other => panic!("first record should decode: {other:?}"),
        };

        // Flip the first payload byte of the second record on disk.
        let corrupt_at = (first_len + WAL_HEADER_LEN) as u64;
        let flipped = [bytes[corrupt_at as usize] ^ 0xff];
        disk.write_at(&segment, corrupt_at, &flipped)
            .await
            .expect("write_at succeeds");

        let report = recover_wal(config, disk.clone())
            .await
            .expect("recovery succeeds");

        assert_eq!(
            report.records.len(),
            1,
            "only the record before the corruption survives"
        );
        assert_eq!(report.records[0].record.lsn, Lsn::FIRST);
        assert_eq!(report.valid_bytes, first_len as u64);
        assert!(report.truncated_bytes > 0, "corrupt tail must be truncated");
        assert!(
            report
                .warnings
                .iter()
                .any(|w| matches!(w, WalWarning::BadPayloadChecksum { .. })),
            "expected a BadPayloadChecksum warning, got {:?}",
            report.warnings
        );
        assert!(
            report
                .warnings
                .iter()
                .any(|w| matches!(w, WalWarning::TailTruncated { .. })),
            "expected a TailTruncated warning, got {:?}",
            report.warnings
        );

        // The segment on disk was physically truncated to the durable prefix.
        let new_len = disk.file_len(&segment).await.expect("file_len succeeds");
        assert_eq!(new_len, first_len as u64);
    });
}
