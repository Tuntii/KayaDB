use std::fmt;

use kaya_core::{crc32c, Bytes, KayaError, Lsn, Result, SequenceNumber};

pub const WAL_MAGIC: u32 = 0x4b41_5941;
pub const WAL_VERSION: u16 = 1;
pub const WAL_HEADER_LEN: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalRecordType {
    Put = 1,
    Delete = 2,
    Noop = 3,
}

impl WalRecordType {
    pub fn from_wire(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::Put),
            2 => Some(Self::Delete),
            3 => Some(Self::Noop),
            _ => None,
        }
    }

    pub const fn as_wire(self) -> u16 {
        self as u16
    }
}

impl fmt::Display for WalRecordType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put => write!(f, "PUT"),
            Self::Delete => write!(f, "DELETE"),
            Self::Noop => write!(f, "NOOP"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalPayload {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
    Noop,
}

impl WalPayload {
    pub const fn record_type(&self) -> WalRecordType {
        match self {
            Self::Put { .. } => WalRecordType::Put,
            Self::Delete { .. } => WalRecordType::Delete,
            Self::Noop => WalRecordType::Noop,
        }
    }

    pub fn key_len(&self) -> Option<usize> {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => Some(key.len()),
            Self::Noop => None,
        }
    }

    pub fn value_len(&self) -> Option<usize> {
        match self {
            Self::Put { value, .. } => Some(value.len()),
            Self::Delete { .. } | Self::Noop => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub flags: u16,
    pub lsn: Lsn,
    pub sequence: SequenceNumber,
    pub payload: WalPayload,
}

impl WalRecord {
    pub fn new(lsn: Lsn, sequence: SequenceNumber, payload: WalPayload) -> Self {
        Self {
            flags: 0,
            lsn,
            sequence,
            payload,
        }
    }

    pub const fn record_type(&self) -> WalRecordType {
        self.payload.record_type()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalWarning {
    PartialHeader {
        offset: u64,
    },
    PartialPayload {
        offset: u64,
        expected: usize,
        actual: usize,
    },
    BadMagic {
        offset: u64,
        found: u32,
    },
    UnsupportedVersion {
        offset: u64,
        found: u16,
    },
    BadHeaderLength {
        offset: u64,
        found: u16,
    },
    UnknownFlags {
        offset: u64,
        found: u16,
    },
    UnknownRecordType {
        offset: u64,
        found: u16,
    },
    OversizedPayload {
        offset: u64,
        found: u32,
        max: u32,
    },
    BadHeaderChecksum {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    BadPayloadChecksum {
        offset: u64,
        expected: u32,
        actual: u32,
    },
    MalformedPayload {
        offset: u64,
        message: String,
    },
    NonMonotonicLsn {
        offset: u64,
        expected: u64,
        found: u64,
    },
    TailTruncated {
        path: String,
        valid_len: u64,
        truncated_bytes: u64,
    },
    TrailingSegmentsIgnored {
        count: usize,
    },
}

impl fmt::Display for WalWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialHeader { offset } => write!(f, "PartialHeader offset={offset}"),
            Self::PartialPayload {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "PartialPayload offset={offset} expected={expected} actual={actual}"
            ),
            Self::BadMagic { offset, found } => {
                write!(f, "BadMagic offset={offset} found=0x{found:08x}")
            }
            Self::UnsupportedVersion { offset, found } => {
                write!(f, "UnsupportedVersion offset={offset} found={found}")
            }
            Self::BadHeaderLength { offset, found } => {
                write!(f, "BadHeaderLength offset={offset} found={found}")
            }
            Self::UnknownFlags { offset, found } => {
                write!(f, "UnknownFlags offset={offset} found=0x{found:04x}")
            }
            Self::UnknownRecordType { offset, found } => {
                write!(f, "UnknownRecordType offset={offset} found={found}")
            }
            Self::OversizedPayload { offset, found, max } => write!(
                f,
                "OversizedPayload offset={offset} found={found} max={max}"
            ),
            Self::BadHeaderChecksum {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "BadHeaderChecksum offset={offset} expected=0x{expected:08x} actual=0x{actual:08x}"
            ),
            Self::BadPayloadChecksum {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "BadPayloadChecksum offset={offset} expected=0x{expected:08x} actual=0x{actual:08x}"
            ),
            Self::MalformedPayload { offset, message } => {
                write!(f, "MalformedPayload offset={offset} {message}")
            }
            Self::NonMonotonicLsn {
                offset,
                expected,
                found,
            } => write!(
                f,
                "NonMonotonicLsn offset={offset} expected={expected} found={found}"
            ),
            Self::TailTruncated {
                path,
                valid_len,
                truncated_bytes,
            } => write!(
                f,
                "TailTruncated path={path} valid_len={valid_len} truncated_bytes={truncated_bytes}"
            ),
            Self::TrailingSegmentsIgnored { count } => {
                write!(f, "TrailingSegmentsIgnored count={count}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeRecordResult {
    Complete {
        record: WalRecord,
        bytes_read: usize,
    },
    Incomplete {
        warning: WalWarning,
    },
    Invalid {
        warning: WalWarning,
    },
}

pub fn encode_record(record: &WalRecord) -> Result<Vec<u8>> {
    let payload = encode_payload(&record.payload)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| KayaError::invalid_argument("WAL payload length does not fit into u32"))?;
    let payload_crc = crc32c(&payload);

    let mut encoded = Vec::with_capacity(WAL_HEADER_LEN + payload.len());
    encoded.extend_from_slice(&WAL_MAGIC.to_le_bytes());
    encoded.extend_from_slice(&WAL_VERSION.to_le_bytes());
    encoded.extend_from_slice(&(WAL_HEADER_LEN as u16).to_le_bytes());
    encoded.extend_from_slice(&record.flags.to_le_bytes());
    encoded.extend_from_slice(&record.record_type().as_wire().to_le_bytes());
    encoded.extend_from_slice(&record.lsn.get().to_le_bytes());
    encoded.extend_from_slice(&record.sequence.get().to_le_bytes());
    encoded.extend_from_slice(&payload_len.to_le_bytes());
    encoded.extend_from_slice(&0_u32.to_le_bytes());
    encoded.extend_from_slice(&payload_crc.to_le_bytes());

    let header_crc = crc32c(&encoded[..WAL_HEADER_LEN]);
    encoded[32..36].copy_from_slice(&header_crc.to_le_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

pub fn decode_record(input: &[u8], offset: u64, max_payload_len: u32) -> DecodeRecordResult {
    if input.is_empty() {
        return DecodeRecordResult::Incomplete {
            warning: WalWarning::PartialHeader { offset },
        };
    }
    if input.len() < WAL_HEADER_LEN {
        return DecodeRecordResult::Incomplete {
            warning: WalWarning::PartialHeader { offset },
        };
    }

    let magic = read_u32(&input[0..4]);
    if magic != WAL_MAGIC {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::BadMagic {
                offset,
                found: magic,
            },
        };
    }

    let version = read_u16(&input[4..6]);
    if version != WAL_VERSION {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::UnsupportedVersion {
                offset,
                found: version,
            },
        };
    }

    let header_len = read_u16(&input[6..8]);
    if usize::from(header_len) != WAL_HEADER_LEN {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::BadHeaderLength {
                offset,
                found: header_len,
            },
        };
    }

    let flags = read_u16(&input[8..10]);
    if flags != 0 {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::UnknownFlags {
                offset,
                found: flags,
            },
        };
    }

    let record_type_raw = read_u16(&input[10..12]);
    let Some(record_type) = WalRecordType::from_wire(record_type_raw) else {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::UnknownRecordType {
                offset,
                found: record_type_raw,
            },
        };
    };

    let lsn = read_u64(&input[12..20]);
    let sequence = read_u64(&input[20..28]);
    let payload_len = read_u32(&input[28..32]);
    if payload_len > max_payload_len {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::OversizedPayload {
                offset,
                found: payload_len,
                max: max_payload_len,
            },
        };
    }

    let actual_header_crc = read_u32(&input[32..36]);
    let mut header = [0_u8; WAL_HEADER_LEN];
    header.copy_from_slice(&input[..WAL_HEADER_LEN]);
    header[32..36].copy_from_slice(&0_u32.to_le_bytes());
    let expected_header_crc = crc32c(&header);
    if actual_header_crc != expected_header_crc {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::BadHeaderChecksum {
                offset,
                expected: expected_header_crc,
                actual: actual_header_crc,
            },
        };
    }

    let actual_payload_crc = read_u32(&input[36..40]);
    let total_len = WAL_HEADER_LEN + payload_len as usize;
    if input.len() < total_len {
        return DecodeRecordResult::Incomplete {
            warning: WalWarning::PartialPayload {
                offset,
                expected: total_len,
                actual: input.len(),
            },
        };
    }

    let payload_bytes = &input[WAL_HEADER_LEN..total_len];
    let expected_payload_crc = crc32c(payload_bytes);
    if actual_payload_crc != expected_payload_crc {
        return DecodeRecordResult::Invalid {
            warning: WalWarning::BadPayloadChecksum {
                offset,
                expected: expected_payload_crc,
                actual: actual_payload_crc,
            },
        };
    }

    let payload = match decode_payload(record_type, payload_bytes) {
        Ok(payload) => payload,
        Err(error) => {
            return DecodeRecordResult::Invalid {
                warning: WalWarning::MalformedPayload {
                    offset,
                    message: error.to_string(),
                },
            };
        }
    };

    DecodeRecordResult::Complete {
        record: WalRecord {
            flags,
            lsn: Lsn::new(lsn),
            sequence: SequenceNumber::new(sequence),
            payload,
        },
        bytes_read: total_len,
    }
}

fn encode_payload(payload: &WalPayload) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    match payload {
        WalPayload::Put { key, value } => {
            let key_len = u32::try_from(key.len()).map_err(|_| {
                KayaError::invalid_argument("WAL PUT key length does not fit into u32")
            })?;
            let value_len = u32::try_from(value.len()).map_err(|_| {
                KayaError::invalid_argument("WAL PUT value length does not fit into u32")
            })?;
            encoded.extend_from_slice(&key_len.to_le_bytes());
            encoded.extend_from_slice(&value_len.to_le_bytes());
            encoded.extend_from_slice(key);
            encoded.extend_from_slice(value);
        }
        WalPayload::Delete { key } => {
            let key_len = u32::try_from(key.len()).map_err(|_| {
                KayaError::invalid_argument("WAL DELETE key length does not fit into u32")
            })?;
            encoded.extend_from_slice(&key_len.to_le_bytes());
            encoded.extend_from_slice(key);
        }
        WalPayload::Noop => {}
    }
    Ok(encoded)
}

fn decode_payload(record_type: WalRecordType, payload: &[u8]) -> Result<WalPayload> {
    match record_type {
        WalRecordType::Put => {
            if payload.len() < 8 {
                return Err(KayaError::corruption("PUT payload header is too short"));
            }
            let key_len = read_u32(&payload[0..4]) as usize;
            let value_len = read_u32(&payload[4..8]) as usize;
            let expected = 8_usize
                .checked_add(key_len)
                .and_then(|len| len.checked_add(value_len))
                .ok_or_else(|| KayaError::corruption("PUT payload length overflows usize"))?;
            if payload.len() != expected {
                return Err(KayaError::corruption(format!(
                    "PUT payload length mismatch: expected {expected}, got {}",
                    payload.len()
                )));
            }
            let key = payload[8..8 + key_len].to_vec();
            let value = payload[8 + key_len..].to_vec();
            Ok(WalPayload::Put { key, value })
        }
        WalRecordType::Delete => {
            if payload.len() < 4 {
                return Err(KayaError::corruption("DELETE payload header is too short"));
            }
            let key_len = read_u32(&payload[0..4]) as usize;
            let expected = 4_usize
                .checked_add(key_len)
                .ok_or_else(|| KayaError::corruption("DELETE payload length overflows usize"))?;
            if payload.len() != expected {
                return Err(KayaError::corruption(format!(
                    "DELETE payload length mismatch: expected {expected}, got {}",
                    payload.len()
                )));
            }
            Ok(WalPayload::Delete {
                key: payload[4..].to_vec(),
            })
        }
        WalRecordType::Noop => {
            if !payload.is_empty() {
                return Err(KayaError::corruption("NOOP payload must be empty"));
            }
            Ok(WalPayload::Noop)
        }
    }
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("slice length checked by caller"))
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("slice length checked by caller"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("slice length checked by caller"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_roundtrip() {
        let record = WalRecord::new(
            Lsn::new(1),
            SequenceNumber::new(1),
            WalPayload::Put {
                key: b"user:1".to_vec(),
                value: b"Ada".to_vec(),
            },
        );
        let encoded = encode_record(&record).expect("record encodes");
        match decode_record(&encoded, 0, 1024) {
            DecodeRecordResult::Complete {
                record: decoded, ..
            } => assert_eq!(decoded, record),
            other => panic!("unexpected decode result: {other:?}"),
        }
    }

    #[test]
    fn rejects_bad_magic_without_panic() {
        let record = WalRecord::new(Lsn::new(1), SequenceNumber::new(1), WalPayload::Noop);
        let mut encoded = encode_record(&record).expect("record encodes");
        encoded[0] = 0;
        match decode_record(&encoded, 0, 1024) {
            DecodeRecordResult::Invalid {
                warning: WalWarning::BadMagic { .. },
            } => {}
            other => panic!("unexpected decode result: {other:?}"),
        }
    }
}
