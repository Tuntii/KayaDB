use std::fs;
use std::path::Path;

use kaya_core::{crc32c, Bytes, KayaError, Result, SequenceNumber};

pub const MANIFEST_MAGIC: u32 = 0x4b4d414e; // "KMAN"
pub const MANIFEST_VERSION: u16 = 1;
/// Fixed size of the manifest frame header in bytes.
pub const MANIFEST_HEADER_LEN: usize = 32;

pub const MANIFEST_FILE_NAME: &str = "MANIFEST-000001";
pub const CURRENT_FILE_NAME: &str = "CURRENT";
pub const CURRENT_TMP_FILE_NAME: &str = "CURRENT.tmp";

// ---- Edit type ----

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ManifestEditType {
    CreateTable = 1,
    DeleteTable = 2,
    SetLastSequence = 3,
}

impl ManifestEditType {
    fn from_wire(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::CreateTable),
            2 => Some(Self::DeleteTable),
            3 => Some(Self::SetLastSequence),
            _ => None,
        }
    }
}

// ---- Table metadata ----

/// Metadata for one live SSTable, stored in every `CREATE_TABLE` edit.
///
/// Payload layout (all LE integers, fixed part = 60 bytes):
///   table_id:           u64  offset 0
///   level:              u32  offset 8
///   path_len:           u32  offset 12
///   smallest_key_len:   u32  offset 16
///   largest_key_len:    u32  offset 20
///   min_sequence:       u64  offset 24
///   max_sequence:       u64  offset 32
///   entry_count:        u64  offset 40
///   file_size:          u64  offset 48
///   footer_checksum:    u32  offset 56
///   path:               bytes  (path_len)
///   smallest_key:       bytes  (smallest_key_len)
///   largest_key:        bytes  (largest_key_len)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMetadata {
    pub table_id: u64,
    pub level: u32,
    pub path: String,
    pub smallest_key: Bytes,
    pub largest_key: Bytes,
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
    pub entry_count: u64,
    pub file_size: u64,
    pub footer_checksum: u32,
}

// ---- Edit enum ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestEdit {
    CreateTable(TableMetadata),
    DeleteTable { table_id: u64 },
    SetLastSequence { sequence: SequenceNumber },
}

// ---- In-memory manifest state ----

#[derive(Debug, Clone)]
pub struct ManifestState {
    pub live_tables: Vec<TableMetadata>,
    pub last_sequence: SequenceNumber,
    pub last_edit_seq: u64,
}

impl Default for ManifestState {
    fn default() -> Self {
        Self {
            live_tables: Vec::new(),
            last_sequence: SequenceNumber::FIRST,
            last_edit_seq: 0,
        }
    }
}

impl ManifestState {
    pub fn apply(&mut self, edit: &ManifestEdit, edit_seq: u64) {
        self.last_edit_seq = edit_seq;
        match edit {
            ManifestEdit::CreateTable(meta) => {
                self.live_tables.push(meta.clone());
            }
            ManifestEdit::DeleteTable { table_id } => {
                self.live_tables.retain(|t| t.table_id != *table_id);
            }
            ManifestEdit::SetLastSequence { sequence } => {
                self.last_sequence = *sequence;
            }
        }
    }

    /// Tables sorted newest-first (highest table_id first) for point-lookup
    /// priority ordering.
    pub fn tables_newest_first(&self) -> Vec<&TableMetadata> {
        let mut tables: Vec<&TableMetadata> = self.live_tables.iter().collect();
        tables.sort_by(|a, b| b.table_id.cmp(&a.table_id));
        tables
    }
}

// ---- Inspect output ----

#[derive(Debug, Clone)]
pub struct ManifestInspection {
    pub path: String,
    pub state: ManifestState,
    pub warnings: Vec<String>,
}

// ---- Encoding helpers ----

fn put_u16_le(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u32_le(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u64_le(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    if offset + 2 > bytes.len() {
        return Err(KayaError::corruption("truncated u16"));
    }
    Ok(u16::from_le_bytes(
        bytes[offset..offset + 2].try_into().unwrap(),
    ))
}

fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    if offset + 4 > bytes.len() {
        return Err(KayaError::corruption("truncated u32"));
    }
    Ok(u32::from_le_bytes(
        bytes[offset..offset + 4].try_into().unwrap(),
    ))
}

fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64> {
    if offset + 8 > bytes.len() {
        return Err(KayaError::corruption("truncated u64"));
    }
    Ok(u64::from_le_bytes(
        bytes[offset..offset + 8].try_into().unwrap(),
    ))
}

// ---- Payload encode/decode ----

fn encode_table_metadata(meta: &TableMetadata) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u64_le(&mut buf, meta.table_id);
    put_u32_le(&mut buf, meta.level);
    put_u32_le(&mut buf, meta.path.len() as u32);
    put_u32_le(&mut buf, meta.smallest_key.len() as u32);
    put_u32_le(&mut buf, meta.largest_key.len() as u32);
    put_u64_le(&mut buf, meta.min_sequence.get());
    put_u64_le(&mut buf, meta.max_sequence.get());
    put_u64_le(&mut buf, meta.entry_count);
    put_u64_le(&mut buf, meta.file_size);
    put_u32_le(&mut buf, meta.footer_checksum);
    buf.extend_from_slice(meta.path.as_bytes());
    buf.extend_from_slice(&meta.smallest_key);
    buf.extend_from_slice(&meta.largest_key);
    buf
}

fn decode_table_metadata(bytes: &[u8]) -> Result<TableMetadata> {
    if bytes.len() < 60 {
        return Err(KayaError::corruption("table metadata too short"));
    }
    let table_id = read_u64_le(bytes, 0)?;
    let level = read_u32_le(bytes, 8)?;
    let path_len = read_u32_le(bytes, 12)? as usize;
    let smallest_len = read_u32_le(bytes, 16)? as usize;
    let largest_len = read_u32_le(bytes, 20)? as usize;
    let min_sequence = SequenceNumber::new(read_u64_le(bytes, 24)?);
    let max_sequence = SequenceNumber::new(read_u64_le(bytes, 32)?);
    let entry_count = read_u64_le(bytes, 40)?;
    let file_size = read_u64_le(bytes, 48)?;
    let footer_checksum = read_u32_le(bytes, 56)?;
    let mut offset = 60;
    if offset + path_len + smallest_len + largest_len > bytes.len() {
        return Err(KayaError::corruption(
            "table metadata variable fields truncated",
        ));
    }
    let path = String::from_utf8(bytes[offset..offset + path_len].to_vec())
        .map_err(|_| KayaError::corruption("table path is not valid UTF-8"))?;
    offset += path_len;
    let smallest_key = bytes[offset..offset + smallest_len].to_vec();
    offset += smallest_len;
    let largest_key = bytes[offset..offset + largest_len].to_vec();
    Ok(TableMetadata {
        table_id,
        level,
        path,
        smallest_key,
        largest_key,
        min_sequence,
        max_sequence,
        entry_count,
        file_size,
        footer_checksum,
    })
}

fn encode_edit_payload(edit: &ManifestEdit) -> (u16, Vec<u8>) {
    match edit {
        ManifestEdit::CreateTable(meta) => (
            ManifestEditType::CreateTable as u16,
            encode_table_metadata(meta),
        ),
        ManifestEdit::DeleteTable { table_id } => {
            let mut buf = Vec::new();
            put_u64_le(&mut buf, *table_id);
            (ManifestEditType::DeleteTable as u16, buf)
        }
        ManifestEdit::SetLastSequence { sequence } => {
            let mut buf = Vec::new();
            put_u64_le(&mut buf, sequence.get());
            (ManifestEditType::SetLastSequence as u16, buf)
        }
    }
}

// ---- Frame encode ----
//
// Frame layout (32-byte header + payload):
//   magic:           u32  offset 0   (MANIFEST_MAGIC)
//   version:         u16  offset 4
//   header_len:      u16  offset 6   (always 32)
//   record_type:     u16  offset 8
//   flags:           u16  offset 10  (reserved, 0)
//   edit_seq:        u64  offset 12
//   payload_len:     u32  offset 20
//   header_crc32c:   u32  offset 24  (CRC over bytes 0..24)
//   payload_crc32c:  u32  offset 28
//   payload:         var

pub fn encode_manifest_edit(edit: &ManifestEdit, edit_seq: u64) -> Vec<u8> {
    let (record_type, payload) = encode_edit_payload(edit);
    let payload_crc = crc32c(&payload);

    let mut header = Vec::with_capacity(MANIFEST_HEADER_LEN);
    put_u32_le(&mut header, MANIFEST_MAGIC); // 0
    put_u16_le(&mut header, MANIFEST_VERSION); // 4
    put_u16_le(&mut header, MANIFEST_HEADER_LEN as u16); // 6
    put_u16_le(&mut header, record_type); // 8
    put_u16_le(&mut header, 0u16); // 10 flags
    put_u64_le(&mut header, edit_seq); // 12
    put_u32_le(&mut header, payload.len() as u32); // 20
                                                   // Header CRC covers first 24 bytes (before the two CRC fields).
    let header_crc = crc32c(&header[..24]);
    put_u32_le(&mut header, header_crc); // 24
    put_u32_le(&mut header, payload_crc); // 28
    debug_assert_eq!(header.len(), MANIFEST_HEADER_LEN);

    let mut out = header;
    out.extend_from_slice(&payload);
    out
}

// ---- Frame decode ----

pub enum DecodeEditResult {
    Complete {
        edit: ManifestEdit,
        edit_seq: u64,
        bytes_read: usize,
    },
    /// Not enough bytes yet; caller should stop and treat tail as incomplete.
    Incomplete,
    /// Bytes present but invalid (bad magic or CRC); treat as corrupt tail.
    Invalid { message: String },
}

pub fn decode_manifest_edit(bytes: &[u8]) -> DecodeEditResult {
    if bytes.len() < MANIFEST_HEADER_LEN {
        return DecodeEditResult::Incomplete;
    }
    let magic = match read_u32_le(bytes, 0) {
        Ok(v) => v,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    if magic != MANIFEST_MAGIC {
        return DecodeEditResult::Invalid {
            message: format!("bad manifest magic: {magic:#010x}"),
        };
    }
    let version = match read_u16_le(bytes, 4) {
        Ok(v) => v,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    if version != MANIFEST_VERSION {
        return DecodeEditResult::Invalid {
            message: format!("unsupported manifest version: {version}"),
        };
    }
    // Validate header CRC (covers bytes 0..24).
    let expected_hcrc = match read_u32_le(bytes, 24) {
        Ok(v) => v,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    let actual_hcrc = crc32c(&bytes[..24]);
    if expected_hcrc != actual_hcrc {
        return DecodeEditResult::Invalid {
            message: "manifest header CRC mismatch".into(),
        };
    }
    let record_type = match read_u16_le(bytes, 8) {
        Ok(v) => v,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    let edit_seq = match read_u64_le(bytes, 12) {
        Ok(v) => v,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    let payload_len = match read_u32_le(bytes, 20) {
        Ok(v) => v as usize,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    let expected_pcrc = match read_u32_le(bytes, 28) {
        Ok(v) => v,
        Err(_) => return DecodeEditResult::Incomplete,
    };
    if bytes.len() < MANIFEST_HEADER_LEN + payload_len {
        return DecodeEditResult::Incomplete;
    }
    let payload = &bytes[MANIFEST_HEADER_LEN..MANIFEST_HEADER_LEN + payload_len];
    let actual_pcrc = crc32c(payload);
    if expected_pcrc != actual_pcrc {
        return DecodeEditResult::Invalid {
            message: "manifest payload CRC mismatch".into(),
        };
    }
    let edit_type = match ManifestEditType::from_wire(record_type) {
        Some(t) => t,
        None => {
            return DecodeEditResult::Invalid {
                message: format!("unknown manifest edit type: {record_type}"),
            };
        }
    };
    let edit = match edit_type {
        ManifestEditType::CreateTable => match decode_table_metadata(payload) {
            Ok(meta) => ManifestEdit::CreateTable(meta),
            Err(e) => {
                return DecodeEditResult::Invalid {
                    message: e.to_string(),
                }
            }
        },
        ManifestEditType::DeleteTable => {
            if payload.len() < 8 {
                return DecodeEditResult::Invalid {
                    message: "DELETE_TABLE payload too short".into(),
                };
            }
            match read_u64_le(payload, 0) {
                Ok(id) => ManifestEdit::DeleteTable { table_id: id },
                Err(_) => return DecodeEditResult::Incomplete,
            }
        }
        ManifestEditType::SetLastSequence => {
            if payload.len() < 8 {
                return DecodeEditResult::Invalid {
                    message: "SET_LAST_SEQUENCE payload too short".into(),
                };
            }
            match read_u64_le(payload, 0) {
                Ok(seq) => ManifestEdit::SetLastSequence {
                    sequence: SequenceNumber::new(seq),
                },
                Err(_) => return DecodeEditResult::Incomplete,
            }
        }
    };
    DecodeEditResult::Complete {
        edit,
        edit_seq,
        bytes_read: MANIFEST_HEADER_LEN + payload_len,
    }
}

// ---- Replay ----

pub fn replay_manifest(bytes: &[u8]) -> (ManifestState, Vec<String>) {
    let mut state = ManifestState::default();
    let mut warnings = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        match decode_manifest_edit(&bytes[offset..]) {
            DecodeEditResult::Complete {
                edit,
                edit_seq,
                bytes_read,
            } => {
                state.apply(&edit, edit_seq);
                offset += bytes_read;
            }
            DecodeEditResult::Incomplete => {
                if offset < bytes.len() {
                    warnings.push(format!(
                        "manifest truncated at offset {offset}: {} trailing bytes",
                        bytes.len() - offset
                    ));
                }
                break;
            }
            DecodeEditResult::Invalid { message } => {
                warnings.push(format!("manifest invalid at offset {offset}: {message}"));
                break;
            }
        }
    }
    (state, warnings)
}

// ---- Inspect helper (for kayactl) ----

pub fn inspect_manifest_path(path: impl AsRef<Path>) -> Result<ManifestInspection> {
    let bytes = fs::read(path.as_ref()).map_err(|e| KayaError::Io {
        message: e.to_string(),
    })?;
    let path_str = path.as_ref().display().to_string();
    let (state, warnings) = replay_manifest(&bytes);
    Ok(ManifestInspection {
        path: path_str,
        state,
        warnings,
    })
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u64) -> TableMetadata {
        TableMetadata {
            table_id: id,
            level: 0,
            path: format!("sst/{id:016x}.sst"),
            smallest_key: vec![0],
            largest_key: vec![255],
            min_sequence: SequenceNumber::new(1),
            max_sequence: SequenceNumber::new(10),
            entry_count: 5,
            file_size: 1024,
            footer_checksum: 0xdeadbeef,
        }
    }

    #[test]
    fn encode_decode_create_table() {
        let edit = ManifestEdit::CreateTable(meta(1));
        let bytes = encode_manifest_edit(&edit, 0);
        match decode_manifest_edit(&bytes) {
            DecodeEditResult::Complete {
                edit: decoded,
                edit_seq,
                bytes_read,
            } => {
                assert_eq!(edit, decoded);
                assert_eq!(edit_seq, 0);
                assert_eq!(bytes_read, bytes.len());
            }
            _ => panic!("expected Complete"),
        }
    }

    #[test]
    fn encode_decode_set_last_sequence() {
        let edit = ManifestEdit::SetLastSequence {
            sequence: SequenceNumber::new(42),
        };
        let bytes = encode_manifest_edit(&edit, 7);
        match decode_manifest_edit(&bytes) {
            DecodeEditResult::Complete {
                edit: decoded,
                edit_seq,
                ..
            } => {
                assert_eq!(edit, decoded);
                assert_eq!(edit_seq, 7);
            }
            _ => panic!("expected Complete"),
        }
    }

    #[test]
    fn replay_build_and_destroy() {
        let mut buf = Vec::new();
        buf.extend(encode_manifest_edit(&ManifestEdit::CreateTable(meta(1)), 0));
        buf.extend(encode_manifest_edit(&ManifestEdit::CreateTable(meta(2)), 1));
        buf.extend(encode_manifest_edit(
            &ManifestEdit::SetLastSequence {
                sequence: SequenceNumber::new(20),
            },
            2,
        ));
        buf.extend(encode_manifest_edit(
            &ManifestEdit::DeleteTable { table_id: 1 },
            3,
        ));

        let (state, warnings) = replay_manifest(&buf);
        assert!(warnings.is_empty());
        assert_eq!(state.live_tables.len(), 1);
        assert_eq!(state.live_tables[0].table_id, 2);
        assert_eq!(state.last_sequence, SequenceNumber::new(20));
        assert_eq!(state.last_edit_seq, 3);
    }

    #[test]
    fn corrupt_tail_is_truncated() {
        let mut buf = Vec::new();
        buf.extend(encode_manifest_edit(&ManifestEdit::CreateTable(meta(1)), 0));
        buf.extend(b"garbagebytes"); // corrupt tail
        let (state, warnings) = replay_manifest(&buf);
        assert!(!warnings.is_empty());
        assert_eq!(state.live_tables.len(), 1);
    }
}
