use std::fs;
use std::path::Path;

use kaya_core::{crc32c, Bytes, KayaError, Result, SequenceNumber};

pub const SST_MAGIC: u32 = 0x4b535354; // "KSST"
pub const SST_VERSION: u16 = 1;
/// Fixed size of the SSTable footer in bytes.
pub const SST_FOOTER_LEN: usize = 48;

const ENTRY_KIND_PUT: u8 = 1;
const ENTRY_KIND_DELETE: u8 = 2;

// ---- Public types ----

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstEntry {
    pub key: Bytes,
    /// `None` means tombstone (delete).
    pub value: Option<Bytes>,
    pub sequence: SequenceNumber,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFooter {
    pub index_block_offset: u64,
    pub index_block_len: u32,
    pub table_min_seq: u64,
    pub table_max_seq: u64,
    pub entry_count: u64,
    pub format_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstInspection {
    pub path: String,
    pub footer: SstFooter,
    pub entries: Vec<SstEntry>,
    pub warnings: Vec<String>,
}

// ---- Internal types ----

#[derive(Debug, Clone)]
struct IndexEntry {
    separator_key: Bytes,
    block_offset: u64,
    block_len: u32,
    first_seq: u64,
    last_seq: u64,
}

// ---- Encoding helpers ----

fn put_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

fn put_u16_le(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u32_le(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn put_u64_le(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn read_u8(bytes: &[u8], offset: usize) -> Result<u8> {
    bytes
        .get(offset)
        .copied()
        .ok_or_else(|| KayaError::corruption("truncated u8"))
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

// ---- Data block encode/decode ----
//
// Layout:
//   entry_count:   u32 LE
//   restart_count: u32 LE  (0 in MVP, no prefix compression)
//   entries:       var     (concatenated encoded entries)
//   block_crc32c:  u32 LE  (covers everything above)
//
// Entry layout:
//   key_len:   u32 LE
//   value_len: u32 LE  (0 for delete)
//   sequence:  u64 LE
//   kind:      u8      (1=put, 2=delete)
//   key:       bytes
//   value:     bytes

fn encode_entry(buf: &mut Vec<u8>, entry: &SstEntry) {
    let (value_len, kind) = match &entry.value {
        Some(v) => (v.len() as u32, ENTRY_KIND_PUT),
        None => (0u32, ENTRY_KIND_DELETE),
    };
    put_u32_le(buf, entry.key.len() as u32);
    put_u32_le(buf, value_len);
    put_u64_le(buf, entry.sequence.get());
    put_u8(buf, kind);
    buf.extend_from_slice(&entry.key);
    if let Some(v) = &entry.value {
        buf.extend_from_slice(v);
    }
}

fn finalize_data_block(entry_count: u32, entries_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u32_le(&mut buf, entry_count);
    put_u32_le(&mut buf, 0u32); // restart_count = 0 (no prefix compression in MVP)
    buf.extend_from_slice(entries_bytes);
    let crc = crc32c(&buf);
    put_u32_le(&mut buf, crc);
    buf
}

fn decode_data_block(bytes: &[u8]) -> Result<Vec<SstEntry>> {
    if bytes.len() < 12 {
        return Err(KayaError::corruption("data block too short"));
    }
    let data_len = bytes.len() - 4; // exclude trailing CRC
    let expected_crc = read_u32_le(bytes, data_len)?;
    let actual_crc = crc32c(&bytes[..data_len]);
    if expected_crc != actual_crc {
        return Err(KayaError::corruption("SSTable data block CRC mismatch"));
    }
    let entry_count = read_u32_le(bytes, 0)? as usize;
    // offset 4 = restart_count (ignored in MVP)
    let mut offset = 8;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        if offset + 17 > data_len {
            return Err(KayaError::corruption("truncated data block entry header"));
        }
        let key_len = read_u32_le(bytes, offset)? as usize;
        let value_len = read_u32_le(bytes, offset + 4)? as usize;
        let sequence = read_u64_le(bytes, offset + 8)?;
        let kind = read_u8(bytes, offset + 16)?;
        offset += 17;
        if offset + key_len + value_len > data_len {
            return Err(KayaError::corruption("truncated entry key/value"));
        }
        let key = bytes[offset..offset + key_len].to_vec();
        offset += key_len;
        let value = match kind {
            ENTRY_KIND_PUT => {
                let v = bytes[offset..offset + value_len].to_vec();
                offset += value_len;
                Some(v)
            }
            ENTRY_KIND_DELETE => None,
            _ => return Err(KayaError::corruption(format!("unknown entry kind: {kind}"))),
        };
        entries.push(SstEntry {
            key,
            value,
            sequence: SequenceNumber::new(sequence),
        });
    }
    Ok(entries)
}

// ---- Index block encode/decode ----
//
// Layout:
//   entry_count:    u32 LE
//   index entries:  var  (concatenated)
//   index_crc32c:   u32 LE  (covers everything above)
//
// Index entry layout (32 fixed bytes + separator key):
//   separator_len:  u32 LE
//   block_offset:   u64 LE
//   block_len:      u32 LE
//   first_seq:      u64 LE
//   last_seq:       u64 LE
//   separator_key:  bytes

fn encode_index_entry(buf: &mut Vec<u8>, ie: &IndexEntry) {
    put_u32_le(buf, ie.separator_key.len() as u32);
    put_u64_le(buf, ie.block_offset);
    put_u32_le(buf, ie.block_len);
    put_u64_le(buf, ie.first_seq);
    put_u64_le(buf, ie.last_seq);
    buf.extend_from_slice(&ie.separator_key);
}

fn finalize_index_block(index_entries: &[IndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    put_u32_le(&mut buf, index_entries.len() as u32);
    for ie in index_entries {
        encode_index_entry(&mut buf, ie);
    }
    let crc = crc32c(&buf);
    put_u32_le(&mut buf, crc);
    buf
}

fn decode_index_block(bytes: &[u8]) -> Result<Vec<IndexEntry>> {
    if bytes.len() < 8 {
        return Err(KayaError::corruption("index block too short"));
    }
    let data_len = bytes.len() - 4;
    let expected_crc = read_u32_le(bytes, data_len)?;
    let actual_crc = crc32c(&bytes[..data_len]);
    if expected_crc != actual_crc {
        return Err(KayaError::corruption("SSTable index block CRC mismatch"));
    }
    let entry_count = read_u32_le(bytes, 0)? as usize;
    let mut offset = 4;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        // Fixed part: 4 + 8 + 4 + 8 + 8 = 32 bytes
        if offset + 32 > data_len {
            return Err(KayaError::corruption("truncated index entry header"));
        }
        let sep_len = read_u32_le(bytes, offset)? as usize;
        let block_offset = read_u64_le(bytes, offset + 4)?;
        let block_len = read_u32_le(bytes, offset + 12)?;
        let first_seq = read_u64_le(bytes, offset + 16)?;
        let last_seq = read_u64_le(bytes, offset + 24)?;
        offset += 32;
        if offset + sep_len > data_len {
            return Err(KayaError::corruption("truncated index separator key"));
        }
        let separator_key = bytes[offset..offset + sep_len].to_vec();
        offset += sep_len;
        entries.push(IndexEntry {
            separator_key,
            block_offset,
            block_len,
            first_seq,
            last_seq,
        });
    }
    Ok(entries)
}

// ---- Footer encode/decode ----
//
// Fixed 48-byte layout (little-endian):
//   index_block_offset:  u64  offset 0
//   index_block_len:     u32  offset 8
//   table_min_seq:       u64  offset 12
//   table_max_seq:       u64  offset 20
//   entry_count:         u64  offset 28
//   format_version:      u16  offset 36
//   footer_len:          u16  offset 38  (always 48)
//   footer_crc32c:       u32  offset 40  (CRC over bytes 0..40)
//   magic:               u32  offset 44  (SST_MAGIC)

fn encode_footer(footer: &SstFooter) -> Vec<u8> {
    let mut buf = Vec::with_capacity(SST_FOOTER_LEN);
    put_u64_le(&mut buf, footer.index_block_offset); // 0
    put_u32_le(&mut buf, footer.index_block_len); //    8
    put_u64_le(&mut buf, footer.table_min_seq); //      12
    put_u64_le(&mut buf, footer.table_max_seq); //      20
    put_u64_le(&mut buf, footer.entry_count); //        28
    put_u16_le(&mut buf, footer.format_version); //     36
    put_u16_le(&mut buf, SST_FOOTER_LEN as u16); //     38
    let crc = crc32c(&buf); // CRC over first 40 bytes
    put_u32_le(&mut buf, crc); //                       40
    put_u32_le(&mut buf, SST_MAGIC); //                 44
    debug_assert_eq!(buf.len(), SST_FOOTER_LEN);
    buf
}

/// Decode and validate the footer from the last `SST_FOOTER_LEN` bytes.
pub fn decode_footer(bytes: &[u8]) -> Result<SstFooter> {
    if bytes.len() < SST_FOOTER_LEN {
        return Err(KayaError::corruption("file too short for SSTable footer"));
    }
    let footer_start = bytes.len() - SST_FOOTER_LEN;
    let fb = &bytes[footer_start..];
    let magic = read_u32_le(fb, 44)?;
    if magic != SST_MAGIC {
        return Err(KayaError::corruption(format!(
            "bad SSTable magic: {magic:#010x} (expected {SST_MAGIC:#010x})"
        )));
    }
    let expected_crc = read_u32_le(fb, 40)?;
    let actual_crc = crc32c(&fb[..40]);
    if expected_crc != actual_crc {
        return Err(KayaError::corruption("SSTable footer CRC mismatch"));
    }
    let format_version = read_u16_le(fb, 36)?;
    if format_version != SST_VERSION {
        return Err(KayaError::corruption(format!(
            "unsupported SSTable version: {format_version}"
        )));
    }
    Ok(SstFooter {
        index_block_offset: read_u64_le(fb, 0)?,
        index_block_len: read_u32_le(fb, 8)?,
        table_min_seq: read_u64_le(fb, 12)?,
        table_max_seq: read_u64_le(fb, 20)?,
        entry_count: read_u64_le(fb, 28)?,
        format_version,
    })
}

// ---- SstableBuilder ----

/// Builds an SSTable in memory.  Call `add` for each entry in sorted key
/// order, then `finish` to get the final byte vector.
#[derive(Debug)]
pub struct SstableBuilder {
    target_block_bytes: usize,
    // current block accumulator
    current_bytes: Vec<u8>,
    current_count: u32,
    current_first_seq: Option<u64>,
    current_last_seq: Option<u64>,
    current_last_key: Option<Bytes>,
    // finished data blocks
    data_bytes: Vec<u8>,
    index_entries: Vec<IndexEntry>,
    // table-wide stats
    smallest_key: Option<Bytes>,
    largest_key: Option<Bytes>,
    table_min_seq: Option<u64>,
    table_max_seq: Option<u64>,
    total_entries: u64,
}

impl SstableBuilder {
    pub fn new(target_block_bytes: usize) -> Self {
        Self {
            target_block_bytes: target_block_bytes.max(1),
            current_bytes: Vec::new(),
            current_count: 0,
            current_first_seq: None,
            current_last_seq: None,
            current_last_key: None,
            data_bytes: Vec::new(),
            index_entries: Vec::new(),
            smallest_key: None,
            largest_key: None,
            table_min_seq: None,
            table_max_seq: None,
            total_entries: 0,
        }
    }

    pub fn add(&mut self, entry: SstEntry) {
        encode_entry(&mut self.current_bytes, &entry);
        self.current_count += 1;
        self.total_entries += 1;
        let seq = entry.sequence.get();
        if self.current_first_seq.is_none() {
            self.current_first_seq = Some(seq);
        }
        self.current_last_seq = Some(seq);
        self.current_last_key = Some(entry.key.clone());
        if self.smallest_key.is_none() {
            self.smallest_key = Some(entry.key.clone());
        }
        self.largest_key = Some(entry.key.clone());
        self.table_min_seq = Some(self.table_min_seq.map_or(seq, |m| m.min(seq)));
        self.table_max_seq = Some(self.table_max_seq.map_or(seq, |m| m.max(seq)));
        if self.current_bytes.len() >= self.target_block_bytes {
            self.flush_current_block();
        }
    }

    fn flush_current_block(&mut self) {
        if self.current_count == 0 {
            return;
        }
        let block_offset = self.data_bytes.len() as u64;
        let block_data = finalize_data_block(self.current_count, &self.current_bytes);
        let block_len = block_data.len() as u32;
        self.data_bytes.extend_from_slice(&block_data);
        self.index_entries.push(IndexEntry {
            separator_key: self.current_last_key.clone().unwrap_or_default(),
            block_offset,
            block_len,
            first_seq: self.current_first_seq.unwrap_or(1),
            last_seq: self.current_last_seq.unwrap_or(1),
        });
        self.current_bytes.clear();
        self.current_count = 0;
        self.current_first_seq = None;
        self.current_last_seq = None;
        self.current_last_key = None;
    }

    pub fn is_empty(&self) -> bool {
        self.total_entries == 0
    }

    pub fn smallest_key(&self) -> Option<&[u8]> {
        self.smallest_key.as_deref()
    }

    pub fn largest_key(&self) -> Option<&[u8]> {
        self.largest_key.as_deref()
    }

    pub fn table_min_seq(&self) -> u64 {
        self.table_min_seq.unwrap_or(1)
    }

    pub fn table_max_seq(&self) -> u64 {
        self.table_max_seq.unwrap_or(1)
    }

    pub fn total_entries(&self) -> u64 {
        self.total_entries
    }

    /// Finalise the builder and return the complete SSTable byte vector.
    pub fn finish(mut self) -> Result<Vec<u8>> {
        if self.is_empty() {
            return Err(KayaError::invalid_argument("cannot build empty SSTable"));
        }
        self.flush_current_block();
        let mut out = self.data_bytes;

        // Index block
        let index_offset = out.len() as u64;
        let index_bytes = finalize_index_block(&self.index_entries);
        let index_len = index_bytes.len() as u32;
        out.extend_from_slice(&index_bytes);

        // Footer
        let footer = SstFooter {
            index_block_offset: index_offset,
            index_block_len: index_len,
            table_min_seq: self.table_min_seq.unwrap_or(1),
            table_max_seq: self.table_max_seq.unwrap_or(1),
            entry_count: self.total_entries,
            format_version: SST_VERSION,
        };
        out.extend_from_slice(&encode_footer(&footer));
        Ok(out)
    }
}

// ---- SstableReader ----

/// In-memory SSTable reader.  Holds all bytes in memory (acceptable for MVP).
#[derive(Debug)]
pub struct SstableReader {
    bytes: Vec<u8>,
    footer: SstFooter,
    index: Vec<IndexEntry>,
}

impl SstableReader {
    /// Validate and load a complete SSTable from its byte vector.
    pub fn open(bytes: Vec<u8>) -> Result<Self> {
        let footer = decode_footer(&bytes)?;
        let idx_start = footer.index_block_offset as usize;
        let idx_end = idx_start + footer.index_block_len as usize;
        let content_end = bytes.len() - SST_FOOTER_LEN;
        if idx_end > content_end {
            return Err(KayaError::corruption(
                "index block range exceeds content area",
            ));
        }
        let index = decode_index_block(&bytes[idx_start..idx_end])?;
        Ok(Self {
            bytes,
            footer,
            index,
        })
    }

    pub fn footer(&self) -> &SstFooter {
        &self.footer
    }

    /// Point lookup.  Returns the entry with the highest sequence for `key`
    /// in the block that could contain it, or `None` if absent.
    pub fn get(&self, key: &[u8]) -> Result<Option<SstEntry>> {
        // The index is sorted by separator key (= last key of each block).
        // The key lives in the first block whose separator_key >= key.
        for ie in &self.index {
            if ie.separator_key.as_slice() >= key {
                let block = self.read_data_block(ie)?;
                for entry in block {
                    if entry.key.as_slice() == key {
                        return Ok(Some(entry));
                    }
                }
                return Ok(None);
            }
        }
        // key may be larger than all separators; check the last block.
        if let Some(ie) = self.index.last() {
            let block = self.read_data_block(ie)?;
            for entry in block {
                if entry.key.as_slice() == key {
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }

    /// Returns all entries whose key starts with `prefix`, in sorted order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<SstEntry>> {
        let mut result = Vec::new();
        let mut started = false;
        for ie in &self.index {
            // Skip blocks whose separator is strictly before the prefix.
            if !ie.separator_key.is_empty() && ie.separator_key.as_slice() < prefix {
                continue;
            }
            let block = self.read_data_block(ie)?;
            for entry in block {
                if entry.key.starts_with(prefix) {
                    result.push(entry);
                    started = true;
                } else if started {
                    // Keys are sorted; once we leave the prefix range we are done.
                    return Ok(result);
                }
            }
        }
        Ok(result)
    }

    /// Returns every entry in the SSTable in sorted key order.
    pub fn all_entries(&self) -> Result<Vec<SstEntry>> {
        let mut result = Vec::new();
        for ie in &self.index {
            result.extend(self.read_data_block(ie)?);
        }
        Ok(result)
    }

    fn read_data_block(&self, ie: &IndexEntry) -> Result<Vec<SstEntry>> {
        let start = ie.block_offset as usize;
        let end = start + ie.block_len as usize;
        if end > self.bytes.len().saturating_sub(SST_FOOTER_LEN) {
            return Err(KayaError::corruption("data block offset out of bounds"));
        }
        decode_data_block(&self.bytes[start..end])
    }
}

// ---- Inspect helper (for kayactl) ----

pub fn inspect_sstable_path(path: impl AsRef<Path>) -> Result<SstInspection> {
    let bytes = fs::read(path.as_ref()).map_err(|e| KayaError::Io {
        message: e.to_string(),
    })?;
    let path_str = path.as_ref().display().to_string();
    match SstableReader::open(bytes) {
        Ok(reader) => {
            let entries = reader.all_entries()?;
            Ok(SstInspection {
                path: path_str,
                footer: reader.footer().clone(),
                entries,
                warnings: Vec::new(),
            })
        }
        Err(e) => Ok(SstInspection {
            path: path_str,
            footer: SstFooter {
                index_block_offset: 0,
                index_block_len: 0,
                table_min_seq: 0,
                table_max_seq: 0,
                entry_count: 0,
                format_version: 0,
            },
            entries: Vec::new(),
            warnings: vec![e.to_string()],
        }),
    }
}

/// Expose decode_data_block for fuzzing.
pub fn fuzz_decode_data_block(bytes: &[u8]) {
    let _ = decode_data_block(bytes);
}

/// Expose decode_index_block for fuzzing.
pub fn fuzz_decode_index_block(bytes: &[u8]) {
    let _ = decode_index_block(bytes);
}

// ---- Tests ----

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_put(key: &[u8], value: &[u8], seq: u64) -> SstEntry {
        SstEntry {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            sequence: SequenceNumber::new(seq),
        }
    }

    fn entry_del(key: &[u8], seq: u64) -> SstEntry {
        SstEntry {
            key: key.to_vec(),
            value: None,
            sequence: SequenceNumber::new(seq),
        }
    }

    #[test]
    fn roundtrip_single_block() {
        let mut builder = SstableBuilder::new(64 * 1024);
        builder.add(entry_put(b"aaa", b"v1", 1));
        builder.add(entry_put(b"bbb", b"v2", 2));
        builder.add(entry_del(b"ccc", 3));
        let bytes = builder.finish().unwrap();

        let reader = SstableReader::open(bytes).unwrap();
        assert_eq!(reader.footer().entry_count, 3);

        assert_eq!(
            reader.get(b"aaa").unwrap().unwrap().value,
            Some(b"v1".to_vec())
        );
        assert_eq!(
            reader.get(b"bbb").unwrap().unwrap().value,
            Some(b"v2".to_vec())
        );
        assert_eq!(reader.get(b"ccc").unwrap().unwrap().value, None); // tombstone
        assert!(reader.get(b"zzz").unwrap().is_none());
    }

    #[test]
    fn multi_block_roundtrip() {
        // Very small block target forces multiple blocks.
        let mut builder = SstableBuilder::new(1);
        for i in 0_u8..10 {
            builder.add(entry_put(&[i], &[i * 2], u64::from(i) + 1));
        }
        let bytes = builder.finish().unwrap();
        let reader = SstableReader::open(bytes).unwrap();
        assert_eq!(reader.footer().entry_count, 10);
        for i in 0_u8..10 {
            let entry = reader.get(&[i]).unwrap().unwrap();
            assert_eq!(entry.value, Some(vec![i * 2]));
        }
    }

    #[test]
    fn scan_prefix() {
        let mut builder = SstableBuilder::new(64 * 1024);
        // Entries MUST be added in sorted key order for SSTable correctness.
        builder.add(entry_put(b"other:x", b"X", 3));
        builder.add(entry_put(b"user:alice", b"A", 1));
        builder.add(entry_put(b"user:bob", b"B", 2));
        let bytes = builder.finish().unwrap();
        let reader = SstableReader::open(bytes).unwrap();
        let hits = reader.scan_prefix(b"user:").unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].key, b"user:alice");
        assert_eq!(hits[1].key, b"user:bob");
    }

    #[test]
    fn rejects_corrupted_footer_magic() {
        let mut builder = SstableBuilder::new(64 * 1024);
        builder.add(entry_put(b"k", b"v", 1));
        let mut bytes = builder.finish().unwrap();
        // Corrupt the magic bytes (last 4 bytes).
        let len = bytes.len();
        bytes[len - 4..len].fill(0);
        assert!(SstableReader::open(bytes).is_err());
    }

    #[test]
    fn rejects_corrupted_data_block_crc() {
        let mut builder = SstableBuilder::new(64 * 1024);
        builder.add(entry_put(b"k", b"v", 1));
        let mut bytes = builder.finish().unwrap();
        // Flip a byte inside the first data block.
        bytes[0] ^= 0xff;
        let reader = SstableReader::open(bytes).unwrap();
        // The footer and index are fine; the data block CRC should fail.
        assert!(reader.get(b"k").is_err());
    }
}
