use kaya_core::{Result, WalConfig};
use kaya_lsm::{
    inspect_manifest_path, inspect_sstable_path, user_key_of, ManifestInspection, SstEntry,
    SstInspection, SST_VERSION_V4,
};
use kaya_wal::{inspect_wal_path, WalInspection};

use crate::cli::{json_escape, json_string, option_usize_json};

pub(crate) fn inspect_wal(path: &str, json: bool) -> Result<()> {
    let inspection = inspect_wal_path(path, WalConfig::default().max_record_bytes)?;
    if json {
        print_inspection_json(&inspection);
    } else {
        print_inspection_human(&inspection);
    }
    Ok(())
}

pub(crate) fn inspect_sstable(path: &str, json: bool) -> Result<()> {
    let inspection = inspect_sstable_path(path)?;
    if json {
        print_sst_inspection_json(&inspection);
    } else {
        print_sst_inspection_human(&inspection);
    }
    Ok(())
}

pub(crate) fn inspect_manifest(path: &str, json: bool) -> Result<()> {
    let inspection = inspect_manifest_path(path)?;
    if json {
        print_manifest_inspection_json(&inspection);
    } else {
        print_manifest_inspection_human(&inspection);
    }
    Ok(())
}

fn print_inspection_human(inspection: &WalInspection) {
    println!("segment: {}", inspection.segment);
    println!("records: {}", inspection.rows.len());
    println!();
    for row in &inspection.rows {
        match row.value_len {
            Some(value_len) => println!(
                "offset={} lsn={} seq={} type={} key_len={} value_len={} checksum=ok",
                row.offset,
                row.lsn,
                row.sequence,
                row.record_type,
                row.key_len.unwrap_or_default(),
                value_len
            ),
            None => println!(
                "offset={} lsn={} seq={} type={} key_len={} checksum=ok",
                row.offset,
                row.lsn,
                row.sequence,
                row.record_type,
                row.key_len.unwrap_or_default()
            ),
        }
    }
    for warning in &inspection.warnings {
        println!("CORRUPTION {warning}");
    }
}

fn print_inspection_json(inspection: &WalInspection) {
    print!(
        "{{\"segment\":\"{}\",\"records\":[",
        json_escape(&inspection.segment)
    );
    for (index, row) in inspection.rows.iter().enumerate() {
        if index > 0 {
            print!(",");
        }
        print!(
            "{{\"offset\":{},\"lsn\":{},\"sequence\":{},\"type\":\"{}\",\"key_len\":{}",
            row.offset,
            row.lsn,
            row.sequence,
            row.record_type,
            option_usize_json(row.key_len)
        );
        print!(",\"value_len\":{}}}", option_usize_json(row.value_len));
    }
    print!("],\"warnings\":[");
    for (index, warning) in inspection.warnings.iter().enumerate() {
        if index > 0 {
            print!(",");
        }
        print!("\"{}\"", json_escape(&warning.to_string()));
    }
    println!("]}}");
}

/// True when the table is multi-version (SST v4+) or inspect finds repeated keys
/// (defensive: still label versions even if footer version is unexpected).
fn is_multi_version_sst(inspection: &SstInspection) -> bool {
    if inspection.footer.format_version >= SST_VERSION_V4 {
        return true;
    }
    // Detect multi-version rows by repeated user_keys among entries.
    let mut seen = std::collections::HashSet::new();
    for entry in &inspection.entries {
        let uk = display_user_key_bytes(entry);
        if !seen.insert(uk) {
            return true;
        }
    }
    false
}

/// SST entry keys store the logical user_key; sequence holds commit_ts.
/// If a wire-encoded internal key ever appears (user_key ‖ inverted ts), peel it.
fn display_user_key_bytes(entry: &SstEntry) -> Vec<u8> {
    let key = entry.key.as_slice();
    // Wire internal keys are ≥ 8 bytes and encode inverted commit_ts in the suffix.
    // Only peel when the decoded ts matches the entry sequence (avoid stripping
    // legitimate user_keys that happen to be long).
    if key.len() >= 8 {
        let peeled = user_key_of(key);
        let ts = kaya_lsm::commit_ts_of(key);
        if peeled.len() < key.len() && ts == entry.sequence.get() {
            return peeled.to_vec();
        }
    }
    key.to_vec()
}

fn display_user_key(entry: &SstEntry) -> String {
    String::from_utf8_lossy(&display_user_key_bytes(entry)).into_owned()
}

fn print_sst_inspection_human(inspection: &SstInspection) {
    println!("sstable: {}", inspection.path);
    let f = &inspection.footer;
    let mvcc = is_multi_version_sst(inspection);
    if mvcc {
        println!(
            "version={} (multi-version MVCC) entries={} min_seq={} max_seq={}",
            f.format_version, f.entry_count, f.table_min_seq, f.table_max_seq
        );
    } else {
        println!(
            "version={} entries={} min_seq={} max_seq={}",
            f.format_version, f.entry_count, f.table_min_seq, f.table_max_seq
        );
    }
    println!();
    for entry in &inspection.entries {
        let seq = entry.sequence.get();
        let user_key = display_user_key(entry);
        match &entry.value {
            Some(v) => {
                let value = String::from_utf8_lossy(v);
                if mvcc {
                    println!(
                        "seq={seq} commit_ts={seq} PUT user_key={user_key} value_len={}  {value}",
                        v.len()
                    );
                } else {
                    println!(
                        "seq={seq} PUT key={user_key} value_len={}  {value}",
                        v.len()
                    );
                }
            }
            None => {
                if mvcc {
                    println!("seq={seq} commit_ts={seq} DEL user_key={user_key}");
                } else {
                    println!("seq={seq} DEL key={user_key}");
                }
            }
        }
    }
    for warning in &inspection.warnings {
        println!("WARNING {warning}");
    }
}

fn print_sst_inspection_json(inspection: &SstInspection) {
    let f = &inspection.footer;
    let mvcc = is_multi_version_sst(inspection);
    print!(
        "{{\"path\":{},\"version\":{},\"mvcc\":{},\"entry_count\":{},\"min_seq\":{},\"max_seq\":{},\"entries\":[",
        json_string(&inspection.path),
        f.format_version,
        if mvcc { "true" } else { "false" },
        f.entry_count,
        f.table_min_seq,
        f.table_max_seq
    );
    for (i, entry) in inspection.entries.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        let seq = entry.sequence.get();
        let user_key = display_user_key(entry);
        // Keep legacy `key` field (same as user_key) for consumers; add explicit MVCC fields.
        match &entry.value {
            Some(v) => {
                let value = String::from_utf8_lossy(v);
                if mvcc {
                    print!(
                        "{{\"seq\":{seq},\"commit_ts\":{seq},\"type\":\"put\",\"user_key\":{},\"key\":{},\"value\":{}}}",
                        json_string(&user_key),
                        json_string(&user_key),
                        json_string(&value)
                    );
                } else {
                    print!(
                        "{{\"seq\":{seq},\"type\":\"put\",\"key\":{},\"value\":{}}}",
                        json_string(&user_key),
                        json_string(&value)
                    );
                }
            }
            None => {
                if mvcc {
                    print!(
                        "{{\"seq\":{seq},\"commit_ts\":{seq},\"type\":\"del\",\"user_key\":{},\"key\":{}}}",
                        json_string(&user_key),
                        json_string(&user_key)
                    );
                } else {
                    print!(
                        "{{\"seq\":{seq},\"type\":\"del\",\"key\":{}}}",
                        json_string(&user_key)
                    );
                }
            }
        }
    }
    print!("],\"warnings\":[");
    for (i, w) in inspection.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(w));
    }
    println!("]}}");
}

fn print_manifest_inspection_human(inspection: &ManifestInspection) {
    println!("manifest: {}", inspection.path);
    let s = &inspection.state;
    println!(
        "live_tables={} last_sequence={} last_edit_seq={}",
        s.live_tables.len(),
        s.last_sequence.get(),
        s.last_edit_seq
    );
    println!();
    for t in &s.live_tables {
        let sk = String::from_utf8_lossy(&t.smallest_key);
        let lk = String::from_utf8_lossy(&t.largest_key);
        println!(
            "table_id={} level={} entries={} path={} min_seq={} max_seq={} smallest={sk} largest={lk}",
            t.table_id, t.level, t.entry_count, t.path,
            t.min_sequence.get(), t.max_sequence.get()
        );
    }
    for warning in &inspection.warnings {
        println!("WARNING {warning}");
    }
}

fn print_manifest_inspection_json(inspection: &ManifestInspection) {
    let s = &inspection.state;
    print!(
        "{{\"path\":{},\"last_sequence\":{},\"last_edit_seq\":{},\"live_tables\":[",
        json_string(&inspection.path),
        s.last_sequence.get(),
        s.last_edit_seq
    );
    for (i, t) in s.live_tables.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        let sk = String::from_utf8_lossy(&t.smallest_key);
        let lk = String::from_utf8_lossy(&t.largest_key);
        print!(
            "{{\"table_id\":{},\"level\":{},\"path\":{},\"entries\":{},\"min_seq\":{},\"max_seq\":{},\"smallest\":{},\"largest\":{}}}",
            t.table_id,
            t.level,
            json_string(&t.path),
            t.entry_count,
            t.min_sequence.get(),
            t.max_sequence.get(),
            json_string(&sk),
            json_string(&lk)
        );
    }
    print!("],\"warnings\":[");
    for (i, w) in inspection.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_core::SequenceNumber;
    use kaya_lsm::{encode_internal_key, SstEntry, SstFooter};

    fn footer(version: u16) -> SstFooter {
        SstFooter {
            index_block_offset: 0,
            index_block_len: 0,
            table_min_seq: 1,
            table_max_seq: 2,
            entry_count: 2,
            format_version: version,
            bloom_offset: 0,
            bloom_len: 0,
            bloom_hash_count: 0,
            compression_codec: 0,
        }
    }

    #[test]
    fn multi_version_when_v4() {
        let inspection = SstInspection {
            path: "t.sst".into(),
            footer: footer(SST_VERSION_V4),
            entries: vec![],
            warnings: vec![],
        };
        assert!(is_multi_version_sst(&inspection));
    }

    #[test]
    fn multi_version_when_duplicate_keys() {
        let e1 = SstEntry {
            key: b"k".to_vec(),
            value: Some(b"v1".to_vec()),
            sequence: SequenceNumber::new(1),
        };
        let e2 = SstEntry {
            key: b"k".to_vec(),
            value: Some(b"v2".to_vec()),
            sequence: SequenceNumber::new(2),
        };
        let inspection = SstInspection {
            path: "t.sst".into(),
            footer: footer(3),
            entries: vec![e1, e2],
            warnings: vec![],
        };
        assert!(is_multi_version_sst(&inspection));
    }

    #[test]
    fn display_user_key_plain() {
        let e = SstEntry {
            key: b"hello".to_vec(),
            value: Some(b"v".to_vec()),
            sequence: SequenceNumber::new(7),
        };
        assert_eq!(display_user_key(&e), "hello");
    }

    #[test]
    fn display_user_key_peels_wire_internal_key() {
        let wire = encode_internal_key(b"user", 42);
        let e = SstEntry {
            key: wire,
            value: Some(b"v".to_vec()),
            sequence: SequenceNumber::new(42),
        };
        assert_eq!(display_user_key(&e), "user");
    }

    #[test]
    fn display_user_key_does_not_strip_long_plain_keys() {
        // 12-byte plain user_key must not be treated as wire-encoded unless ts matches.
        let e = SstEntry {
            key: b"abcdefghijkl".to_vec(),
            value: Some(b"v".to_vec()),
            sequence: SequenceNumber::new(1),
        };
        assert_eq!(display_user_key(&e), "abcdefghijkl");
    }
}
