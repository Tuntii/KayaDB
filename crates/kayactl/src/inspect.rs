use kaya_core::{Result, WalConfig};
use kaya_lsm::{inspect_manifest_path, inspect_sstable_path, ManifestInspection, SstInspection};
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

fn print_sst_inspection_human(inspection: &SstInspection) {
    println!("sstable: {}", inspection.path);
    let f = &inspection.footer;
    println!(
        "version={} entries={} min_seq={} max_seq={}",
        f.format_version, f.entry_count, f.table_min_seq, f.table_max_seq
    );
    println!();
    for entry in &inspection.entries {
        let key = String::from_utf8_lossy(&entry.key);
        match &entry.value {
            Some(v) => {
                let value = String::from_utf8_lossy(v);
                println!(
                    "seq={} PUT key={key} value_len={}  {value}",
                    entry.sequence.get(),
                    v.len()
                );
            }
            None => {
                println!("seq={} DEL key={key}", entry.sequence.get());
            }
        }
    }
    for warning in &inspection.warnings {
        println!("WARNING {warning}");
    }
}

fn print_sst_inspection_json(inspection: &SstInspection) {
    let f = &inspection.footer;
    print!(
        "{{\"path\":{},\"version\":{},\"entry_count\":{},\"min_seq\":{},\"max_seq\":{},\"entries\":[",
        json_string(&inspection.path),
        f.format_version,
        f.entry_count,
        f.table_min_seq,
        f.table_max_seq
    );
    for (i, entry) in inspection.entries.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        let key = String::from_utf8_lossy(&entry.key);
        match &entry.value {
            Some(v) => {
                let value = String::from_utf8_lossy(v);
                print!(
                    "{{\"seq\":{},\"type\":\"put\",\"key\":{},\"value\":{}}}",
                    entry.sequence.get(),
                    json_string(&key),
                    json_string(&value)
                );
            }
            None => {
                print!(
                    "{{\"seq\":{},\"type\":\"del\",\"key\":{}}}",
                    entry.sequence.get(),
                    json_string(&key)
                );
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
