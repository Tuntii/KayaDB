//! `kayactl backup` — copy a node's durable state to a backup directory, with
//! an incremental mode that skips already-copied immutable files.
//!
//! SSTables and sealed WAL segments are immutable and uniquely named, so an
//! incremental backup only needs to copy files that are absent from the
//! destination or whose size changed (the active WAL segment and manifest).
//!
//! Consistency note: for a point-in-time-consistent snapshot, stop the node
//! first (see `docs/runbooks/backup-restore.md`). A live backup is safe for the
//! immutable SSTables but the WAL/manifest may be mid-write.

use std::fs;
use std::path::{Path, PathBuf};

use kaya_core::{KayaError, Result};

use crate::cli::{remove_flag, remove_value_flag};

struct BackupReport {
    copied: usize,
    skipped: usize,
    bytes_copied: u64,
}

/// Entry point for `kayactl backup --data <src> --out <dest> [--incremental]`.
pub fn run_backup(mut args: Vec<String>, data_dir: &str) -> Result<()> {
    // Drop the "backup" verb.
    if args.first().map(String::as_str) == Some("backup") {
        args.remove(0);
    }
    let json = remove_flag(&mut args, "--json");
    let incremental = remove_flag(&mut args, "--incremental");
    let out = remove_value_flag(&mut args, "--out").ok_or_else(|| {
        KayaError::invalid_argument(
            "usage: kayactl backup --data <src> --out <dest> [--incremental]",
        )
    })?;

    let src = Path::new(data_dir);
    let dest = Path::new(&out);
    if !src.is_dir() {
        return Err(KayaError::invalid_argument(format!(
            "source data directory does not exist: {data_dir}"
        )));
    }
    fs::create_dir_all(dest)?;

    let mut report = BackupReport {
        copied: 0,
        skipped: 0,
        bytes_copied: 0,
    };
    copy_tree(src, dest, incremental, &mut report)?;

    if json {
        println!(
            r#"{{"copied":{},"skipped":{},"bytes_copied":{},"incremental":{},"out":"{}"}}"#,
            report.copied,
            report.skipped,
            report.bytes_copied,
            incremental,
            out.replace('\\', "\\\\").replace('"', "\\\"")
        );
    } else {
        let mode = if incremental { "incremental" } else { "full" };
        println!(
            "Backup ({mode}) complete: {} file(s) copied ({} bytes), {} unchanged file(s) skipped -> {out}",
            report.copied, report.bytes_copied, report.skipped
        );
    }
    Ok(())
}

/// Recursively copy `src` into `dest`, preserving relative structure. In
/// incremental mode, a destination file with the same size is treated as
/// already-backed-up and skipped (immutable SSTables/sealed segments).
fn copy_tree(src: &Path, dest: &Path, incremental: bool, report: &mut BackupReport) -> Result<()> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name();
        let src_path = entry.path();
        let dest_path = dest.join(&name);

        if file_type.is_dir() {
            fs::create_dir_all(&dest_path)?;
            copy_tree(&src_path, &dest_path, incremental, report)?;
        } else if file_type.is_file() {
            let src_len = entry.metadata()?.len();
            if incremental && same_size(&dest_path, src_len) {
                report.skipped += 1;
                continue;
            }
            copy_file_atomic(&src_path, &dest_path)?;
            report.copied += 1;
            report.bytes_copied += src_len;
        }
        // Symlinks and other special files are intentionally skipped.
    }
    Ok(())
}

/// True when `dest` exists and its length equals `src_len`.
fn same_size(dest: &Path, src_len: u64) -> bool {
    fs::metadata(dest)
        .map(|m| m.len() == src_len)
        .unwrap_or(false)
}

/// Copy to a temp file then rename, so a partial copy never leaves a truncated
/// file at the destination path.
fn copy_file_atomic(src: &Path, dest: &Path) -> Result<()> {
    let tmp: PathBuf = dest.with_extension("kaya-backup-tmp");
    fs::copy(src, &tmp)?;
    fs::rename(&tmp, dest)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("kaya-backup-{tag}-{}", std::process::id()))
    }

    #[test]
    fn full_then_incremental_backup_skips_unchanged() {
        let src = unique_dir("src");
        let dest = unique_dir("dest");
        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dest);
        fs::create_dir_all(src.join("sst")).unwrap();
        fs::write(src.join("sst/000001.sst"), b"immutable-a").unwrap();
        fs::write(src.join("CURRENT"), b"manifest-0001").unwrap();

        let data = src.to_string_lossy().into_owned();
        let out = dest.to_string_lossy().into_owned();

        // Full backup copies both files.
        run_backup(
            vec![
                "backup".into(),
                "--out".into(),
                out.clone(),
                "--json".into(),
            ],
            &data,
        )
        .unwrap();
        assert_eq!(
            fs::read(dest.join("sst/000001.sst")).unwrap(),
            b"immutable-a"
        );
        assert_eq!(fs::read(dest.join("CURRENT")).unwrap(), b"manifest-0001");

        // Add a new immutable SSTable; incremental should copy only the new one.
        fs::write(src.join("sst/000002.sst"), b"immutable-b").unwrap();
        let mut report = BackupReport {
            copied: 0,
            skipped: 0,
            bytes_copied: 0,
        };
        copy_tree(&src, &dest, true, &mut report).unwrap();
        assert_eq!(report.copied, 1, "only the new SSTable is copied");
        assert!(report.skipped >= 2, "unchanged files are skipped");
        assert_eq!(
            fs::read(dest.join("sst/000002.sst")).unwrap(),
            b"immutable-b"
        );

        let _ = fs::remove_dir_all(&src);
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn missing_out_flag_is_an_error() {
        let err = run_backup(vec!["backup".into()], ".").unwrap_err();
        assert_eq!(err.exit_code(), 4); // InvalidArgument
    }
}
