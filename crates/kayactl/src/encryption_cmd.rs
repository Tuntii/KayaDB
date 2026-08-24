//! `kayactl encryption` — operator commands for AES-GCM key rotation (#28).
//!
//! Operates on a keyring *file*, independent of a running server: rotation
//! only needs to update the key material an `EncryptedDisk` reads/writes
//! with, so it never touches the engine, WAL, or SSTables directly.

use std::path::{Path, PathBuf};

use kaya_core::{KayaError, Result};

use crate::cli::{json_string, remove_value_flag};

pub fn run_encryption(mut args: Vec<String>, data_dir: String, json: bool) -> Result<()> {
    // Drop the "encryption" verb.
    if args.first().map(String::as_str) == Some("encryption") {
        args.remove(0);
    }
    let sub = if args.is_empty() {
        return Err(KayaError::invalid_argument(
            "usage: kayactl encryption <init|rotate|list|verify> [...]",
        ));
    } else {
        args.remove(0)
    };

    match sub.as_str() {
        "init" => run_init(args, json),
        "rotate" => run_rotate(args, json),
        "list" => run_list(args, json),
        "verify" => run_verify(args, data_dir, json),
        other => Err(KayaError::invalid_argument(format!(
            "unknown encryption subcommand '{other}'; expected init|rotate|list|verify"
        ))),
    }
}

fn keyring_path(args: &mut Vec<String>) -> Result<PathBuf> {
    remove_value_flag(args, "--keyring")
        .map(PathBuf::from)
        .ok_or_else(|| KayaError::invalid_argument("--keyring <path> is required"))
}

fn run_init(mut args: Vec<String>, json: bool) -> Result<()> {
    let path = keyring_path(&mut args)?;
    let from_key_file = remove_value_flag(&mut args, "--from-key-file");
    if path.exists() {
        return Err(KayaError::invalid_argument(format!(
            "{} already exists; refusing to overwrite a keyring",
            path.display()
        )));
    }
    let key = match from_key_file {
        Some(p) => kaya_io::load_key_file(&p)
            .map_err(|e| KayaError::invalid_argument(format!("--from-key-file {p}: {e}")))?,
        None => kaya_io::generate_key(),
    };
    let keyring = kaya_io::Keyring::new(0, key);
    kaya_io::save_keyring_file(&path, &keyring)?;
    print_ids("initialized", &path, &keyring, json);
    Ok(())
}

fn run_rotate(mut args: Vec<String>, json: bool) -> Result<()> {
    let path = keyring_path(&mut args)?;
    let keyring = kaya_io::load_keyring_file(&path)?;
    let new_id = keyring.key_ids().into_iter().max().unwrap_or(0) + 1;
    let new_key = kaya_io::generate_key();
    let rotated = keyring.rotate(new_id, new_key)?;
    kaya_io::save_keyring_file(&path, &rotated)?;
    print_ids("rotated", &path, &rotated, json);
    Ok(())
}

fn run_list(mut args: Vec<String>, json: bool) -> Result<()> {
    let path = keyring_path(&mut args)?;
    let keyring = kaya_io::load_keyring_file(&path)?;
    print_ids("keyring", &path, &keyring, json);
    Ok(())
}

fn print_ids(action: &str, path: &Path, keyring: &kaya_io::Keyring, json: bool) {
    let ids = keyring.key_ids();
    if json {
        let ids_json = ids.iter().map(u32::to_string).collect::<Vec<_>>().join(",");
        println!(
            "{{\"action\":{},\"keyring\":{},\"active_id\":{},\"key_ids\":[{}]}}",
            json_string(action),
            json_string(&path.display().to_string()),
            keyring.active_id(),
            ids_json
        );
    } else {
        let ids_str = ids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "{action} {}: active_id={} key_ids=[{ids_str}]",
            path.display(),
            keyring.active_id()
        );
    }
}

fn run_verify(mut args: Vec<String>, data_dir: String, json: bool) -> Result<()> {
    let keyring_arg = remove_value_flag(&mut args, "--keyring");
    let key_file_arg = remove_value_flag(&mut args, "--key-file");
    let keyring = match (keyring_arg, key_file_arg) {
        (Some(p), None) => kaya_io::load_keyring_file(&p)?,
        (None, Some(p)) => kaya_io::Keyring::new(0, kaya_io::load_key_file(&p)?),
        (Some(_), Some(_)) => {
            return Err(KayaError::invalid_argument(
                "--keyring and --key-file are mutually exclusive",
            ));
        }
        (None, None) => {
            return Err(KayaError::invalid_argument(
                "one of --keyring <path> or --key-file <path> is required",
            ));
        }
    };

    let mut files = Vec::new();
    walk_files(Path::new(&data_dir), &mut files)?;

    let mut checked = 0u64;
    let mut ok = 0u64;
    let mut failed: Vec<(String, String)> = Vec::new();
    for path in &files {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.len() < 8 {
            continue;
        }
        let is_sealed = &bytes[..8] == kaya_io::ENC_MAGIC.as_slice()
            || &bytes[..8] == kaya_io::ENC_MAGIC2.as_slice();
        if !is_sealed {
            continue;
        }
        checked += 1;
        match kaya_io::verify_sealed(&keyring, &bytes) {
            Ok(()) => ok += 1,
            Err(e) => failed.push((path.display().to_string(), e.to_string())),
        }
    }

    if json {
        let failed_json = failed
            .iter()
            .map(|(p, e)| {
                format!(
                    "{{\"path\":{},\"error\":{}}}",
                    json_string(p),
                    json_string(e)
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        println!("{{\"checked\":{checked},\"ok\":{ok},\"failed\":[{failed_json}]}}");
    } else {
        println!(
            "checked {checked} encrypted file(s): {ok} ok, {} failed",
            failed.len()
        );
        for (p, e) in &failed {
            println!("  FAIL {p}: {e}");
        }
    }

    if failed.is_empty() {
        Ok(())
    } else {
        Err(KayaError::corruption(format!(
            "{} of {checked} encrypted file(s) failed to decrypt with the given keyring",
            failed.len()
        )))
    }
}

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Err(KayaError::invalid_argument(format!(
            "data directory does not exist: {}",
            dir.display()
        )));
    }
    walk_files_inner(dir, out)?;
    Ok(())
}

fn walk_files_inner(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_files_inner(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_rotate_list_verify_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let keyring_path = dir.path().join("keyring.txt");

        run_encryption(
            vec![
                "init".into(),
                "--keyring".into(),
                keyring_path.display().to_string(),
            ],
            dir.path().display().to_string(),
            false,
        )
        .unwrap();
        let ring = kaya_io::load_keyring_file(&keyring_path).unwrap();
        assert_eq!(ring.key_ids(), vec![0]);

        // Seal a file directly with the freshly-initialized key (id 0) to
        // stand in for engine-written data.
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let sealed_path = data_dir.join("some.sst");
        // Round-trip via kaya_io so the on-disk bytes are a real sealed blob.
        {
            let disk = kaya_io::EncryptedDisk::with_keyring(
                kaya_io::FileDisk::new(&data_dir),
                kaya_io::load_keyring_file(&keyring_path).unwrap(),
            );
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let rel = kaya_io::RelativePath::new("some.sst").unwrap();
            rt.block_on(async {
                use kaya_io::Disk;
                disk.append(&rel, b"payload").await.unwrap();
            });
        }
        assert!(sealed_path.exists());

        // Rotate: id 0 -> previous, id 1 -> active.
        run_encryption(
            vec![
                "rotate".into(),
                "--keyring".into(),
                keyring_path.display().to_string(),
            ],
            dir.path().display().to_string(),
            false,
        )
        .unwrap();
        let rotated = kaya_io::load_keyring_file(&keyring_path).unwrap();
        assert_eq!(rotated.active_id(), 1);
        assert_eq!(rotated.key_ids(), vec![0, 1]);

        // Old file (sealed under id 0) is still verifiable through the window.
        run_encryption(
            vec![
                "verify".into(),
                "--keyring".into(),
                keyring_path.display().to_string(),
            ],
            data_dir.display().to_string(),
            false,
        )
        .unwrap();

        // list never errors and reports the current active id.
        run_encryption(
            vec![
                "list".into(),
                "--keyring".into(),
                keyring_path.display().to_string(),
            ],
            dir.path().display().to_string(),
            false,
        )
        .unwrap();
    }

    #[test]
    fn init_refuses_to_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let keyring_path = dir.path().join("keyring.txt");
        std::fs::write(&keyring_path, "active 0\n").unwrap();
        let err = run_encryption(
            vec![
                "init".into(),
                "--keyring".into(),
                keyring_path.display().to_string(),
            ],
            dir.path().display().to_string(),
            false,
        )
        .unwrap_err();
        assert!(matches!(err, KayaError::InvalidArgument { .. }));
    }
}
