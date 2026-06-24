//! Shared Disk trait contract checks for FileDisk, SimDisk, and IoUringDisk.

use std::sync::Arc;

use kaya_core::Result;

use crate::{Disk, RelativePath};

/// Append, fsync, and read back persisted bytes.
pub async fn test_append_fsync_read<D: Disk>(disk: Arc<D>) -> Result<()> {
    let path = RelativePath::new("contract/append.bin")?;
    let offset = disk.append(&path, b"hello").await?;
    assert_eq!(offset, 0);
    disk.fsync_file(&path).await?;

    let mut buf = [0u8; 5];
    let n = disk.read_at(&path, 0, &mut buf).await?;
    assert_eq!(n, 5);
    assert_eq!(&buf, b"hello");
    Ok(())
}

/// Write at offset, truncate, rename, and remove.
pub async fn test_write_truncate_rename_remove<D: Disk>(disk: Arc<D>) -> Result<()> {
    let from = RelativePath::new("contract/from.bin")?;
    let to = RelativePath::new("contract/to.bin")?;

    disk.write_at(&from, 0, b"payload").await?;
    disk.fsync_file(&from).await?;
    disk.truncate(&from, 3).await?;
    disk.rename(&from, &to).await?;
    disk.fsync_dir(&RelativePath::new("contract")?).await?;

    let mut buf = [0u8; 8];
    let n = disk.read_at(&to, 0, &mut buf).await?;
    assert_eq!(n, 3);
    assert_eq!(&buf[..3], b"pay");

    disk.remove_file(&to).await?;
    let entries = disk.list_dir(&RelativePath::new("contract")?).await?;
    assert!(!entries.iter().any(|e| e.path.as_str() == "contract/to.bin"));
    Ok(())
}

/// List directory entries after creating nested files.
pub async fn test_list_dir<D: Disk>(disk: Arc<D>) -> Result<()> {
    let a = RelativePath::new("contract/list/a.bin")?;
    let b = RelativePath::new("contract/list/b.bin")?;
    disk.append(&a, b"a").await?;
    disk.append(&b, b"b").await?;

    let entries = disk.list_dir(&RelativePath::new("contract/list")?).await?;
    assert_eq!(entries.len(), 2);
    assert!(entries
        .iter()
        .any(|e| e.path.as_str() == "contract/list/a.bin"));
    assert!(entries
        .iter()
        .any(|e| e.path.as_str() == "contract/list/b.bin"));
    Ok(())
}
