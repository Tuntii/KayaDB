use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use kaya_core::Result;

use crate::{DirEntry, Disk, RelativePath};

/// Real-filesystem [`Disk`] rooted at a directory.
///
/// # Concurrency
///
/// `append` honors the [`Disk::append`] contract by serializing all appends
/// through a per-instance lock that is held across the open + length probe +
/// write, so concurrent appends via one `FileDisk` (or its clones, which
/// share the lock) never interleave and always return the correct offset.
///
/// Two *separately constructed* `FileDisk` instances on the same root do not
/// share that lock and rely only on the OS's `O_APPEND` atomicity.
#[derive(Debug, Clone)]
pub struct FileDisk {
    root: PathBuf,
    /// Serializes `append` calls on this instance (shared by clones).
    append_lock: Arc<Mutex<()>>,
}

impl FileDisk {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            append_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn root(&self) -> &PathBuf {
        &self.root
    }

    fn resolve(&self, path: &RelativePath) -> PathBuf {
        let mut resolved = self.root.clone();
        for component in path.components() {
            resolved.push(component);
        }
        resolved
    }

    fn ensure_parent(&self, path: &RelativePath) -> Result<()> {
        if let Some(parent) = self.resolve(path).parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }
}

impl Disk for FileDisk {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut file = OpenOptions::new().read(true).open(self.resolve(path))?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(file.read(buf)?)
    }

    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> Result<usize> {
        self.ensure_parent(path)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.resolve(path))?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(file.write(buf)?)
    }

    async fn append(&self, path: &RelativePath, buf: &[u8]) -> Result<u64> {
        // Hold the per-instance lock across open + length probe + write so
        // concurrent appends through this instance (or its clones) are atomic
        // and serialized per the `Disk::append` contract.
        let _guard = self
            .append_lock
            .lock()
            .expect("file disk append mutex poisoned");
        self.ensure_parent(path)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(self.resolve(path))?;
        let offset = file.metadata()?.len();
        file.write_all(buf)?;
        Ok(offset)
    }

    async fn fsync_file(&self, path: &RelativePath) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.resolve(path))?;
        file.sync_all()?;
        Ok(())
    }

    async fn fsync_dir(&self, path: &RelativePath) -> Result<()> {
        #[cfg(unix)]
        {
            // On Unix, durability of a directory *entry* (after create, rename
            // or remove) is only guaranteed once the directory itself is
            // fsync'd. Open the directory read-only and sync its file
            // descriptor. Without this, an acknowledged rename/publish can be
            // lost on crash even though the file's own data was fsync'd.
            let dir = self.resolve(path);
            let file = OpenOptions::new().read(true).open(&dir)?;
            file.sync_all()?;
            Ok(())
        }
        #[cfg(not(unix))]
        {
            // On Windows there is no portable way to flush a directory handle
            // (`FlushFileBuffers` on a directory requires a handle opened with
            // backup semantics and is not guaranteed by the platform). The call
            // boundary is preserved so the durability intent stays explicit;
            // directory-entry durability relies on the underlying filesystem.
            // Documented as an accepted platform limitation in
            // `spec/docs/disk-and-io-spec.md` §4.4.
            let _ = path;
            Ok(())
        }
    }

    async fn truncate(&self, path: &RelativePath, len: u64) -> Result<()> {
        let file = OpenOptions::new().write(true).open(self.resolve(path))?;
        file.set_len(len)?;
        Ok(())
    }

    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<()> {
        self.ensure_parent(to)?;
        fs::rename(self.resolve(from), self.resolve(to))?;
        Ok(())
    }

    async fn remove_file(&self, path: &RelativePath) -> Result<()> {
        match fs::remove_file(self.resolve(path)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn list_dir(&self, path: &RelativePath) -> Result<Vec<DirEntry>> {
        let dir = self.resolve(path);
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let child_path = path.join(file_name)?;
            let metadata = entry.metadata()?;
            entries.push(DirEntry {
                path: child_path,
                is_dir: metadata.is_dir(),
                len: metadata.len(),
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    async fn file_len(&self, path: &RelativePath) -> Result<u64> {
        Ok(fs::metadata(self.resolve(path))?.len())
    }
}
