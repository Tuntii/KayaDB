use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use kaya_core::Result;

use crate::{DirEntry, Disk, RelativePath};

#[derive(Debug, Clone)]
pub struct FileDisk {
    root: PathBuf,
}

impl FileDisk {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
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
        let _ = path;
        Ok(())
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
