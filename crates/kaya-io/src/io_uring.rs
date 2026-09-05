//! Linux io_uring-backed Disk prototype (feature `io_uring`).

use std::fmt;
use std::fs::{self, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Mutex;

use io_uring::{opcode, types, IoUring};
use kaya_core::Result;

use crate::{DirEntry, Disk, RelativePath};

/// File-backed disk using io_uring for read/write/fsync hot paths.
pub struct IoUringDisk {
    root: PathBuf,
    ring: Mutex<IoUring>,
}

impl fmt::Debug for IoUringDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoUringDisk")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl IoUringDisk {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let ring = IoUring::new(256).map_err(|e| std::io::Error::other(e))?;
        Ok(Self {
            root: root.into(),
            ring: Mutex::new(ring),
        })
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

    fn uring_read(&self, fd: types::Fd, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut ring = self.ring.lock().unwrap();
        let read_e = opcode::Read::new(fd, buf.as_mut_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(0x01);

        unsafe {
            ring.submission()
                .push(&read_e)
                .map_err(|e| std::io::Error::other(e))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| std::io::Error::other(e))?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| std::io::Error::other("missing io_uring completion"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(std::io::Error::from_raw_os_error(-res).into());
        }
        Ok(res as usize)
    }

    /// Write all of `buf` through an already-locked ring.
    ///
    /// Callers lock `self.ring` once and hold the guard for the whole write
    /// (and, for `append`, across the length probe too) so multi-submission
    /// writes are never interleaved with a concurrent write on this instance.
    fn uring_write_all(
        ring: &mut IoUring,
        fd: types::Fd,
        mut offset: u64,
        buf: &[u8],
    ) -> Result<()> {
        let mut written = 0usize;
        while written < buf.len() {
            let n = Self::uring_write(ring, fd, offset, &buf[written..])?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "io_uring write returned 0 bytes",
                )
                .into());
            }
            written += n;
            offset += n as u64;
        }
        Ok(())
    }

    fn uring_write(ring: &mut IoUring, fd: types::Fd, offset: u64, buf: &[u8]) -> Result<usize> {
        let write_e = opcode::Write::new(fd, buf.as_ptr(), buf.len() as u32)
            .offset(offset)
            .build()
            .user_data(0x02);

        unsafe {
            ring.submission()
                .push(&write_e)
                .map_err(|e| std::io::Error::other(e))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| std::io::Error::other(e))?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| std::io::Error::other("missing io_uring completion"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(std::io::Error::from_raw_os_error(-res).into());
        }
        Ok(res as usize)
    }

    fn uring_fsync(&self, fd: types::Fd) -> Result<()> {
        let mut ring = self.ring.lock().unwrap();
        let fsync_e = opcode::Fsync::new(fd).build().user_data(0x03);

        unsafe {
            ring.submission()
                .push(&fsync_e)
                .map_err(|e| std::io::Error::other(e))?;
        }
        ring.submit_and_wait(1)
            .map_err(|e| std::io::Error::other(e))?;

        let cqe = ring
            .completion()
            .next()
            .ok_or_else(|| std::io::Error::other("missing io_uring completion"))?;
        let res = cqe.result();
        if res < 0 {
            return Err(std::io::Error::from_raw_os_error(-res).into());
        }
        Ok(())
    }
}

impl Disk for IoUringDisk {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let file = OpenOptions::new().read(true).open(self.resolve(path))?;
        let fd = types::Fd(file.as_raw_fd());
        self.uring_read(fd, offset, buf)
    }

    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> Result<usize> {
        self.ensure_parent(path)?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.resolve(path))?;
        let fd = types::Fd(file.as_raw_fd());
        let mut ring = self.ring.lock().unwrap();
        Self::uring_write_all(&mut ring, fd, offset, buf)?;
        Ok(buf.len())
    }

    async fn append(&self, path: &RelativePath, buf: &[u8]) -> Result<u64> {
        self.ensure_parent(path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .write(true)
            .open(self.resolve(path))?;
        // Lock the ring before probing the length so concurrent appends via
        // this instance are serialized per the `Disk::append` contract.
        let mut ring = self.ring.lock().unwrap();
        let offset = file.metadata()?.len();
        let fd = types::Fd(file.as_raw_fd());
        Self::uring_write_all(&mut ring, fd, offset, buf)?;
        Ok(offset)
    }

    async fn fsync_file(&self, path: &RelativePath) -> Result<()> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.resolve(path))?;
        let fd = types::Fd(file.as_raw_fd());
        self.uring_fsync(fd)
    }

    async fn fsync_dir(&self, path: &RelativePath) -> Result<()> {
        // Linux directory-entry durability: open the directory read-only and
        // fsync its descriptor through the ring, mirroring `fsync_file`. This
        // makes create/rename/remove entries durable after an atomic publish.
        let file = OpenOptions::new().read(true).open(self.resolve(path))?;
        let fd = types::Fd(file.as_raw_fd());
        self.uring_fsync(fd)
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
