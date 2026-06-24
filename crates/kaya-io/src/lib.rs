#![allow(async_fn_in_trait)]

mod contract;
mod file;
mod path;
mod sim;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod io_uring;

pub use contract::{test_append_fsync_read, test_list_dir, test_write_truncate_rename_remove};
pub use file::FileDisk;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring::IoUringDisk;
use kaya_core::Result;
pub use path::RelativePath;
pub use sim::{CrashReport, FaultKind, FaultRule, FaultSchedule, SimDisk, SimDiskEvent, SimSeed};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub path: RelativePath,
    pub is_dir: bool,
    pub len: u64,
}

pub trait Disk: Send + Sync + 'static {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> Result<usize>;
    async fn append(&self, path: &RelativePath, buf: &[u8]) -> Result<u64>;
    async fn fsync_file(&self, path: &RelativePath) -> Result<()>;
    async fn fsync_dir(&self, path: &RelativePath) -> Result<()>;
    async fn truncate(&self, path: &RelativePath, len: u64) -> Result<()>;
    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<()>;
    async fn remove_file(&self, path: &RelativePath) -> Result<()>;
    async fn list_dir(&self, path: &RelativePath) -> Result<Vec<DirEntry>>;
    async fn file_len(&self, path: &RelativePath) -> Result<u64>;
}
