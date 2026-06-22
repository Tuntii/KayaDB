use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use kaya_core::{KayaError, Result};

use crate::{DirEntry, Disk, RelativePath};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimSeed(pub u64);

/// The kind of fault to inject at a specific write operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultKind {
    /// The fsync operation returns `KayaError::FsyncFailed`.
    FsyncFailed,
    /// The operation returns `KayaError::Io`.
    IoError,
    /// The operation returns `KayaError::DiskFull`.
    DiskFull,
    /// `append` or `write_at` writes only the first `bytes` bytes and returns
    /// the start offset as if the call succeeded.  This models a torn write.
    PartialWrite { bytes: usize },
}

/// A single fault to inject at the given zero-based write-operation index.
///
/// Write operations are: `write_at`, `append`, `fsync_file`, `fsync_dir`,
/// `truncate`, `rename`, `remove_file`.  Read-only operations (`read_at`,
/// `list_dir`, `file_len`) do not advance the counter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultRule {
    /// Zero-based index among write operations on this disk.
    pub operation_index: u64,
    /// Fault to inject when the counter reaches this index.
    pub kind: FaultKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultSchedule {
    pub seed: SimSeed,
    pub rules: Vec<FaultRule>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimDiskEvent {
    pub event_id: u64,
    pub kind: String,
    pub path: String,
    pub offset: Option<u64>,
    pub requested_len: Option<usize>,
    pub actual_len: Option<usize>,
    pub result: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrashReport {
    pub files_restored: usize,
    pub lost_bytes: u64,
}

#[derive(Debug, Clone, Default)]
struct SimFile {
    volatile: Vec<u8>,
    stable: Vec<u8>,
}

#[derive(Debug, Default)]
struct SimState {
    files: BTreeMap<String, SimFile>,
    events: Vec<SimDiskEvent>,
    next_event_id: u64,
    /// Counts write operations (write_at, append, fsync_file, fsync_dir,
    /// truncate, rename, remove_file) for fault-schedule matching.
    write_op_count: u64,
    fault_schedule: Option<FaultSchedule>,
}

impl SimState {
    /// Advance the write-operation counter and return the injected fault, if any.
    fn take_fault(&mut self) -> Option<FaultKind> {
        let index = self.write_op_count;
        self.write_op_count += 1;
        self.fault_schedule
            .as_ref()?
            .rules
            .iter()
            .find(|r| r.operation_index == index)
            .map(|r| r.kind.clone())
    }
}

#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    state: Arc<Mutex<SimState>>,
}

impl SimDisk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `SimDisk` that injects faults according to the given schedule.
    pub fn with_faults(schedule: FaultSchedule) -> Self {
        Self {
            state: Arc::new(Mutex::new(SimState {
                fault_schedule: Some(schedule),
                ..SimState::default()
            })),
        }
    }

    pub fn crash(&self) -> CrashReport {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        let mut lost_bytes = 0_u64;
        for file in state.files.values_mut() {
            if file.volatile.len() > file.stable.len() {
                lost_bytes += (file.volatile.len() - file.stable.len()) as u64;
            }
            file.volatile = file.stable.clone();
        }
        CrashReport {
            files_restored: state.files.len(),
            lost_bytes,
        }
    }

    pub fn restart(&self) {}

    pub fn events(&self) -> Vec<SimDiskEvent> {
        self.state
            .lock()
            .expect("sim disk mutex poisoned")
            .events
            .clone()
    }

    /// Count successful `fsync_file` operations (optionally filtered by path prefix).
    pub fn fsync_file_count(&self, path_prefix: Option<&str>) -> u64 {
        self.events()
            .iter()
            .filter(|event| {
                event.kind == "fsync_file"
                    && event.result == "ok"
                    && path_prefix.is_none_or(|prefix| event.path.starts_with(prefix))
            })
            .count() as u64
    }

    fn record_event(
        state: &mut SimState,
        kind: impl Into<String>,
        path: &RelativePath,
        offset: Option<u64>,
        requested_len: Option<usize>,
        actual_len: Option<usize>,
        result: impl Into<String>,
    ) {
        let event = SimDiskEvent {
            event_id: state.next_event_id,
            kind: kind.into(),
            path: path.as_str().to_owned(),
            offset,
            requested_len,
            actual_len,
            result: result.into(),
        };
        state.next_event_id += 1;
        state.events.push(event);
    }

    fn key(path: &RelativePath) -> String {
        path.as_str().to_owned()
    }
}

impl Disk for SimDisk {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        let key = Self::key(path);
        let file = state.files.get(&key).ok_or(KayaError::NotFound)?;
        let start = offset as usize;
        if start >= file.volatile.len() {
            Self::record_event(
                &mut state,
                "read_at",
                path,
                Some(offset),
                Some(buf.len()),
                Some(0),
                "eof",
            );
            return Ok(0);
        }
        let available = file.volatile.len() - start;
        let actual = available.min(buf.len());
        buf[..actual].copy_from_slice(&file.volatile[start..start + actual]);
        Self::record_event(
            &mut state,
            "read_at",
            path,
            Some(offset),
            Some(buf.len()),
            Some(actual),
            "ok",
        );
        Ok(actual)
    }

    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> Result<usize> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        if let Some(fault) = state.take_fault() {
            let result_str = fault_result_str(&fault);
            Self::record_event(
                &mut state,
                "write_at",
                path,
                Some(offset),
                Some(buf.len()),
                Some(0),
                result_str,
            );
            return Err(fault_to_error(fault));
        }
        let file = state.files.entry(Self::key(path)).or_default();
        let start = offset as usize;
        let end = start + buf.len();
        if file.volatile.len() < end {
            file.volatile.resize(end, 0);
        }
        file.volatile[start..end].copy_from_slice(buf);
        Self::record_event(
            &mut state,
            "write_at",
            path,
            Some(offset),
            Some(buf.len()),
            Some(buf.len()),
            "ok",
        );
        Ok(buf.len())
    }

    async fn append(&self, path: &RelativePath, buf: &[u8]) -> Result<u64> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        if let Some(fault) = state.take_fault() {
            match fault {
                FaultKind::PartialWrite { bytes } => {
                    let write_len = bytes.min(buf.len());
                    let file = state.files.entry(Self::key(path)).or_default();
                    let offset = file.volatile.len() as u64;
                    file.volatile.extend_from_slice(&buf[..write_len]);
                    Self::record_event(
                        &mut state,
                        "append",
                        path,
                        Some(offset),
                        Some(buf.len()),
                        Some(write_len),
                        "partial_write",
                    );
                    return Ok(offset);
                }
                other => {
                    let result_str = fault_result_str(&other);
                    Self::record_event(
                        &mut state,
                        "append",
                        path,
                        None,
                        Some(buf.len()),
                        Some(0),
                        result_str,
                    );
                    return Err(fault_to_error(other));
                }
            }
        }
        let file = state.files.entry(Self::key(path)).or_default();
        let offset = file.volatile.len() as u64;
        file.volatile.extend_from_slice(buf);
        Self::record_event(
            &mut state,
            "append",
            path,
            Some(offset),
            Some(buf.len()),
            Some(buf.len()),
            "ok",
        );
        Ok(offset)
    }

    async fn fsync_file(&self, path: &RelativePath) -> Result<()> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        if let Some(fault) = state.take_fault() {
            if !matches!(fault, FaultKind::PartialWrite { .. }) {
                let result_str = fault_result_str(&fault);
                Self::record_event(&mut state, "fsync_file", path, None, None, None, result_str);
                return Err(fault_to_error(fault));
            }
        }
        let file = state
            .files
            .get_mut(&Self::key(path))
            .ok_or(KayaError::NotFound)?;
        file.stable = file.volatile.clone();
        Self::record_event(&mut state, "fsync_file", path, None, None, None, "ok");
        Ok(())
    }

    async fn fsync_dir(&self, path: &RelativePath) -> Result<()> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        if let Some(fault) = state.take_fault() {
            if !matches!(fault, FaultKind::PartialWrite { .. }) {
                let result_str = fault_result_str(&fault);
                Self::record_event(&mut state, "fsync_dir", path, None, None, None, result_str);
                return Err(fault_to_error(fault));
            }
        }
        Self::record_event(&mut state, "fsync_dir", path, None, None, None, "ok");
        Ok(())
    }

    async fn truncate(&self, path: &RelativePath, len: u64) -> Result<()> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        let _ = state.take_fault(); // advance counter; truncate is not fault-injected
        let file = state
            .files
            .get_mut(&Self::key(path))
            .ok_or(KayaError::NotFound)?;
        file.volatile.truncate(len as usize);
        if file.stable.len() > len as usize {
            file.stable.truncate(len as usize);
        }
        Self::record_event(&mut state, "truncate", path, None, None, None, "ok");
        Ok(())
    }

    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<()> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        let _ = state.take_fault(); // advance counter; rename is not fault-injected
        let file = state
            .files
            .remove(&Self::key(from))
            .ok_or(KayaError::NotFound)?;
        state.files.insert(Self::key(to), file);
        Self::record_event(&mut state, "rename", from, None, None, None, to.as_str());
        Ok(())
    }

    async fn remove_file(&self, path: &RelativePath) -> Result<()> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
        let _ = state.take_fault(); // advance counter; remove_file is not fault-injected
        state.files.remove(&Self::key(path));
        Self::record_event(&mut state, "remove_file", path, None, None, None, "ok");
        Ok(())
    }

    async fn list_dir(&self, path: &RelativePath) -> Result<Vec<DirEntry>> {
        let state = self.state.lock().expect("sim disk mutex poisoned");
        let prefix = if path.is_root() {
            String::new()
        } else {
            format!("{}/", path.as_str())
        };
        let mut seen = BTreeSet::new();
        let mut entries = Vec::new();
        for (file_path, file) in &state.files {
            if !file_path.starts_with(&prefix) {
                continue;
            }
            let remainder = &file_path[prefix.len()..];
            if remainder.is_empty() {
                continue;
            }
            let first = remainder.split('/').next().unwrap_or_default();
            if !seen.insert(first.to_owned()) {
                continue;
            }
            let child = path.join(first)?;
            let is_dir = remainder.contains('/');
            entries.push(DirEntry {
                path: child,
                is_dir,
                len: if is_dir {
                    0
                } else {
                    file.volatile.len() as u64
                },
            });
        }
        Ok(entries)
    }

    async fn file_len(&self, path: &RelativePath) -> Result<u64> {
        let state = self.state.lock().expect("sim disk mutex poisoned");
        let file = state
            .files
            .get(&Self::key(path))
            .ok_or(KayaError::NotFound)?;
        Ok(file.volatile.len() as u64)
    }
}

fn fault_to_error(fault: FaultKind) -> KayaError {
    match fault {
        FaultKind::FsyncFailed => KayaError::FsyncFailed,
        FaultKind::IoError => KayaError::Io {
            message: "simulated io error".into(),
        },
        FaultKind::DiskFull => KayaError::DiskFull,
        FaultKind::PartialWrite { .. } => KayaError::Io {
            message: "simulated io error".into(),
        },
    }
}

fn fault_result_str(fault: &FaultKind) -> &'static str {
    match fault {
        FaultKind::FsyncFailed => "fsync_failed",
        FaultKind::IoError => "io_error",
        FaultKind::DiskFull => "disk_full",
        FaultKind::PartialWrite { .. } => "partial_write",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn sim_disk_basic_write_read_fsync_crash() {
        let disk = SimDisk::new();
        let path = RelativePath::new("data.bin").unwrap();

        block_on(disk.append(&path, b"hello")).unwrap();
        block_on(disk.fsync_file(&path)).unwrap();

        // After fsync, volatile == stable.  Crash should retain data.
        disk.crash();

        let mut buf = [0u8; 5];
        let n = block_on(disk.read_at(&path, 0, &mut buf)).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn sim_disk_unsynced_write_lost_after_crash() {
        let disk = SimDisk::new();
        let path = RelativePath::new("data.bin").unwrap();

        block_on(disk.append(&path, b"lost")).unwrap();
        // No fsync → only in volatile.
        disk.crash();

        let mut buf = [0u8; 4];
        let n = block_on(disk.read_at(&path, 0, &mut buf)).unwrap();
        assert_eq!(n, 0, "unsynced bytes should be gone after crash");
    }

    #[test]
    fn sim_disk_partial_write_fault() {
        // op 0 = append (PartialWrite 3 of 5 bytes), op 1 = fsync_file
        let schedule = FaultSchedule {
            seed: SimSeed(42),
            rules: vec![FaultRule {
                operation_index: 0,
                kind: FaultKind::PartialWrite { bytes: 3 },
            }],
        };
        let disk = SimDisk::with_faults(schedule);
        let path = RelativePath::new("test.bin").unwrap();

        block_on(disk.append(&path, b"hello")).unwrap();
        block_on(disk.fsync_file(&path)).unwrap(); // makes 3 partial bytes stable

        disk.crash();

        let mut buf = [0u8; 10];
        let n = block_on(disk.read_at(&path, 0, &mut buf)).unwrap();
        assert_eq!(n, 3, "only the 3 partially written bytes should survive");
        assert_eq!(&buf[..3], b"hel");
    }

    #[test]
    fn sim_disk_fsync_fail_fault() {
        // op 0 = append (ok), op 1 = fsync_file (FsyncFailed)
        let schedule = FaultSchedule {
            seed: SimSeed(1),
            rules: vec![FaultRule {
                operation_index: 1,
                kind: FaultKind::FsyncFailed,
            }],
        };
        let disk = SimDisk::with_faults(schedule);
        let path = RelativePath::new("test.bin").unwrap();

        block_on(disk.append(&path, b"data")).unwrap();
        let result = block_on(disk.fsync_file(&path));
        assert_eq!(result, Err(KayaError::FsyncFailed));
    }

    #[test]
    fn sim_disk_disk_full_fault_on_append() {
        let schedule = FaultSchedule {
            seed: SimSeed(2),
            rules: vec![FaultRule {
                operation_index: 0,
                kind: FaultKind::DiskFull,
            }],
        };
        let disk = SimDisk::with_faults(schedule);
        let path = RelativePath::new("test.bin").unwrap();

        let result = block_on(disk.append(&path, b"data"));
        assert_eq!(result, Err(KayaError::DiskFull));
    }
}
