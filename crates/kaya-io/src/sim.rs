use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use kaya_core::{KayaError, Result};

use crate::{DirEntry, Disk, RelativePath};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimSeed(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultRule {
    pub operation_index: u64,
    pub description: String,
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
}

#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    state: Arc<Mutex<SimState>>,
}

impl SimDisk {
    pub fn new() -> Self {
        Self::default()
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
        Self::record_event(&mut state, "fsync_dir", path, None, None, None, "ok");
        Ok(())
    }

    async fn truncate(&self, path: &RelativePath, len: u64) -> Result<()> {
        let mut state = self.state.lock().expect("sim disk mutex poisoned");
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
