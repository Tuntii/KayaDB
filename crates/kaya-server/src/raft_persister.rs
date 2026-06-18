//! Periodic persistence of durable Raft state (hard-state + log) to disk.

use std::io;
use std::path::Path;

use kaya_raft::{
    default_hard_state, DiskRaftStorage, LogIndex, PersistedRaftState, RaftStorage,
    RaftStorageError,
};

/// Tracks the last flushed Raft view and writes changes to [`DiskRaftStorage`].
pub struct RaftPersister {
    storage: DiskRaftStorage,
    last_persisted: Option<PersistedRaftState>,
}

impl RaftPersister {
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        Ok(Self {
            storage: DiskRaftStorage::open(data_dir),
            last_persisted: None,
        })
    }

    /// Seed the last-flushed snapshot (e.g. after startup recover).
    pub fn seed_last_persisted(&mut self, state: PersistedRaftState) {
        self.last_persisted = Some(state);
    }

    /// Load persisted state. Returns `None` when the on-disk store is fresh/empty.
    pub fn load_state(&self) -> Result<Option<PersistedRaftState>, String> {
        let state = self.storage.load().map_err(|e| e.to_string())?;
        if state.hard_state == default_hard_state() && state.log.last_index() == LogIndex(0) {
            Ok(None)
        } else {
            Ok(Some(state))
        }
    }

    /// Persist `view` when it differs from the last successful flush.
    pub fn flush_view(&mut self, view: PersistedRaftState) -> Result<(), String> {
        if self.last_persisted.as_ref() == Some(&view) {
            return Ok(());
        }
        self.storage
            .save_hard_state(&view.hard_state)
            .map_err(storage_err)?;
        self.storage
            .save_log(&view.log, &view.hard_state)
            .map_err(storage_err)?;
        self.storage.sync().map_err(storage_err)?;
        self.last_persisted = Some(view);
        Ok(())
    }
}

fn storage_err(e: RaftStorageError) -> String {
    e.to_string()
}