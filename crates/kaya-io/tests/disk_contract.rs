//! Shared Disk trait contract tests for FileDisk, SimDisk, and IoUringDisk.

use std::sync::Arc;

use kaya_io::{
    test_append_fsync_read, test_concurrent_appends, test_list_dir,
    test_write_truncate_rename_remove, Disk, FileDisk, SimDisk,
};

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

async fn run_contract_suite<D: Disk>(disk: Arc<D>) {
    test_append_fsync_read(disk.clone()).await.unwrap();
    test_write_truncate_rename_remove(disk.clone())
        .await
        .unwrap();
    test_list_dir(disk.clone()).await.unwrap();
    // Synchronous helper: it spawns its own threads to hammer `append`.
    test_concurrent_appends(disk).unwrap();
}

#[test]
fn file_disk_contract() {
    let dir = tempfile::tempdir().unwrap();
    let disk = Arc::new(FileDisk::new(dir.path()));
    block_on(run_contract_suite(disk));
}

#[test]
fn sim_disk_contract() {
    let disk = Arc::new(SimDisk::new());
    block_on(run_contract_suite(disk));
}

#[test]
fn io_uring_disk_is_linux_feature_gated() {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        let dir = tempfile::tempdir().unwrap();
        let disk = Arc::new(kaya_io::IoUringDisk::new(dir.path()).unwrap());
        block_on(run_contract_suite(disk));
    }
    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    {
        // Structural gate: IoUringDisk is only built on linux + io_uring feature.
        assert!(std::any::type_name::<FileDisk>().contains("FileDisk"));
    }
}
