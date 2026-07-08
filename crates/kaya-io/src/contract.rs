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

/// Hammer `append` on one path from many threads and verify the
/// [`Disk::append`] concurrency contract: no torn or interleaved appends.
///
/// Spawns `N_THREADS` threads, each appending `APPENDS_PER_THREAD` payloads
/// to the same file. Each payload is a single byte value repeated `32 +
/// thread` times, with the byte encoding the thread id and the low bit
/// alternating per append so adjacent appends from one thread never merge
/// into a single run. Afterwards the file must have the exact total length
/// and parse into exactly `N_THREADS * APPENDS_PER_THREAD` contiguous runs,
/// each with its thread's exact payload length.
///
/// This helper is synchronous because it manages its own threads; each
/// thread drives the async `append` calls with a minimal executor.
pub fn test_concurrent_appends<D: Disk>(disk: Arc<D>) -> Result<()> {
    const N_THREADS: usize = 8;
    const APPENDS_PER_THREAD: usize = 50;

    fn payload_len(thread: usize) -> usize {
        32 + thread
    }

    let path = RelativePath::new("contract/concurrent_append.bin")?;

    let handles: Vec<_> = (0..N_THREADS)
        .map(|thread| {
            let disk = Arc::clone(&disk);
            let path = path.clone();
            std::thread::spawn(move || -> Result<()> {
                for seq in 0..APPENDS_PER_THREAD {
                    // Byte encodes (thread, seq parity): consecutive appends
                    // from the same thread always differ, so every append is
                    // its own run in the final file.
                    let byte = (thread as u8) * 2 + (seq as u8) % 2;
                    let payload = vec![byte; payload_len(thread)];
                    block_on(disk.append(&path, &payload))?;
                }
                Ok(())
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("concurrent append thread panicked")?;
    }

    let expected_len: usize = (0..N_THREADS).map(payload_len).sum::<usize>() * APPENDS_PER_THREAD;
    let actual_len = block_on(disk.file_len(&path))?;
    assert_eq!(
        actual_len, expected_len as u64,
        "concurrent appends lost or duplicated bytes"
    );

    // Read the whole file back.
    let mut contents = vec![0u8; expected_len];
    let mut read = 0usize;
    while read < expected_len {
        let n = block_on(disk.read_at(&path, read as u64, &mut contents[read..]))?;
        assert!(n > 0, "unexpected EOF at offset {read}");
        read += n;
    }

    // Parse into maximal runs of identical bytes and check each is exactly
    // one append's payload.
    let mut runs_per_thread = [0usize; N_THREADS];
    let mut total_runs = 0usize;
    let mut position = 0usize;
    while position < contents.len() {
        let byte = contents[position];
        let run_start = position;
        while position < contents.len() && contents[position] == byte {
            position += 1;
        }
        let run_len = position - run_start;
        let thread = usize::from(byte / 2);
        assert!(
            thread < N_THREADS && byte < (N_THREADS as u8) * 2,
            "unexpected byte {byte:#04x} at offset {run_start}"
        );
        assert_eq!(
            run_len,
            payload_len(thread),
            "torn or interleaved append: run of byte {byte:#04x} at offset {run_start}"
        );
        runs_per_thread[thread] += 1;
        total_runs += 1;
    }
    assert_eq!(
        total_runs,
        N_THREADS * APPENDS_PER_THREAD,
        "wrong number of contiguous runs"
    );
    for (thread, runs) in runs_per_thread.iter().enumerate() {
        assert_eq!(
            *runs, APPENDS_PER_THREAD,
            "thread {thread} appends missing or split"
        );
    }
    Ok(())
}

/// Drive a future to completion on the current thread.
///
/// Minimal executor so contract helpers can run appends from plain threads
/// without pulling an async runtime into the library's dependencies.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Wake, Waker};

    struct ThreadWaker(std::thread::Thread);
    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
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
