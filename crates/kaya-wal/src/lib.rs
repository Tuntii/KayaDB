mod batch;
mod codec;
mod inspect;
mod recovery;
mod writer;

pub use codec::{
    decode_record, encode_record, DecodeRecordResult, WalPayload, WalRecord, WalRecordType,
    WalWarning, WAL_HEADER_LEN, WAL_MAGIC, WAL_VERSION,
};
pub use inspect::{inspect_wal_path, WalInspection, WalInspectionRow};
pub use recovery::{recover_wal, RecoveredRecord, WalRecoveryReport};
pub use writer::{AppendResult, SegmentId, WalWriter};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaya_core::{DurabilityMode, WalBatchConfig, WalConfig};
    use kaya_io::SimDisk;

    use super::*;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn block_on_multi<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn batch_config(records: usize) -> WalConfig {
        WalConfig {
            batch: WalBatchConfig {
                batch_max_records: records,
                ..WalBatchConfig::default()
            },
            ..WalConfig::default()
        }
    }

    async fn strict_put(writer: &WalWriter<SimDisk>, key: u8) {
        writer
            .append(
                WalPayload::Put {
                    key: vec![key],
                    value: vec![key, key],
                },
                DurabilityMode::Strict,
            )
            .await
            .unwrap();
    }

    // KD-0206 — durable-prefix invariant:
    // Every record appended with Strict durability before a crash must appear
    // in the recovery report after crash+restart.
    #[test]
    fn wal_durable_prefix_survives_crash() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = WalConfig::default();
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            let mut strict_lsns = Vec::new();
            for i in 0_u8..5 {
                let result = writer
                    .append(
                        WalPayload::Put {
                            key: vec![i],
                            value: vec![i, i],
                        },
                        DurabilityMode::Strict,
                    )
                    .await
                    .unwrap();
                strict_lsns.push(result.lsn);
            }

            disk.crash();

            let report = recover_wal(config, disk).await.unwrap();
            let recovered_lsns: Vec<_> = report.records.iter().map(|r| r.record.lsn).collect();
            for lsn in &strict_lsns {
                assert!(
                    recovered_lsns.contains(lsn),
                    "strictly written LSN {lsn} missing after crash"
                );
            }
            assert_eq!(
                report.records.len(),
                strict_lsns.len(),
                "expected exactly the strict records"
            );
        });
    }

    // Relaxed (non-fsynced) records are not in stable storage, so they must be
    // absent after a crash that resets volatile to stable.
    #[test]
    fn wal_relaxed_writes_lost_after_crash() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = WalConfig::default();
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            for i in 0_u8..3 {
                writer
                    .append(
                        WalPayload::Put {
                            key: vec![i],
                            value: vec![i],
                        },
                        DurabilityMode::Relaxed,
                    )
                    .await
                    .unwrap();
            }

            // Crash before any fsync.
            disk.crash();

            let report = recover_wal(config, disk).await.unwrap();
            assert_eq!(
                report.records.len(),
                0,
                "relaxed-only records should be lost after crash"
            );
        });
    }

    // Recovery must be idempotent: running it twice on the same disk produces
    // the same set of records.
    #[test]
    fn wal_recovery_is_idempotent() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = WalConfig::default();
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            for i in 0_u8..3 {
                writer
                    .append(
                        WalPayload::Put {
                            key: vec![i],
                            value: vec![i],
                        },
                        DurabilityMode::Strict,
                    )
                    .await
                    .unwrap();
            }

            disk.crash();

            let report1 = recover_wal(config.clone(), disk.clone()).await.unwrap();
            let report2 = recover_wal(config, disk).await.unwrap();

            assert_eq!(
                report1.records.len(),
                report2.records.len(),
                "recovery must be idempotent"
            );
            for (r1, r2) in report1.records.iter().zip(report2.records.iter()) {
                assert_eq!(r1.record.lsn, r2.record.lsn);
                assert_eq!(r1.record.payload, r2.record.payload);
            }
        });
    }

    // Strictly ACKed records survive crash; trailing unsynced data does not.
    // The recovery path must truncate the partial tail and return only the
    // durable prefix.
    #[test]
    fn wal_partial_tail_truncated_after_crash() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = WalConfig::default();
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            // One strict record → fsynced to stable.
            writer
                .append(
                    WalPayload::Put {
                        key: b"k1".to_vec(),
                        value: b"v1".to_vec(),
                    },
                    DurabilityMode::Strict,
                )
                .await
                .unwrap();

            // One relaxed record → stays in volatile only.
            writer
                .append(
                    WalPayload::Put {
                        key: b"k2".to_vec(),
                        value: b"v2".to_vec(),
                    },
                    DurabilityMode::Relaxed,
                )
                .await
                .unwrap();

            disk.crash();

            let report = recover_wal(config, disk).await.unwrap();
            assert_eq!(report.records.len(), 1, "only the strict record survives");
            assert_eq!(
                report.records[0].record.payload,
                WalPayload::Put {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec()
                }
            );
        });
    }

    // Multi-segment WAL: writing across a segment boundary must be transparent
    // to recovery.
    #[test]
    fn wal_multi_segment_recovery() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            // Tiny segment size to force rotation after the first record.
            let config = WalConfig {
                segment_max_bytes: 64,
                ..WalConfig::default()
            };
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            let mut lsns = Vec::new();
            for i in 0_u8..4 {
                let r = writer
                    .append(
                        WalPayload::Put {
                            key: vec![i],
                            value: vec![i],
                        },
                        DurabilityMode::Strict,
                    )
                    .await
                    .unwrap();
                lsns.push(r.lsn);
            }

            disk.crash();

            let report = recover_wal(config, disk).await.unwrap();
            assert_eq!(report.records.len(), lsns.len());
            for (expected, recovered) in lsns.iter().zip(report.records.iter()) {
                assert_eq!(*expected, recovered.record.lsn);
            }
        });
    }

    // Batch disabled (default): each strict append performs its own fsync.
    #[test]
    fn wal_batch_disabled_one_fsync_per_strict_append() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = WalConfig::default();
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            for key in 0_u8..4 {
                strict_put(&writer, key).await;
            }

            assert_eq!(disk.fsync_file_count(Some("wal")), 4);
        });
    }

    // Batch enabled: N strict appends share one group fsync.
    #[test]
    fn wal_batch_enabled_group_commit_single_fsync() {
        block_on_multi(async {
            let disk = Arc::new(SimDisk::new());
            let config = batch_config(4);
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            let mut handles = Vec::new();
            for key in 0_u8..4 {
                let writer = writer.clone();
                handles.push(tokio::spawn(async move {
                    strict_put(&writer, key).await;
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }

            assert_eq!(disk.fsync_file_count(Some("wal")), 1);
        });
    }

    // Crash while a partial batch is still in volatile storage: only the durable prefix survives.
    #[test]
    fn wal_batch_crash_mid_batch_keeps_durable_prefix_only() {
        block_on_multi(async {
            let disk = Arc::new(SimDisk::new());
            let config = batch_config(4);
            let writer = WalWriter::open(config.clone(), disk.clone()).await.unwrap();

            // Complete one full batch (4 records, 1 fsync).
            let mut flushed = Vec::new();
            for key in 0_u8..4 {
                let writer = writer.clone();
                flushed.push(tokio::spawn(async move {
                    strict_put(&writer, key).await;
                }));
            }
            for handle in flushed {
                handle.await.unwrap();
            }

            // Start a second batch but crash before it is group-committed.
            let mut pending = Vec::new();
            for key in 10_u8..13 {
                let writer = writer.clone();
                pending.push(tokio::spawn(async move {
                    strict_put(&writer, key).await;
                }));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            disk.crash();
            for handle in pending {
                handle.abort();
            }

            let report = recover_wal(config, disk).await.unwrap();
            assert_eq!(report.records.len(), 4, "only the first batch should survive");
            let recovered_keys: Vec<u8> = report
                .records
                .iter()
                .filter_map(|r| match &r.record.payload {
                    WalPayload::Put { key, .. } => key.first().copied(),
                    _ => None,
                })
                .collect();
            assert_eq!(recovered_keys, vec![0, 1, 2, 3]);
        });
    }

    // KD-0503: malformed WAL record input must not panic.
    #[test]
    fn fuzz_wal_decoder_no_panic() {
        let cases: &[&[u8]] = &[
            b"",
            &[0u8; 1],
            &[0u8; 39],
            &[0u8; 40],
            &[0xffu8; 40],
            &[0u8; 100],
            &[0xffu8; 100],
            b"\x4b\x41\x59\x41\x01\x00garbage_after_magic",
            b"\x00\xff\x80\x7f\xde\xad\xbe\xef\xca\xfe\xba\xbe",
        ];
        for input in cases {
            let _ = decode_record(input, 0, u32::MAX); // must not panic
        }
    }
}
