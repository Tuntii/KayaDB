//! Bank workload unit + optional integration tests (M17).
//!
//! Offline sum-invariant tests always run. Full cluster bank scenario:
//!
//! ```text
//! cargo test -p kaya-jepsen-test --test bank_workload bank_scenario_cluster -- --ignored --nocapture
//! ```

use kaya_jepsen_test::{
    bank_account_key, bank_expected_total, bank_scenario, bank_transfer, check_balances_sum,
    check_transfer_history, encode_balance, parse_balance, seed_bank_accounts, verify_bank_sum_live,
    BankModel, BankTransfer, ClusterController, TestConfig, TestRunner, WorkloadType,
    BANK_INITIAL_BALANCE, BANK_NUM_ACCOUNTS,
};
use std::time::Duration;

#[test]
fn bank_keys_and_balances_round_trip() {
    assert_eq!(bank_account_key(3), b"acct:3");
    assert_eq!(parse_balance(&encode_balance(42)).unwrap(), 42);
    assert_eq!(
        bank_expected_total(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE),
        1000
    );
}

#[test]
fn mock_transfer_history_preserves_constant_sum() {
    let history = vec![
        BankTransfer {
            from: 0,
            to: 1,
            amount: 25,
            committed: true,
        },
        BankTransfer {
            from: 1,
            to: 2,
            amount: 10,
            committed: true,
        },
        BankTransfer {
            from: 0,
            to: 1,
            amount: 1000,
            committed: false,
        },
        BankTransfer {
            from: 2,
            to: 0,
            amount: 5,
            committed: true,
        },
    ];
    let model = check_transfer_history(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE, &history).unwrap();
    let expected = bank_expected_total(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE);
    model.check_sum_invariant(expected).unwrap();
    assert_eq!(model.total(), expected);
}

#[test]
fn mock_history_detects_sum_violation() {
    assert!(check_balances_sum(&[100, 100, 50], 300).is_err());
    let mut model = BankModel::new(2, 100);
    let bad = [50i64, 100];
    assert!(check_balances_sum(&bad, 200).is_err());
    model.transfer(0, 1, 40).unwrap();
    model
        .check_sum_invariant(bank_expected_total(2, 100))
        .unwrap();
}

#[test]
fn bank_scenario_descriptor() {
    let s = bank_scenario();
    assert_eq!(s.workload.workload_type, WorkloadType::Bank);
    assert_eq!(s.id, "bank");
}

/// Integration: short bank scenario against an in-process 3-node cluster.
///
/// Ignored by default (heavier than unit tests). Run with `--ignored`.
#[tokio::test]
#[ignore = "requires full in-process cluster; use --ignored"]
async fn bank_scenario_cluster_sum_invariant() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .expect("spawn cluster");

    let mut scenario = bank_scenario();
    scenario.duration_secs = 20;
    scenario.workload.duration = Duration::from_secs(20);
    scenario.workload.clients = 3;
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(8);
        nemesis.duration = Duration::from_secs(3);
    }

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("bank scenario should complete");

    assert!(
        result.passed,
        "bank sum invariant failed: {:?}",
        result.violations
    );

    cluster.shutdown_all().await;
}

/// Integration: seed + single transfer + sum check (no nemesis).
#[tokio::test]
#[ignore = "requires full in-process cluster; use --ignored"]
async fn bank_single_transfer_integration() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .expect("spawn");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let leader = cluster
        .wait_for_leader(Duration::from_secs(20))
        .await
        .expect("leader");

    let mut client = kaya_client::KayaClient::connect(leader.client_addr)
        .await
        .expect("connect");
    client.set_max_redirects(10);

    seed_bank_accounts(&mut client, 4, 50).await.expect("seed");
    assert!(bank_transfer(&mut client, 0, 1, 20).await.expect("xfer"));
    verify_bank_sum_live(&mut client, 4, 200, Duration::from_secs(3))
        .await
        .expect("sum");

    cluster.shutdown_all().await;
}
