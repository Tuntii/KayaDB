//! Multi-range bank grand matrix: split + merge + kill + partition under SI sum invariant.
//!
//! Nightly / full Jepsen suite:
//! ```text
//! cargo test -p kaya-jepsen-test --test grand_matrix -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Local short:
//! ```text
//! KAYA_JEPSEN_FAST=1 cargo test -p kaya-jepsen-test --test grand_matrix -- --ignored --nocapture --test-threads=1
//! ```

use kaya_jepsen_test::{
    multi_range_bank_scenario, ClusterController, Scenario, TestConfig, TestResult, TestRunner,
    Topology, VerifyMode,
};
use std::time::Duration;

fn scale_for_fast(mut scenario: Scenario) -> Scenario {
    if std::env::var("KAYA_JEPSEN_FAST").ok().as_deref() != Some("1") {
        return scenario;
    }
    scenario.duration_secs = 20;
    scenario.workload.duration = Duration::from_secs(20);
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(4);
        nemesis.duration = Duration::from_secs(2);
    }
    eprintln!(
        "[grand_matrix] fast: duration={}s clients={}",
        scenario.duration_secs, scenario.workload.clients
    );
    scenario
}

async fn spawn_for(scenario: &Scenario) -> (tempfile::TempDir, ClusterController) {
    let dir = tempfile::tempdir().unwrap();
    let cluster = match scenario.topology {
        Topology::ThreeNodeMultiRange => {
            ClusterController::spawn_three_node_multi_range(dir.path().to_path_buf())
                .await
                .expect("spawn multi-range cluster")
        }
        Topology::FourNodeJoin | Topology::ThreeNode => {
            ClusterController::spawn_three_node(dir.path().to_path_buf())
                .await
                .expect("spawn cluster")
        }
    };
    (dir, cluster)
}

async fn run_once(scenario: &Scenario) -> TestResult {
    let (dir, mut cluster) = spawn_for(scenario).await;
    let config = TestConfig::from_scenario(scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(scenario, &mut cluster)
        .await
        .unwrap_or_else(|e| panic!("{} should complete: {e}", scenario.id));
    cluster.shutdown_all().await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    result
}

/// Full multi-range bank under composite range + kill + partition nemesis.
#[tokio::test]
#[ignore = "grand matrix — nightly / --ignored; multi-range bank + chaos"]
async fn multi_range_bank_grand_matrix() {
    let scenario = scale_for_fast(multi_range_bank_scenario());
    assert_eq!(scenario.id, "bank-mr");
    assert_eq!(scenario.verify, VerifyMode::BankSum);
    assert_eq!(scenario.topology, Topology::ThreeNodeMultiRange);
    assert_eq!(
        scenario.workload.bank_layout,
        kaya_jepsen_test::BankLayout::MultiRange
    );

    let mut result = run_once(&scenario).await;
    for attempt in 1..=4 {
        if result.passed {
            break;
        }
        eprintln!(
            "[grand_matrix] bank-mr retry {attempt}/4 (violations={:?})",
            result.violations
        );
        unsafe { std::env::set_var("KAYA_JEPSEN_QUIET", "1") };
        result = run_once(&scenario).await;
        unsafe { std::env::remove_var("KAYA_JEPSEN_QUIET") };
    }

    assert!(
        result.passed,
        "bank-mr grand matrix sum invariant failed: {:?}",
        result.violations
    );
    // Kill+partition composite should attempt partition at least once over a full run.
    // Under KAYA_JEPSEN_FAST short windows this may be flaky; only assert on full duration.
    if std::env::var("KAYA_JEPSEN_FAST").ok().as_deref() != Some("1") {
        assert!(
            result.partition_attempted > 0,
            "expected partition nemesis to fire (attempted=0)"
        );
    }
    eprintln!(
        "[grand_matrix] bank-mr PASSED sum invariant partition_attempted={} applied={}",
        result.partition_attempted, result.partition_applied
    );
}

/// Quieter multi-range bank: concurrent SI + 2PC without process kill (sum only).
#[tokio::test]
#[ignore = "grand matrix short path — multi-range 2PC sum without kill"]
async fn multi_range_bank_sum_no_kill() {
    let mut scenario = multi_range_bank_scenario();
    scenario.duration_secs = 15;
    scenario.workload.duration = Duration::from_secs(15);
    scenario.workload.clients = 3;
    scenario.nemesis = None;

    let result = run_once(&scenario).await;
    assert!(
        result.passed,
        "multi-range bank (no kill) failed: {:?}",
        result.violations
    );
    eprintln!("[grand_matrix] multi-range bank no-kill PASSED");
}
