//! Linux partition proof: iptables partition nemesis applies at least once.
//!
//! Run on Linux CI / nightly: `cargo test -p kaya-jepsen-test --test partition_nemesis -- --ignored --nocapture`

#[cfg(target_os = "linux")]
use kaya_jepsen_test::{t2_scenario, ClusterController, TestConfig, TestRunner};
#[cfg(target_os = "linux")]
use std::time::Duration;

#[cfg(target_os = "linux")]
async fn run_partition_proof_once() -> kaya_jepsen_test::TestResult {
    let mut scenario = t2_scenario();
    scenario.duration_secs = 12;
    scenario.workload.duration = Duration::from_secs(12);
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(4);
        nemesis.duration = Duration::from_secs(3);
    }

    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .expect("spawn cluster");

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("T2 partition proof should complete");

    cluster.shutdown_all().await;
    result
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "linux partition proof — nightly with sudo/iptables"]
async fn partition_nemesis_applies() {
    eprintln!("[partition_nemesis] T2-style scenario: assert partition_applied > 0");
    let result = run_partition_proof_once().await;

    eprintln!(
        "[partition_nemesis] stats: attempted={} applied={} failed={}",
        result.partition_attempted, result.partition_applied, result.partition_failed
    );
    assert!(
        result.partition_attempted > 0,
        "partition nemesis should attempt at least once"
    );
    assert!(
        result.partition_applied > 0,
        "iptables partition should apply on linux (attempted={}, failed={})",
        result.partition_attempted,
        result.partition_failed
    );
    assert!(
        result.passed,
        "linearizability should hold under partition: {:?}",
        result.violations
    );
    eprintln!(
        "[partition_nemesis] PASSED partition_applied={}",
        result.partition_applied
    );
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn partition_nemesis_linux_only_gate() {
    eprintln!(
        "[partition_nemesis] SKIP: partition_applied>0 proof requires linux CI (see jepsen.yml full-suite)"
    );
}
