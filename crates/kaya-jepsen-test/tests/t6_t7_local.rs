use kaya_jepsen_test::{t6_scenario, t7_scenario, ClusterController, TestConfig, TestRunner};
use std::time::Duration;

fn shorten_scenario(
    scenario: &mut kaya_jepsen_test::Scenario,
    duration_secs: u64,
    nemesis_interval_secs: u64,
    nemesis_down_secs: u64,
) {
    scenario.duration_secs = duration_secs;
    scenario.workload.duration = Duration::from_secs(duration_secs);
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(nemesis_interval_secs);
        nemesis.duration = Duration::from_secs(nemesis_down_secs);
    }
}

#[tokio::test]
async fn t6_membership_local_short() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let mut scenario = t6_scenario();
    shorten_scenario(&mut scenario, 30, 8, 5);
    // Single client keeps sequential verify consistent with recording order.
    scenario.workload.clients = 1;
    scenario.verify = kaya_jepsen_test::VerifyMode::Sequential;

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("T6 scenario should complete");

    assert!(
        result.passed,
        "T6 local short failed: {:?}",
        result.violations
    );

    cluster.shutdown_all().await;
}

#[tokio::test]
async fn t7_snapshot_local_short() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let mut scenario = t7_scenario();
    shorten_scenario(&mut scenario, 30, 10, 5);
    scenario.workload.clients = 1;
    scenario.verify = kaya_jepsen_test::VerifyMode::Sequential;

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("T7 scenario should complete");

    assert!(
        result.passed,
        "T7 local short failed: {:?}",
        result.violations
    );

    cluster.shutdown_all().await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t6_membership_full() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let scenario = t6_scenario();
    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("T6 full scenario should complete");

    assert!(
        result.passed,
        "T6 full gate failed: {:?}",
        result.violations
    );

    cluster.shutdown_all().await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t7_snapshot_full() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let scenario = t7_scenario();
    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("T7 full scenario should complete");

    assert!(
        result.passed,
        "T7 full gate failed: {:?}",
        result.violations
    );

    cluster.shutdown_all().await;
}