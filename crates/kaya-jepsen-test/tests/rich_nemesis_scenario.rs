//! Integration: rich nemesis scenario (ClockSkew + DiskLatency) through TestRunner.

use kaya_jepsen_test::{rich_nemesis_scenario, ClusterController, TestConfig, TestRunner};
use std::time::Duration;

#[tokio::test]
async fn rich_nemesis_scenario_runs_via_runner() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let mut scenario = rich_nemesis_scenario();
    scenario.duration_secs = 12;
    scenario.workload.duration = Duration::from_secs(12);
    scenario.workload.clients = 1;
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(5);
        nemesis.duration = Duration::from_secs(2);
    }

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("rich scenario should complete");

    assert!(
        result.passed,
        "rich nemesis scenario failed: {:?}",
        result.violations
    );
    assert!(result.stats.total > 0);

    cluster.shutdown_all().await;
}