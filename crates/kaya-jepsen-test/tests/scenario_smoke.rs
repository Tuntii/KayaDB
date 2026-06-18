use kaya_jepsen_test::{smoke_scenario, ClusterController, TestConfig, TestRunner};
use std::time::Duration;

#[tokio::test]
async fn smoke_scenario_runs_with_cluster_controller() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let mut scenario = smoke_scenario();
    scenario.duration_secs = 15;
    scenario.workload.duration = Duration::from_secs(15);
    // Single client keeps sequential verify consistent with recording order.
    scenario.workload.clients = 1;
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(8);
        nemesis.duration = Duration::from_secs(3);
    }

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .expect("scenario run should complete");

    assert!(
        result.passed,
        "smoke scenario failed: {:?}",
        result.violations
    );
    assert!(result.stats.total > 0, "expected workload operations");

    cluster.shutdown_all().await;
}