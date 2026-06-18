use kaya_jepsen_test::{smoke_scenario, ClusterController, TestConfig, TestRunner};

/// PR chaos-smoke gate: `smoke_scenario()` runs 30s with kill-node nemesis and sequential verify.
#[tokio::test]
async fn chaos_smoke_kill_and_linearize() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();
    let scenario = smoke_scenario();
    let result = TestRunner::new(TestConfig::from_scenario(&scenario, dir.path()))
        .run_scenario(&scenario, &mut cluster)
        .await
        .unwrap();
    assert!(result.passed, "{:?}", result.violations);
    cluster.shutdown_all().await;
}