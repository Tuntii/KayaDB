//! Nightly full WGL gate: T1–T7 scenarios with concurrent linearizability verify.
//!
//! Run locally: `cargo test -p kaya-jepsen-test --test full_gate -- --ignored --nocapture --test-threads=1`

use kaya_jepsen_test::{
    t1_scenario, t2_scenario, t3_scenario, t4_scenario, t5_scenario, t6_scenario, t7_scenario,
    ClusterController, Scenario, TestConfig, TestResult, TestRunner, VerifyMode,
};
use std::path::Path;

async fn run_full_gate(scenario: Scenario) {
    assert_eq!(
        scenario.verify,
        VerifyMode::Concurrent,
        "full gate expects WGL concurrent verify for {}",
        scenario.id
    );

    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let config = TestConfig::from_scenario(&scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(&scenario, &mut cluster)
        .await
        .unwrap_or_else(|e| panic!("{} full scenario should complete: {e}", scenario.id));

    persist_trace_on_failure(dir.path(), scenario.id, &result);

    assert!(
        result.passed,
        "{} full gate failed: {:?}",
        scenario.id,
        result.violations
    );

    cluster.shutdown_all().await;
}

fn persist_trace_on_failure(base_dir: &Path, scenario_id: &str, result: &TestResult) {
    if let Some(ref trace) = result.trace {
        let traces_dir = base_dir.join("traces");
        let _ = std::fs::create_dir_all(&traces_dir);
        let path = traces_dir.join(format!("{scenario_id}-0xdeadbeef.jsonl"));
        let _ = std::fs::write(path, trace);
    }
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t1_single_node_kill_recovery() {
    run_full_gate(t1_scenario()).await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t2_majority_partition() {
    run_full_gate(t2_scenario()).await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t3_leader_kill_re_election() {
    run_full_gate(t3_scenario()).await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t4_rolling_restart() {
    run_full_gate(t4_scenario()).await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t5_stress_kill_partition() {
    run_full_gate(t5_scenario()).await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t6_membership_joint_consensus() {
    run_full_gate(t6_scenario()).await;
}

#[tokio::test]
#[ignore = "full WGL gate — run nightly with --ignored"]
async fn t7_snapshot_catch_up() {
    run_full_gate(t7_scenario()).await;
}