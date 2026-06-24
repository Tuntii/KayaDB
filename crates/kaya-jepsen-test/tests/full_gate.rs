//! Nightly full WGL gate: T1–T7 scenarios with concurrent linearizability verify.
//!
//! Run locally: `cargo test -p kaya-jepsen-test --test full_gate -- --ignored --nocapture --test-threads=1`

use kaya_jepsen_test::{
    scenario_uses_partition, t1_scenario, t2_scenario, t3_scenario, t4_scenario, t5_scenario,
    t6_scenario, t7_scenario, ClusterController, Scenario, TestConfig, TestResult, TestRunner,
    VerifyMode,
};
use std::path::Path;
use std::time::Duration;

fn scale_for_fast_verify(mut scenario: Scenario) -> Scenario {
    if std::env::var("KAYA_JEPSEN_FAST").ok().as_deref() != Some("1") {
        return scenario;
    }
    scenario.duration_secs = match scenario.id {
        "t5" => 15,
        "t7" => 20,
        _ => 8,
    };
    scenario.workload.duration = Duration::from_secs(scenario.duration_secs);
    if let Some(ref mut nemesis) = scenario.nemesis {
        nemesis.interval = Duration::from_secs(3);
        nemesis.duration = Duration::from_secs(2);
    }
    eprintln!(
        "[full_gate] fast verify: {} duration={}s",
        scenario.id, scenario.duration_secs
    );
    scenario
}

async fn run_full_gate(scenario: Scenario) {
    let mut scenario = scale_for_fast_verify(scenario);
    // WGL concurrent checker supports at most 14 ops; workload keeps running for chaos.
    scenario.workload.verify_max_ops = Some(14);
    // Single client keeps recorded history consistent under kill/partition nemesis.
    scenario.workload.clients = 1;
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
        .await;

    cluster.shutdown_all().await;
    // Windows: allow ports from aborted node tasks to be released before the next scenario.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let result =
        result.unwrap_or_else(|e| panic!("{} full scenario should complete: {e}", scenario.id));

    persist_trace_on_failure(dir.path(), scenario.id, &result);

    assert!(
        result.passed,
        "{} full gate failed: {:?}",
        scenario.id, result.violations
    );

    if scenario_uses_partition(scenario.nemesis.as_ref()) {
        assert!(
            result.partition_attempted > 0,
            "{} expected partition nemesis to fire at least once",
            scenario.id
        );
        eprintln!(
            "[full_gate] {} partition stats: attempted={} applied={} failed={}",
            scenario.id,
            result.partition_attempted,
            result.partition_applied,
            result.partition_failed
        );
    }
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
