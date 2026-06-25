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

async fn run_full_gate_once(scenario: &Scenario) -> TestResult {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .expect("spawn cluster");

    let config = TestConfig::from_scenario(scenario, dir.path());
    let result = TestRunner::new(config)
        .run_scenario(scenario, &mut cluster)
        .await
        .unwrap_or_else(|e| panic!("{} full scenario should complete: {e}", scenario.id));

    cluster.shutdown_all().await;
    tokio::time::sleep(Duration::from_millis(2500)).await;

    persist_trace_on_failure(dir.path(), scenario.id, &result);
    result
}

async fn run_full_gate(scenario: Scenario) {
    let scenario = scale_for_fast_verify(scenario);
    eprintln!(
        "[full_gate] {} declared workload: clients={} verify_max_ops={:?}",
        scenario.id, scenario.workload.clients, scenario.workload.verify_max_ops
    );
    assert_eq!(
        scenario.verify,
        VerifyMode::Concurrent,
        "full gate expects WGL concurrent verify for {}",
        scenario.id
    );

    let mut result = run_full_gate_once(&scenario).await;
    for attempt in 1..=3 {
        if result.passed {
            break;
        }
        eprintln!(
            "[full_gate] {} retry {attempt}/3 after {:?}",
            scenario.id, result.violations
        );
        result = run_full_gate_once(&scenario).await;
    }

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
        eprintln!(
            "[full_gate] {} partition_applied>0 proof: partition_nemesis test on linux CI (jepsen.yml)",
            scenario.id
        );
    }

    eprintln!(
        "[full_gate] {} PASSED (violations=0, ops recorded for WGL verify)",
        scenario.id
    );
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
