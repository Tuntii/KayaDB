//! Registry integrity checks for smoke + T1–T7 scenarios.

use kaya_jepsen_test::{
    scenario_registry, smoke_scenario, t1_scenario, t2_scenario, t3_scenario, t4_scenario,
    t5_scenario, t6_scenario, t7_scenario, VerifyMode,
};

#[test]
fn registry_has_eight_entries_in_order() {
    let registry = scenario_registry();
    let ids: Vec<_> = registry.iter().map(|s| s.id).collect();
    assert_eq!(ids, vec!["smoke", "t1", "t2", "t3", "t4", "t5", "t6", "t7"]);
}

#[test]
fn smoke_uses_sequential_verify_full_gate_uses_concurrent() {
    assert_eq!(smoke_scenario().verify, VerifyMode::Sequential);
    for scenario in [
        t1_scenario(),
        t2_scenario(),
        t3_scenario(),
        t4_scenario(),
        t5_scenario(),
        t6_scenario(),
        t7_scenario(),
    ] {
        assert_eq!(scenario.verify, VerifyMode::Concurrent);
    }
}

#[test]
fn every_scenario_has_positive_duration_and_clients() {
    for scenario in scenario_registry() {
        assert!(scenario.duration_secs > 0, "{} duration", scenario.id);
        assert!(scenario.workload.clients > 0, "{} clients", scenario.id);
        assert!(scenario.nemesis.is_some(), "{} nemesis", scenario.id);
    }
}
