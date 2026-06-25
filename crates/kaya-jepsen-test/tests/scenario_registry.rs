//! Registry integrity checks for smoke + T1–T7 scenarios.

use kaya_jepsen_test::{
    register_key, scenario_registry, smoke_scenario, t1_scenario, t2_scenario, t3_scenario,
    t4_scenario, t5_scenario, t6_scenario, t7_scenario, VerifyMode, WGL_VERIFY_MAX_OPS,
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

#[test]
fn concurrent_registry_scenarios_declare_wgl_cap_and_design_clients() {
    let cases: [(&str, usize); 7] = [
        ("t1", 5),
        ("t2", 5),
        ("t3", 10),
        ("t4", 5),
        ("t5", 20),
        ("t6", 5),
        ("t7", 5),
    ];
    for (id, clients) in cases {
        let scenario = match id {
            "t1" => t1_scenario(),
            "t2" => t2_scenario(),
            "t3" => t3_scenario(),
            "t4" => t4_scenario(),
            "t5" => t5_scenario(),
            "t6" => t6_scenario(),
            "t7" => t7_scenario(),
            _ => unreachable!(),
        };
        assert_eq!(scenario.id, id);
        assert_eq!(
            scenario.workload.verify_max_ops,
            Some(WGL_VERIFY_MAX_OPS),
            "{id} must cap recorded ops for WGL concurrent verify"
        );
        assert_eq!(
            scenario.workload.clients, clients,
            "{id} client count must match jepsen-design"
        );
    }
    assert_eq!(smoke_scenario().workload.verify_max_ops, None);
}

#[test]
fn wgl_register_uses_per_client_keys_not_shared_register() {
    for client_id in 0..5 {
        let key = register_key(client_id, Some(WGL_VERIFY_MAX_OPS));
        assert_ne!(
            key, b"register",
            "WGL cap must partition register by client"
        );
        assert_eq!(key, format!("register-{client_id}").into_bytes());
    }
    assert_eq!(register_key(0, None), b"register");
}
