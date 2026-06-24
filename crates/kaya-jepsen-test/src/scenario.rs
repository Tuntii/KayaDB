//! Scenario registry for Jepsen-style chaos tests.

use crate::nemesis::{MemberSpec, NemesisConfig, NemesisType};
use crate::workload::{WorkloadConfig, WorkloadType};
use std::time::Duration;

/// How to verify operation history after a scenario completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyMode {
    /// Sequential linearizability checker (PR smoke).
    Sequential,
    /// WGL concurrent linearizability checker (nightly full gate).
    Concurrent,
}

/// Timed workload actions fired during a scenario (e.g. T7 snapshot forcing).
#[derive(Debug, Clone)]
pub enum WorkloadHook {
    /// Write `count` keys `{prefix}-{i}` with values `v{i}` via the leader.
    BurstWrites {
        count: usize,
        key_prefix: &'static str,
    },
}

/// Cluster topology required by a scenario.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// Standard three-node Raft cluster.
    ThreeNode,
    /// Three-node cluster with a fourth node joining via membership change.
    FourNodeJoin,
}

/// A declarative chaos test scenario.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub id: &'static str,
    pub workload: WorkloadConfig,
    pub hooks: Vec<WorkloadHook>,
    pub duration_secs: u64,
    pub verify: VerifyMode,
    pub topology: Topology,
    pub nemesis: Option<NemesisConfig>,
}

fn kill_nemesis(interval_secs: u64, down_secs: u64) -> NemesisConfig {
    NemesisConfig {
        nemesis_type: NemesisType::KillNode,
        interval: Duration::from_secs(interval_secs),
        duration: Duration::from_secs(down_secs),
        probability: 1.0,
    }
}

fn workload(workload_type: WorkloadType, clients: usize, duration_secs: u64) -> WorkloadConfig {
    WorkloadConfig {
        workload_type,
        clients,
        duration: Duration::from_secs(duration_secs),
        rate_limit: 0,
        verify_max_ops: None,
    }
}

/// Short smoke scenario: Register workload (1 client), kill-node nemesis, sequential verify.
/// 1 client ensures sequential ops for checker. Workload retries to record only confirmed successes.
pub fn smoke_scenario() -> Scenario {
    Scenario {
        id: "smoke",
        workload: workload(WorkloadType::Register, 1, 30), // 1 client to keep operations sequential for the simple checker
        hooks: vec![],
        duration_secs: 30,
        verify: VerifyMode::Sequential,
        topology: Topology::ThreeNode,
        nemesis: Some(kill_nemesis(10, 5)),
    }
}

/// T1: single node kill + recovery (jepsen-design).
pub fn t1_scenario() -> Scenario {
    Scenario {
        id: "t1",
        workload: workload(WorkloadType::Register, 5, 120),
        hooks: vec![],
        duration_secs: 120,
        verify: VerifyMode::Concurrent,
        topology: Topology::ThreeNode,
        nemesis: Some(kill_nemesis(30, 20)),
    }
}

/// T2: majority partition with set workload.
pub fn t2_scenario() -> Scenario {
    Scenario {
        id: "t2",
        workload: workload(WorkloadType::Set, 5, 120),
        hooks: vec![],
        duration_secs: 120,
        verify: VerifyMode::Concurrent,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::PartitionById(3),
            interval: Duration::from_secs(10),
            duration: Duration::from_secs(50),
            probability: 1.0,
        }),
    }
}

/// T3: leader kill + re-election.
pub fn t3_scenario() -> Scenario {
    Scenario {
        id: "t3",
        workload: workload(WorkloadType::Register, 10, 90),
        hooks: vec![],
        duration_secs: 90,
        verify: VerifyMode::Concurrent,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::KillNode,
            interval: Duration::from_secs(10),
            duration: Duration::from_secs(20),
            probability: 1.0,
        }),
    }
}

/// T4: rolling restart with set workload.
pub fn t4_scenario() -> Scenario {
    Scenario {
        id: "t4",
        workload: workload(WorkloadType::Set, 5, 120),
        hooks: vec![],
        duration_secs: 120,
        verify: VerifyMode::Concurrent,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::KillNodeById(1),
                NemesisType::KillNodeById(2),
                NemesisType::KillNodeById(3),
            ]),
            interval: Duration::from_secs(10),
            duration: Duration::from_secs(5),
            probability: 1.0,
        }),
    }
}

/// T5: stress test with composite kill + partition nemeses.
pub fn t5_scenario() -> Scenario {
    Scenario {
        id: "t5",
        workload: workload(WorkloadType::Register, 20, 300),
        hooks: vec![],
        duration_secs: 300,
        verify: VerifyMode::Concurrent,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::KillNode,
                NemesisType::Partition,
            ]),
            interval: Duration::from_secs(30),
            duration: Duration::from_secs(20),
            probability: 1.0,
        }),
    }
}

/// T6: membership change during joint consensus with kill nemesis.
pub fn t6_scenario() -> Scenario {
    Scenario {
        id: "t6",
        workload: workload(WorkloadType::Register, 5, 120),
        hooks: vec![],
        duration_secs: 120,
        verify: VerifyMode::Concurrent,
        topology: Topology::FourNodeJoin,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::AddMember(MemberSpec {
                    node_id: 4,
                    raft_addr: "127.0.0.1:0".to_string(),
                    client_addr: "127.0.0.1:0".to_string(),
                }),
                NemesisType::KillNode,
            ]),
            interval: Duration::from_secs(20),
            duration: Duration::from_secs(10),
            probability: 1.0,
        }),
    }
}

/// T7: snapshot catch-up after killing a follower mid-compaction.
pub fn t7_scenario() -> Scenario {
    Scenario {
        id: "t7",
        workload: workload(WorkloadType::Register, 5, 120),
        hooks: vec![WorkloadHook::BurstWrites {
            count: 128,
            key_prefix: "snap",
        }],
        duration_secs: 120,
        verify: VerifyMode::Concurrent,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::KillFollower,
            interval: Duration::from_secs(5),
            duration: Duration::from_secs(15),
            probability: 1.0,
        }),
    }
}

/// All registered scenarios (smoke + T1–T7).
pub fn scenario_registry() -> Vec<Scenario> {
    vec![
        smoke_scenario(),
        t1_scenario(),
        t2_scenario(),
        t3_scenario(),
        t4_scenario(),
        t5_scenario(),
        t6_scenario(),
        t7_scenario(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nemesis::NemesisType;
    use crate::runner::scenario_uses_partition;
    use crate::workload::WorkloadType;

    #[test]
    fn registry_contains_smoke_and_t1_through_t7() {
        let registry = scenario_registry();
        assert_eq!(registry.len(), 8);
        assert_eq!(registry[0].id, "smoke");
        assert_eq!(registry[1].id, "t1");
        assert_eq!(registry[7].id, "t7");
    }

    #[test]
    fn durations_increase_from_smoke_through_t5() {
        let smoke = smoke_scenario();
        let t1 = t1_scenario();
        let t5 = t5_scenario();
        assert!(smoke.duration_secs < t1.duration_secs);
        assert!(t1.duration_secs <= t5.duration_secs);
    }

    #[test]
    fn t1_matches_jepsen_design() {
        let s = t1_scenario();
        assert_eq!(s.workload.workload_type, WorkloadType::Register);
        assert_eq!(s.workload.clients, 5);
        assert_eq!(s.duration_secs, 120);
        assert_eq!(s.verify, VerifyMode::Concurrent);
        assert_eq!(s.topology, Topology::ThreeNode);
        assert!(matches!(
            s.nemesis.as_ref().map(|n| &n.nemesis_type),
            Some(NemesisType::KillNode)
        ));
    }

    #[test]
    fn t2_matches_jepsen_design_partition() {
        let s = t2_scenario();
        assert_eq!(s.workload.workload_type, WorkloadType::Set);
        assert_eq!(s.workload.clients, 5);
        assert_eq!(s.duration_secs, 120);
        assert!(scenario_uses_partition(s.nemesis.as_ref()));
        assert!(matches!(
            s.nemesis.as_ref().map(|n| &n.nemesis_type),
            Some(NemesisType::PartitionById(3))
        ));
    }

    #[test]
    fn t5_matches_jepsen_design_stress_partition() {
        let s = t5_scenario();
        assert_eq!(s.workload.clients, 20);
        assert_eq!(s.duration_secs, 300);
        assert!(scenario_uses_partition(s.nemesis.as_ref()));
    }

    #[test]
    fn t6_four_node_join_topology() {
        let s = t6_scenario();
        assert_eq!(s.topology, Topology::FourNodeJoin);
        assert_eq!(s.verify, VerifyMode::Concurrent);
    }

    #[test]
    fn t7_has_burst_write_hook() {
        let s = t7_scenario();
        assert_eq!(s.hooks.len(), 1);
        assert!(matches!(
            &s.hooks[0],
            WorkloadHook::BurstWrites {
                count: 128,
                key_prefix: "snap"
            }
        ));
    }

    #[test]
    fn full_gate_scenarios_use_concurrent_verify() {
        for scenario in [
            t1_scenario(),
            t2_scenario(),
            t3_scenario(),
            t4_scenario(),
            t5_scenario(),
            t6_scenario(),
            t7_scenario(),
        ] {
            assert_eq!(
                scenario.verify,
                VerifyMode::Concurrent,
                "{} must use WGL concurrent verify",
                scenario.id
            );
        }
    }
}
