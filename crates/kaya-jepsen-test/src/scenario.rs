//! Scenario registry for Jepsen-style chaos tests.

use crate::bank::BankLayout;
use crate::nemesis::{MemberSpec, NemesisConfig, NemesisType};
use crate::workload::{WorkloadConfig, WorkloadType, WGL_VERIFY_MAX_OPS};
use std::time::Duration;

/// How to verify operation history after a scenario completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyMode {
    /// Sequential linearizability checker (PR smoke).
    Sequential,
    /// WGL concurrent linearizability checker (nightly full gate).
    Concurrent,
    /// M17 bank transfer invariant: sum of account balances is constant.
    BankSum,
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
    /// Three-node multi-raft with static ranges `[a,m)→g1`, `[m,z)→g2`.
    ThreeNodeMultiRange,
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
        bank_layout: BankLayout::Single,
    }
}

/// Concurrent (WGL) scenario workload: declared client count, verify cap for checker bound.
fn concurrent_workload(
    workload_type: WorkloadType,
    clients: usize,
    duration_secs: u64,
) -> WorkloadConfig {
    WorkloadConfig {
        workload_type,
        clients,
        duration: Duration::from_secs(duration_secs),
        rate_limit: 0,
        verify_max_ops: Some(WGL_VERIFY_MAX_OPS),
        bank_layout: BankLayout::Single,
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
        workload: concurrent_workload(WorkloadType::Register, 5, 120),
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
        workload: concurrent_workload(WorkloadType::Set, 5, 120),
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
        workload: concurrent_workload(WorkloadType::Register, 10, 90),
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
        workload: concurrent_workload(WorkloadType::Set, 5, 120),
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
        workload: concurrent_workload(WorkloadType::Register, 20, 300),
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
        workload: concurrent_workload(WorkloadType::Register, 5, 120),
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
        workload: concurrent_workload(WorkloadType::Register, 5, 120),
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

/// Rich nemesis scenario: clock skew + disk latency injection (harness-level).
pub fn rich_nemesis_scenario() -> Scenario {
    Scenario {
        id: "rich",
        workload: workload(WorkloadType::Register, 1, 20),
        hooks: vec![],
        duration_secs: 20,
        verify: VerifyMode::Sequential,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::ClockSkew {
                    node_id: 2,
                    skew_ms: 30,
                },
                NemesisType::DiskLatency { delay_ms: 20 },
            ]),
            interval: Duration::from_secs(6),
            duration: Duration::from_secs(4),
            probability: 1.0,
        }),
    }
}

/// M17 bank workload: multi-key transfers under kill + partition; sum invariant.
pub fn bank_scenario() -> Scenario {
    Scenario {
        id: "bank",
        workload: WorkloadConfig {
            workload_type: WorkloadType::Bank,
            clients: 5,
            duration: Duration::from_secs(60),
            rate_limit: 0,
            verify_max_ops: None,
            bank_layout: BankLayout::Single,
        },
        hooks: vec![],
        duration_secs: 60,
        verify: VerifyMode::BankSum,
        topology: Topology::ThreeNode,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::KillNode,
                NemesisType::Partition,
            ]),
            interval: Duration::from_secs(15),
            duration: Duration::from_secs(8),
            probability: 1.0,
        }),
    }
}

/// Multi-range bank under split + merge + kill + partition (grand matrix).
///
/// Static ranges `[a,m)→g1` / `[m,z)→g2` with multi-range account keys so SI
/// transfers frequently cross groups (sequential 2PC). Range ops soft-fail when
/// the meta table is already split/merged; kill/partition stress leadership.
/// Live MOVE_RANGE rebalance has its own scenario ([`multi_range_bank_move_scenario`]).
pub fn multi_range_bank_scenario() -> Scenario {
    Scenario {
        id: "bank-mr",
        workload: WorkloadConfig {
            workload_type: WorkloadType::Bank,
            clients: 5,
            duration: Duration::from_secs(90),
            rate_limit: 0,
            verify_max_ops: None,
            bank_layout: BankLayout::MultiRange,
        },
        hooks: vec![],
        duration_secs: 90,
        verify: VerifyMode::BankSum,
        topology: Topology::ThreeNodeMultiRange,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::SplitRange {
                    split_key: b"c".to_vec(),
                },
                NemesisType::MergeRange {
                    left_start: b"a".to_vec(),
                },
                NemesisType::KillNode,
                NemesisType::Partition,
            ]),
            interval: Duration::from_secs(12),
            duration: Duration::from_secs(6),
            probability: 1.0,
        }),
    }
}

/// Multi-range bank under live MOVE_RANGE + kill (#24 chaos gate).
///
/// **Documented subset of the nightly matrix:** move + kill only (no split /
/// merge / partition), so the sum invariant isolates the cutover. `[m,z)` is
/// flapped between its owning group and a fresh group while SI transfers run;
/// a move whose target already owns the range soft-fails like split/merge.
pub fn multi_range_bank_move_scenario() -> Scenario {
    Scenario {
        id: "bank-mr-move",
        workload: WorkloadConfig {
            workload_type: WorkloadType::Bank,
            clients: 5,
            duration: Duration::from_secs(90),
            rate_limit: 0,
            verify_max_ops: None,
            bank_layout: BankLayout::MultiRange,
        },
        hooks: vec![],
        duration_secs: 90,
        verify: VerifyMode::BankSum,
        topology: Topology::ThreeNodeMultiRange,
        nemesis: Some(NemesisConfig {
            nemesis_type: NemesisType::Composite(vec![
                NemesisType::MoveRange {
                    range_start: b"m".to_vec(),
                    target_group: 3,
                },
                NemesisType::KillNode,
                NemesisType::MoveRange {
                    range_start: b"m".to_vec(),
                    target_group: 2,
                },
            ]),
            interval: Duration::from_secs(12),
            duration: Duration::from_secs(6),
            probability: 1.0,
        }),
    }
}

/// All registered scenarios (smoke + rich + T1–T7 + bank + multi-range bank).
pub fn scenario_registry() -> Vec<Scenario> {
    vec![
        smoke_scenario(),
        rich_nemesis_scenario(),
        t1_scenario(),
        t2_scenario(),
        t3_scenario(),
        t4_scenario(),
        t5_scenario(),
        t6_scenario(),
        t7_scenario(),
        bank_scenario(),
        multi_range_bank_scenario(),
        multi_range_bank_move_scenario(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nemesis::NemesisType;
    use crate::runner::scenario_uses_partition;
    use crate::workload::{WorkloadType, WGL_VERIFY_MAX_OPS};

    #[test]
    fn registry_contains_smoke_rich_t1_through_t7_and_bank() {
        let registry = scenario_registry();
        assert_eq!(registry.len(), 12);
        assert_eq!(registry[0].id, "smoke");
        assert_eq!(registry[1].id, "rich");
        assert_eq!(registry[2].id, "t1");
        assert_eq!(registry[8].id, "t7");
        assert_eq!(registry[9].id, "bank");
        assert_eq!(registry[10].id, "bank-mr");
        assert_eq!(registry[11].id, "bank-mr-move");
    }

    /// #24: the move chaos gate is a documented subset — MOVE_RANGE + kill only.
    #[test]
    fn multi_range_bank_move_scenario_uses_move_range_nemesis() {
        let s = multi_range_bank_move_scenario();
        assert_eq!(s.id, "bank-mr-move");
        assert_eq!(s.topology, Topology::ThreeNodeMultiRange);
        assert_eq!(s.workload.bank_layout, BankLayout::MultiRange);
        assert_eq!(s.verify, VerifyMode::BankSum);
        let Some(NemesisType::Composite(types)) = s.nemesis.as_ref().map(|n| &n.nemesis_type)
        else {
            panic!("expected composite nemesis");
        };
        let moves = types
            .iter()
            .filter(|t| matches!(t, NemesisType::MoveRange { .. }))
            .count();
        assert_eq!(moves, 2, "move there and back");
        assert!(types.iter().any(|t| matches!(t, NemesisType::KillNode)));
        // Documented subset: no partition in this gate.
        assert!(!scenario_uses_partition(s.nemesis.as_ref()));
    }

    #[test]
    fn multi_range_bank_uses_multi_range_layout_and_topology() {
        let s = multi_range_bank_scenario();
        assert_eq!(s.topology, Topology::ThreeNodeMultiRange);
        assert_eq!(s.workload.bank_layout, BankLayout::MultiRange);
        assert_eq!(s.verify, VerifyMode::BankSum);
        assert!(scenario_uses_partition(s.nemesis.as_ref()));
    }

    #[test]
    fn bank_scenario_uses_bank_sum_and_kill_partition() {
        let s = bank_scenario();
        assert_eq!(s.workload.workload_type, WorkloadType::Bank);
        assert_eq!(s.verify, VerifyMode::BankSum);
        assert!(scenario_uses_partition(s.nemesis.as_ref()));
        assert!(matches!(
            s.nemesis.as_ref().map(|n| &n.nemesis_type),
            Some(NemesisType::Composite(_))
        ));
    }

    #[test]
    fn rich_scenario_uses_clock_skew_and_disk_latency() {
        let s = rich_nemesis_scenario();
        let nemesis = s.nemesis.as_ref().expect("rich scenario has nemesis");
        match &nemesis.nemesis_type {
            NemesisType::Composite(types) => {
                assert!(
                    types
                        .iter()
                        .any(|t| matches!(t, NemesisType::ClockSkew { .. })),
                    "rich scenario must include ClockSkew"
                );
                assert!(
                    types
                        .iter()
                        .any(|t| matches!(t, NemesisType::DiskLatency { .. })),
                    "rich scenario must include DiskLatency"
                );
            }
            other => panic!("rich scenario nemesis must be Composite, got {other:?}"),
        }
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
            assert_eq!(
                scenario.workload.verify_max_ops,
                Some(WGL_VERIFY_MAX_OPS),
                "{} must declare WGL verify cap",
                scenario.id
            );
        }
    }
}
