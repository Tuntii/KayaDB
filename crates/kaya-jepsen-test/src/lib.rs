//! Jepsen-style correctness testing for KayaDB clusters.
//!
//! This crate provides a framework for testing KayaDB clusters under
//! failure conditions (node crashes, network partitions) while running
//! concurrent client workloads.
//!
//! # Architecture
//!
//! - [`workload`] - Concurrent client workload generators
//! - [`nemesis`] - Failure injectors (kill, partition, delay)
//! - [`history`] - Operation history recording
//! - [`runner`] - Test orchestration and verification
//!
//! # Example
//!
//! ```rust,ignore
//! use kaya_jepsen_test::{TestConfig, TestRunner, WorkloadConfig};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     let config = TestConfig {
//!         nodes: vec![
//!             "127.0.0.1:7379".parse().unwrap(),
//!             "127.0.0.1:7380".parse().unwrap(),
//!             "127.0.0.1:7381".parse().unwrap(),
//!         ],
//!         workload: WorkloadConfig {
//!             clients: 5,
//!             duration: Duration::from_secs(60),
//!             ..Default::default()
//!         },
//!         duration_secs: 60,
//!         ..Default::default()
//!     };
//!
//!     let result = TestRunner::new(config).run().await;
//!     assert!(result.is_ok(), "Linearizability violation: {:?}", result);
//! }
//! ```

pub mod cluster_controller;
pub mod history;
pub mod nemesis;
pub mod runner;
pub mod scenario;
pub mod workload;

pub use cluster_controller::{ClusterController, LeaderInfo, ManagedNode};
pub use history::{History, Operation, OperationResult};
pub use nemesis::{MemberSpec, Nemesis, NemesisConfig, NemesisType};
pub use runner::{TestConfig, TestResult, TestRunner};
pub use scenario::{
    scenario_registry, smoke_scenario, t1_scenario, t2_scenario, t3_scenario, t4_scenario,
    t5_scenario, t6_scenario, t7_scenario, Scenario, Topology, VerifyMode,
};
pub use workload::{Workload, WorkloadConfig, WorkloadType};
