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

pub mod history;
pub mod nemesis;
pub mod runner;
pub mod workload;

pub use history::{History, Operation, OperationResult};
pub use nemesis::{Nemesis, NemesisConfig, NemesisType};
pub use runner::{TestConfig, TestResult, TestRunner};
pub use workload::{Workload, WorkloadConfig};
