//! Concurrent client workload generators.

use crate::history::{History, OperationResult};
use kaya_client::KayaClient;
use kaya_sim::Op;
use rand::rngs::StdRng;
use rand::RngExt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Workload configuration.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Workload type
    pub workload_type: WorkloadType,
    /// Number of concurrent clients
    pub clients: usize,
    /// Test duration
    pub duration: Duration,
    /// Operations per second per client (0 = unlimited)
    pub rate_limit: u32,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            workload_type: WorkloadType::Register,
            clients: 5,
            duration: Duration::from_secs(60),
            rate_limit: 0,
        }
    }
}

/// Type of workload to generate.
#[derive(Debug, Clone, Copy)]
pub enum WorkloadType {
    /// Single-key read/write (register)
    Register,
    /// Counter increment/read
    Counter,
    /// Set append/scan
    Set,
    /// Multi-key map operations
    Map,
}

/// A workload generator.
pub struct Workload {
    config: WorkloadConfig,
    nodes: Vec<SocketAddr>,
    history: Arc<History>,
}

impl Workload {
    /// Create a new workload.
    pub fn new(config: WorkloadConfig, nodes: Vec<SocketAddr>, history: Arc<History>) -> Self {
        Self {
            config,
            nodes,
            history,
        }
    }

    /// Run the workload with concurrent clients.
    pub async fn run(&self) {
        let mut handles = Vec::new();

        for client_id in 0..self.config.clients {
            let nodes = self.nodes.clone();
            let history = self.history.clone();
            let workload_type = self.config.workload_type;
            let duration = self.config.duration;
            let rate_limit = self.config.rate_limit;

            let handle = tokio::spawn(async move {
                run_client(
                    client_id,
                    nodes,
                    history,
                    workload_type,
                    duration,
                    rate_limit,
                )
                .await;
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }
    }
}

async fn run_client(
    client_id: usize,
    nodes: Vec<SocketAddr>,
    history: Arc<History>,
    workload_type: WorkloadType,
    duration: Duration,
    rate_limit: u32,
) {
    let mut rng: StdRng = rand::make_rng();
    let start = std::time::Instant::now();

    // Connect to a random node
    let initial_node = nodes[rng.random_range(0..nodes.len())];
    let mut client = match KayaClient::connect(initial_node).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Client {} failed to connect: {}", client_id, e);
            return;
        }
    };

    let min_interval = if rate_limit > 0 {
        Some(Duration::from_micros(1_000_000 / rate_limit as u64))
    } else {
        None
    };

    while start.elapsed() < duration {
        let op_start = std::time::Instant::now();

        match workload_type {
            WorkloadType::Register => {
                run_register_op(&mut client, client_id, &history, &mut rng).await;
            }
            WorkloadType::Counter => {
                run_counter_op(&mut client, client_id, &history, &mut rng).await;
            }
            WorkloadType::Set => {
                run_set_op(&mut client, client_id, &history, &mut rng).await;
            }
            WorkloadType::Map => {
                run_map_op(&mut client, client_id, &history, &mut rng).await;
            }
        }

        // Reconnect on error to handle killed nodes / leader changes (helps avoid stale connections causing spurious errors/violations)
        // Simple heuristic: if last op had error recorded, try a different node.
        // Note: actual errors are inside the op functions; here we just periodically re-pick to be resilient.
        if rng.random_bool(0.1) {
            // occasionally re-resolve to handle partitions/kills
            let new_node = nodes[rng.random_range(0..nodes.len())];
            if let Ok(new_client) = KayaClient::connect(new_node).await {
                client = new_client;
            }
        }

        // Rate limiting
        if let Some(interval) = min_interval {
            let elapsed = op_start.elapsed();
            if elapsed < interval {
                sleep(interval - elapsed).await;
            }
        }
    }
}

async fn run_register_op<R: RngExt>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
) {
    let key = b"register";

    // 70% GET, 30% PUT
    // Retry until success to avoid recording indeterminate results that cause false linearizability violations
    // under node kills (response may be lost even if op committed).
    if rng.random_bool(0.7) {
        // GET - retry until we get a value or timeout per op
        let op = Op::Get { key: key.to_vec() };
        let mut got = None;
        for _ in 0..5 {
            match client.get(key).await {
                Ok(value) => {
                    got = Some(value);
                    break;
                }
                Err(_) => {
                    sleep(Duration::from_millis(20)).await;
                }
            }
        }
        if let Some(value) = got {
            history.record(client_id, op, OperationResult::Value(value));
        } else {
            history.record(
                client_id,
                op,
                OperationResult::Error("get failed after retries".into()),
            );
        }
    } else {
        // PUT - retry until Ok
        let value: [u8; 8] = rng.random();
        let op = Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let mut success = false;
        for _ in 0..5 {
            match client.put(key, &value).await {
                Ok(()) => {
                    history.record(client_id, op.clone(), OperationResult::Ok);
                    success = true;
                    break;
                }
                Err(_) => {
                    sleep(Duration::from_millis(20)).await;
                }
            }
        }
        if !success {
            history.record(
                client_id,
                op,
                OperationResult::Error("put failed after retries".into()),
            );
        }
    }
}

async fn run_counter_op<R: RngExt>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
) {
    let key = b"counter";

    // 50% GET, 50% increment (read-modify-write)
    if rng.random_bool(0.5) {
        // GET
        let op = Op::Get { key: key.to_vec() };
        match client.get(key).await {
            Ok(value) => {
                history.record(client_id, op, OperationResult::Value(value));
            }
            Err(e) => {
                history.record(client_id, op, OperationResult::Error(e.to_string()));
            }
        }
    } else {
        // Increment: read current value, increment, write back
        let current = match client.get(key).await {
            Ok(Some(v)) => {
                let mut bytes = [0u8; 8];
                let len = v.len().min(8);
                bytes[..len].copy_from_slice(&v[..len]);
                u64::from_le_bytes(bytes)
            }
            Ok(None) => 0,
            Err(e) => {
                let op = Op::Get { key: key.to_vec() };
                history.record(client_id, op, OperationResult::Error(e.to_string()));
                return;
            }
        };

        let new_value = current + 1;
        let op = Op::Put {
            key: key.to_vec(),
            value: new_value.to_le_bytes().to_vec(),
        };
        match client.put(key, &new_value.to_le_bytes()).await {
            Ok(()) => {
                history.record(client_id, op, OperationResult::Ok);
            }
            Err(e) => {
                history.record(client_id, op, OperationResult::Error(e.to_string()));
            }
        }
    }
}

async fn run_set_op<R: RngExt>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
) {
    // 60% append, 40% scan
    if rng.random_bool(0.6) {
        // Append: PUT set:<client_id>:<random>
        let unique_id: u64 = rng.random();
        let key = format!("set:{}:{}", client_id, unique_id);
        let value = unique_id.to_le_bytes().to_vec();

        let op = Op::Put {
            key: key.as_bytes().to_vec(),
            value: value.clone(),
        };
        match client.put(key.as_bytes(), &value).await {
            Ok(()) => {
                history.record(client_id, op, OperationResult::Ok);
            }
            Err(e) => {
                history.record(client_id, op, OperationResult::Error(e.to_string()));
            }
        }
    } else {
        // Scan: SCAN prefix=set:
        let prefix = b"set:";
        let op = Op::Scan {
            prefix: prefix.to_vec(),
        };
        match client.scan(prefix).await {
            Ok(items) => {
                history.record(client_id, op, OperationResult::Scan(items));
            }
            Err(e) => {
                history.record(client_id, op, OperationResult::Error(e.to_string()));
            }
        }
    }
}

async fn run_map_op<R: RngExt>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
) {
    // Simple map: 50% PUT, 50% GET on random keys
    let key_id: u32 = rng.random_range(0..10);
    let key = format!("map:{}", key_id);

    if rng.random_bool(0.5) {
        // PUT
        let value: [u8; 8] = rng.random();
        let op = Op::Put {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
        };
        match client.put(key.as_bytes(), &value).await {
            Ok(()) => {
                history.record(client_id, op, OperationResult::Ok);
            }
            Err(e) => {
                history.record(client_id, op, OperationResult::Error(e.to_string()));
            }
        }
    } else {
        // GET
        let op = Op::Get {
            key: key.as_bytes().to_vec(),
        };
        match client.get(key.as_bytes()).await {
            Ok(value) => {
                history.record(client_id, op, OperationResult::Value(value));
            }
            Err(e) => {
                history.record(client_id, op, OperationResult::Error(e.to_string()));
            }
        }
    }
}
