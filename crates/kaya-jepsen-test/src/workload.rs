//! Concurrent client workload generators.

use crate::history::{History, OperationResult};
use kaya_client::KayaClient;
use kaya_sim::Op;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

const CLIENT_OP_TIMEOUT: Duration = Duration::from_millis(750);

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
    /// Cap recorded ops for WGL concurrent verify (workload still runs for chaos).
    pub verify_max_ops: Option<usize>,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            workload_type: WorkloadType::Register,
            clients: 5,
            duration: Duration::from_secs(60),
            rate_limit: 0,
            verify_max_ops: None,
        }
    }
}

fn should_record(history: &History, verify_max_ops: Option<usize>) -> bool {
    verify_max_ops.is_none_or(|max| history.len() < max)
}

fn record_completed(
    history: &History,
    client_id: usize,
    op: Op,
    result: OperationResult,
    op_start: Instant,
    verify_max_ops: Option<usize>,
) {
    if should_record(history, verify_max_ops) {
        history.record_timed(client_id, op, result, op_start, Instant::now());
    }
}

/// Type of workload to generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            let verify_max_ops = self.config.verify_max_ops;

            let handle = tokio::spawn(async move {
                run_client(
                    client_id,
                    nodes,
                    history,
                    workload_type,
                    duration,
                    rate_limit,
                    verify_max_ops,
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
    verify_max_ops: Option<usize>,
) {
    let mut rng = StdRng::from_entropy();
    let start = std::time::Instant::now();

    // Connect to a random node
    let initial_node = nodes[rng.gen_range(0..nodes.len())];
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
                run_register_op(
                    &mut client,
                    client_id,
                    &history,
                    &mut rng,
                    verify_max_ops,
                    op_start,
                )
                .await;
            }
            WorkloadType::Counter => {
                run_counter_op(
                    &mut client,
                    client_id,
                    &history,
                    &mut rng,
                    verify_max_ops,
                    op_start,
                )
                .await;
            }
            WorkloadType::Set => {
                run_set_op(
                    &mut client,
                    client_id,
                    &history,
                    &mut rng,
                    verify_max_ops,
                    op_start,
                )
                .await;
            }
            WorkloadType::Map => {
                run_map_op(
                    &mut client,
                    client_id,
                    &history,
                    &mut rng,
                    verify_max_ops,
                    op_start,
                )
                .await;
            }
        }

        // Reconnect on error to handle killed nodes / leader changes (helps avoid stale connections causing spurious errors/violations)
        // Simple heuristic: if last op had error recorded, try a different node.
        // Note: actual errors are inside the op functions; here we just periodically re-pick to be resilient.
        if rng.gen_bool(0.1) {
            // occasionally re-resolve to handle partitions/kills
            let new_node = nodes[rng.gen_range(0..nodes.len())];
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

async fn run_register_op<R: Rng>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
    verify_max_ops: Option<usize>,
    op_start: Instant,
) {
    let key = b"register";

    // 70% GET, 30% PUT
    // Retry until success to avoid recording indeterminate results that cause false linearizability violations
    // under node kills (response may be lost even if op committed).
    if rng.gen_bool(0.7) {
        // GET - retry until we get a value or timeout per op
        let op = Op::Get { key: key.to_vec() };
        let mut got = None;
        for _ in 0..5 {
            match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
                Ok(Ok(value)) => {
                    got = Some(value);
                    break;
                }
                _ => {
                    sleep(Duration::from_millis(20)).await;
                }
            }
        }
        if let Some(value) = got {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Value(value),
                op_start,
                verify_max_ops,
            );
        } else {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Error("get failed after retries".into()),
                op_start,
                verify_max_ops,
            );
        }
    } else {
        // PUT - retry until Ok
        let value: [u8; 8] = rng.gen();
        let op = Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let mut success = false;
        for _ in 0..5 {
            match timeout(CLIENT_OP_TIMEOUT, client.put(key, &value)).await {
                Ok(Ok(())) => {
                    record_completed(
                        history,
                        client_id,
                        op.clone(),
                        OperationResult::Ok,
                        op_start,
                        verify_max_ops,
                    );
                    success = true;
                    break;
                }
                _ => {
                    sleep(Duration::from_millis(20)).await;
                }
            }
        }
        if !success {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Error("put failed after retries".into()),
                op_start,
                verify_max_ops,
            );
        }
    }
}

async fn run_counter_op<R: Rng>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
    verify_max_ops: Option<usize>,
    op_start: Instant,
) {
    let key = b"counter";

    // 50% GET, 50% increment (read-modify-write)
    if rng.gen_bool(0.5) {
        // GET
        let op = Op::Get { key: key.to_vec() };
        match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
            Ok(Ok(value)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Value(value),
                    op_start,
                    verify_max_ops,
                );
            }
            Ok(Err(e)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
            }
            Err(_) => {}
        }
    } else {
        // Increment: read current value, increment, write back
        let current = match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
            Ok(Ok(Some(v))) => {
                let mut bytes = [0u8; 8];
                let len = v.len().min(8);
                bytes[..len].copy_from_slice(&v[..len]);
                u64::from_le_bytes(bytes)
            }
            Ok(Ok(None)) => 0,
            Ok(Err(e)) => {
                let op = Op::Get { key: key.to_vec() };
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
                return;
            }
            Err(_) => return,
        };

        let new_value = current + 1;
        let op = Op::Put {
            key: key.to_vec(),
            value: new_value.to_le_bytes().to_vec(),
        };
        match timeout(CLIENT_OP_TIMEOUT, client.put(key, &new_value.to_le_bytes())).await {
            Ok(Ok(())) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Ok,
                    op_start,
                    verify_max_ops,
                );
            }
            Ok(Err(e)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
            }
            Err(_) => {}
        }
    }
}

async fn run_set_op<R: Rng>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
    verify_max_ops: Option<usize>,
    op_start: Instant,
) {
    // Under verify cap, only PUT unique keys (SCAN is not key-partition friendly for WGL).
    let do_put = verify_max_ops.is_some() || rng.gen_bool(0.6);
    if do_put {
        // Append: PUT set:<client_id>:<random>
        let unique_id: u64 = rng.gen();
        let key = format!("set:{}:{}", client_id, unique_id);
        let value = unique_id.to_le_bytes().to_vec();

        let op = Op::Put {
            key: key.as_bytes().to_vec(),
            value: value.clone(),
        };
        match timeout(CLIENT_OP_TIMEOUT, client.put(key.as_bytes(), &value)).await {
            Ok(Ok(())) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Ok,
                    op_start,
                    verify_max_ops,
                );
            }
            Ok(Err(e)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
            }
            Err(_) => {}
        }
    } else {
        // Scan: SCAN prefix=set:
        let prefix = b"set:";
        let op = Op::Scan {
            prefix: prefix.to_vec(),
        };
        match timeout(CLIENT_OP_TIMEOUT, client.scan(prefix)).await {
            Ok(Ok(items)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Scan(items),
                    op_start,
                    verify_max_ops,
                );
            }
            Ok(Err(e)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
            }
            Err(_) => {}
        }
    }
}

async fn run_map_op<R: Rng>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
    verify_max_ops: Option<usize>,
    op_start: Instant,
) {
    // Simple map: 50% PUT, 50% GET on random keys
    let key_id: u32 = rng.gen_range(0..10);
    let key = format!("map:{}", key_id);

    if rng.gen_bool(0.5) {
        // PUT
        let value: [u8; 8] = rng.gen();
        let op = Op::Put {
            key: key.as_bytes().to_vec(),
            value: value.to_vec(),
        };
        match timeout(CLIENT_OP_TIMEOUT, client.put(key.as_bytes(), &value)).await {
            Ok(Ok(())) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Ok,
                    op_start,
                    verify_max_ops,
                );
            }
            Ok(Err(e)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
            }
            Err(_) => {}
        }
    } else {
        // GET
        let op = Op::Get {
            key: key.as_bytes().to_vec(),
        };
        match timeout(CLIENT_OP_TIMEOUT, client.get(key.as_bytes())).await {
            Ok(Ok(value)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Value(value),
                    op_start,
                    verify_max_ops,
                );
            }
            Ok(Err(e)) => {
                record_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Error(e.to_string()),
                    op_start,
                    verify_max_ops,
                );
            }
            Err(_) => {}
        }
    }
}
