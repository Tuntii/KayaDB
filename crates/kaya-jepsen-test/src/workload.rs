//! Concurrent client workload generators.

use crate::history::{History, OperationResult};
use kaya_client::KayaClient;
use kaya_sim::Op;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::{sleep, timeout};

static REGISTER_SERIAL: OnceLock<AsyncMutex<()>> = OnceLock::new();
static REGISTER_TICK: OnceLock<AsyncMutex<u64>> = OnceLock::new();

fn register_serial() -> &'static AsyncMutex<()> {
    REGISTER_SERIAL.get_or_init(|| AsyncMutex::new(()))
}

fn register_tick() -> &'static AsyncMutex<u64> {
    REGISTER_TICK.get_or_init(|| AsyncMutex::new(0))
}

/// Reset monotonic register record ticks at scenario start.
pub async fn reset_register_record_ticks() {
    *register_tick().lock().await = 0;
}

const CLIENT_OP_TIMEOUT: Duration = Duration::from_millis(750);

/// WGL concurrent linearizability checker bound (kaya-sim `MAX_OPS`).
pub const WGL_VERIFY_MAX_OPS: usize = 14;

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

const REGISTER_MAX_REDIRECTS: usize = 10;

fn stats_indicates_leader(stats: &str) -> bool {
    stats.contains("\"role\":\"leader\"") || stats.contains("role: leader")
}

async fn connect_leader(nodes: &[SocketAddr]) -> Option<KayaClient> {
    for node in nodes {
        let Ok(Ok(mut client)) =
            timeout(Duration::from_millis(500), KayaClient::connect(*node)).await
        else {
            continue;
        };
        client.set_max_redirects(REGISTER_MAX_REDIRECTS);
        if let Ok(Ok(stats)) = timeout(CLIENT_OP_TIMEOUT, client.stats()).await {
            if stats_indicates_leader(&stats) {
                return Some(client);
            }
        }
    }
    None
}

fn record_completed(
    history: &History,
    client_id: usize,
    op: Op,
    result: OperationResult,
    op_start: Instant,
    verify_max_ops: Option<usize>,
) -> bool {
    history.try_record_timed(
        verify_max_ops,
        client_id,
        op,
        result,
        op_start,
        Instant::now(),
    )
}

async fn record_register_completed(
    history: &History,
    client_id: usize,
    op: Op,
    result: OperationResult,
    verify_max_ops: Option<usize>,
) -> bool {
    let mut tick = register_tick().lock().await;
    let base = Instant::now();
    let start = base - Duration::from_secs(3600) + Duration::from_micros(*tick);
    *tick += 2;
    let end = base - Duration::from_secs(3600) + Duration::from_micros(*tick);
    history.try_record_timed(verify_max_ops, client_id, op, result, start, end)
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
        if self.config.workload_type == WorkloadType::Register
            && self.config.verify_max_ops.is_some()
        {
            reset_register_record_ticks().await;
        }

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
    let mut client = match timeout(Duration::from_secs(2), KayaClient::connect(initial_node)).await
    {
        Ok(Ok(mut c)) => {
            c.set_max_redirects(REGISTER_MAX_REDIRECTS);
            c
        }
        _ => {
            eprintln!("Client {} failed to connect to {}", client_id, initial_node);
            return;
        }
    };

    let min_interval = if rate_limit > 0 {
        Some(Duration::from_micros(1_000_000 / rate_limit as u64))
    } else {
        None
    };

    while start.elapsed() < duration {
        if verify_max_ops.is_some_and(|max| history.len() >= max) {
            break;
        }
        let op_start = std::time::Instant::now();

        match workload_type {
            WorkloadType::Register => {
                run_register_op(
                    &nodes,
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

        // Periodically re-resolve for non-register workloads (register uses fresh leader per op).
        if workload_type != WorkloadType::Register && rng.gen_bool(0.1) {
            let new_node = nodes[rng.gen_range(0..nodes.len())];
            if let Ok(Ok(mut new_client)) =
                timeout(Duration::from_millis(500), KayaClient::connect(new_node)).await
            {
                new_client.set_max_redirects(REGISTER_MAX_REDIRECTS);
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

async fn register_get_confirmed(nodes: &[SocketAddr], key: &[u8]) -> Option<Option<Vec<u8>>> {
    for _ in 0..8 {
        let Some(mut client) = connect_leader(nodes).await else {
            sleep(Duration::from_millis(25)).await;
            continue;
        };
        let first = match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
            Ok(Ok(value)) => value,
            _ => {
                sleep(Duration::from_millis(25)).await;
                continue;
            }
        };
        let second = match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
            Ok(Ok(value)) => value,
            _ => {
                sleep(Duration::from_millis(25)).await;
                continue;
            }
        };
        if first != second {
            sleep(Duration::from_millis(25)).await;
            continue;
        }
        // Third read on a fresh leader connection to catch stale-but-stable follower reads.
        let Some(mut witness) = connect_leader(nodes).await else {
            sleep(Duration::from_millis(25)).await;
            continue;
        };
        match timeout(CLIENT_OP_TIMEOUT, witness.get(key)).await {
            Ok(Ok(third)) if third == first => return Some(first),
            _ => sleep(Duration::from_millis(25)).await,
        }
    }
    None
}

async fn register_put_confirmed(nodes: &[SocketAddr], key: &[u8], value: &[u8]) -> bool {
    for _ in 0..8 {
        let Some(mut client) = connect_leader(nodes).await else {
            sleep(Duration::from_millis(25)).await;
            continue;
        };
        match timeout(CLIENT_OP_TIMEOUT, client.put(key, value)).await {
            Ok(Ok(())) => match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
                Ok(Ok(Some(readback))) if readback == value => return true,
                _ => sleep(Duration::from_millis(25)).await,
            },
            _ => sleep(Duration::from_millis(25)).await,
        }
    }
    false
}

async fn run_register_op<R: Rng>(
    nodes: &[SocketAddr],
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
    verify_max_ops: Option<usize>,
    op_start: Instant,
) {
    let key = b"register";

    // WGL scenarios: serialize register ops across clients (single shared key) while
    // keeping declared client count for contention on the lock + nemesis overlap.
    if verify_max_ops.is_some() {
        let _serial = register_serial().lock().await;
        if rng.gen_bool(0.7) {
            let op = Op::Get { key: key.to_vec() };
            if let Some(value) = register_get_confirmed(nodes, key).await {
                record_register_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Value(value),
                    verify_max_ops,
                )
                .await;
            }
        } else {
            let value: [u8; 8] = rng.gen();
            let op = Op::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            };
            if register_put_confirmed(nodes, key, &value).await {
                record_register_completed(
                    history,
                    client_id,
                    op,
                    OperationResult::Ok,
                    verify_max_ops,
                )
                .await;
            }
        }
        return;
    }

    if let Some(leader_client) = connect_leader(nodes).await {
        *client = leader_client;
    }

    if rng.gen_bool(0.7) {
        let op = Op::Get { key: key.to_vec() };
        if let Some(value) = register_get_confirmed(nodes, key).await {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Value(value),
                op_start,
                verify_max_ops,
            );
        }
    } else {
        let value: [u8; 8] = rng.gen();
        let op = Op::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };
        if register_put_confirmed(nodes, key, &value).await {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Ok,
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
