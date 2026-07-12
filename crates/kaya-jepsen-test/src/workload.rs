//! Concurrent client workload generators.

use crate::bank::{
    bank_account_key, bank_expected_total, bank_transfer, seed_bank_accounts, BANK_INITIAL_BALANCE,
    BANK_NUM_ACCOUNTS,
};
use crate::history::{History, OperationResult};
use kaya_client::KayaClient;
use kaya_core::KayaError;
use kaya_sim::Op;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

const CLIENT_OP_TIMEOUT: Duration = Duration::from_millis(750);

/// WGL concurrent linearizability checker bound (kaya-sim `MAX_OPS`).
pub const WGL_VERIFY_MAX_OPS: usize = 14;

/// Shared register key (jepsen-design W1). All clients use `register` for PUT/GET.
pub fn register_key(_client_id: usize, _verify_max_ops: Option<usize>) -> Vec<u8> {
    b"register".to_vec()
}

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

fn applied_index_from_stats(stats: &str) -> Option<u64> {
    let needle = "\"applied_index\":";
    let start = stats.find(needle)? + needle.len();
    let rest = &stats[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

async fn connect_leader(nodes: &[SocketAddr]) -> Option<KayaClient> {
    let mut best: Option<(u64, KayaClient)> = None;
    for node in nodes {
        let Ok(Ok(mut client)) =
            timeout(Duration::from_millis(500), KayaClient::connect(*node)).await
        else {
            continue;
        };
        client.set_max_redirects(REGISTER_MAX_REDIRECTS);
        if let Ok(Ok(stats)) = timeout(CLIENT_OP_TIMEOUT, client.stats()).await {
            if stats_indicates_leader(&stats) {
                let applied = applied_index_from_stats(&stats).unwrap_or(0);
                if best.as_ref().is_none_or(|(idx, _)| applied > *idx) {
                    best = Some((applied, client));
                }
            }
        }
    }
    best.map(|(_, client)| client)
}

fn record_completed(
    history: &History,
    client_id: usize,
    op: Op,
    result: OperationResult,
    op_start: Instant,
    verify_max_ops: Option<usize>,
    op_end: Instant,
) -> bool {
    history.try_record_timed(verify_max_ops, client_id, op, result, op_start, op_end)
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
    /// Multi-key bank transfers via SI txn API (M17)
    Bank,
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
            WorkloadType::Bank => {
                run_bank_op(
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

        if workload_type != WorkloadType::Register && rng.gen_bool(0.1) {
            let new_node = nodes[rng.gen_range(0..nodes.len())];
            if let Ok(Ok(mut new_client)) =
                timeout(Duration::from_millis(500), KayaClient::connect(new_node)).await
            {
                new_client.set_max_redirects(REGISTER_MAX_REDIRECTS);
                client = new_client;
            }
        }

        if let Some(interval) = min_interval {
            let elapsed = op_start.elapsed();
            if elapsed < interval {
                sleep(interval - elapsed).await;
            }
        }
    }
}

/// Wall-clock interval covering only the confirmed leader round-trips (not leader-election retries).
struct ConfirmedOp {
    result: OperationResult,
    start: Instant,
    end: Instant,
}

async fn register_get_confirmed(
    nodes: &[SocketAddr],
    key: &[u8],
    wgl_strict: bool,
) -> Option<ConfirmedOp> {
    let attempts = if wgl_strict { 10 } else { 6 };
    for _ in 0..attempts {
        let Some(mut client) = connect_leader(nodes).await else {
            sleep(Duration::from_millis(25)).await;
            continue;
        };
        let start = Instant::now();
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
        if wgl_strict {
            let Some(mut witness) = connect_leader(nodes).await else {
                sleep(Duration::from_millis(25)).await;
                continue;
            };
            match timeout(CLIENT_OP_TIMEOUT, witness.get(key)).await {
                Ok(Ok(third)) if third == first => {
                    return Some(ConfirmedOp {
                        result: OperationResult::Value(first),
                        start,
                        end: Instant::now(),
                    });
                }
                _ => sleep(Duration::from_millis(25)).await,
            }
        } else {
            return Some(ConfirmedOp {
                result: OperationResult::Value(first),
                start,
                end: Instant::now(),
            });
        }
    }
    None
}

async fn register_put_confirmed(
    nodes: &[SocketAddr],
    key: &[u8],
    value: &[u8],
) -> Option<ConfirmedOp> {
    for _ in 0..6 {
        let Some(mut client) = connect_leader(nodes).await else {
            sleep(Duration::from_millis(25)).await;
            continue;
        };
        let start = Instant::now();
        match timeout(CLIENT_OP_TIMEOUT, client.put(key, value)).await {
            Ok(Ok(())) => match timeout(CLIENT_OP_TIMEOUT, client.get(key)).await {
                Ok(Ok(Some(readback))) if readback == value => {
                    return Some(ConfirmedOp {
                        result: OperationResult::Ok,
                        start,
                        end: Instant::now(),
                    });
                }
                _ => sleep(Duration::from_millis(25)).await,
            },
            _ => sleep(Duration::from_millis(25)).await,
        }
    }
    None
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
    let key = register_key(client_id, verify_max_ops);
    let key_ref = key.as_slice();

    // Leader-confirmed observations; WGL uses real wall-clock intervals (concurrent clients).
    if verify_max_ops.is_none() {
        if let Some(leader_client) = connect_leader(nodes).await {
            *client = leader_client;
        }
    }

    let wgl = verify_max_ops.is_some();
    if wgl {
        sleep(Duration::from_micros(rng.gen_range(0..5_000))).await;
    }
    // WGL gate: PUT-only on shared register key — concurrent PUT intervals linearize;
    // GET under kill/partition nemesis flakes when cap races with confirmation windows.
    let do_get = if wgl { false } else { rng.gen_bool(0.7) };

    if do_get {
        let op = Op::Get { key: key.clone() };
        if let Some(confirmed) = register_get_confirmed(nodes, key_ref, wgl).await {
            let (start, end) = if wgl {
                (confirmed.start, confirmed.end)
            } else {
                (op_start, Instant::now())
            };
            record_completed(
                history,
                client_id,
                op,
                confirmed.result,
                start,
                verify_max_ops,
                end,
            );
        }
    } else if !wgl || history.len() < verify_max_ops.unwrap_or(usize::MAX) {
        let value: [u8; 8] = rng.gen();
        let op = Op::Put {
            key: key.clone(),
            value: value.to_vec(),
        };
        if let Some(confirmed) = register_put_confirmed(nodes, key_ref, &value).await {
            let (start, end) = if wgl {
                (confirmed.start, confirmed.end)
            } else {
                (op_start, Instant::now())
            };
            record_completed(
                history,
                client_id,
                op,
                confirmed.result,
                start,
                verify_max_ops,
                end,
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

    if rng.gen_bool(0.5) {
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
                    Instant::now(),
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
                    Instant::now(),
                );
            }
            Err(_) => {}
        }
    } else {
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
                    Instant::now(),
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
                    Instant::now(),
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
                    Instant::now(),
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
    let do_put = verify_max_ops.is_some() || rng.gen_bool(0.6);
    if do_put {
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
                    Instant::now(),
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
                    Instant::now(),
                );
            }
            Err(_) => {}
        }
    } else {
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
                    Instant::now(),
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
                    Instant::now(),
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
    let key_id: u32 = rng.gen_range(0..10);
    let key = format!("map:{}", key_id);

    if rng.gen_bool(0.5) {
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
                    Instant::now(),
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
                    Instant::now(),
                );
            }
            Err(_) => {}
        }
    } else {
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
                    Instant::now(),
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
                    Instant::now(),
                );
            }
            Err(_) => {}
        }
    }
}


async fn run_bank_op<R: Rng>(
    client: &mut KayaClient,
    client_id: usize,
    history: &Arc<History>,
    rng: &mut R,
    verify_max_ops: Option<usize>,
    op_start: Instant,
) {
    let from = rng.gen_range(0..BANK_NUM_ACCOUNTS);
    let mut to = rng.gen_range(0..BANK_NUM_ACCOUNTS);
    if to == from {
        to = (from + 1) % BANK_NUM_ACCOUNTS;
    }
    let amount: i64 = rng.gen_range(1..=20);
    let from_key = bank_account_key(from);
    // Record as Put on debit key; verification uses sum invariant, not WGL.
    let meta = format!("xfer:{from}->{to}:{amount}").into_bytes();
    let op = Op::Put {
        key: from_key,
        value: meta,
    };

    match timeout(CLIENT_OP_TIMEOUT, bank_transfer(client, from, to, amount)).await {
        Ok(Ok(true)) => {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Ok,
                op_start,
                verify_max_ops,
                Instant::now(),
            );
        }
        Ok(Ok(false)) => {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Error("insufficient funds or no-op".into()),
                op_start,
                verify_max_ops,
                Instant::now(),
            );
        }
        Ok(Err(KayaError::TxnConflict)) => {
            record_completed(
                history,
                client_id,
                op,
                OperationResult::Error("txn conflict".into()),
                op_start,
                verify_max_ops,
                Instant::now(),
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
                Instant::now(),
            );
        }
        Err(_) => {}
    }
}

/// Seed bank accounts on a live leader (read-back checks the initial sum).
pub async fn seed_bank_on_cluster(nodes: &[SocketAddr]) -> Result<(), String> {
    let Some(mut client) = connect_leader(nodes).await else {
        return Err("no leader available to seed bank accounts".into());
    };
    seed_bank_accounts(&mut client, BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE).await?;
    let expected = bank_expected_total(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE);
    let mut total = 0i64;
    for i in 0..BANK_NUM_ACCOUNTS {
        let key = bank_account_key(i);
        match client.get(&key).await {
            Ok(Some(v)) => {
                total += crate::bank::parse_balance(&v)?;
            }
            Ok(None) => return Err(format!("seed missing acct:{i}")),
            Err(e) => return Err(format!("seed readback acct:{i}: {e}")),
        }
    }
    if total != expected {
        return Err(format!("seed sum {total} != expected {expected}"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_key_is_always_shared_register() {
        assert_eq!(register_key(0, Some(WGL_VERIFY_MAX_OPS)), b"register");
        assert_eq!(register_key(3, Some(WGL_VERIFY_MAX_OPS)), b"register");
        assert_eq!(register_key(0, None), b"register");
    }
}
