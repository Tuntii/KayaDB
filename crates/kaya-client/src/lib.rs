mod retry;

pub use retry::{ClientObserver, OpObservation, OpOutcome, RetryPolicy, SharedObserver};

use kaya_core::{KayaError, Result};
use kaya_net::{
    decode_cdc_poll_response, decode_error_payload, decode_hello_response,
    decode_list_ranges_response, decode_scan_response, decode_txn_begin_response,
    decode_txn_commit_response, decode_value_payload, encode_cdc_checkpoint_request,
    encode_cdc_poll_request, encode_client_auth_payload, encode_hello_request, encode_key_payload,
    encode_put_payload, encode_scan_payload, encode_split_range_request, encode_txn_id_payload,
    encode_txn_op_payload, request_on_stream, CDC_CHECKPOINT_OPCODE, CDC_EVENT_DELETE,
    CDC_EVENT_PUT, CDC_POLL_OPCODE, HELLO_OPCODE, LIST_RANGES_OPCODE, PROTO_VERSION,
    SPLIT_RANGE_OPCODE, STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
    STATUS_RANGE_MOVED, STATUS_TXN_CONFLICT, TXN_BEGIN_OPCODE, TXN_COMMIT_OPCODE, TXN_OP_DELETE,
    TXN_OP_GET, TXN_OP_OPCODE, TXN_OP_PUT, TXN_ROLLBACK_OPCODE,
};

#[cfg(feature = "tls")]
use kaya_net::{roundtrip_tls, TlsConfig};
#[cfg(feature = "trace")]
use kaya_sim::{LinearizabilityChecker, Op, OpResult};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Instant;
use tokio::net::TcpStream;

/// Internal per-attempt failure, so the retry loop can distinguish a timeout
/// from a transport error when reporting to an observer.
enum RoundtripError {
    Timeout,
    Transport(KayaError),
}

pub struct KayaClient {
    addr: SocketAddr,
    max_redirects: usize,
    client_token: Option<String>,
    retry_policy: RetryPolicy,
    observer: Option<SharedObserver>,
    /// Reproducible jitter state for the retry policy.
    backoff_seed: u64,
    /// Reused keep-alive connection for the plain-TCP path (TLS reconnects per
    /// op). Invalidated on error or leader redirect.
    conn: Option<TcpStream>,
    conn_addr: Option<SocketAddr>,
    /// Cached range table meta epoch + descriptors (M21 client range cache).
    range_cache: Option<RangeCache>,
    #[cfg(feature = "trace")]
    trace: Option<LinearizabilityChecker>,
    #[cfg(feature = "tls")]
    tls_config: Option<TlsConfig>,
}

/// One cached range descriptor from LIST_RANGES.
#[derive(Debug, Clone)]
pub struct CachedRange {
    pub range_id: u64,
    pub epoch: u64,
    pub group_id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

/// Client-side range cache for RANGE_MOVED retries (M21).
#[derive(Debug, Clone)]
pub struct RangeCache {
    pub meta_epoch: u64,
    pub ranges: Vec<CachedRange>,
}

impl RangeCache {
    pub fn lookup_group(&self, key: &[u8]) -> Option<u64> {
        for r in &self.ranges {
            if key < r.start_key.as_slice() {
                continue;
            }
            if r.end_key.is_empty() || key < r.end_key.as_slice() {
                return Some(r.group_id);
            }
        }
        None
    }

    /// Upsert descriptors from a LIST_RANGES / RANGE_MOVED body.
    ///
    /// Replaces same `range_id` entries and drops any cached range that
    /// overlaps a newly reported interval so a post-split half does not leave
    /// a stale full-span entry pointing at the wrong group.
    pub fn apply_descriptors(&mut self, meta_epoch: u64, ranges: Vec<CachedRange>) {
        if meta_epoch > self.meta_epoch {
            self.meta_epoch = meta_epoch;
        }
        for nr in ranges {
            self.ranges
                .retain(|r| r.range_id != nr.range_id && !ranges_overlap(r, &nr));
            self.ranges.push(nr);
        }
        self.ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
    }
}

/// Half-open key intervals overlap (empty end_key = unbounded upper).
fn ranges_overlap(a: &CachedRange, b: &CachedRange) -> bool {
    let a_lo = a.start_key.as_slice();
    let b_lo = b.start_key.as_slice();
    let a_hi = if a.end_key.is_empty() {
        None
    } else {
        Some(a.end_key.as_slice())
    };
    let b_hi = if b.end_key.is_empty() {
        None
    } else {
        Some(b.end_key.as_slice())
    };
    // [a_lo, a_hi) overlaps [b_lo, b_hi) iff a_lo < b_hi && b_lo < a_hi
    let left = match b_hi {
        Some(bh) => a_lo < bh,
        None => true,
    };
    let right = match a_hi {
        Some(ah) => b_lo < ah,
        None => true,
    };
    left && right
}

/// Snapshot Isolation transaction handle.
///
/// Obtained via [`KayaClient::begin_txn`]. Stages writes as intents on the
/// leader; [`commit`](Transaction::commit) materializes them or
/// [`rollback`](Transaction::rollback) discards them. A local write buffer
/// provides client-side read-your-writes for keys written in this txn.
///
/// Cross-range (multi-group) commits are handled entirely by the server via
/// 2PC when staged keys map to more than one Raft group. The client protocol
/// is unchanged: same `TXN_BEGIN` / `TXN_OP` / `TXN_COMMIT` opcodes.
pub struct Transaction<'a> {
    client: &'a mut KayaClient,
    txn_id: u64,
    snapshot_ts: u64,
    /// Local write buffer: `None` means deleted within this txn.
    local: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl KayaClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            addr,
            max_redirects: 3,
            client_token: None,
            retry_policy: RetryPolicy::default(),
            observer: None,
            backoff_seed: seed_from_addr(addr),
            conn: None,
            conn_addr: None,
            range_cache: None,
            #[cfg(feature = "trace")]
            trace: None,
            #[cfg(feature = "tls")]
            tls_config: None,
        })
    }

    #[cfg(feature = "tls")]
    pub async fn connect_tls(addr: SocketAddr, tls_config: TlsConfig) -> Result<Self> {
        Ok(Self {
            addr,
            max_redirects: 3,
            client_token: None,
            retry_policy: RetryPolicy::default(),
            observer: None,
            backoff_seed: seed_from_addr(addr),
            conn: None,
            conn_addr: None,
            range_cache: None,
            #[cfg(feature = "trace")]
            trace: None,
            tls_config: Some(tls_config),
        })
    }

    pub fn set_max_redirects(&mut self, max: usize) {
        self.max_redirects = max;
    }

    /// Replace the retry policy (backoff, jitter, attempts, per-attempt timeout).
    pub fn set_retry_policy(&mut self, policy: RetryPolicy) {
        self.retry_policy = policy;
    }

    /// Install a per-operation observability hook (metrics/tracing).
    pub fn set_observer(&mut self, observer: SharedObserver) {
        self.observer = Some(observer);
    }

    /// Set the client token used for data-path operations (PUT/GET/DELETE/SCAN/STATS/TXN).
    pub fn set_client_token(&mut self, token: impl Into<String>) {
        self.client_token = Some(token.into());
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Optional protocol version handshake (HELLO opcode 0).
    /// Returns the negotiated server protocol version on success.
    pub async fn handshake(&mut self) -> Result<u16> {
        let payload = encode_hello_request(PROTO_VERSION);
        let (status, body) = self.send_with_retry(HELLO_OPCODE, &payload).await?;
        if status == STATUS_OK {
            decode_hello_response(&body).map_err(KayaError::corruption)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    #[cfg(feature = "trace")]
    pub fn enable_tracing(&mut self) {
        self.trace = Some(LinearizabilityChecker::new());
    }

    #[cfg(feature = "trace")]
    pub fn disable_tracing(&mut self) {
        self.trace = None;
    }

    #[cfg(feature = "trace")]
    pub fn trace_len(&self) -> usize {
        self.trace.as_ref().map_or(0, |t| t.len())
    }

    #[cfg(feature = "trace")]
    pub fn take_trace(&mut self, seed: u64) -> Option<String> {
        self.trace.take().map(|checker| {
            let s = checker.to_trace_string(seed);
            self.trace = Some(LinearizabilityChecker::new());
            s
        })
    }

    #[cfg(feature = "trace")]
    pub fn check_trace(&self) -> Option<std::result::Result<(), Vec<String>>> {
        self.trace.as_ref().map(|c| c.check_sequential())
    }

    #[cfg(feature = "trace")]
    fn record(&mut self, op: Op, result: OpResult) {
        if let Some(ref mut checker) = self.trace {
            checker.record_next(op, result);
        }
    }

    fn wire_payload(&self, opcode: u8, payload: &[u8]) -> Vec<u8> {
        // Data-path + TXN + CDC + range opcodes may carry an optional client token prefix.
        if matches!(opcode, 1..=4 | 6 | 9..=17) {
            encode_client_auth_payload(payload, self.client_token.as_deref())
        } else {
            payload.to_vec()
        }
    }

    /// Fetch the meta range table and refresh the local range cache.
    pub async fn list_ranges(&mut self) -> Result<RangeCache> {
        let (status, body) = self.send_with_retry(LIST_RANGES_OPCODE, &[]).await?;
        if status == STATUS_OK {
            let (meta_epoch, ranges) =
                decode_list_ranges_response(&body).map_err(KayaError::corruption)?;
            let cache = RangeCache {
                meta_epoch,
                ranges: ranges
                    .into_iter()
                    .map(
                        |(range_id, epoch, group_id, start_key, end_key)| CachedRange {
                            range_id,
                            epoch,
                            group_id,
                            start_key,
                            end_key,
                        },
                    )
                    .collect(),
            };
            self.range_cache = Some(cache.clone());
            Ok(cache)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Split the range containing `split_key` (leader of group 0). Refreshes range cache.
    pub async fn split_range(&mut self, split_key: &[u8]) -> Result<RangeCache> {
        let payload = encode_split_range_request(split_key);
        let (status, body) = self.send_with_retry(SPLIT_RANGE_OPCODE, &payload).await?;
        if status == STATUS_OK {
            // Response is the two half descriptors; refresh full table for cache.
            let _ = decode_list_ranges_response(&body).map_err(KayaError::corruption)?;
            self.list_ranges().await
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else if status == STATUS_RANGE_MOVED {
            // Stale view — refresh and surface.
            let _ = self.list_ranges().await;
            Err(KayaError::invalid_argument(
                "range moved during split; cache refreshed",
            ))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Current range cache (if previously listed / refreshed).
    pub fn range_cache(&self) -> Option<&RangeCache> {
        self.range_cache.as_ref()
    }

    /// Apply a RANGE_MOVED / LIST_RANGES body into the local range cache.
    ///
    /// Returns `true` when the body decoded successfully.
    fn apply_range_moved_body(&mut self, body: &[u8]) -> bool {
        let Ok((meta_epoch, ranges)) = decode_list_ranges_response(body) else {
            return false;
        };
        let cached: Vec<CachedRange> = ranges
            .into_iter()
            .map(
                |(range_id, epoch, group_id, start_key, end_key)| CachedRange {
                    range_id,
                    epoch,
                    group_id,
                    start_key,
                    end_key,
                },
            )
            .collect();
        match &mut self.range_cache {
            Some(cache) => cache.apply_descriptors(meta_epoch, cached),
            None => {
                self.range_cache = Some(RangeCache {
                    meta_epoch,
                    ranges: cached,
                });
            }
        }
        true
    }

    /// Poll changefeed events after `from_seq` (at-least-once), up to `limit`.
    ///
    /// Events are produced on the Raft apply path (Put/Delete/TxnCommit). Returns
    /// `(seq, is_put, key, value)` — `value` is `Some` for puts.
    pub async fn cdc_poll(
        &mut self,
        consumer_id: &str,
        from_seq: u64,
        limit: u32,
    ) -> Result<Vec<(u64, bool, Vec<u8>, Option<Vec<u8>>)>> {
        let payload = encode_cdc_poll_request(consumer_id, from_seq, limit);
        let (status, body) = self.send_with_retry(CDC_POLL_OPCODE, &payload).await?;
        if status == STATUS_OK {
            let events = decode_cdc_poll_response(&body).map_err(KayaError::corruption)?;
            Ok(events
                .into_iter()
                .map(|(seq, op, key, value)| {
                    let is_put = op == CDC_EVENT_PUT;
                    debug_assert!(is_put || op == CDC_EVENT_DELETE);
                    (seq, is_put, key, value)
                })
                .collect())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Persist the consumer's last polled CDC sequence on the leader.
    pub async fn cdc_checkpoint(&mut self, consumer_id: &str) -> Result<()> {
        let payload = encode_cdc_checkpoint_request(consumer_id);
        let (status, body) = self
            .send_with_retry(CDC_CHECKPOINT_OPCODE, &payload)
            .await?;
        if status == STATUS_OK {
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Drop the reused connection so the next attempt reconnects.
    fn invalidate_conn(&mut self) {
        self.conn = None;
        self.conn_addr = None;
    }

    /// One request/response, reusing the keep-alive connection on the plain
    /// path and reconnecting when the target address changes.
    async fn raw_roundtrip(
        &mut self,
        addr: SocketAddr,
        opcode: u8,
        payload: &[u8],
    ) -> std::io::Result<(u16, Vec<u8>)> {
        #[cfg(feature = "tls")]
        if let Some(tls_cfg) = &self.tls_config {
            // TLS sessions are not pooled here; connect per operation.
            return roundtrip_tls(addr, opcode, payload, tls_cfg).await;
        }
        if self.conn.is_none() || self.conn_addr != Some(addr) {
            let stream = TcpStream::connect(addr).await?;
            self.conn = Some(stream);
            self.conn_addr = Some(addr);
        }
        let stream = self.conn.as_mut().expect("connection just established");
        request_on_stream(stream, opcode, payload).await
    }

    /// `raw_roundtrip` wrapped in the policy's per-attempt timeout.
    async fn one_roundtrip(
        &mut self,
        addr: SocketAddr,
        opcode: u8,
        payload: &[u8],
    ) -> std::result::Result<(u16, Vec<u8>), RoundtripError> {
        match self.retry_policy.request_timeout {
            Some(timeout) => {
                match tokio::time::timeout(timeout, self.raw_roundtrip(addr, opcode, payload)).await
                {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(RoundtripError::Transport(e.into())),
                    Err(_) => Err(RoundtripError::Timeout),
                }
            }
            None => self
                .raw_roundtrip(addr, opcode, payload)
                .await
                .map(Ok)
                .unwrap_or_else(|e| Err(RoundtripError::Transport(e.into()))),
        }
    }

    fn observe(
        &self,
        opcode: u8,
        attempts: usize,
        redirects: usize,
        outcome: OpOutcome,
        latency: std::time::Duration,
    ) {
        if let Some(observer) = &self.observer {
            observer.on_operation(&OpObservation {
                opcode,
                attempts,
                redirects,
                outcome,
                latency,
            });
        }
    }

    async fn send_with_retry(&mut self, opcode: u8, payload: &[u8]) -> Result<(u16, Vec<u8>)> {
        let wire_payload = self.wire_payload(opcode, payload);
        let mut current_addr = self.addr;
        let start = Instant::now();
        let mut transport_attempts = 0usize;
        let mut redirects = 0usize;
        // Bounded like leader redirects: RANGE_MOVED refreshes the range cache
        // then reissues the same request (M21 routing recovery).
        let mut range_moved = 0usize;

        loop {
            match self
                .one_roundtrip(current_addr, opcode, &wire_payload)
                .await
            {
                Ok((status, body)) if status == STATUS_NOT_LEADER => {
                    // A redirect: the connection points at the wrong node.
                    self.invalidate_conn();
                    if redirects >= self.max_redirects {
                        self.observe(
                            opcode,
                            transport_attempts + 1,
                            redirects,
                            OpOutcome::ServerError,
                            start.elapsed(),
                        );
                        return Ok((status, body));
                    }
                    if let Some(addr) = parse_leader_hint(&body) {
                        current_addr = addr;
                        self.addr = addr;
                    }
                    let backoff = self
                        .retry_policy
                        .backoff(redirects as u32, &mut self.backoff_seed);
                    redirects += 1;
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                }
                Ok((status, body)) if status == STATUS_RANGE_MOVED => {
                    // Stale range routing: decode list-ranges body, refresh
                    // cache, retry (data path put/get/delete/scan).
                    let applied = self.apply_range_moved_body(&body);
                    if range_moved >= self.max_redirects || !applied {
                        self.observe(
                            opcode,
                            transport_attempts + 1,
                            redirects,
                            OpOutcome::ServerError,
                            start.elapsed(),
                        );
                        return Ok((status, body));
                    }
                    let backoff = self
                        .retry_policy
                        .backoff(range_moved as u32, &mut self.backoff_seed);
                    range_moved += 1;
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                }
                Ok((status, body)) => {
                    self.observe(
                        opcode,
                        transport_attempts + 1,
                        redirects,
                        outcome_from_status(status),
                        start.elapsed(),
                    );
                    return Ok((status, body));
                }
                Err(err) => {
                    self.invalidate_conn();
                    transport_attempts += 1;
                    let (timed_out, error) = match err {
                        RoundtripError::Timeout => (true, KayaError::internal("request timed out")),
                        RoundtripError::Transport(e) => (false, e),
                    };
                    if transport_attempts >= self.retry_policy.max_attempts {
                        let outcome = if timed_out {
                            OpOutcome::Timeout
                        } else {
                            OpOutcome::ConnectionError
                        };
                        self.observe(
                            opcode,
                            transport_attempts,
                            redirects,
                            outcome,
                            start.elapsed(),
                        );
                        return Err(KayaError::internal(format!(
                            "Cluster request failed after {transport_attempts} attempt(s) \
                             and {redirects} redirect(s). Last error: {error:?}"
                        )));
                    }
                    let backoff = self
                        .retry_policy
                        .backoff(transport_attempts as u32 - 1, &mut self.backoff_seed);
                    if !backoff.is_zero() {
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }
    }

    /// Begin a Snapshot Isolation transaction.
    ///
    /// Returns a [`Transaction`] bound to this client. The server assigns a
    /// `txn_id` and `snapshot_ts` (`read_ts`). Leader redirects are followed
    /// like ordinary put/get.
    pub async fn begin_txn(&mut self) -> Result<Transaction<'_>> {
        let (status, body) = self.send_with_retry(TXN_BEGIN_OPCODE, &[]).await?;
        if status == STATUS_OK {
            let (txn_id, snapshot_ts) =
                decode_txn_begin_response(&body).map_err(KayaError::corruption)?;
            Ok(Transaction {
                client: self,
                txn_id,
                snapshot_ts,
                local: HashMap::new(),
            })
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            Err(map_txn_status(status, &body))
        }
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let payload = encode_put_payload(key, value);
        let (status, body) = self.send_with_retry(1, &payload).await?;
        if status == STATUS_OK {
            #[cfg(feature = "trace")]
            self.record(
                Op::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                OpResult::Ok,
            );
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::internal(msg))
        }
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let payload = encode_key_payload(key);
        let (status, body) = self.send_with_retry(2, &payload).await?;
        if status == STATUS_OK {
            let val = decode_value_payload(&body).map_err(KayaError::corruption)?;
            #[cfg(feature = "trace")]
            self.record(
                Op::Get { key: key.to_vec() },
                OpResult::Value(Some(val.clone())),
            );
            Ok(Some(val))
        } else if status == STATUS_NOT_FOUND {
            #[cfg(feature = "trace")]
            self.record(Op::Get { key: key.to_vec() }, OpResult::Value(None));
            Ok(None)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(Op::Get { key: key.to_vec() }, OpResult::Error(msg.clone()));
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(Op::Get { key: key.to_vec() }, OpResult::Error(msg.clone()));
            Err(KayaError::internal(msg))
        }
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<()> {
        let payload = encode_key_payload(key);
        let (status, body) = self.send_with_retry(3, &payload).await?;
        if status == STATUS_OK {
            #[cfg(feature = "trace")]
            self.record(Op::Delete { key: key.to_vec() }, OpResult::Ok);
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Delete { key: key.to_vec() },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Delete { key: key.to_vec() },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::internal(msg))
        }
    }

    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let payload = encode_scan_payload(prefix);
        let (status, body) = self.send_with_retry(4, &payload).await?;
        if status == STATUS_OK {
            let items = decode_scan_response(&body).map_err(KayaError::corruption)?;
            #[cfg(feature = "trace")]
            self.record(
                Op::Scan {
                    prefix: prefix.to_vec(),
                },
                OpResult::Scan(items.clone()),
            );
            Ok(items)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Scan {
                    prefix: prefix.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Scan {
                    prefix: prefix.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::internal(msg))
        }
    }

    pub async fn health(&mut self) -> Result<String> {
        let (status, body) = self.send_with_retry(5, &[]).await?;
        if status == STATUS_OK {
            let s = String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
            Ok(s)
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    pub async fn stats(&mut self) -> Result<String> {
        let (status, body) = self.send_with_retry(6, &[]).await?;
        if status == STATUS_OK {
            let s = String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
            Ok(s)
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }
}

impl Transaction<'_> {
    /// Server-assigned transaction id.
    pub fn txn_id(&self) -> u64 {
        self.txn_id
    }

    /// Snapshot / read timestamp for this transaction.
    pub fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// Point get under the txn snapshot, with local read-your-writes.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(local) = self.local.get(key) {
            return Ok(local.clone());
        }
        let payload = encode_txn_op_payload(self.txn_id, TXN_OP_GET, key, None);
        let (status, body) = self.client.send_with_retry(TXN_OP_OPCODE, &payload).await?;
        if status == STATUS_OK {
            let val = decode_value_payload(&body).map_err(KayaError::corruption)?;
            Ok(Some(val))
        } else if status == STATUS_NOT_FOUND {
            Ok(None)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            Err(map_txn_status(status, &body))
        }
    }

    /// Stage a put intent (write-write conflicts may fail immediately).
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let payload = encode_txn_op_payload(self.txn_id, TXN_OP_PUT, key, Some(value));
        let (status, body) = self.client.send_with_retry(TXN_OP_OPCODE, &payload).await?;
        if status == STATUS_OK {
            self.local.insert(key.to_vec(), Some(value.to_vec()));
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            Err(map_txn_status(status, &body))
        }
    }

    /// Stage a delete intent.
    pub async fn delete(&mut self, key: &[u8]) -> Result<()> {
        let payload = encode_txn_op_payload(self.txn_id, TXN_OP_DELETE, key, None);
        let (status, body) = self.client.send_with_retry(TXN_OP_OPCODE, &payload).await?;
        if status == STATUS_OK {
            self.local.insert(key.to_vec(), None);
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            Err(map_txn_status(status, &body))
        }
    }

    /// Commit staged intents. Returns the commit timestamp on success.
    ///
    /// Range-transparent: single-group commits use ordinary `TxnCommit`; when
    /// keys span multiple Raft groups the server runs 2PC. No client API or
    /// wire difference.
    ///
    /// Write-write conflicts map to [`KayaError::TxnConflict`].
    pub async fn commit(self) -> Result<u64> {
        let payload = encode_txn_id_payload(self.txn_id);
        let (status, body) = self
            .client
            .send_with_retry(TXN_COMMIT_OPCODE, &payload)
            .await?;
        if status == STATUS_OK {
            decode_txn_commit_response(&body).map_err(KayaError::corruption)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            Err(map_txn_status(status, &body))
        }
    }

    /// Discard staged intents without committing.
    pub async fn rollback(self) -> Result<()> {
        let payload = encode_txn_id_payload(self.txn_id);
        let (status, body) = self
            .client
            .send_with_retry(TXN_ROLLBACK_OPCODE, &payload)
            .await?;
        if status == STATUS_OK {
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            Err(map_txn_status(status, &body))
        }
    }
}

/// Derive a stable, non-zero jitter seed from the initial address so two
/// clients pointed at different nodes stagger their backoffs deterministically.
fn seed_from_addr(addr: SocketAddr) -> u64 {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for byte in addr.to_string().as_bytes() {
        seed = seed.rotate_left(5) ^ u64::from(*byte);
    }
    seed | 1
}

/// Parse a `NOT_LEADER` hint body (`host:port` UTF-8, possibly empty) into a
/// leader address to redirect to.
fn parse_leader_hint(body: &[u8]) -> Option<SocketAddr> {
    let hint = std::str::from_utf8(body).ok()?.trim();
    if hint.is_empty() {
        return None;
    }
    hint.parse::<SocketAddr>().ok()
}

/// Map a wire status code to a coarse observability outcome.
fn outcome_from_status(status: u16) -> OpOutcome {
    match status {
        STATUS_OK => OpOutcome::Ok,
        STATUS_NOT_FOUND => OpOutcome::NotFound,
        STATUS_INVALID_ARGUMENT => OpOutcome::InvalidArgument,
        STATUS_TXN_CONFLICT | STATUS_RANGE_MOVED => OpOutcome::ServerError,
        _ => OpOutcome::ServerError,
    }
}

/// Map TXN-related non-OK status codes to a clear client error.
fn map_txn_status(status: u16, body: &[u8]) -> KayaError {
    if status == STATUS_TXN_CONFLICT {
        return KayaError::TxnConflict;
    }
    if status == STATUS_NOT_LEADER {
        let hint = std::str::from_utf8(body).unwrap_or("").trim();
        if hint.is_empty() {
            return KayaError::internal("not leader (no leader hint)");
        }
        return KayaError::internal(format!("not leader; hint={hint}"));
    }
    let msg = decode_error_payload(body).unwrap_or_else(|_| "Unknown error".to_string());
    KayaError::internal(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_net::{
        decode_txn_begin_response, decode_txn_commit_response, decode_txn_op_payload,
        encode_list_ranges_response, encode_range_moved_payload, encode_txn_begin_response,
        encode_txn_commit_response, encode_txn_op_payload, TXN_OP_PUT,
    };

    #[test]
    fn map_txn_status_conflict_is_txn_conflict() {
        let err = map_txn_status(STATUS_TXN_CONFLICT, &[]);
        assert!(matches!(err, KayaError::TxnConflict));
        assert_eq!(err.exit_code(), 7);
        assert!(err.guidance().is_some());
    }

    #[test]
    fn map_txn_status_not_leader_uses_hint() {
        let err = map_txn_status(STATUS_NOT_LEADER, b"127.0.0.1:7379");
        let msg = err.to_string();
        assert!(msg.contains("not leader"), "{msg}");
        assert!(msg.contains("127.0.0.1:7379"), "{msg}");
    }

    #[test]
    fn txn_payloads_round_trip_for_client_shapes() {
        let begin = encode_txn_begin_response(7, 42);
        assert_eq!(decode_txn_begin_response(&begin).unwrap(), (7, 42));

        let put = encode_txn_op_payload(7, TXN_OP_PUT, b"k", Some(b"v"));
        let (id, op, key, val) = decode_txn_op_payload(&put).unwrap();
        assert_eq!(
            (id, op, key, val),
            (7, TXN_OP_PUT, b"k".to_vec(), Some(b"v".to_vec()))
        );

        let commit = encode_txn_commit_response(99);
        assert_eq!(decode_txn_commit_response(&commit).unwrap(), 99);
    }

    #[test]
    fn outcome_maps_txn_conflict() {
        assert!(matches!(
            outcome_from_status(STATUS_TXN_CONFLICT),
            OpOutcome::ServerError
        ));
    }

    #[test]
    fn outcome_maps_range_moved() {
        assert!(matches!(
            outcome_from_status(STATUS_RANGE_MOVED),
            OpOutcome::ServerError
        ));
    }

    #[test]
    fn range_cache_apply_descriptors_replaces_overlap_after_split() {
        // Full span group 0, then RANGE_MOVED for right half → group 1.
        let mut cache = RangeCache {
            meta_epoch: 0,
            ranges: vec![CachedRange {
                range_id: 1,
                epoch: 1,
                group_id: 0,
                start_key: b"a".to_vec(),
                end_key: vec![],
            }],
        };
        cache.apply_descriptors(
            2,
            vec![CachedRange {
                range_id: 2,
                epoch: 1,
                group_id: 1,
                start_key: b"m".to_vec(),
                end_key: vec![],
            }],
        );
        assert_eq!(cache.meta_epoch, 2);
        assert_eq!(cache.lookup_group(b"z"), Some(1));
        // Overlapping full span was dropped; keys left of split have no entry.
        assert_eq!(cache.lookup_group(b"b"), None);
    }

    #[test]
    fn apply_range_moved_body_seeds_and_updates_cache() {
        // Build a minimal client shell without connecting.
        let mut client = KayaClient {
            addr: "127.0.0.1:9".parse().unwrap(),
            max_redirects: 3,
            client_token: None,
            retry_policy: RetryPolicy::none(),
            observer: None,
            backoff_seed: 1,
            conn: None,
            conn_addr: None,
            range_cache: None,
            #[cfg(feature = "trace")]
            trace: None,
            #[cfg(feature = "tls")]
            tls_config: None,
        };

        let body = encode_range_moved_payload(3, 2, 1, 7, b"k", b"z");
        assert!(client.apply_range_moved_body(&body));
        let cache = client.range_cache().expect("cache seeded");
        assert_eq!(cache.meta_epoch, 3);
        assert_eq!(cache.lookup_group(b"m"), Some(7));
        assert_eq!(cache.ranges.len(), 1);

        // Second moved half with higher meta epoch replaces by range_id.
        let body2 = encode_list_ranges_response(4, &[(2, 2, 9, b"k".to_vec(), b"z".to_vec())]);
        assert!(client.apply_range_moved_body(&body2));
        let cache = client.range_cache().unwrap();
        assert_eq!(cache.meta_epoch, 4);
        assert_eq!(cache.lookup_group(b"m"), Some(9));
        assert_eq!(cache.ranges.len(), 1);
    }

    #[test]
    fn apply_range_moved_body_rejects_garbage() {
        let mut client = KayaClient {
            addr: "127.0.0.1:9".parse().unwrap(),
            max_redirects: 3,
            client_token: None,
            retry_policy: RetryPolicy::none(),
            observer: None,
            backoff_seed: 1,
            conn: None,
            conn_addr: None,
            range_cache: None,
            #[cfg(feature = "trace")]
            trace: None,
            #[cfg(feature = "tls")]
            tls_config: None,
        };
        assert!(!client.apply_range_moved_body(&[0x01, 0x02]));
        assert!(client.range_cache().is_none());
    }
}
