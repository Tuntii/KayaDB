//! Read-only HTTP JSON dashboard (M22 v1 + issue #31 Phase A).
//!
//! Optional listener (`--dashboard-addr`). Endpoints:
//! - `GET /health` → `{"ok":true}` (unchanged; do not add fields here)
//! - `GET /v1/cluster` → node_id, drain, range_count, leader_group_ids, meta_epoch
//! - `GET /v1/ranges` → meta_epoch + range descriptors + per-range `healthy`
//! - `GET /v1/raft` → per-group leader / term / commit
//! - `GET /v1/leadership` → group_id → {leader_id, term, role, is_leader}
//! - `GET /v1/errors` → recent error ring (cap 50)
//!
//! Phase B (eBPF/fsync attribution) and Phase C (profiling CI) are deferred:
//! they need a Linux perf/capability runner. See `docs/runbooks/dashboard.md`.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use kaya_raft::{GroupId, MultiRaftHost, StaticRangeTable};

/// Shared raft host (same shape as cluster internals).
pub type DashboardRaft = Arc<Mutex<MultiRaftHost>>;
/// Shared range table (live snapshot for `/v1/ranges`).
pub type DashboardRanges = Arc<RwLock<StaticRangeTable>>;

/// Cap for the recent-error ring served by `GET /v1/errors`.
pub const ERROR_RING_CAP: usize = 50;

/// One recorded dashboard / cluster error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorEvent {
    pub ts_unix_ms: u64,
    pub kind: String,
    pub message: String,
}

/// Shared recent-error ring (`GET /v1/errors`).
pub type DashboardErrors = Arc<Mutex<VecDeque<ErrorEvent>>>;

/// Empty error ring.
pub fn new_error_ring() -> DashboardErrors {
    Arc::new(Mutex::new(VecDeque::new()))
}

/// Push `kind`/`message` onto the ring, dropping the oldest event past the cap.
pub fn record_error(errors: &DashboardErrors, kind: impl Into<String>, message: impl Into<String>) {
    let event = ErrorEvent {
        ts_unix_ms: unix_ms(),
        kind: kind.into(),
        message: message.into(),
    };
    let mut guard = errors.lock().unwrap_or_else(|e| e.into_inner());
    if guard.len() >= ERROR_RING_CAP {
        guard.pop_front();
    }
    guard.push_back(event);
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Bind `addr` and serve dashboard GETs until the task is cancelled.
pub async fn serve(
    addr: SocketAddr,
    node_id: u64,
    drain: bool,
    raft: DashboardRaft,
    ranges: DashboardRanges,
    errors: DashboardErrors,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    eprintln!("[node {node_id}] dashboard listening on {bound}");
    accept_loop(listener, node_id, drain, raft, ranges, errors).await
}

async fn accept_loop(
    listener: TcpListener,
    node_id: u64,
    drain: bool,
    raft: DashboardRaft,
    ranges: DashboardRanges,
    errors: DashboardErrors,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let ranges = ranges.clone();
        let errors = errors.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, node_id, drain, raft, ranges, errors).await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    node_id: u64,
    drain: bool,
    raft: DashboardRaft,
    ranges: DashboardRanges,
    errors: DashboardErrors,
) -> std::io::Result<()> {
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf).await?;
    let request = std::str::from_utf8(&buf[..n]).unwrap_or("");
    let path = request_path(request);

    let (status_line, content_type, body) = match path {
        "/health" | "/health/" => (
            "HTTP/1.1 200 OK",
            "application/json",
            health_json().to_owned(),
        ),
        "/v1/cluster" | "/v1/cluster/" => {
            let body = {
                let table = ranges.read().await;
                let host = raft.lock().unwrap_or_else(|e| e.into_inner());
                cluster_json(node_id, drain, &table, &host)
            };
            ("HTTP/1.1 200 OK", "application/json", body)
        }
        "/v1/ranges" | "/v1/ranges/" => {
            let body = {
                let table = ranges.read().await;
                let host = raft.lock().unwrap_or_else(|e| e.into_inner());
                ranges_json(&table, &host)
            };
            ("HTTP/1.1 200 OK", "application/json", body)
        }
        "/v1/raft" | "/v1/raft/" => {
            let body = {
                let guard = raft.lock().unwrap_or_else(|e| e.into_inner());
                raft_json(&guard)
            };
            ("HTTP/1.1 200 OK", "application/json", body)
        }
        "/v1/leadership" | "/v1/leadership/" => {
            let body = {
                let guard = raft.lock().unwrap_or_else(|e| e.into_inner());
                leadership_json(&guard)
            };
            ("HTTP/1.1 200 OK", "application/json", body)
        }
        "/v1/errors" | "/v1/errors/" => {
            ("HTTP/1.1 200 OK", "application/json", errors_json(&errors))
        }
        _ => {
            record_error(&errors, "http_404", path);
            (
                "HTTP/1.1 404 Not Found",
                "application/json",
                r#"{"error":"not_found"}"#.to_owned(),
            )
        }
    };

    let response = format!(
        "{status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

fn request_path(request: &str) -> &str {
    // First line: METHOD path HTTP/x.y
    let line = request.lines().next().unwrap_or("");
    let mut parts = line.split_whitespace();
    let _method = parts.next();
    let path = parts.next().unwrap_or("/");
    // Strip query string if present.
    path.split('?').next().unwrap_or(path)
}

/// `GET /health` body.
pub fn health_json() -> &'static str {
    r#"{"ok":true}"#
}

/// `GET /v1/cluster` body.
pub fn cluster_json(
    node_id: u64,
    drain: bool,
    table: &StaticRangeTable,
    host: &MultiRaftHost,
) -> String {
    let leader_group_ids: Vec<u64> = host
        .sorted_group_ids()
        .into_iter()
        .filter(|&gid| host.is_leader_of(gid))
        .map(|gid| gid.0)
        .collect();
    format!(
        r#"{{"node_id":{},"drain":{},"range_count":{},"leader_group_ids":{},"meta_epoch":{}}}"#,
        node_id,
        if drain { "true" } else { "false" },
        table.ranges().len(),
        json_u64_array(&leader_group_ids),
        table.meta_epoch(),
    )
}

/// `GET /v1/ranges` body from a live range-table snapshot.
///
/// `healthy` is true when the hosting Raft group has a known leader.
pub fn ranges_json(table: &StaticRangeTable, host: &MultiRaftHost) -> String {
    let mut out = format!(r#"{{"meta_epoch":{},"ranges":["#, table.meta_epoch());
    for (i, r) in table.ranges().iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let healthy = group_has_known_leader(host, r.group_id);
        out.push_str(&format!(
            r#"{{"range_id":{},"epoch":{},"group_id":{},"start":{},"end":{},"healthy":{}}}"#,
            r.range_id,
            r.epoch,
            r.group_id.0,
            json_bytes(&r.start_key),
            json_bytes(&r.end_key),
            if healthy { "true" } else { "false" },
        ));
    }
    out.push_str("]}");
    out
}

/// `GET /v1/raft` body: per hosted group leader/term/commit.
pub fn raft_json(host: &MultiRaftHost) -> String {
    let mut out = String::from(r#"{"groups":["#);
    for (i, gid) in host.sorted_group_ids().into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&group_status_object(host, gid));
    }
    out.push_str("]}");
    out
}

/// `GET /v1/leadership` body: map of group_id → leader/term/role.
pub fn leadership_json(host: &MultiRaftHost) -> String {
    let mut out = String::from(r#"{"groups":{"#);
    for (i, gid) in host.sorted_group_ids().into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let (leader_id, term, _commit, role, is_leader) = group_fields(host, gid);
        let leader_json = match leader_id {
            Some(id) => id.to_string(),
            None => "null".to_owned(),
        };
        out.push_str(&format!(
            r#""{}":{{"leader_id":{},"term":{},"role":"{}","is_leader":{}}}"#,
            gid.0,
            leader_json,
            term,
            role,
            if is_leader { "true" } else { "false" },
        ));
    }
    out.push_str("}}");
    out
}

/// `GET /v1/errors` body from the recent-error ring (oldest first).
pub fn errors_json(errors: &DashboardErrors) -> String {
    let guard = errors.lock().unwrap_or_else(|e| e.into_inner());
    let mut out = String::from(r#"{"errors":["#);
    for (i, ev) in guard.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            r#"{{"ts_unix_ms":{},"kind":{},"message":{}}}"#,
            ev.ts_unix_ms,
            json_string(&ev.kind),
            json_string(&ev.message),
        ));
    }
    out.push_str("]}");
    out
}

fn group_has_known_leader(host: &MultiRaftHost, group_id: GroupId) -> bool {
    match host.status_of(group_id) {
        Some(s) => s.leader_id.is_some() || matches!(s.role, kaya_raft::Role::Leader),
        None => false,
    }
}

fn group_fields(host: &MultiRaftHost, gid: GroupId) -> (Option<u64>, u64, u64, String, bool) {
    match host.status_of(gid) {
        Some(s) => (
            s.leader_id.map(|id| id.0),
            s.current_term.0,
            s.commit_index.0,
            format!("{:?}", s.role).to_lowercase(),
            matches!(s.role, kaya_raft::Role::Leader),
        ),
        None => (None, 0, 0, "unknown".to_owned(), false),
    }
}

fn group_status_object(host: &MultiRaftHost, gid: GroupId) -> String {
    let (leader_id, term, commit, role, is_leader) = group_fields(host, gid);
    let leader_json = match leader_id {
        Some(id) => id.to_string(),
        None => "null".to_owned(),
    };
    format!(
        r#"{{"group_id":{},"leader_id":{},"term":{},"commit":{},"role":"{}","is_leader":{}}}"#,
        gid.0,
        leader_json,
        term,
        commit,
        role,
        if is_leader { "true" } else { "false" },
    )
}

fn json_u64_array(ids: &[u64]) -> String {
    let mut out = String::from("[");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&id.to_string());
    }
    out.push(']');
    out
}

fn json_bytes(bytes: &[u8]) -> String {
    json_string(&String::from_utf8_lossy(bytes))
}

fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_raft::{GroupId, NodeId, StaticRange, StaticRangeTable};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn health_body_is_ok() {
        assert_eq!(health_json(), r#"{"ok":true}"#);
    }

    #[test]
    fn ranges_json_includes_meta_epoch_and_descriptor() {
        let table = StaticRangeTable::single_group(GroupId::ZERO);
        let host = MultiRaftHost::new();
        let body = ranges_json(&table, &host);
        assert!(body.contains(r#""meta_epoch":1"#), "body={body}");
        assert!(body.contains(r#""group_id":0"#), "body={body}");
        assert!(body.contains(r#""range_id":1"#), "body={body}");
        assert!(body.contains(r#""healthy":false"#), "body={body}");
    }

    #[test]
    fn raft_json_lists_hosted_groups() {
        let mut host = MultiRaftHost::new();
        host.insert_single_node(GroupId::ZERO, NodeId(1));
        let body = raft_json(&host);
        assert!(body.contains(r#""group_id":0"#), "body={body}");
        assert!(body.contains(r#""term":"#), "body={body}");
        assert!(body.contains(r#""commit":"#), "body={body}");
    }

    #[test]
    fn ranges_json_after_split() {
        let mut table = StaticRangeTable::single_group(GroupId::ZERO);
        table.split_at(b"m").unwrap();
        let host = MultiRaftHost::new();
        let body = ranges_json(&table, &host);
        assert!(body.contains(r#""meta_epoch":2"#), "body={body}");
        // two ranges
        assert_eq!(body.matches(r#""range_id":"#).count(), 2);
    }

    #[test]
    fn cluster_json_reports_node_drain_and_leaders() {
        let table = StaticRangeTable::single_group(GroupId::ZERO);
        let mut host = MultiRaftHost::new();
        host.insert_single_node(GroupId::ZERO, NodeId(3));
        for _ in 0..20 {
            let _ = host.tick_all();
        }
        let body = cluster_json(3, true, &table, &host);
        assert!(body.contains(r#""node_id":3"#), "body={body}");
        assert!(body.contains(r#""drain":true"#), "body={body}");
        assert!(body.contains(r#""range_count":1"#), "body={body}");
        assert!(body.contains(r#""meta_epoch":1"#), "body={body}");
        assert!(body.contains(r#""leader_group_ids":[0]"#), "body={body}");
    }

    #[test]
    fn leadership_json_is_group_map() {
        let mut host = MultiRaftHost::new();
        host.insert_single_node(GroupId::ZERO, NodeId(9));
        for _ in 0..20 {
            let _ = host.tick_all();
        }
        let body = leadership_json(&host);
        assert!(body.contains(r#""0":{"#), "body={body}");
        assert!(body.contains(r#""leader_id":9"#), "body={body}");
        assert!(body.contains(r#""is_leader":true"#), "body={body}");
        assert!(body.contains(r#""role":"leader""#), "body={body}");
        assert!(!body.contains(r#""commit":"#), "body={body}");
    }

    #[test]
    fn error_ring_caps_at_50() {
        let errors = new_error_ring();
        for i in 0..60 {
            record_error(&errors, "t", format!("{i}"));
        }
        {
            let guard = errors.lock().unwrap();
            assert_eq!(guard.len(), ERROR_RING_CAP);
            assert_eq!(guard.front().unwrap().message, "10");
            assert_eq!(guard.back().unwrap().message, "59");
        }
        let body = errors_json(&errors);
        assert!(body.contains(r#""kind":"t""#), "body={body}");
        assert!(body.contains(r#""message":"10""#), "body={body}");
        assert!(body.contains(r#""message":"59""#), "body={body}");
        assert!(!body.contains(r#""message":"9""#), "body={body}");
    }

    #[tokio::test]
    async fn dashboard_http_get_endpoints() {
        let mut host = MultiRaftHost::new();
        // Single-node group elects quickly with short timeouts.
        host.insert_single_node(GroupId::ZERO, NodeId(7));
        for _ in 0..20 {
            let _ = host.tick_all();
        }
        assert!(host.is_leader_of(GroupId::ZERO));

        let raft = Arc::new(Mutex::new(host));
        let ranges = Arc::new(RwLock::new(StaticRangeTable::from_ranges(vec![
            StaticRange {
                start_key: vec![],
                end_key: b"m".to_vec(),
                group_id: GroupId::ZERO,
                range_id: 1,
                epoch: 1,
            },
            StaticRange {
                start_key: b"m".to_vec(),
                end_key: vec![],
                group_id: GroupId(1),
                range_id: 2,
                epoch: 1,
            },
        ])));
        let errors = new_error_ring();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let raft_c = raft.clone();
        let ranges_c = ranges.clone();
        let errors_c = errors.clone();
        tokio::spawn(async move {
            let _ = accept_loop(listener, 7, true, raft_c, ranges_c, errors_c).await;
        });

        // Wait for listener.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
            stream.write_all(req.as_bytes()).await.unwrap();
            let mut buf = vec![0u8; 16_384];
            let n = stream.read(&mut buf).await.unwrap();
            let raw = String::from_utf8_lossy(&buf[..n]).to_string();
            let status = raw
                .lines()
                .next()
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);
            let body = raw
                .split("\r\n\r\n")
                .nth(1)
                .unwrap_or("")
                .trim_end_matches('\0')
                .to_owned();
            (status, body)
        }

        let (st, body) = http_get(addr, "/health").await;
        assert_eq!(st, 200);
        assert_eq!(body, r#"{"ok":true}"#);

        let (st, body) = http_get(addr, "/v1/ranges").await;
        assert_eq!(st, 200);
        assert!(body.contains(r#""meta_epoch":1"#), "body={body}");
        assert!(body.contains(r#""group_id":0"#), "body={body}");
        assert!(body.contains(r#""group_id":1"#), "body={body}");
        assert!(body.contains(r#""healthy":true"#), "body={body}");
        assert!(body.contains(r#""healthy":false"#), "body={body}");

        let (st, body) = http_get(addr, "/v1/raft").await;
        assert_eq!(st, 200);
        assert!(body.contains(r#""group_id":0"#), "body={body}");
        assert!(body.contains(r#""term":"#), "body={body}");
        assert!(body.contains(r#""commit":"#), "body={body}");
        assert!(body.contains(r#""is_leader":true"#), "body={body}");

        let (st, body) = http_get(addr, "/v1/cluster").await;
        assert_eq!(st, 200);
        assert!(body.contains(r#""node_id":7"#), "body={body}");
        assert!(body.contains(r#""drain":true"#), "body={body}");
        assert!(body.contains(r#""range_count":2"#), "body={body}");
        assert!(body.contains(r#""leader_group_ids":[0]"#), "body={body}");
        assert!(body.contains(r#""meta_epoch":1"#), "body={body}");

        let (st, body) = http_get(addr, "/v1/leadership").await;
        assert_eq!(st, 200);
        assert!(body.contains(r#""0":{"#), "body={body}");
        assert!(body.contains(r#""leader_id":7"#), "body={body}");
        assert!(body.contains(r#""is_leader":true"#), "body={body}");
        assert!(body.contains(r#""role":"leader""#), "body={body}");

        let (st, body) = http_get(addr, "/v1/errors").await;
        assert_eq!(st, 200);
        assert_eq!(body, r#"{"errors":[]}"#);

        let (st, body) = http_get(addr, "/nope").await;
        assert_eq!(st, 404);
        assert!(body.contains("not_found"), "body={body}");

        let (st, body) = http_get(addr, "/v1/errors").await;
        assert_eq!(st, 200);
        assert!(body.contains(r#""kind":"http_404""#), "body={body}");
        assert!(body.contains(r#""message":"/nope""#), "body={body}");
        assert!(body.contains(r#""ts_unix_ms":"#), "body={body}");
    }
}
