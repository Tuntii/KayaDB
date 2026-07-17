//! Read-only HTTP JSON dashboard (M22 v1).
//!
//! Optional listener (`--dashboard-addr`). Endpoints:
//! - `GET /health` → `{"ok":true}`
//! - `GET /v1/ranges` → meta_epoch + range descriptors
//! - `GET /v1/raft` → per-group leader / term / commit

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use kaya_raft::{MultiRaftHost, StaticRangeTable};

/// Shared raft host (same shape as cluster internals).
pub type DashboardRaft = Arc<Mutex<MultiRaftHost>>;
/// Shared range table (live snapshot for `/v1/ranges`).
pub type DashboardRanges = Arc<RwLock<StaticRangeTable>>;

/// Bind `addr` and serve dashboard GETs until the task is cancelled.
pub async fn serve(
    addr: SocketAddr,
    node_id: u64,
    raft: DashboardRaft,
    ranges: DashboardRanges,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    eprintln!("[node {node_id}] dashboard listening on {bound}");
    accept_loop(listener, raft, ranges).await
}

async fn accept_loop(
    listener: TcpListener,
    raft: DashboardRaft,
    ranges: DashboardRanges,
) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let raft = raft.clone();
        let ranges = ranges.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, raft, ranges).await;
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    raft: DashboardRaft,
    ranges: DashboardRanges,
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
        "/v1/ranges" | "/v1/ranges/" => {
            let body = {
                let guard = ranges.read().await;
                ranges_json(&guard)
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
        _ => (
            "HTTP/1.1 404 Not Found",
            "application/json",
            r#"{"error":"not_found"}"#.to_owned(),
        ),
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

/// `GET /v1/ranges` body from a live range-table snapshot.
pub fn ranges_json(table: &StaticRangeTable) -> String {
    let mut out = format!(r#"{{"meta_epoch":{},"ranges":["#, table.meta_epoch());
    for (i, r) in table.ranges().iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            r#"{{"range_id":{},"epoch":{},"group_id":{},"start":{},"end":{}}}"#,
            r.range_id,
            r.epoch,
            r.group_id.0,
            json_bytes(&r.start_key),
            json_bytes(&r.end_key),
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
        let status = host.status_of(gid);
        let (leader_id, term, commit, role, is_leader) = match status {
            Some(s) => (
                s.leader_id.map(|id| id.0),
                s.current_term.0,
                s.commit_index.0,
                format!("{:?}", s.role).to_lowercase(),
                matches!(s.role, kaya_raft::Role::Leader),
            ),
            None => (None, 0, 0, "unknown".to_owned(), false),
        };
        let leader_json = match leader_id {
            Some(id) => id.to_string(),
            None => "null".to_owned(),
        };
        out.push_str(&format!(
            r#"{{"group_id":{},"leader_id":{},"term":{},"commit":{},"role":"{}","is_leader":{}}}"#,
            gid.0,
            leader_json,
            term,
            commit,
            role,
            if is_leader { "true" } else { "false" },
        ));
    }
    out.push_str("]}");
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
        let body = ranges_json(&table);
        assert!(body.contains(r#""meta_epoch":1"#), "body={body}");
        assert!(body.contains(r#""group_id":0"#), "body={body}");
        assert!(body.contains(r#""range_id":1"#), "body={body}");
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
        let body = ranges_json(&table);
        assert!(body.contains(r#""meta_epoch":2"#), "body={body}");
        // two ranges
        assert_eq!(body.matches(r#""range_id":"#).count(), 2);
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

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let raft_c = raft.clone();
        let ranges_c = ranges.clone();
        tokio::spawn(async move {
            let _ = accept_loop(listener, raft_c, ranges_c).await;
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

        let (st, body) = http_get(addr, "/v1/raft").await;
        assert_eq!(st, 200);
        assert!(body.contains(r#""group_id":0"#), "body={body}");
        assert!(body.contains(r#""term":"#), "body={body}");
        assert!(body.contains(r#""commit":"#), "body={body}");
        assert!(body.contains(r#""is_leader":true"#), "body={body}");

        let (st, body) = http_get(addr, "/nope").await;
        assert_eq!(st, 404);
        assert!(body.contains("not_found"), "body={body}");
    }
}
