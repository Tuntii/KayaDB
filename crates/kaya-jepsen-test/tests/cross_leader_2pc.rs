//! Cross-group 2PC forwarding to a remote participant leader (#26).
//!
//! Before #26 a cross-range `TXN_COMMIT` could only be served by a node that led
//! *every* participant group; otherwise the commit failed `NOT_LEADER`. The
//! coordinator now ships each 2PC command to that group's own leader over the
//! client RPC (`TXN_FORWARD`, opcode 21) — the wire path exercised here.
//!
//! Note on coverage: this harness cannot pin per-group leadership on distinct
//! nodes. Election timeouts are staggered by node id (`15 + offset`) and
//! `transfer_leadership` is a plain step-down with no `TimeoutNow`, so the
//! lowest-id live node re-wins every group. Leader divergence is therefore only
//! transient (during failover), and the full multi-leader commit is exercised by
//! the `bank-mr` grand matrix under kills rather than by a pinned scenario.

use std::net::SocketAddr;
use std::time::Duration;

use kaya_jepsen_test::ClusterController;
use kaya_net::{
    decode_value_payload, encode_key_payload, encode_txn_forward_payload, roundtrip,
    STATUS_NOT_LEADER, STATUS_OK, TXN_FORWARD_OPCODE,
};
use kaya_raft::RaftCommand;

const GET_OPCODE: u8 = 2;
/// Range `[a,m)` → group 1; `[m,z)` → group 2 (`multi_range_static_ranges`).
const RIGHT_GROUP: u64 = 2;
const RIGHT_KEY: &[u8] = b"mango";

/// One `TXN_FORWARD` round trip: what a coordinator sends to a participant leader.
async fn forward(addr: SocketAddr, group: u64, cmd: &RaftCommand) -> (u16, Vec<u8>) {
    // No operator token configured in this harness: raw body, as the
    // coordinator sends it.
    let payload = encode_txn_forward_payload(group, &cmd.encode());
    roundtrip(addr, TXN_FORWARD_OPCODE, &payload)
        .await
        .expect("TXN_FORWARD round trip")
}

#[tokio::test]
async fn forwarded_2pc_commands_replicate_on_the_participant_leader() {
    let dir = tempfile::tempdir().unwrap();
    let mut cc = ClusterController::spawn_three_node_multi_range(dir.path().to_path_buf())
        .await
        .unwrap();
    cc.wait_for_leader(Duration::from_secs(20))
        .await
        .expect("cluster leader");
    let endpoints = cc.client_endpoints();
    let txn_id = 4242u64;

    // Locate the group-2 leader the way a coordinator does: the node that
    // accepts a forwarded command for that group. Followers must refuse, so the
    // coordinator can retry against the real leader.
    let mut leader = None;
    for _ in 0..60 {
        let mut followers = Vec::new();
        for addr in &endpoints {
            let probe = RaftCommand::TxnAbort2pc { txn_id: 1 };
            match forward(*addr, RIGHT_GROUP, &probe).await.0 {
                STATUS_OK => leader = Some(*addr),
                status => followers.push((*addr, status)),
            }
        }
        if leader.is_some() {
            for (addr, status) in followers {
                assert_eq!(
                    status, STATUS_NOT_LEADER,
                    "follower {addr} must reject a forwarded 2PC command"
                );
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let leader = leader.expect("a node must lead group 2");

    // Full prepare → decide → commit, every phase delivered over TXN_FORWARD.
    let prepare = RaftCommand::TxnPrepare {
        txn_id,
        coordinator_group: 1,
        mutations: vec![(RIGHT_KEY.to_vec(), Some(b"yellow".to_vec()))],
    };
    let (status, body) = forward(leader, RIGHT_GROUP, &prepare).await;
    assert_eq!(
        status,
        STATUS_OK,
        "forwarded TxnPrepare: {}",
        String::from_utf8_lossy(&body)
    );

    // Prepared intents stay invisible to readers.
    let key_payload = encode_key_payload(RIGHT_KEY);
    let (status, _) = roundtrip(leader, GET_OPCODE, &key_payload).await.unwrap();
    assert_ne!(status, STATUS_OK, "prepared intent must not be readable");

    // Durable global decision on the meta group, then the participant commit.
    let (status, body) = forward(
        leader,
        0,
        &RaftCommand::TxnDecision {
            txn_id,
            commit: true,
        },
    )
    .await;
    assert_eq!(
        status,
        STATUS_OK,
        "forwarded TxnDecision: {}",
        String::from_utf8_lossy(&body)
    );

    let (status, body) = forward(leader, RIGHT_GROUP, &RaftCommand::TxnCommit2pc { txn_id }).await;
    assert_eq!(
        status,
        STATUS_OK,
        "forwarded TxnCommit2pc: {}",
        String::from_utf8_lossy(&body)
    );

    let (status, body) = roundtrip(leader, GET_OPCODE, &key_payload).await.unwrap();
    assert_eq!(status, STATUS_OK, "committed key must be readable");
    assert_eq!(decode_value_payload(&body).unwrap(), b"yellow");

    cc.shutdown_all().await;
}
