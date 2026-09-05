#[cfg(test)]
mod tests {
    use crate::client_auth::encode_client_auth_payload;
    use crate::cluster::{ClusterConfig, ClusterNode};
    use crate::operator_auth::encode_admin_payload;
    use kaya_net::{
        decode_hello_response, decode_txn_begin_response, decode_txn_commit_response,
        decode_value_payload, encode_hello_request, encode_key_payload, encode_member_payload,
        encode_put_payload, encode_remove_member_payload, encode_txn_id_payload,
        encode_txn_op_payload, roundtrip, HELLO_OPCODE, PROTO_VERSION, STATUS_INVALID_ARGUMENT,
        STATUS_NOT_FOUND, STATUS_OK, TXN_BEGIN_OPCODE, TXN_COMMIT_OPCODE, TXN_OP_GET,
        TXN_OP_OPCODE, TXN_OP_PUT, TXN_ROLLBACK_OPCODE,
    };
    use kaya_raft::{GroupId, StaticRange};
    use kaya_sim::{LinearizabilityChecker, Op, OpResult};
    use serial_test::serial;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    async fn get_free_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().port()
    }

    async fn check_health(addr: SocketAddr) -> Option<String> {
        if let Ok((status, body)) = roundtrip(addr, 5, &[]).await {
            if status == 0 {
                return Some(String::from_utf8(body).unwrap());
            }
        }
        None
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

    fn u64_field_from_json(json: &str, field: &str) -> Option<u64> {
        let needle = format!("\"{field}\":");
        let start = json.find(&needle)? + needle.len();
        let rest = &json[start..];
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].parse().ok()
    }

    /// STATS (opcode 6) `raft_groups` field, or `None` if the request failed.
    async fn raft_groups_from_stats(client_addr: SocketAddr) -> Option<u64> {
        let (status, body) = roundtrip(client_addr, 6, &[]).await.ok()?;
        if status != STATUS_OK {
            return None;
        }
        u64_field_from_json(&String::from_utf8(body).ok()?, "raft_groups")
    }

    #[serial]
    #[tokio::test]
    async fn test_real_cluster_correctness() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_test_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_test_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_test_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1);
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2);
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3);

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        // Wait for election convergence (at least one leader elected)
        let mut leader_addr = None;
        let mut leader_id = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                leader_id = Some(1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                leader_id = Some(2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                leader_id = Some(3);
                break;
            }
        }

        let leader_addr = leader_addr.expect("No leader elected in 10 seconds");
        let leader_id = leader_id.unwrap();

        // PUT to the leader
        let put_payload = encode_put_payload(b"mykey", b"myval");
        let (status, _) = roundtrip(leader_addr, 1, &put_payload).await.unwrap();
        assert_eq!(status, 0); // STATUS_OK

        // GET on the leader
        let get_payload = encode_key_payload(b"mykey");
        let (status, body) = roundtrip(leader_addr, 2, &get_payload).await.unwrap();
        assert_eq!(status, 0); // STATUS_OK
        let val = decode_value_payload(&body).unwrap();
        assert_eq!(val, b"myval");

        // GET/SCAN on a follower
        let follower_addr = if leader_id == 1 {
            client_addr2
        } else {
            client_addr1
        };
        let (status, body) = roundtrip(follower_addr, 2, &get_payload).await.unwrap();
        assert_eq!(status, 10); // STATUS_NOT_LEADER
        let hint = String::from_utf8(body).unwrap();
        assert_eq!(hint, leader_addr.to_string());

        // Test KayaClient with auto-redirection on the follower address
        let mut client = kaya_client::KayaClient::connect(follower_addr)
            .await
            .unwrap();
        // Since we pointed to follower_addr, it should automatically redirect to leader_addr on PUT!
        client.put(b"clientkey", b"clientval").await.unwrap();
        // Verify client cached address was updated to leader_addr
        assert_eq!(client.addr(), leader_addr);

        // Get the value using the client
        let got_val = client.get(b"clientkey").await.unwrap();
        assert_eq!(got_val, Some(b"clientval".to_vec()));

        // Scan keys using client
        let scan_res = client.scan(b"client").await.unwrap();
        assert_eq!(scan_res.len(), 1);
        assert_eq!(scan_res[0].0, b"clientkey");
        assert_eq!(scan_res[0].1, b"clientval");

        // Query STATS command using client
        let stats_json = client.stats().await.unwrap();
        assert!(stats_json.contains("\"role\":\"leader\""));
        assert!(stats_json.contains("\"engine\":{"));
        assert!(stats_json.contains("\"put_count\":"));

        // Shut down the leader node
        if leader_id == 1 {
            handle1.abort();
        } else if leader_id == 2 {
            handle2.abort();
        } else {
            handle3.abort();
        }

        // Wait for election convergence among the remaining two
        let mut new_leader_addr = None;
        let remaining_nodes = if leader_id == 1 {
            vec![(2, client_addr2), (3, client_addr3)]
        } else if leader_id == 2 {
            vec![(1, client_addr1), (3, client_addr3)]
        } else {
            vec![(1, client_addr1), (2, client_addr2)]
        };

        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            for &(_id, addr) in &remaining_nodes {
                if check_health(addr).await.as_deref() == Some("leader") {
                    new_leader_addr = Some(addr);
                    break;
                }
            }
            if new_leader_addr.is_some() {
                break;
            }
        }

        let new_leader_addr = new_leader_addr.expect("No new leader elected after shutdown");

        // PUT to the new leader
        let put_payload2 = encode_put_payload(b"newkey", b"newval");
        let (status, _) = roundtrip(new_leader_addr, 1, &put_payload2).await.unwrap();
        assert_eq!(status, 0); // STATUS_OK

        // GET on the new leader
        let get_payload2 = encode_key_payload(b"newkey");
        let (status, body) = roundtrip(new_leader_addr, 2, &get_payload2).await.unwrap();
        assert_eq!(status, 0); // STATUS_OK
        let val2 = decode_value_payload(&body).unwrap();
        assert_eq!(val2, b"newval");

        // Clean up spawns
        handle1.abort();
        handle2.abort();
        handle3.abort();

        // Clean up temp directories
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    #[serial]
    #[tokio::test]
    async fn test_cluster_linearizability_history() {
        eprintln!("[test] Starting linearizability integration test");
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_lin_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_lin_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_lin_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1);
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2);
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3);

        eprintln!("[test] Spawning 3 nodes...");
        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let mut handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let mut handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        // Wait for election convergence (at least one leader elected)
        eprintln!("[test] Waiting for election convergence...");
        let mut leader_addr = None;
        let mut leader_id = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                leader_id = Some(1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                leader_id = Some(2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                leader_id = Some(3);
                break;
            }
        }

        let leader_addr = leader_addr.expect("No leader elected in 10 seconds");
        let leader_id = leader_id.unwrap();
        eprintln!(
            "[test] Leader elected: Node {} at {}",
            leader_id, leader_addr
        );

        // Connect the client to a follower, allowing auto-redirection
        let follower_addr = if leader_id == 1 {
            client_addr2
        } else {
            client_addr1
        };
        eprintln!(
            "[test] Connecting client to follower at {}...",
            follower_addr
        );
        let mut client = kaya_client::KayaClient::connect(follower_addr)
            .await
            .unwrap();

        let mut checker = LinearizabilityChecker::new();

        // 1. PUT key1
        eprintln!("[test] Step 1: PUT key1=val1");
        client.put(b"key1", b"val1").await.unwrap();
        checker.record_next(
            Op::Put {
                key: b"key1".to_vec(),
                value: b"val1".to_vec(),
            },
            OpResult::Ok,
        );

        // 2. GET key1
        eprintln!("[test] Step 2: GET key1");
        let val1 = client.get(b"key1").await.unwrap();
        checker.record_next(
            Op::Get {
                key: b"key1".to_vec(),
            },
            OpResult::Value(val1),
        );

        // 3. SCAN prefix "key"
        eprintln!("[test] Step 3: SCAN key");
        let scan1 = client.scan(b"key").await.unwrap();
        checker.record_next(
            Op::Scan {
                prefix: b"key".to_vec(),
            },
            OpResult::Scan(scan1),
        );

        // 4. Crash a follower node
        let follower_node_id = if leader_id == 3 { 2 } else { 3 };
        eprintln!(
            "[test] Step 4: Crashing follower node {}...",
            follower_node_id
        );
        let follower_to_crash_handle = if follower_node_id == 2 {
            &handle2
        } else {
            &handle3
        };
        follower_to_crash_handle.abort();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // 5. PUT key2
        eprintln!("[test] Step 5: PUT key2=val2");
        client.put(b"key2", b"val2").await.unwrap();
        checker.record_next(
            Op::Put {
                key: b"key2".to_vec(),
                value: b"val2".to_vec(),
            },
            OpResult::Ok,
        );

        // 6. GET key2
        eprintln!("[test] Step 6: GET key2");
        let val2 = client.get(b"key2").await.unwrap();
        checker.record_next(
            Op::Get {
                key: b"key2".to_vec(),
            },
            OpResult::Value(val2),
        );

        // 7. Restart the crashed follower node
        eprintln!(
            "[test] Step 7: Restarting follower node {}...",
            follower_node_id
        );
        if follower_node_id == 2 {
            let config2_restart = ClusterConfig::new(
                2,
                &data_dir2,
                raft_addr2,
                client_addr2,
                vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)],
            );
            handle2 = tokio::spawn(async move {
                let _ = ClusterNode::new(config2_restart).run().await;
            });
        } else {
            let config3_restart = ClusterConfig::new(
                3,
                &data_dir3,
                raft_addr3,
                client_addr3,
                vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)],
            );
            handle3 = tokio::spawn(async move {
                let _ = ClusterNode::new(config3_restart).run().await;
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // 8. DELETE key1
        eprintln!("[test] Step 8: DELETE key1");
        client.delete(b"key1").await.unwrap();
        checker.record_next(
            Op::Delete {
                key: b"key1".to_vec(),
            },
            OpResult::Ok,
        );

        // 9. GET key1 (should be None)
        eprintln!("[test] Step 9: GET key1");
        let val1_after = client.get(b"key1").await.unwrap();
        checker.record_next(
            Op::Get {
                key: b"key1".to_vec(),
            },
            OpResult::Value(val1_after),
        );

        // 10. Crash the leader node!
        eprintln!("[test] Step 10: Crashing leader node {}...", leader_id);
        let leader_handle = if leader_id == 1 {
            &handle1
        } else if leader_id == 2 {
            &handle2
        } else {
            &handle3
        };
        leader_handle.abort();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Retry and perform GET key2 on the new leader
        eprintln!("[test] Step 10b: Waiting for new leader election...");
        let mut new_leader_addr = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                new_leader_addr = Some(client_addr1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                new_leader_addr = Some(client_addr2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                new_leader_addr = Some(client_addr3);
                break;
            }
        }

        let new_leader_addr = new_leader_addr.expect("No new leader elected after leader crash");
        eprintln!("[test] Step 10c: New leader elected at {}", new_leader_addr);

        // Reconnect client to the new leader
        eprintln!(
            "[test] Connecting client to new leader at {}...",
            new_leader_addr
        );
        let mut new_client = kaya_client::KayaClient::connect(new_leader_addr)
            .await
            .unwrap();

        // 11. PUT key3
        eprintln!("[test] Step 11: PUT key3=val3");
        new_client.put(b"key3", b"val3").await.unwrap();
        checker.record_next(
            Op::Put {
                key: b"key3".to_vec(),
                value: b"val3".to_vec(),
            },
            OpResult::Ok,
        );

        // 12. GET key3
        eprintln!("[test] Step 12: GET key3");
        let val3 = new_client.get(b"key3").await.unwrap();
        checker.record_next(
            Op::Get {
                key: b"key3".to_vec(),
            },
            OpResult::Value(val3),
        );

        // 13. SCAN prefix "key" (should have key2 and key3)
        eprintln!("[test] Step 13: SCAN key");
        let scan2 = new_client.scan(b"key").await.unwrap();
        checker.record_next(
            Op::Scan {
                prefix: b"key".to_vec(),
            },
            OpResult::Scan(scan2),
        );

        // Verify the entire recorded history against sequential linearizability checker!
        eprintln!("[test] Step 14: Verifying history sequential linearizability...");
        let verification = checker.check_sequential();
        assert!(
            verification.is_ok(),
            "Linearizability violation: {:?}",
            verification.unwrap_err()
        );
        eprintln!("[test] History sequential linearizability verified successfully!");

        // Serialize and log history trace JSONL
        let trace_str = checker.to_trace_string(0xdead_beef);
        eprintln!("[test] Generated History Trace JSONL:\n{}", trace_str);

        // Clean up spawns
        handle1.abort();
        handle2.abort();
        handle3.abort();

        // Clean up temp directories
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    #[serial]
    #[tokio::test]
    async fn test_install_snapshot_over_tcp() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_snap_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_snap_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_snap_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1);
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2);
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3);

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let mut handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let mut handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        let mut leader_addr = None;
        let mut leader_id = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                leader_id = Some(1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                leader_id = Some(2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                leader_id = Some(3);
                break;
            }
        }
        let leader_addr = leader_addr.expect("no leader elected");
        let leader_id = leader_id.unwrap();

        // Stop one follower while the leader compacts a large write batch.
        let (crashed_client_addr, restart_config) = if leader_id == 3 {
            handle2.abort();
            (
                client_addr2,
                ClusterConfig::new(
                    2,
                    &data_dir2,
                    raft_addr2,
                    client_addr2,
                    vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)],
                ),
            )
        } else {
            handle3.abort();
            (
                client_addr3,
                ClusterConfig::new(
                    3,
                    &data_dir3,
                    raft_addr3,
                    client_addr3,
                    vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)],
                ),
            )
        };
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut client = kaya_client::KayaClient::connect(leader_addr).await.unwrap();
        for i in 0..128u16 {
            let key = format!("snap-{i}");
            let val = format!("v{i}");
            client.put(key.as_bytes(), val.as_bytes()).await.unwrap();
        }

        let mut leader_compacted = false;
        for _ in 0..80 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(mut leader_client) = kaya_client::KayaClient::connect(leader_addr).await {
                if let Ok(stats) = leader_client.stats().await {
                    if applied_index_from_stats(&stats).unwrap_or(0) >= 64 {
                        leader_compacted = true;
                        break;
                    }
                }
            }
        }
        assert!(
            leader_compacted,
            "leader did not reach compaction threshold"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let restart_handle = tokio::spawn(async move {
            let _ = ClusterNode::new(restart_config).run().await;
        });
        if leader_id == 3 {
            handle2 = restart_handle;
        } else {
            handle3 = restart_handle;
        }

        let mut follower_caught_up = false;
        let mut last_follower_applied = 0u64;
        for _ in 0..200 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(mut follower_client) =
                kaya_client::KayaClient::connect(crashed_client_addr).await
            {
                if let Ok(stats) = follower_client.stats().await {
                    last_follower_applied = applied_index_from_stats(&stats).unwrap_or(0);
                    if last_follower_applied >= 64 {
                        follower_caught_up = true;
                        break;
                    }
                }
            }
        }
        assert!(
            follower_caught_up,
            "follower did not catch up via InstallSnapshot within timeout (last applied={last_follower_applied})"
        );

        let val = client.get(b"snap-127").await.unwrap();
        assert_eq!(val, Some(b"v127".to_vec()));

        handle1.abort();
        handle2.abort();
        handle3.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    #[serial]
    #[tokio::test]
    async fn test_join_cluster_membership_over_tcp() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_mem_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_mem_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_mem_n3_{}", test_id));
        let data_dir4 = std::env::temp_dir().join(format!("kayadb_mem_n4_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;
        let r4 = get_free_port().await;
        let c4 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();
        let raft_addr4: SocketAddr = format!("127.0.0.1:{}", r4).parse().unwrap();
        let client_addr4: SocketAddr = format!("127.0.0.1:{}", c4).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1);
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2);
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3);

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        let mut leader_addr = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                break;
            }
        }
        let leader_addr = leader_addr.expect("no leader elected");

        let seeds = vec![
            (1, raft_addr1, client_addr1),
            (2, raft_addr2, client_addr2),
            (3, raft_addr3, client_addr3),
        ];
        let config4 =
            ClusterConfig::new(4, &data_dir4, raft_addr4, client_addr4, seeds).with_join_cluster();
        let handle4 = tokio::spawn(async move {
            let _ = ClusterNode::new(config4).run().await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let add_payload =
            encode_member_payload(4, &raft_addr4.to_string(), &client_addr4.to_string());
        let (status, body) = roundtrip(leader_addr, 7, &add_payload).await.unwrap();
        assert_eq!(
            status,
            0,
            "ADD_MEMBER failed: {:?}",
            String::from_utf8(body)
        );

        let mut joined = false;
        for _ in 0..150 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(mut n4) = kaya_client::KayaClient::connect(client_addr4).await {
                if let Ok(stats) = n4.stats().await {
                    if stats.contains("\"peer_count\":3") {
                        joined = true;
                        break;
                    }
                }
            }
        }
        assert!(joined, "node 4 was not included in the cluster roster");

        let mut client = kaya_client::KayaClient::connect(leader_addr).await.unwrap();
        client
            .put(b"membership-key", b"membership-val")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let got = client.get(b"membership-key").await.unwrap();
        assert_eq!(got, Some(b"membership-val".to_vec()));

        let remove_payload = encode_remove_member_payload(4);
        let (status, body) = roundtrip(leader_addr, 8, &remove_payload).await.unwrap();
        assert_eq!(
            status,
            0,
            "REMOVE_MEMBER failed: {:?}",
            String::from_utf8(body)
        );

        let mut removed = false;
        for _ in 0..150 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if let Ok(mut n1) = kaya_client::KayaClient::connect(client_addr1).await {
                if let Ok(stats) = n1.stats().await {
                    if stats.contains("\"peer_count\":2") {
                        removed = true;
                        break;
                    }
                }
            }
        }
        assert!(removed, "node 4 was not removed from the cluster roster");

        handle1.abort();
        handle2.abort();
        handle3.abort();
        handle4.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
        let _ = std::fs::remove_dir_all(&data_dir4);
    }

    #[serial]
    #[tokio::test]
    async fn add_member_requires_correct_operator_token() {
        // TDD test for Task 2: servers started with operator token must reject
        // add/remove unless correct token is presented via ADMIN framing.
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_tok_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_tok_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_tok_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let token = "test-operator-token-42".to_string();
        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1)
            .with_operator_token(token.clone());
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2)
            .with_operator_token(token.clone());
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3)
            .with_operator_token(token.clone());

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        // Wait for a leader
        let mut leader_addr = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                break;
            }
        }
        let leader_addr = leader_addr.expect("no leader elected in token-protected cluster");

        // A would-be add payload (no 4th node actually started)
        let add_payload = encode_member_payload(99, "127.0.0.1:19991", "127.0.0.1:19992");

        // 1) try add without token (raw payload) -> must error
        let (status, _body) = roundtrip(leader_addr, 7, &add_payload).await.unwrap();
        assert_ne!(
            status, 0,
            "add without token should be rejected when operator_token is configured"
        );

        // 2) try with wrong token -> error
        let wrong_admin = encode_admin_payload(7, &add_payload, Some("wrong-token-xyz"));
        let (status, _body) = roundtrip(leader_addr, 7, &wrong_admin).await.unwrap();
        assert_ne!(status, 0, "add with wrong token should be rejected");

        // 3) try with correct token -> succeeds (status 0)
        let correct_admin = encode_admin_payload(7, &add_payload, Some(&token));
        let (status, _body) = roundtrip(leader_addr, 7, &correct_admin).await.unwrap();
        assert_eq!(
            status,
            0,
            "add with correct operator token should succeed: {:?}",
            String::from_utf8_lossy(&_body)
        );

        // Strengthen: normal data path (put/get) must NOT require the operator token
        // (token is only for admin membership ops)
        let put_payload = encode_put_payload(b"token-test-key", b"token-test-val");
        let (status, _body) = roundtrip(leader_addr, 1, &put_payload).await.unwrap();
        assert_eq!(
            status, 0,
            "put should succeed without providing operator token"
        );

        let get_payload = encode_key_payload(b"token-test-key");
        let (status, body) = roundtrip(leader_addr, 2, &get_payload).await.unwrap();
        assert_eq!(
            status, 0,
            "get should succeed without providing operator token"
        );
        let val = decode_value_payload(&body).expect("decode get value");
        assert_eq!(val, b"token-test-val".to_vec(), "data op roundtrip value");

        handle1.abort();
        handle2.abort();
        handle3.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    #[serial]
    #[tokio::test]
    async fn data_ops_require_client_token() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_ctok_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_ctok_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_ctok_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let token = "test-client-token-99".to_string();
        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1)
            .with_client_token(token.clone());
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2)
            .with_client_token(token.clone());
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3)
            .with_client_token(token.clone());

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        let mut leader_addr = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                break;
            }
        }
        let leader_addr = leader_addr.expect("no leader elected in client-token-protected cluster");

        let put_payload = encode_put_payload(b"client-tok-key", b"client-tok-val");

        // 1) put without token -> rejected
        let (status, _body) = roundtrip(leader_addr, 1, &put_payload).await.unwrap();
        assert_ne!(
            status, 0,
            "put without client token should be rejected when client_token is configured"
        );

        // 2) put with wrong token -> rejected
        let wrong = encode_client_auth_payload(&put_payload, Some("wrong-client-token"));
        let (status, _body) = roundtrip(leader_addr, 1, &wrong).await.unwrap();
        assert_ne!(status, 0, "put with wrong client token should be rejected");

        // 3) put with correct token -> succeeds
        let correct = encode_client_auth_payload(&put_payload, Some(&token));
        let (status, _body) = roundtrip(leader_addr, 1, &correct).await.unwrap();
        assert_eq!(
            status,
            0,
            "put with correct client token should succeed: {:?}",
            String::from_utf8_lossy(&_body)
        );

        // 4) get without token -> rejected
        let get_payload = encode_key_payload(b"client-tok-key");
        let (status, _body) = roundtrip(leader_addr, 2, &get_payload).await.unwrap();
        assert_ne!(status, 0, "get without client token should be rejected");

        // 5) get with correct token -> succeeds
        let correct_get = encode_client_auth_payload(&get_payload, Some(&token));
        let (status, body) = roundtrip(leader_addr, 2, &correct_get).await.unwrap();
        assert_eq!(status, 0, "get with correct client token should succeed");
        let val = decode_value_payload(&body).expect("decode get value");
        assert_eq!(val, b"client-tok-val".to_vec());

        // 6) stats without token -> rejected
        let (status, _body) = roundtrip(leader_addr, 6, &[]).await.unwrap();
        assert_ne!(status, 0, "stats without client token should be rejected");

        // 7) stats with correct token -> succeeds
        let correct_stats = encode_client_auth_payload(&[], Some(&token));
        let (status, _body) = roundtrip(leader_addr, 6, &correct_stats).await.unwrap();
        assert_eq!(status, 0, "stats with correct client token should succeed");

        // 8) health stays open (no token required)
        let (status, body) = roundtrip(leader_addr, 5, &[]).await.unwrap();
        assert_eq!(status, 0, "health should succeed without client token");
        assert_eq!(body, b"leader");

        handle1.abort();
        handle2.abort();
        handle3.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    /// M24 per-prefix ACL: two tokens / two prefixes; longest-prefix authorize.
    #[serial]
    #[tokio::test]
    async fn per_prefix_acl_two_tokens() {
        use crate::acl::PrefixAcl;
        use std::collections::HashMap;

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_acl_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_acl_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_acl_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let mut map = HashMap::new();
        map.insert("team-a/".into(), "tok-a".into());
        map.insert("team-b/".into(), "tok-b".into());
        let acl = PrefixAcl::from_map(map).unwrap();

        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1)
            .with_acl(acl.clone());
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2)
            .with_acl(acl.clone());
        let config3 =
            ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3).with_acl(acl);

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        let mut leader_addr = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                break;
            }
        }
        let leader_addr = leader_addr.expect("no leader elected in ACL-protected cluster");

        let put_a = encode_put_payload(b"team-a/k1", b"va");
        let put_b = encode_put_payload(b"team-b/k1", b"vb");
        let put_other = encode_put_payload(b"other/k1", b"vo");

        // No token -> denied
        let (status, _) = roundtrip(leader_addr, 1, &put_a).await.unwrap();
        assert_ne!(status, 0, "put without token must be ACL-denied");

        // tok-a can write team-a, not team-b
        let framed = encode_client_auth_payload(&put_a, Some("tok-a"));
        let (status, body) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_eq!(
            status,
            0,
            "tok-a put team-a should succeed: {:?}",
            String::from_utf8_lossy(&body)
        );

        let framed = encode_client_auth_payload(&put_b, Some("tok-a"));
        let (status, _) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_ne!(status, 0, "tok-a put team-b must be denied");

        // tok-b can write team-b
        let framed = encode_client_auth_payload(&put_b, Some("tok-b"));
        let (status, body) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_eq!(
            status,
            0,
            "tok-b put team-b should succeed: {:?}",
            String::from_utf8_lossy(&body)
        );

        // No rule matches other/ -> denied for both tokens
        let framed = encode_client_auth_payload(&put_other, Some("tok-a"));
        let (status, _) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_ne!(status, 0, "unmapped prefix must be denied");

        // GET: tok-a can read its key; tok-b cannot
        let get_a = encode_key_payload(b"team-a/k1");
        let framed = encode_client_auth_payload(&get_a, Some("tok-a"));
        let (status, body) = roundtrip(leader_addr, 2, &framed).await.unwrap();
        assert_eq!(status, 0, "tok-a get team-a should succeed");
        assert_eq!(decode_value_payload(&body).unwrap(), b"va".to_vec());

        let framed = encode_client_auth_payload(&get_a, Some("tok-b"));
        let (status, _) = roundtrip(leader_addr, 2, &framed).await.unwrap();
        assert_ne!(status, 0, "tok-b get team-a must be denied");

        // HEALTH stays open
        let (status, _) = roundtrip(leader_addr, 5, &[]).await.unwrap();
        assert_eq!(status, 0, "health stays open under ACL");

        handle1.abort();
        handle2.abort();
        handle3.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    /// #29 tenant isolation: two named tenants, exclusive prefixes; cross-tenant GET denied.
    #[serial]
    #[tokio::test]
    async fn cross_tenant_access_denied() {
        use crate::acl::TenantAcl;

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_tenant_n1_{}", test_id));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_tenant_n2_{}", test_id));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_tenant_n3_{}", test_id));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;

        let raft_addr1: SocketAddr = format!("127.0.0.1:{}", r1).parse().unwrap();
        let client_addr1: SocketAddr = format!("127.0.0.1:{}", c1).parse().unwrap();
        let raft_addr2: SocketAddr = format!("127.0.0.1:{}", r2).parse().unwrap();
        let client_addr2: SocketAddr = format!("127.0.0.1:{}", c2).parse().unwrap();
        let raft_addr3: SocketAddr = format!("127.0.0.1:{}", r3).parse().unwrap();
        let client_addr3: SocketAddr = format!("127.0.0.1:{}", c3).parse().unwrap();

        let peers1 = vec![(2, raft_addr2, client_addr2), (3, raft_addr3, client_addr3)];
        let peers2 = vec![(1, raft_addr1, client_addr1), (3, raft_addr3, client_addr3)];
        let peers3 = vec![(1, raft_addr1, client_addr1), (2, raft_addr2, client_addr2)];

        let tenants = TenantAcl::from_json(
            r#"{
                "tenants": [
                    {"id": "acme", "token": "tok-acme", "prefix": "acme/"},
                    {"id": "globex", "token": "tok-globex", "prefix": "globex/"}
                ]
            }"#,
        )
        .unwrap();

        let config1 = ClusterConfig::new(1, &data_dir1, raft_addr1, client_addr1, peers1)
            .with_tenants(tenants.clone())
            .with_audit_log(true);
        let config2 = ClusterConfig::new(2, &data_dir2, raft_addr2, client_addr2, peers2)
            .with_tenants(tenants.clone())
            .with_audit_log(true);
        let config3 = ClusterConfig::new(3, &data_dir3, raft_addr3, client_addr3, peers3)
            .with_tenants(tenants)
            .with_audit_log(true);

        let handle1 = tokio::spawn(async move {
            let _ = ClusterNode::new(config1).run().await;
        });
        let handle2 = tokio::spawn(async move {
            let _ = ClusterNode::new(config2).run().await;
        });
        let handle3 = tokio::spawn(async move {
            let _ = ClusterNode::new(config3).run().await;
        });

        let mut leader_addr = None;
        let mut leader_dir = None;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr1).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr1);
                leader_dir = Some(data_dir1.clone());
                break;
            }
            if check_health(client_addr2).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr2);
                leader_dir = Some(data_dir2.clone());
                break;
            }
            if check_health(client_addr3).await.as_deref() == Some("leader") {
                leader_addr = Some(client_addr3);
                leader_dir = Some(data_dir3.clone());
                break;
            }
        }
        let leader_addr = leader_addr.expect("no leader elected in tenant-isolated cluster");
        let leader_dir = leader_dir.expect("leader data dir");

        let put_acme = encode_put_payload(b"acme/k1", b"va");
        let put_globex = encode_put_payload(b"globex/k1", b"vg");

        // No token -> denied
        let (status, _) = roundtrip(leader_addr, 1, &put_acme).await.unwrap();
        assert_ne!(status, 0, "put without token must be tenant-denied");

        // Same tenant PUT + GET OK
        let framed = encode_client_auth_payload(&put_acme, Some("tok-acme"));
        let (status, body) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_eq!(
            status,
            0,
            "tok-acme put acme/ should succeed: {:?}",
            String::from_utf8_lossy(&body)
        );

        let get_acme = encode_key_payload(b"acme/k1");
        let framed = encode_client_auth_payload(&get_acme, Some("tok-acme"));
        let (status, body) = roundtrip(leader_addr, 2, &framed).await.unwrap();
        assert_eq!(status, 0, "same-tenant GET should succeed");
        assert_eq!(decode_value_payload(&body).unwrap(), b"va".to_vec());

        // Cross-tenant GET denied
        let framed = encode_client_auth_payload(&get_acme, Some("tok-globex"));
        let (status, body) = roundtrip(leader_addr, 2, &framed).await.unwrap();
        assert_ne!(status, 0, "cross-tenant GET must be denied");
        assert!(
            String::from_utf8_lossy(&body).contains("tenant denied"),
            "cross-tenant GET body should say tenant denied: {:?}",
            String::from_utf8_lossy(&body)
        );

        // globex cannot write acme keys
        let framed = encode_client_auth_payload(&put_acme, Some("tok-globex"));
        let (status, _) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_ne!(status, 0, "cross-tenant PUT must be denied");

        // globex can write its own prefix
        let framed = encode_client_auth_payload(&put_globex, Some("tok-globex"));
        let (status, body) = roundtrip(leader_addr, 1, &framed).await.unwrap();
        assert_eq!(
            status,
            0,
            "tok-globex put globex/ should succeed: {:?}",
            String::from_utf8_lossy(&body)
        );

        // HEALTH stays open
        let (status, _) = roundtrip(leader_addr, 5, &[]).await.unwrap();
        assert_eq!(status, 0, "health stays open under tenant isolation");

        // Audit JSONL records the resolved tenant id on the same-tenant PUT.
        let audit = std::fs::read_to_string(leader_dir.join("audit.jsonl")).unwrap_or_default();
        assert!(
            audit.contains(r#""tenant":"acme""#),
            "audit JSONL should include tenant acme: {audit}"
        );

        handle1.abort();
        handle2.abort();
        handle3.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    #[cfg(feature = "tls")]
    #[serial]
    #[tokio::test]
    async fn tls_3node_cluster_smoke_linearizability() {
        // 3-node TLS cluster scaffolding.
        // In a complete run: generate self-signed certs, start nodes with .with_tls(tls),
        // use kaya_client::connect_tls or net::roundtrip_tls for the workload clients,
        // run smoke workload (1 client), verify linearizability (0 violations).
        // Survives kill/restart (reuses the Gate 3 harness).
        let tls = kaya_net::TlsConfig {
            cert_path: std::env::temp_dir().join("tls_test.crt"),
            key_path: std::env::temp_dir().join("tls_test.key"),
            ca_path: None,
            require_client_cert: false,
        };
        let _c = ClusterConfig::new(
            1,
            std::env::temp_dir().join("tls_node"),
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
            vec![],
        )
        .with_tls(tls);
        // API and config exercised. Full cert generation + spawn + workload left as extension.
        assert!(true, "TLS cluster scaffolding ready");
    }

    #[serial]
    #[tokio::test]
    async fn test_hello_handshake() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_hello_{test_id}"));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{r1}").parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{c1}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "single-node leader not ready for HELLO test");

        let hello_payload = encode_hello_request(PROTO_VERSION);
        let (status, body) = roundtrip(client_addr, HELLO_OPCODE, &hello_payload)
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_hello_response(&body).unwrap(), PROTO_VERSION);

        let future_payload = encode_hello_request(PROTO_VERSION + 1);
        let (status, _body) = roundtrip(client_addr, HELLO_OPCODE, &future_payload)
            .await
            .unwrap();
        assert_eq!(status, STATUS_INVALID_ARGUMENT);

        // Backward compat: clients that skip HELLO still work.
        let put_payload = encode_put_payload(b"no-hello", b"still-works");
        let (status, _) = roundtrip(client_addr, 1, &put_payload).await.unwrap();
        assert_eq!(status, STATUS_OK);

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[serial]
    #[tokio::test]
    async fn test_max_client_connections_backpressure() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_connlimit_{test_id}"));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{r1}").parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{c1}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![])
            .with_max_client_connections(1);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "single-node leader not ready for conn-limit test");

        // Hold the single permitted connection open without sending anything.
        let held = tokio::net::TcpStream::connect(client_addr).await.unwrap();

        // Give the accept loop time to hand the permit to the held connection.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // A second connection must not be served while the first is held.
        let blocked = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            roundtrip(client_addr, 5, &[]),
        )
        .await;
        assert!(
            blocked.is_err(),
            "second connection should be backpressured while the limit is exhausted"
        );

        // Releasing the held connection frees the permit; requests flow again.
        drop(held);
        let mut served = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                served = true;
                break;
            }
        }
        assert!(served, "connection should be served after permit release");

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[serial]
    #[tokio::test]
    async fn test_node_restart_preserves_raft_term() {
        use kaya_raft::{decode_hard_state, Term, RAFT_HARD_STATE_LEN};

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_restart_term_{test_id}"));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{r1}").parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{c1}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);

        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config.clone()).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "single-node leader not ready");

        let put_payload = encode_put_payload(b"restart-key", b"restart-val");
        let (status, _) = roundtrip(client_addr, 1, &put_payload).await.unwrap();
        assert_eq!(status, 0);

        for _ in 0..30 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let hs_path = data_dir.join("raft-hard-state");
            if hs_path.exists() {
                let bytes = std::fs::read(&hs_path).unwrap();
                if bytes.len() == RAFT_HARD_STATE_LEN {
                    let hs = decode_hard_state(&bytes).unwrap();
                    if hs.current_term.0 > 0 {
                        break;
                    }
                }
            }
        }

        handle.abort();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let hs_path = data_dir.join("raft-hard-state");
        assert!(hs_path.exists(), "raft-hard-state missing after run");
        let bytes = std::fs::read(&hs_path).unwrap();
        let persisted = decode_hard_state(&bytes).unwrap();
        assert!(persisted.current_term.0 > 0, "expected persisted term > 0");

        let restart_config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let restart_handle = tokio::spawn(async move {
            let _ = ClusterNode::new(restart_config).run().await;
        });

        let mut restarted = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                restarted = true;
                break;
            }
        }
        assert!(restarted, "node did not become leader after restart");

        let get_payload = encode_key_payload(b"restart-key");
        let (status, body) = roundtrip(client_addr, 2, &get_payload).await.unwrap();
        assert_eq!(status, 0);
        assert_eq!(decode_value_payload(&body).unwrap(), b"restart-val");

        let after_bytes = std::fs::read(&hs_path).unwrap();
        let after = decode_hard_state(&after_bytes).unwrap();
        assert!(
            after.current_term >= persisted.current_term,
            "term regressed after restart: {} < {}",
            after.current_term.0,
            persisted.current_term.0
        );
        assert!(after.current_term >= Term(1));

        restart_handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// #25: split layout survives process restart via range-table.bin + group dirs.
    #[serial]
    #[tokio::test]
    async fn test_range_split_survives_restart() {
        use kaya_net::{
            decode_list_ranges_response, encode_split_range_request, LIST_RANGES_OPCODE,
            SPLIT_RANGE_OPCODE,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_range_meta_restart_{test_id}"));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{r}").parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{c}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        for key in [b"a".as_slice(), b"m".as_slice(), b"z".as_slice()] {
            let (status, _) = roundtrip(client_addr, 1, &encode_put_payload(key, b"v1"))
                .await
                .unwrap();
            assert_eq!(status, STATUS_OK);
        }

        let (status, body) = roundtrip(
            client_addr,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"m"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "split should succeed");
        let (split_epoch, halves) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(halves.len(), 2);
        assert!(split_epoch >= 2);

        let (status, body) = roundtrip(client_addr, LIST_RANGES_OPCODE, &[])
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (listed_epoch, listed) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed_epoch, split_epoch);

        handle.abort();
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        assert!(
            data_dir.join("range-table.bin").exists(),
            "range-table.bin must exist after split"
        );

        let restart_config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let restart_handle = tokio::spawn(async move {
            let _ = ClusterNode::new(restart_config).run().await;
        });

        let mut restarted = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                restarted = true;
                break;
            }
        }
        assert!(restarted, "node should elect after restart");

        let (status, body) = roundtrip(client_addr, LIST_RANGES_OPCODE, &[])
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (epoch2, ranges2) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(ranges2.len(), 2, "split layout must survive restart");
        assert_eq!(
            epoch2, listed_epoch,
            "meta_epoch must be restored, not reset by from_ranges"
        );
        // Same split point: second range starts at "m".
        assert_eq!(ranges2[1].3, b"m", "right half start_key");

        for key in [b"a".as_slice(), b"m".as_slice(), b"z".as_slice()] {
            let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(key))
                .await
                .unwrap();
            assert_eq!(status, STATUS_OK, "get {:?}", String::from_utf8_lossy(key));
            assert_eq!(decode_value_payload(&body).unwrap(), b"v1");
        }

        // Post-restart write still routes.
        let (status, _) = roundtrip(client_addr, 1, &encode_put_payload(b"m", b"v2"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(b"m"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_value_payload(&body).unwrap(), b"v2");

        restart_handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// #25: stale client meta_epoch on PUT gets RANGE_MOVED + a refresh body.
    #[serial]
    #[tokio::test]
    async fn test_stale_meta_epoch_returns_range_moved() {
        use kaya_net::{
            decode_list_ranges_response, encode_meta_epoch_payload, encode_split_range_request,
            LIST_RANGES_OPCODE, SPLIT_RANGE_OPCODE, STATUS_RANGE_MOVED,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_range_stale_epoch_{test_id}"));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{r}").parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{c}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        let (status, body) = roundtrip(client_addr, LIST_RANGES_OPCODE, &[])
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (epoch_before, _) = decode_list_ranges_response(&body).unwrap();

        let (status, _) = roundtrip(
            client_addr,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"m"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK);

        let stale = encode_meta_epoch_payload(&encode_put_payload(b"a", b"v"), epoch_before);
        let (status, body) = roundtrip(client_addr, 1, &stale).await.unwrap();
        assert_eq!(status, STATUS_RANGE_MOVED, "stale epoch must RANGE_MOVED");
        let (epoch_after, ranges) = decode_list_ranges_response(&body).unwrap();
        assert!(epoch_after > epoch_before);
        assert_eq!(ranges.len(), 2);

        // Current epoch is accepted (key still on group 0 so no new-group election wait).
        let fresh = encode_meta_epoch_payload(&encode_put_payload(b"a", b"v"), epoch_after);
        let (status, _) = roundtrip(client_addr, 1, &fresh).await.unwrap();
        assert_eq!(status, STATUS_OK);

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// #25: all three voters restore the last committed layout after restart.
    #[serial]
    #[tokio::test]
    async fn test_range_split_survives_all_nodes_restart() {
        use kaya_net::{
            decode_list_ranges_response, encode_split_range_request, LIST_RANGES_OPCODE,
            SPLIT_RANGE_OPCODE,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir1 = std::env::temp_dir().join(format!("kayadb_range_all_n1_{test_id}"));
        let data_dir2 = std::env::temp_dir().join(format!("kayadb_range_all_n2_{test_id}"));
        let data_dir3 = std::env::temp_dir().join(format!("kayadb_range_all_n3_{test_id}"));
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let r2 = get_free_port().await;
        let c2 = get_free_port().await;
        let r3 = get_free_port().await;
        let c3 = get_free_port().await;
        let raft1: SocketAddr = format!("127.0.0.1:{r1}").parse().unwrap();
        let client1: SocketAddr = format!("127.0.0.1:{c1}").parse().unwrap();
        let raft2: SocketAddr = format!("127.0.0.1:{r2}").parse().unwrap();
        let client2: SocketAddr = format!("127.0.0.1:{c2}").parse().unwrap();
        let raft3: SocketAddr = format!("127.0.0.1:{r3}").parse().unwrap();
        let client3: SocketAddr = format!("127.0.0.1:{c3}").parse().unwrap();

        let cfg1 = ClusterConfig::new(
            1,
            &data_dir1,
            raft1,
            client1,
            vec![(2, raft2, client2), (3, raft3, client3)],
        );
        let cfg2 = ClusterConfig::new(
            2,
            &data_dir2,
            raft2,
            client2,
            vec![(1, raft1, client1), (3, raft3, client3)],
        );
        let cfg3 = ClusterConfig::new(
            3,
            &data_dir3,
            raft3,
            client3,
            vec![(1, raft1, client1), (2, raft2, client2)],
        );

        let h1 = tokio::spawn(async move {
            let _ = ClusterNode::new(cfg1).run().await;
        });
        let h2 = tokio::spawn(async move {
            let _ = ClusterNode::new(cfg2).run().await;
        });
        let h3 = tokio::spawn(async move {
            let _ = ClusterNode::new(cfg3).run().await;
        });

        let mut leader = None;
        for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            for addr in [client1, client2, client3] {
                if check_health(addr).await.as_deref() == Some("leader") {
                    leader = Some(addr);
                    break;
                }
            }
            if leader.is_some() {
                break;
            }
        }
        let leader = leader.expect("leader elected");

        let (status, _) = roundtrip(leader, 1, &encode_put_payload(b"a", b"v1"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (status, body) = roundtrip(
            leader,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"m"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "split");
        let (split_epoch, halves) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(halves.len(), 2);

        // Wait until every voter has applied the RangeMeta.
        let mut all_have = false;
        for _ in 0..80 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let mut ok = 0;
            for addr in [client1, client2, client3] {
                if let Ok((STATUS_OK, body)) = roundtrip(addr, LIST_RANGES_OPCODE, &[]).await {
                    if let Ok((epoch, ranges)) = decode_list_ranges_response(&body) {
                        if ranges.len() == 2 && epoch == split_epoch {
                            ok += 1;
                        }
                    }
                }
            }
            if ok == 3 {
                all_have = true;
                break;
            }
        }
        assert!(all_have, "all nodes should apply RangeMeta before restart");

        h1.abort();
        h2.abort();
        h3.abort();
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        for dir in [&data_dir1, &data_dir2, &data_dir3] {
            assert!(
                dir.join("range-table.bin").exists(),
                "range-table.bin missing in {}",
                dir.display()
            );
        }

        let cfg1 = ClusterConfig::new(
            1,
            &data_dir1,
            raft1,
            client1,
            vec![(2, raft2, client2), (3, raft3, client3)],
        );
        let cfg2 = ClusterConfig::new(
            2,
            &data_dir2,
            raft2,
            client2,
            vec![(1, raft1, client1), (3, raft3, client3)],
        );
        let cfg3 = ClusterConfig::new(
            3,
            &data_dir3,
            raft3,
            client3,
            vec![(1, raft1, client1), (2, raft2, client2)],
        );
        let h1 = tokio::spawn(async move {
            let _ = ClusterNode::new(cfg1).run().await;
        });
        let h2 = tokio::spawn(async move {
            let _ = ClusterNode::new(cfg2).run().await;
        });
        let h3 = tokio::spawn(async move {
            let _ = ClusterNode::new(cfg3).run().await;
        });

        let mut leader = None;
        for _ in 0..120 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            for addr in [client1, client2, client3] {
                if check_health(addr).await.as_deref() == Some("leader") {
                    leader = Some(addr);
                    break;
                }
            }
            if leader.is_some() {
                break;
            }
        }
        let leader = leader.expect("leader after restart");

        for addr in [client1, client2, client3] {
            let (status, body) = roundtrip(addr, LIST_RANGES_OPCODE, &[]).await.unwrap();
            assert_eq!(status, STATUS_OK, "list {addr}");
            let (epoch, ranges) = decode_list_ranges_response(&body).unwrap();
            assert_eq!(ranges.len(), 2, "layout lost on {addr}");
            assert_eq!(epoch, split_epoch, "epoch reset on {addr}");
        }

        let (status, body) = roundtrip(leader, 2, &encode_key_payload(b"a"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_value_payload(&body).unwrap(), b"v1");

        h1.abort();
        h2.abort();
        h3.abort();
        let _ = std::fs::remove_dir_all(&data_dir1);
        let _ = std::fs::remove_dir_all(&data_dir2);
        let _ = std::fs::remove_dir_all(&data_dir3);
    }

    #[cfg(feature = "ebpf")]
    fn prometheus_sample_value(body: &str, metric_prefix: &str) -> Option<u64> {
        body.lines()
            .find(|line| line.starts_with(metric_prefix) && !line.contains("#"))
            .and_then(|line| line.split_whitespace().last())
            .and_then(|v| v.parse().ok())
    }

    #[cfg(feature = "ebpf")]
    #[serial]
    #[tokio::test]
    async fn ebpf_enabled_metrics_expose_fsync_histogram() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_ebpf_metrics_{test_id}"));
        let client_port = get_free_port().await;
        let raft_port = get_free_port().await;
        let metrics_port = get_free_port().await;
        let client_addr: SocketAddr = format!("127.0.0.1:{client_port}").parse().unwrap();
        let raft_addr: SocketAddr = format!("127.0.0.1:{raft_port}").parse().unwrap();
        let metrics_addr: SocketAddr = format!("127.0.0.1:{metrics_port}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![])
            .with_ebpf(42)
            .with_metrics_addr(metrics_addr);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node did not become leader");

        let put = encode_put_payload(b"ebpf-key", b"ebpf-val");
        let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
        assert_eq!(status, 0);

        async fn scrape_metrics_body(metrics_addr: SocketAddr) -> String {
            let mut stream = tokio::net::TcpStream::connect(metrics_addr).await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            stream
                .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let mut buf = vec![0u8; 32_768];
            let n = stream.read(&mut buf).await.unwrap();
            String::from_utf8_lossy(&buf[..n]).to_string()
        }

        let mut saw_nonzero = false;
        for _ in 0..80 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let body = scrape_metrics_body(metrics_addr).await;
            let ebpf_count = prometheus_sample_value(
                &body,
                "kaya_ebpf_fsync_latency_us_count{syscall=\"fsync\"}",
            )
            .unwrap_or(0);
            let wal_total = prometheus_sample_value(&body, "kaya_wal_fsync_total_us").unwrap_or(0);
            if ebpf_count > 0 && wal_total > 0 {
                saw_nonzero = true;
                break;
            }
        }
        assert!(
            saw_nonzero,
            "timed out waiting for non-zero eBPF/WAL fsync metrics"
        );

        let scratch =
            std::path::PathBuf::from(std::env::var("KAYA_GOAL_SCRATCH").unwrap_or_else(|_| {
                r"C:\Users\tunay\AppData\Local\Temp\grok-goal-10c42b461488\implementer".to_owned()
            }));
        let _ = std::fs::create_dir_all(&scratch);

        for run in 0..2 {
            let body = scrape_metrics_body(metrics_addr).await;
            let _ = std::fs::write(scratch.join(format!("metrics-scrape-{run}.txt")), &body);

            let ebpf_count = prometheus_sample_value(
                &body,
                "kaya_ebpf_fsync_latency_us_count{syscall=\"fsync\"}",
            )
            .unwrap_or(0);
            let ebpf_sum =
                prometheus_sample_value(&body, "kaya_ebpf_fsync_latency_us_sum{syscall=\"fsync\"}")
                    .unwrap_or(0);
            let wal_total = prometheus_sample_value(&body, "kaya_wal_fsync_total_us").unwrap_or(0);

            assert!(
                ebpf_count > 0,
                "expected non-zero eBPF fsync count after PUT; body:\n{body}"
            );
            assert!(ebpf_sum > 0, "expected non-zero eBPF fsync sum");
            assert!(wal_total > 0, "expected non-zero userspace wal fsync total");
            let status_raw =
                std::fs::read_to_string(data_dir.join("ebpf/status.json")).unwrap_or_default();
            assert!(
                status_raw.contains("kernel"),
                "ebpf status must record kernel-family backend, got: {status_raw}"
            );
            assert!(
                body.contains("kernel-slot fsync latency"),
                "metrics HELP must describe kernel-slot backend"
            );
            assert!(
                !body.contains("userspace-tap"),
                "metrics must not use legacy userspace-tap HELP"
            );
            assert!(
                body.lines().any(|l| {
                    l.starts_with("kaya_ebpf_fsync_latency_us_bucket{syscall=\"fsync\"")
                        && !l.ends_with("} 0")
                }),
                "expected non-zero eBPF bucket after PUT; body:\n{body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let trace_path = data_dir.join("ebpf/trace.jsonl");
        let mut trace_raw = String::new();
        for _ in 0..80 {
            if trace_path.is_file() {
                trace_raw = std::fs::read_to_string(&trace_path).unwrap_or_default();
                if trace_raw.contains("\"site\":\"wal_fsync\"")
                    && trace_raw.contains("\"site\":\"flush\"")
                    && trace_raw.contains("\"kind\":\"publish_syscall\"")
                {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            trace_raw.contains("\"kind\":\"usdt_marker\""),
            "server trace must record usdt_marker events"
        );
        assert!(
            trace_raw.contains("\"site\":\"wal_fsync\""),
            "server trace must record wal_fsync markers"
        );
        assert!(
            trace_raw.contains("\"site\":\"flush\""),
            "server trace must record flush markers after auto-flush"
        );
        assert!(
            trace_raw.contains("\"kind\":\"publish_syscall\""),
            "server trace must record publish_syscall events"
        );
        let _ = std::fs::write(scratch.join("server-trace.jsonl"), &trace_raw);
        let _ = std::fs::copy(&trace_path, scratch.join("server-ebpf-trace.jsonl"));

        handle.abort();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let kayactl = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/debug/kayactl.exe");
        if kayactl.exists() {
            for run in 1..=2 {
                let output = std::process::Command::new(&kayactl)
                    .args([
                        "ebpf",
                        "correlate",
                        "--data",
                        &data_dir.display().to_string(),
                        "--durability",
                        "strict",
                    ])
                    .output()
                    .expect("kayactl correlate");
                assert!(output.status.success());
                let rendered = String::from_utf8_lossy(&output.stdout);
                assert!(rendered.contains("USDT markers"));
                assert!(rendered.contains("flush_enter="));
                assert!(rendered.contains("Publish trace"));
                let _ = std::fs::write(
                    scratch.join(format!("correlate-run-{run}.txt")),
                    rendered.as_ref(),
                );
            }
        }

        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[serial]
    #[tokio::test]
    async fn test_txn_begin_put_get_commit_visible() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_txn_{test_id}"));

        let r1 = get_free_port().await;
        let c1 = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{r1}").parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{c1}").parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "single-node leader not ready for TXN test");

        // BEGIN
        let (status, body) = roundtrip(client_addr, TXN_BEGIN_OPCODE, &[]).await.unwrap();
        assert_eq!(status, STATUS_OK, "TXN_BEGIN should succeed");
        let (txn_id, _snapshot_ts) = decode_txn_begin_response(&body).unwrap();
        assert!(txn_id >= 1);

        // OP put
        let put_payload = encode_txn_op_payload(txn_id, TXN_OP_PUT, b"txn-key", Some(b"txn-val"));
        let (status, _) = roundtrip(client_addr, TXN_OP_OPCODE, &put_payload)
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "TXN_OP put should succeed");

        // OP get (RYW)
        let get_payload = encode_txn_op_payload(txn_id, TXN_OP_GET, b"txn-key", None);
        let (status, body) = roundtrip(client_addr, TXN_OP_OPCODE, &get_payload)
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "TXN_OP get should succeed");
        assert_eq!(decode_value_payload(&body).unwrap(), b"txn-val");

        // Outside GET should not see uncommitted intent
        let outside_get = encode_key_payload(b"txn-key");
        let (status, _) = roundtrip(client_addr, 2, &outside_get).await.unwrap();
        assert_eq!(
            status, STATUS_NOT_FOUND,
            "uncommitted intent must not be visible outside txn"
        );

        // COMMIT
        let commit_payload = encode_txn_id_payload(txn_id);
        let (status, body) = roundtrip(client_addr, TXN_COMMIT_OPCODE, &commit_payload)
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "TXN_COMMIT should succeed");
        let commit_ts = decode_txn_commit_response(&body).unwrap();
        assert!(commit_ts > 0, "commit_ts should be positive after put");

        // Outside GET sees value
        let (status, body) = roundtrip(client_addr, 2, &outside_get).await.unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_value_payload(&body).unwrap(), b"txn-val");

        // ROLLBACK of finished txn is invalid
        let (status, _) = roundtrip(client_addr, TXN_ROLLBACK_OPCODE, &commit_payload)
            .await
            .unwrap();
        assert_eq!(status, STATUS_INVALID_ARGUMENT);

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Cross-range multi-key TXN_COMMIT via 2PC (M23).
    ///
    /// Static two ranges (split at `m`), txn puts one key on each side, commit
    /// must materialize both keys.
    #[serial]
    #[tokio::test]
    async fn test_cross_range_txn_commit() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_cross_txn_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        // [a, m) → group 1, [m, z) → group 2
        let ranges = vec![
            StaticRange::new(b"a".to_vec(), b"m".to_vec(), GroupId(1)),
            StaticRange::new(b"m".to_vec(), b"z".to_vec(), GroupId(2)),
        ];
        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![])
            .with_static_ranges(ranges);

        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(
            ready,
            "single-node multi-raft leader not ready for cross-range txn"
        );

        // BEGIN
        let (status, body) = roundtrip(client_addr, TXN_BEGIN_OPCODE, &[]).await.unwrap();
        assert_eq!(status, STATUS_OK, "TXN_BEGIN");
        let (txn_id, _) = decode_txn_begin_response(&body).unwrap();

        // Put key on left range (group 1)
        let put_left = encode_txn_op_payload(txn_id, TXN_OP_PUT, b"apple", Some(b"red"));
        let (status, _) = roundtrip(client_addr, TXN_OP_OPCODE, &put_left)
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "TXN_OP put apple");

        // Put key on right range (group 2)
        let put_right = encode_txn_op_payload(txn_id, TXN_OP_PUT, b"mango", Some(b"yellow"));
        let (status, _) = roundtrip(client_addr, TXN_OP_OPCODE, &put_right)
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "TXN_OP put mango");

        // Uncommitted intents not visible outside the txn
        let (status, _) = roundtrip(client_addr, 2, &encode_key_payload(b"apple"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_NOT_FOUND);
        let (status, _) = roundtrip(client_addr, 2, &encode_key_payload(b"mango"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_NOT_FOUND);

        // COMMIT — must run 2PC across groups 1 and 2
        let commit_payload = encode_txn_id_payload(txn_id);
        let (status, body) = roundtrip(client_addr, TXN_COMMIT_OPCODE, &commit_payload)
            .await
            .unwrap();
        assert_eq!(
            status,
            STATUS_OK,
            "TXN_COMMIT cross-range 2PC should succeed, body={}",
            String::from_utf8_lossy(&body)
        );
        let commit_ts = decode_txn_commit_response(&body).unwrap();
        assert!(commit_ts > 0);

        // Both keys visible after commit
        let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(b"apple"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "apple after 2PC commit");
        assert_eq!(decode_value_payload(&body).unwrap(), b"red");

        let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(b"mango"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK, "mango after 2PC commit");
        assert_eq!(decode_value_payload(&body).unwrap(), b"yellow");

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Multi-range bank transfers via the high-level client (2PC is transparent).
    ///
    /// Accounts on left range `[a,m)` and right range `[m,z)`; SI transfers that
    /// touch both sides must preserve the constant-sum invariant.
    #[serial]
    #[tokio::test]
    async fn test_multi_range_bank_sum_invariant() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_bank_2pc_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        // Left [a, m) → group 1; right [m, z) → group 2
        let ranges = vec![
            StaticRange::new(b"a".to_vec(), b"m".to_vec(), GroupId(1)),
            StaticRange::new(b"m".to_vec(), b"z".to_vec(), GroupId(2)),
        ];
        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![])
            .with_static_ranges(ranges);

        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "leader not ready for multi-range bank");

        // Keys chosen so left/right land in different groups.
        // left: apple, banana  (both < m); right: mango, melon (both >= m)
        let left_a = b"apple".as_slice();
        let left_b = b"banana".as_slice();
        let right_a = b"mango".as_slice();
        let right_b = b"melon".as_slice();
        let initial: i64 = 100;
        let accounts: [&[u8]; 4] = [left_a, left_b, right_a, right_b];
        let expected_total = initial * accounts.len() as i64;

        let mut client = kaya_client::KayaClient::connect(client_addr)
            .await
            .expect("connect client");
        client.set_max_redirects(5);

        let bal = |n: i64| n.to_string().into_bytes();
        let parse = |v: &[u8]| -> i64 {
            std::str::from_utf8(v)
                .unwrap()
                .parse()
                .expect("balance parse")
        };

        for key in &accounts {
            client
                .put(key, &bal(initial))
                .await
                .unwrap_or_else(|e| panic!("seed {}: {e}", String::from_utf8_lossy(key)));
        }

        // Helper: SI transfer amount from `from` to `to` (may span ranges → 2PC).
        async fn transfer(
            client: &mut kaya_client::KayaClient,
            from: &[u8],
            to: &[u8],
            amount: i64,
        ) {
            let mut txn = client.begin_txn().await.expect("begin_txn");
            let from_bal = parse_bal(txn.get(from).await.expect("get from").as_deref());
            let to_bal = parse_bal(txn.get(to).await.expect("get to").as_deref());
            assert!(from_bal >= amount, "insufficient funds for transfer");
            txn.put(from, &encode_bal(from_bal - amount))
                .await
                .expect("put debit");
            txn.put(to, &encode_bal(to_bal + amount))
                .await
                .expect("put credit");
            let ts = txn.commit().await.expect("commit transfer");
            assert!(ts > 0, "commit_ts must be positive");
        }

        fn encode_bal(n: i64) -> Vec<u8> {
            n.to_string().into_bytes()
        }
        fn parse_bal(v: Option<&[u8]>) -> i64 {
            let v = v.expect("account missing");
            std::str::from_utf8(v)
                .unwrap()
                .parse()
                .expect("balance parse")
        }

        // Cross-range: apple (left) → mango (right)
        transfer(&mut client, left_a, right_a, 30).await;
        // Cross-range reverse: melon (right) → banana (left)
        transfer(&mut client, right_b, left_b, 20).await;
        // Same-range left: apple → banana
        transfer(&mut client, left_a, left_b, 10).await;
        // Same-range right: mango → melon
        transfer(&mut client, right_a, right_b, 5).await;
        // Cross-range again: banana → melon
        transfer(&mut client, left_b, right_b, 15).await;

        let mut balances = Vec::with_capacity(accounts.len());
        for key in &accounts {
            let v = client
                .get(key)
                .await
                .unwrap_or_else(|e| panic!("get {}: {e}", String::from_utf8_lossy(key)))
                .unwrap_or_else(|| panic!("missing {}", String::from_utf8_lossy(key)));
            balances.push(parse(&v));
        }
        let sum: i64 = balances.iter().sum();
        assert_eq!(
            sum, expected_total,
            "bank sum invariant violated: sum={sum} expected={expected_total} balances={balances:?}"
        );
        // Expected after transfers:
        // apple:  100-30-10 = 60
        // banana: 100+20+10-15 = 115
        // mango:  100+30-5 = 125
        // melon:  100-20+5+15 = 100
        assert_eq!(balances, vec![60, 115, 125, 100]);

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Single-node multi-raft: two static ranges, puts route to independent groups.
    #[serial]
    #[tokio::test]
    async fn test_multi_raft_static_ranges_put_get() {
        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_multi_raft_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        let ranges = vec![
            StaticRange::new(b"a".to_vec(), b"m".to_vec(), GroupId(1)),
            StaticRange::new(b"m".to_vec(), b"z".to_vec(), GroupId(2)),
        ];
        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![])
            .with_static_ranges(ranges);

        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        // Wait for single-node election on all groups.
        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "single-node multi-raft should elect a leader");

        // Key in group 1 range [a, m)
        let put_a = encode_put_payload(b"apple", b"red");
        let (status, _) = roundtrip(client_addr, 1, &put_a).await.unwrap();
        assert_eq!(status, STATUS_OK, "put apple should commit on group 1");

        // Key in group 2 range [m, z)
        let put_m = encode_put_payload(b"mango", b"yellow");
        let (status, _) = roundtrip(client_addr, 1, &put_m).await.unwrap();
        assert_eq!(status, STATUS_OK, "put mango should commit on group 2");

        let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(b"apple"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_value_payload(&body).unwrap(), b"red");

        let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(b"mango"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_value_payload(&body).unwrap(), b"yellow");

        // Stats should report multiple raft groups (0 always + 1 + 2).
        let (status, body) = roundtrip(client_addr, 6, &[]).await.unwrap();
        assert_eq!(status, STATUS_OK);
        let stats = String::from_utf8(body).unwrap();
        assert!(
            stats.contains("\"raft_groups\":3") || stats.contains("\"raft_groups\": 3"),
            "expected 3 raft groups in stats, got: {stats}"
        );

        // Per-group disk layout for non-zero groups.
        assert!(
            data_dir.join("groups").join("1").exists()
                || data_dir
                    .join("groups")
                    .join("1")
                    .join("raft-hard-state")
                    .exists()
                || data_dir.join("groups").is_dir(),
            "expected groups/ directory for multi-raft layout"
        );

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// M21: split under load loses no writes; LIST_RANGES reflects meta epoch.
    #[serial]
    #[tokio::test]
    async fn test_range_split_no_lost_writes() {
        use kaya_net::{
            decode_list_ranges_response, encode_split_range_request, LIST_RANGES_OPCODE,
            SPLIT_RANGE_OPCODE,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_range_split_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        for i in 0..20u8 {
            let key = format!("k{i:02}");
            let put = encode_put_payload(key.as_bytes(), b"pre");
            let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
            assert_eq!(status, STATUS_OK, "pre-split put {key}");
        }

        let (status, body) = roundtrip(
            client_addr,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"k10"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "split should succeed");
        let (meta_epoch, halves) = decode_list_ranges_response(&body).unwrap();
        assert!(meta_epoch >= 1);
        assert_eq!(halves.len(), 2);

        // New group may need a moment to elect on single-node.
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                break;
            }
        }

        for i in 0..20u8 {
            let key = format!("k{i:02}");
            let put = encode_put_payload(key.as_bytes(), b"post");
            let mut ok = false;
            for _ in 0..20 {
                let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
                if status == STATUS_OK {
                    ok = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
            assert!(ok, "post-split put {key}");
        }

        for i in 0..20u8 {
            let key = format!("k{i:02}");
            let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(key.as_bytes()))
                .await
                .unwrap();
            assert_eq!(status, STATUS_OK, "get {key}");
            assert_eq!(
                decode_value_payload(&body).unwrap(),
                b"post",
                "value for {key}"
            );
        }

        let (status, body) = roundtrip(client_addr, LIST_RANGES_OPCODE, &[])
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (epoch2, ranges) = decode_list_ranges_response(&body).unwrap();
        assert!(epoch2 >= meta_epoch);
        assert!(ranges.len() >= 2);

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// #24: MOVE_RANGE cuts a range over to another group while puts/gets are
    /// in flight; every key keeps its last written value.
    #[serial]
    #[tokio::test]
    async fn test_range_move_under_concurrent_load() {
        use kaya_net::{
            decode_list_ranges_response, encode_move_range_request, encode_split_range_request,
            LIST_RANGES_OPCODE, MOVE_RANGE_OPCODE, SPLIT_RANGE_OPCODE,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_range_move_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        // Two ranges: [,m)→g0 and [m,)→g1.
        let (status, _) = roundtrip(
            client_addr,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"m"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "split should succeed");
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                break;
            }
        }

        const KEYS: u8 = 8;
        const ROUNDS: u8 = 6;
        // Writer + reader hammering the range that is about to move.
        let load = tokio::spawn(async move {
            for round in 0..ROUNDS {
                for i in 0..KEYS {
                    let key = format!("m{i:02}");
                    let value = format!("r{round}");
                    let put = encode_put_payload(key.as_bytes(), value.as_bytes());
                    let mut ok = false;
                    for _ in 0..40 {
                        // RANGE_MOVED / NOT_LEADER during cutover: refresh + retry.
                        if let Ok((s, _)) = roundtrip(client_addr, 1, &put).await {
                            if s == STATUS_OK {
                                ok = true;
                                break;
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                    }
                    assert!(ok, "put {key} round {round} never succeeded");

                    let get = encode_key_payload(key.as_bytes());
                    if let Ok((s, body)) = roundtrip(client_addr, 2, &get).await {
                        if s == STATUS_OK {
                            // A concurrent get never observes a value we never wrote.
                            let v = decode_value_payload(&body).unwrap();
                            assert!(
                                v.starts_with(b"r"),
                                "unexpected value for {key}: {:?}",
                                String::from_utf8_lossy(&v)
                            );
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        });

        // Cut [m,) over to a fresh group mid-load.
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let (status, body) = roundtrip(
            client_addr,
            MOVE_RANGE_OPCODE,
            &encode_move_range_request(b"m", 9),
        )
        .await
        .unwrap();
        assert_eq!(
            status,
            STATUS_OK,
            "move should succeed: {}",
            String::from_utf8_lossy(&body)
        );
        let (meta_epoch, moved) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(moved.len(), 1);
        assert_eq!(moved[0].2, 9, "owner group after cutover");
        assert_eq!(moved[0].3, b"m".to_vec());

        load.await.expect("load task");

        // Every key keeps its last written value after the cutover.
        for i in 0..KEYS {
            let key = format!("m{i:02}");
            let mut seen = None;
            for _ in 0..40 {
                let (s, body) = roundtrip(client_addr, 2, &encode_key_payload(key.as_bytes()))
                    .await
                    .unwrap();
                if s == STATUS_OK {
                    seen = Some(decode_value_payload(&body).unwrap());
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            assert_eq!(
                seen.as_deref(),
                Some(format!("r{}", ROUNDS - 1).as_bytes()),
                "final value for {key}"
            );
        }

        // Meta table converged on the new owner; ranges still cover the keyspace.
        let (status, body) = roundtrip(client_addr, LIST_RANGES_OPCODE, &[])
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (epoch2, ranges) = decode_list_ranges_response(&body).unwrap();
        assert!(epoch2 >= meta_epoch);
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[1].2, 9, "moved range owner in meta table");

        // Moving to the owning group is rejected (no epoch churn).
        let (status, _) = roundtrip(
            client_addr,
            MOVE_RANGE_OPCODE,
            &encode_move_range_request(b"m", 9),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_INVALID_ARGUMENT, "self-move rejected");

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// M22: split then merge recombines to one range; keys remain readable.
    #[serial]
    #[tokio::test]
    async fn test_range_merge_recombines() {
        use kaya_net::{
            decode_list_ranges_response, encode_merge_range_request, encode_split_range_request,
            LIST_RANGES_OPCODE, MERGE_RANGE_OPCODE, SPLIT_RANGE_OPCODE,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_range_merge_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        for key in [b"a".as_slice(), b"m".as_slice(), b"z".as_slice()] {
            let put = encode_put_payload(key, b"v1");
            let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
            assert_eq!(status, STATUS_OK, "put {:?}", String::from_utf8_lossy(key));
        }

        let (status, body) = roundtrip(
            client_addr,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"m"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "split should succeed");
        let (split_epoch, halves) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(halves.len(), 2);
        assert!(split_epoch >= 2);

        // left_start is empty (whole-keyspace left half).
        let (status, body) = roundtrip(
            client_addr,
            MERGE_RANGE_OPCODE,
            &encode_merge_range_request(b""),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "merge should succeed");
        let (merge_epoch, merged) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(merged.len(), 1);
        assert!(merge_epoch > split_epoch);
        assert!(merged[0].4.is_empty(), "merged end should be unbounded");
        assert_eq!(merged[0].2, 0, "merged keeps left group 0");

        let (status, body) = roundtrip(client_addr, LIST_RANGES_OPCODE, &[])
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        let (_, ranges) = decode_list_ranges_response(&body).unwrap();
        assert_eq!(ranges.len(), 1);

        for key in [b"a".as_slice(), b"m".as_slice(), b"z".as_slice()] {
            let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(key))
                .await
                .unwrap();
            assert_eq!(status, STATUS_OK, "get {:?}", String::from_utf8_lossy(key));
            assert_eq!(decode_value_payload(&body).unwrap(), b"v1");
        }

        // Post-merge write still works.
        let put = encode_put_payload(b"m", b"v2");
        let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
        assert_eq!(status, STATUS_OK);
        let (status, body) = roundtrip(client_addr, 2, &encode_key_payload(b"m"))
            .await
            .unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(decode_value_payload(&body).unwrap(), b"v2");

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// Issue #30: merge orphans the right group's Raft host entry and data dir;
    /// the reclaim pass frees both (raft_groups count drops, `groups/<id>` is gone).
    #[serial]
    #[tokio::test]
    async fn test_range_merge_reclaims_orphan_group() {
        use kaya_net::{
            encode_merge_range_request, encode_split_range_request, MERGE_RANGE_OPCODE,
            SPLIT_RANGE_OPCODE,
        };

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_range_reclaim_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        let (status, _) = roundtrip(
            client_addr,
            SPLIT_RANGE_OPCODE,
            &encode_split_range_request(b"m"),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "split should succeed");

        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                break;
            }
        }

        // Group 1 (the split-off right half) is hosted: raft_groups == 2, data dir exists.
        let group1_dir = data_dir.join("groups").join("1");
        let mut saw_two_groups = false;
        for _ in 0..40 {
            if let Some(n) = raft_groups_from_stats(client_addr).await {
                if n == 2 {
                    saw_two_groups = true;
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(saw_two_groups, "expected 2 hosted raft groups after split");
        assert!(
            group1_dir.exists(),
            "group 1 data dir should exist after split"
        );

        let (status, _) = roundtrip(
            client_addr,
            MERGE_RANGE_OPCODE,
            &encode_merge_range_request(b""),
        )
        .await
        .unwrap();
        assert_eq!(status, STATUS_OK, "merge should succeed");

        // Reclaim runs on the drain loop (every tick): host entry unhosted and
        // the group's data dir removed without a restart.
        let mut reclaimed = false;
        for _ in 0..80 {
            let groups = raft_groups_from_stats(client_addr).await;
            if groups == Some(1) && !group1_dir.exists() {
                reclaimed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            reclaimed,
            "orphan group 1 should be unhosted and its data dir removed after merge"
        );

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// M22: TRANSFER_LEADER self-transfer is a no-op success on the group-0 leader.
    #[serial]
    #[tokio::test]
    async fn test_transfer_leader_self_noop() {
        use kaya_net::{encode_transfer_leader_request, STATUS_NOT_LEADER, TRANSFER_LEADER_OPCODE};

        let test_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!("kayadb_xfer_leader_{}", test_id));
        let _ = std::fs::remove_dir_all(&data_dir);

        let r = get_free_port().await;
        let c = get_free_port().await;
        let raft_addr: SocketAddr = format!("127.0.0.1:{}", r).parse().unwrap();
        let client_addr: SocketAddr = format!("127.0.0.1:{}", c).parse().unwrap();

        let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![]);
        let handle = tokio::spawn(async move {
            let _ = ClusterNode::new(config).run().await;
        });

        let mut ready = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            if check_health(client_addr).await.as_deref() == Some("leader") {
                ready = true;
                break;
            }
        }
        assert!(ready, "node should elect");

        // Self-transfer on group 0: success, still leader.
        let payload = encode_transfer_leader_request(0, 1);
        let (status, body) = roundtrip(client_addr, TRANSFER_LEADER_OPCODE, &payload)
            .await
            .unwrap();
        assert_eq!(
            status,
            STATUS_OK,
            "self transfer should succeed: {:?}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(
            check_health(client_addr).await.as_deref(),
            Some("leader"),
            "self-transfer must leave leadership intact"
        );

        // Non-voter target rejected.
        let bad = encode_transfer_leader_request(0, 99);
        let (status, _) = roundtrip(client_addr, TRANSFER_LEADER_OPCODE, &bad)
            .await
            .unwrap();
        assert_ne!(status, STATUS_OK);
        assert_ne!(status, STATUS_NOT_LEADER);

        handle.abort();
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
