#[cfg(test)]
mod tests {
    use crate::cluster::{ClusterConfig, ClusterNode};
    use kaya_net::{
        decode_value_payload, encode_key_payload, encode_member_payload, encode_put_payload,
        encode_remove_member_payload, roundtrip,
    };
    use kaya_sim::{LinearizabilityChecker, Op, OpResult};
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
}
