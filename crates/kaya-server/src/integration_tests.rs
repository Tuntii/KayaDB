#[cfg(test)]
mod tests {
    use crate::cluster::{ClusterConfig, ClusterNode};
    use kaya_net::{decode_value_payload, encode_key_payload, encode_put_payload, roundtrip};
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
}
