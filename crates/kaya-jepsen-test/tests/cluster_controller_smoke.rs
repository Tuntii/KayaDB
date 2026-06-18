use kaya_jepsen_test::ClusterController;
use std::time::Duration;

#[tokio::test]
async fn spawns_three_node_cluster_and_finds_leader() {
    let dir = tempfile::tempdir().unwrap();
    let mut cc = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();
    let leader = cc.wait_for_leader(Duration::from_secs(15)).await.unwrap();
    assert!(leader.client_addr.port() > 0);
    cc.shutdown_all().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn partition_node_returns_ok_or_sudo_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut cc = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();

    let result = cc.partition_node(1).await;
    match &result {
        Ok(()) => {
            cc.heal_partition(1).await.expect("heal after partition");
        }
        Err(e) => {
            let lower = e.to_lowercase();
            assert!(
                lower.contains("iptables")
                    || lower.contains("sudo")
                    || lower.contains("permission")
                    || lower.contains("denied"),
                "unexpected partition error: {e}"
            );
        }
    }

    let _ = cc.heal_partition(1).await;
    cc.shutdown_all().await;
}
