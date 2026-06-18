use kaya_jepsen_test::ClusterController;
use std::time::Duration;

#[tokio::test]
async fn spawns_three_node_cluster_and_finds_leader() {
    let dir = tempfile::tempdir().unwrap();
    let mut cc = ClusterController::spawn_three_node(dir.path().to_path_buf())
        .await
        .unwrap();
    let leader = cc
        .wait_for_leader(Duration::from_secs(15))
        .await
        .unwrap();
    assert!(leader.client_addr.port() > 0);
    cc.shutdown_all().await;
}