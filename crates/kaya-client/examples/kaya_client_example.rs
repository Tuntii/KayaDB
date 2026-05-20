use std::net::SocketAddr;
use kaya_client::KayaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Port 7771 is the default port for local node 1 or a single-node cluster in our docs.
    let addr: SocketAddr = "127.0.0.1:7771".parse()?;
    
    println!("Connecting to KayaDB cluster node at {}...", addr);
    
    let mut client = match KayaClient::connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {}: {:?}", addr, e);
            eprintln!("Please make sure a KayaDB node is running on that port.");
            std::process::exit(1);
        }
    };
    
    println!("Successfully connected!");
    
    // 1. Put key-value pair
    let key = b"my_demo_key";
    let val = b"Hello from kaya-client example!";
    println!("Writing key 'my_demo_key'...");
    client.put(key, val).await?;
    println!("PUT successful.");
    
    // 2. Get key-value pair
    println!("Reading key 'my_demo_key'...");
    if let Some(retrieved) = client.get(key).await? {
        println!("GET successful! Value: '{}'", String::from_utf8_lossy(&retrieved));
    } else {
        println!("GET returned None. Key not found.");
    }
    
    // 3. Query Node Stats
    println!("\nRetrieving cluster node statistics...");
    match client.stats().await {
        Ok(stats_json) => {
            println!("STATS retrieved successfully:\n{}", stats_json);
        }
        Err(e) => {
            eprintln!("Failed to retrieve STATS: {:?}", e);
        }
    }
    
    // 4. Delete key-value pair
    println!("\nCleaning up key 'my_demo_key'...");
    client.delete(key).await?;
    println!("DELETE successful.");
    
    // 5. Verify deletion
    if (client.get(key).await?).is_some() {
        println!("ERROR: Key was not deleted!");
    } else {
        println!("Verification: Key successfully deleted.");
    }
    
    Ok(())
}
