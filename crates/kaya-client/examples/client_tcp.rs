use kaya_client::KayaClient;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let addr_str = "127.0.0.1:7379";
    let addr: SocketAddr = addr_str.parse()?;

    println!("Attempting to connect to KayaDB server at: {}", addr);

    // Attempt connection
    let mut client = match KayaClient::connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!(
                "\n[!] Connection Error: Could not connect to KayaDB at {}",
                addr_str
            );
            eprintln!("    To start a local server first, run:");
            eprintln!("    cargo run -p kaya-server --bin kayadb-server\n");
            return Err(e.into());
        }
    };

    // Attempt simple health query to verify alive status
    match client.health().await {
        Ok(role) => {
            println!("Connected successfully! Node role: {}", role);
        }
        Err(e) => {
            eprintln!("\n[!] Handshake Error: Server did not respond to health check.");
            eprintln!(
                "    Verify that a kayadb-server is running on port {}\n",
                addr.port()
            );
            return Err(e.into());
        }
    }

    // Write a key
    println!("Writing 'my_key' => 'my_value'...");
    client.put(b"my_key", b"my_value").await?;

    // Read it back
    println!("Reading 'my_key'...");
    if let Some(val) = client.get(b"my_key").await? {
        println!("Retrieved value: {}", String::from_utf8(val)?);
    } else {
        println!("Key not found.");
    }

    // Delete it
    println!("Deleting 'my_key'...");
    client.delete(b"my_key").await?;

    // Verify deletion
    println!("Re-reading 'my_key'...");
    if client.get(b"my_key").await?.is_some() {
        println!("Error: Key should have been deleted!");
    } else {
        println!("Verified: Key was deleted successfully.");
    }

    Ok(())
}
