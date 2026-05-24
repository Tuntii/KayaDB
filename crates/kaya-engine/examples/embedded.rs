use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, WriteOptions};
use kaya_io::FileDisk;
use std::sync::Arc;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::temp_dir().join("kayadb_embedded_example");
    println!("Opening local KayaDB engine at: {:?}", data_dir);

    // 1. Configure the engine
    let config = EngineConfig {
        data_dir: data_dir.clone(),
        ..EngineConfig::default()
    };
    let disk = Arc::new(FileDisk::new(data_dir.clone()));

    // 2. Open the storage engine
    let mut engine = Engine::open(config.clone(), disk.clone()).await?;
    println!("Engine opened successfully.");

    // 3. Write some keys
    let write_opts = WriteOptions {
        durability: Some(DurabilityMode::Strict),
        idempotency_key: None,
    };
    println!("Putting key1=value1...");
    engine
        .put(b"key1".to_vec(), b"value1".to_vec(), write_opts.clone())
        .await?;

    println!("Putting key2=value2...");
    engine
        .put(b"key2".to_vec(), b"value2".to_vec(), write_opts.clone())
        .await?;

    // 4. Read the key back
    if let Some(val) = engine.get(b"key1", ReadOptions::default()).await? {
        println!("Retrieved key1: {}", String::from_utf8(val.to_vec())?);
    }

    // 5. Close the engine (simulated by dropping)
    drop(engine);
    println!("Engine closed cleanly.");

    // 6. Re-open and verify durability
    println!("Re-opening engine to verify durability...");
    let mut engine = Engine::open(config, disk).await?;

    if let Some(val) = engine.get(b"key2", ReadOptions::default()).await? {
        println!(
            "Durable retrieval key2: {}",
            String::from_utf8(val.to_vec())?
        );
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&data_dir);
    println!("Cleaned up data directory.");

    Ok(())
}
