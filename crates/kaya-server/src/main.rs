use kaya_server::{server_banner, ServerConfig};

fn main() {
    println!("{}", server_banner(&ServerConfig::default()));
    println!("server networking is intentionally left for a later milestone");
}
