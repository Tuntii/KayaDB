pub mod acl;
pub mod apply_index;
pub mod audit;
pub mod client_auth;
pub mod cluster;
pub mod command;
pub mod dashboard;
pub mod membership;
pub mod metrics;
pub mod operator_auth;
#[cfg(feature = "otel")]
pub mod otel_spans;
pub mod raft_persister;
pub mod range_meta;
pub mod security;

#[cfg(test)]
mod integration_tests;

pub use cluster::{ClusterConfig, ClusterNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 7379,
        }
    }
}

pub fn server_banner(config: &ServerConfig) -> String {
    format!(
        "kayadb-server skeleton listening boundary: {}:{}",
        config.host, config.port
    )
}
