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
