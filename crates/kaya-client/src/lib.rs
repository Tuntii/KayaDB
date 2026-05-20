use kaya_core::{KayaError, Result};
use kaya_net::{
    decode_error_payload, decode_scan_response, decode_value_payload, encode_key_payload,
    encode_put_payload, encode_scan_payload, roundtrip, STATUS_NOT_FOUND, STATUS_NOT_LEADER,
    STATUS_OK,
};
use std::net::SocketAddr;

/// An ergonomic async client for interacting with a KayaDB Raft cluster.
///
/// `KayaClient` connects over TCP and automatically handles leader discovery
/// and retry routing. If a query is sent to a follower node, the follower's
/// redirection hint is captured, and the client transparently reconnects to
/// the active leader and retries the command.
pub struct KayaClient {
    addr: SocketAddr,
    max_redirects: usize,
}

impl KayaClient {
    /// Create a new client pointing to a node in the KayaDB cluster.
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            addr,
            max_redirects: 3,
        })
    }

    /// Set the maximum number of leader redirection retries. Defaults to 3.
    pub fn set_max_redirects(&mut self, max: usize) {
        self.max_redirects = max;
    }

    /// Get the current active socket address being communicated with.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    async fn send_with_retry(&mut self, opcode: u8, payload: &[u8]) -> Result<(u16, Vec<u8>)> {
        let mut current_addr = self.addr;
        let mut last_error = None;

        for attempt in 0..=self.max_redirects {
            match roundtrip(current_addr, opcode, payload).await {
                Ok((status, body)) => {
                    if status == STATUS_NOT_LEADER {
                        if let Ok(hint_str) = std::str::from_utf8(&body) {
                            let hint_str = hint_str.trim();
                            if !hint_str.is_empty() {
                                if let Ok(parsed_addr) = hint_str.parse::<SocketAddr>() {
                                    eprintln!(
                                        "Redirecting to leader at {} (attempt {}/{})",
                                        parsed_addr,
                                        attempt + 1,
                                        self.max_redirects
                                    );
                                    current_addr = parsed_addr;
                                    self.addr = parsed_addr; // Cache the new leader address
                                    continue;
                                }
                            }
                        }
                    }
                    return Ok((status, body));
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(KayaError::internal(format!(
            "Cluster request failed after {} attempts. Last connection error: {:?}",
            self.max_redirects + 1,
            last_error
        )))
    }

    /// Write a key-value pair to the database.
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let payload = encode_put_payload(key, value);
        let (status, body) = self.send_with_retry(1, &payload).await?;
        if status == STATUS_OK {
            Ok(())
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Read the value associated with a key from the database.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let payload = encode_key_payload(key);
        let (status, body) = self.send_with_retry(2, &payload).await?;
        if status == STATUS_OK {
            let val = decode_value_payload(&body).map_err(KayaError::corruption)?;
            Ok(Some(val))
        } else if status == STATUS_NOT_FOUND {
            Ok(None)
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Remove a key-value pair from the database.
    pub async fn delete(&mut self, key: &[u8]) -> Result<()> {
        let payload = encode_key_payload(key);
        let (status, body) = self.send_with_retry(3, &payload).await?;
        if status == STATUS_OK {
            Ok(())
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Scan all active keys starting with the specified prefix.
    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let payload = encode_scan_payload(prefix);
        let (status, body) = self.send_with_retry(4, &payload).await?;
        if status == STATUS_OK {
            let items = decode_scan_response(&body).map_err(KayaError::corruption)?;
            Ok(items)
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Query the health role of the target node (e.g. returns "leader" or "follower").
    pub async fn health(&mut self) -> Result<String> {
        let (status, body) = self.send_with_retry(5, &[]).await?;
        if status == STATUS_OK {
            let s = String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
            Ok(s)
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    /// Query comprehensive node statistics (Raft + LSM Engine metrics) in JSON.
    pub async fn stats(&mut self) -> Result<String> {
        let (status, body) = self.send_with_retry(6, &[]).await?;
        if status == STATUS_OK {
            let s = String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
            Ok(s)
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }
}
