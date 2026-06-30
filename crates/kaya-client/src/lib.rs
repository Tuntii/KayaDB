use kaya_core::{KayaError, Result};
use kaya_net::{
    decode_error_payload, decode_hello_response, decode_scan_response, decode_value_payload,
    encode_client_auth_payload, encode_hello_request, encode_key_payload, encode_put_payload,
    encode_scan_payload, roundtrip, HELLO_OPCODE, PROTO_VERSION, STATUS_INVALID_ARGUMENT,
    STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
};

#[cfg(feature = "tls")]
use kaya_net::{roundtrip_tls, TlsConfig};
#[cfg(feature = "trace")]
use kaya_sim::{LinearizabilityChecker, Op, OpResult};
use std::net::SocketAddr;

pub struct KayaClient {
    addr: SocketAddr,
    max_redirects: usize,
    client_token: Option<String>,
    #[cfg(feature = "trace")]
    trace: Option<LinearizabilityChecker>,
    #[cfg(feature = "tls")]
    tls_config: Option<TlsConfig>,
}

impl KayaClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            addr,
            max_redirects: 3,
            client_token: None,
            #[cfg(feature = "trace")]
            trace: None,
            #[cfg(feature = "tls")]
            tls_config: None,
        })
    }

    #[cfg(feature = "tls")]
    pub async fn connect_tls(addr: SocketAddr, tls_config: TlsConfig) -> Result<Self> {
        Ok(Self {
            addr,
            max_redirects: 3,
            client_token: None,
            #[cfg(feature = "trace")]
            trace: None,
            tls_config: Some(tls_config),
        })
    }

    pub fn set_max_redirects(&mut self, max: usize) {
        self.max_redirects = max;
    }

    /// Set the client token used for data-path operations (PUT/GET/DELETE/SCAN/STATS).
    pub fn set_client_token(&mut self, token: impl Into<String>) {
        self.client_token = Some(token.into());
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Optional protocol version handshake (HELLO opcode 0).
    /// Returns the negotiated server protocol version on success.
    pub async fn handshake(&mut self) -> Result<u16> {
        let payload = encode_hello_request(PROTO_VERSION);
        let (status, body) = self.send_with_retry(HELLO_OPCODE, &payload).await?;
        if status == STATUS_OK {
            decode_hello_response(&body).map_err(KayaError::corruption)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            Err(KayaError::internal(msg))
        }
    }

    #[cfg(feature = "trace")]
    pub fn enable_tracing(&mut self) {
        self.trace = Some(LinearizabilityChecker::new());
    }

    #[cfg(feature = "trace")]
    pub fn disable_tracing(&mut self) {
        self.trace = None;
    }

    #[cfg(feature = "trace")]
    pub fn trace_len(&self) -> usize {
        self.trace.as_ref().map_or(0, |t| t.len())
    }

    #[cfg(feature = "trace")]
    pub fn take_trace(&mut self, seed: u64) -> Option<String> {
        self.trace.take().map(|checker| {
            let s = checker.to_trace_string(seed);
            self.trace = Some(LinearizabilityChecker::new());
            s
        })
    }

    #[cfg(feature = "trace")]
    pub fn check_trace(&self) -> Option<std::result::Result<(), Vec<String>>> {
        self.trace.as_ref().map(|c| c.check_sequential())
    }

    #[cfg(feature = "trace")]
    fn record(&mut self, op: Op, result: OpResult) {
        if let Some(ref mut checker) = self.trace {
            checker.record_next(op, result);
        }
    }

    fn wire_payload(&self, opcode: u8, payload: &[u8]) -> Vec<u8> {
        if matches!(opcode, 1..=4 | 6) {
            encode_client_auth_payload(payload, self.client_token.as_deref())
        } else {
            payload.to_vec()
        }
    }

    async fn send_with_retry(&mut self, opcode: u8, payload: &[u8]) -> Result<(u16, Vec<u8>)> {
        let wire_payload = self.wire_payload(opcode, payload);
        let mut current_addr = self.addr;
        let mut last_error = None;

        for attempt in 0..=self.max_redirects {
            #[cfg(feature = "tls")]
            let res: Result<(u16, Vec<u8>)> = if let Some(ref tls_cfg) = self.tls_config {
                roundtrip_tls(current_addr, opcode, &wire_payload, tls_cfg)
                    .await
                    .map_err(Into::into)
            } else {
                roundtrip(current_addr, opcode, &wire_payload)
                    .await
                    .map_err(Into::into)
            };
            #[cfg(not(feature = "tls"))]
            let res: Result<(u16, Vec<u8>)> = roundtrip(current_addr, opcode, &wire_payload)
                .await
                .map_err(Into::into);
            match res {
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
                                    self.addr = parsed_addr;
                                    continue;
                                }
                            }
                        }
                        if attempt < self.max_redirects {
                            tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                            continue;
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

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let payload = encode_put_payload(key, value);
        let (status, body) = self.send_with_retry(1, &payload).await?;
        if status == STATUS_OK {
            #[cfg(feature = "trace")]
            self.record(
                Op::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                OpResult::Ok,
            );
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::internal(msg))
        }
    }

    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let payload = encode_key_payload(key);
        let (status, body) = self.send_with_retry(2, &payload).await?;
        if status == STATUS_OK {
            let val = decode_value_payload(&body).map_err(KayaError::corruption)?;
            #[cfg(feature = "trace")]
            self.record(
                Op::Get { key: key.to_vec() },
                OpResult::Value(Some(val.clone())),
            );
            Ok(Some(val))
        } else if status == STATUS_NOT_FOUND {
            #[cfg(feature = "trace")]
            self.record(Op::Get { key: key.to_vec() }, OpResult::Value(None));
            Ok(None)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(Op::Get { key: key.to_vec() }, OpResult::Error(msg.clone()));
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(Op::Get { key: key.to_vec() }, OpResult::Error(msg.clone()));
            Err(KayaError::internal(msg))
        }
    }

    pub async fn delete(&mut self, key: &[u8]) -> Result<()> {
        let payload = encode_key_payload(key);
        let (status, body) = self.send_with_retry(3, &payload).await?;
        if status == STATUS_OK {
            #[cfg(feature = "trace")]
            self.record(Op::Delete { key: key.to_vec() }, OpResult::Ok);
            Ok(())
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Delete { key: key.to_vec() },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Delete { key: key.to_vec() },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::internal(msg))
        }
    }

    pub async fn scan(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let payload = encode_scan_payload(prefix);
        let (status, body) = self.send_with_retry(4, &payload).await?;
        if status == STATUS_OK {
            let items = decode_scan_response(&body).map_err(KayaError::corruption)?;
            #[cfg(feature = "trace")]
            self.record(
                Op::Scan {
                    prefix: prefix.to_vec(),
                },
                OpResult::Scan(items.clone()),
            );
            Ok(items)
        } else if status == STATUS_INVALID_ARGUMENT {
            let msg =
                decode_error_payload(&body).unwrap_or_else(|_| "invalid argument".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Scan {
                    prefix: prefix.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::invalid_argument(msg))
        } else {
            let msg = decode_error_payload(&body).unwrap_or_else(|_| "Unknown error".to_string());
            #[cfg(feature = "trace")]
            self.record(
                Op::Scan {
                    prefix: prefix.to_vec(),
                },
                OpResult::Error(msg.clone()),
            );
            Err(KayaError::internal(msg))
        }
    }

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
