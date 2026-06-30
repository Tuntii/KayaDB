//! TCP transport for Raft messages and the client protocol.
//!
//! ## Raft transport
//!
//! Outgoing: [`send_envelopes`] — for each unique destination, opens a fresh
//! TCP connection, writes all frames for that peer, and closes.  Failures are
//! silently dropped; Raft handles message loss gracefully.
//!
//! Incoming: [`start_raft_listener`] — binds a TCP listener, spawns an accept
//! loop, and pushes decoded [`Envelope`]s onto the returned channel.
//!
//! ## Client protocol
//!
//! [`read_client_frame`] / [`write_client_response`] handle the framed
//! request/response exchange used by `kayactl --server`.
//!
//! Client request frame:  `frame_len(u32 LE) | opcode(u8) | payload`
//! Client response frame: `frame_len(u32 LE) | status(u16 LE) | payload`

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

#[cfg(feature = "tls")]
use std::sync::Arc;
#[cfg(feature = "tls")]
use tokio_rustls::rustls::ServerConfig;
#[cfg(feature = "tls")]
use tokio_rustls::{TlsAcceptor, TlsConnector};

use kaya_raft::{Envelope, NodeId};

use crate::codec::{decode_envelope, encode_envelope};
use crate::roster::NodeRoster;
use crate::DEFAULT_MAX_FRAME_LEN;

// ── response status codes ─────────────────────────────────────────────────────

/// The request was fulfilled successfully.
pub const STATUS_OK: u16 = 0;
/// The client request was invalid or malformed.
pub const STATUS_INVALID_ARGUMENT: u16 = 1;
/// GET / SCAN: the key was not found.
pub const STATUS_NOT_FOUND: u16 = 2;
/// The server encountered an error processing the request.
pub const STATUS_ERROR: u16 = 9;
/// This node is not the current Raft leader; the client should retry.
pub const STATUS_NOT_LEADER: u16 = 10;

/// TLS configuration for encrypted connections (Raft + client protocol).
///
/// When `ca_path` is Some and `require_client_cert` true, mTLS is used.
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: Option<PathBuf>,
    pub require_client_cert: bool,
}

#[cfg(feature = "tls")]
mod tls_impl {
    use super::*;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls_pemfile;
    use std::fs::File;
    use std::io::BufReader;

    pub(crate) fn load_certs(path: &PathBuf) -> std::io::Result<Vec<CertificateDer<'static>>> {
        let certfile = File::open(path)?;
        let mut reader = BufReader::new(certfile);
        rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad certs: {e}"))
            })
    }

    pub(crate) fn load_private_key(path: &PathBuf) -> std::io::Result<PrivateKeyDer<'static>> {
        let keyfile = File::open(path)?;
        let mut reader = BufReader::new(keyfile);
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, format!("bad key: {e}"))
            })?;
        keys.pop().map(PrivateKeyDer::Pkcs8).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "no private key found")
        })
    }

    pub(crate) async fn build_server_config(
        tls_config: &TlsConfig,
    ) -> std::io::Result<Arc<ServerConfig>> {
        let certs = load_certs(&tls_config.cert_path)?;
        let key = load_private_key(&tls_config.key_path)?;

        let mut config = if tls_config.require_client_cert {
            if let Some(ca_path) = &tls_config.ca_path {
                let ca_certs = load_certs(ca_path)?;
                let mut root_store = rustls::RootCertStore::empty();
                for cert in ca_certs {
                    root_store
                        .add(cert)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                }
                ServerConfig::builder()
                    .with_client_cert_verifier(
                        rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
                            .build()
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
                    )
                    .with_single_cert(certs, key)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
            } else {
                ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(certs, key)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
            }
        } else {
            ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
        };

        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }
}

// ── raft transport ────────────────────────────────────────────────────────────

/// Send a batch of [`Envelope`]s to their respective peers.
///
/// Envelopes are grouped by destination; one TCP connection is opened per peer.
/// All failures are silently dropped — Raft is designed to tolerate message
/// loss and will retransmit.
pub async fn send_envelopes(envelopes: Vec<Envelope>, roster: &NodeRoster) {
    if envelopes.is_empty() {
        return;
    }

    let mut by_dest: HashMap<NodeId, Vec<Envelope>> = HashMap::new();
    for env in envelopes {
        by_dest.entry(env.to).or_default().push(env);
    }

    for (node_id, envs) in by_dest {
        if let Some(addr) = roster.addr(node_id) {
            tokio::spawn(async move {
                let _ = send_to_addr(addr, &envs).await;
            });
        }
    }
}

async fn send_to_addr(addr: SocketAddr, envs: &[Envelope]) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    for env in envs {
        let frame = encode_envelope(env);
        stream.write_all(&frame).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Bind a TCP listener for incoming Raft messages.
///
/// Spawns a background task that accepts connections and forwards decoded
/// [`Envelope`]s to `tx`.  Returns the actual bound [`SocketAddr`] so the
/// caller can discover the port when `0` was requested.
pub async fn start_raft_listener(
    addr: SocketAddr,
    tx: mpsc::Sender<Envelope>,
    network_partitioned: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        accept_raft_loop(listener, tx, network_partitioned).await;
    });
    Ok(bound)
}

#[cfg(feature = "tls")]
pub async fn start_raft_listener_tls(
    addr: SocketAddr,
    tx: mpsc::Sender<Envelope>,
    tls_config: &TlsConfig,
    network_partitioned: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) -> std::io::Result<SocketAddr> {
    let server_config = tls_impl::build_server_config(tls_config).await?;
    let acceptor = TlsAcceptor::from(server_config);
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        accept_raft_loop_tls(listener, tx, acceptor, network_partitioned).await;
    });
    Ok(bound)
}

#[cfg(feature = "tls")]
async fn accept_raft_loop_tls(
    listener: TcpListener,
    tx: mpsc::Sender<Envelope>,
    acceptor: TlsAcceptor,
    network_partitioned: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    loop {
        tokio::select! {
            _ = tx.closed() => break,
            incoming = listener.accept() => {
                match incoming {
                    Ok((stream, _peer)) => {
                        if network_partitioned
                            .as_ref()
                            .is_some_and(|f| f.load(std::sync::atomic::Ordering::SeqCst))
                        {
                            drop(stream);
                            continue;
                        }
                        let tx = tx.clone();
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            match acceptor.accept(stream).await {
                                Ok(mut tls_stream) => {
                                    while let Ok(env) = read_raft_envelope(&mut tls_stream).await {
                                        if tx.send(env).await.is_err() { break; }
                                    }
                                }
                                Err(_) => {}
                            }
                        });
                    }
                    Err(_) => break,
                }
            }
        }
    }
}

async fn accept_raft_loop(
    listener: TcpListener,
    tx: mpsc::Sender<Envelope>,
    network_partitioned: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    loop {
        tokio::select! {
            _ = tx.closed() => {
                break;
            }
            incoming = listener.accept() => {
                match incoming {
                    Ok((mut stream, _peer_addr)) => {
                        if network_partitioned
                            .as_ref()
                            .is_some_and(|f| f.load(std::sync::atomic::Ordering::SeqCst))
                        {
                            drop(stream);
                            continue;
                        }
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            while let Ok(env) = read_raft_envelope(&mut stream).await {
                                if tx.send(env).await.is_err() {
                                    break; // receiver dropped → shut down
                                }
                            }
                        });
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }
    }
}

/// Read a single Raft envelope from `stream`.
///
/// Wire: `frame_len(u32 LE) | payload(frame_len bytes)`.
async fn read_raft_envelope<S>(stream: &mut S) -> std::io::Result<Envelope>
where
    S: AsyncReadExt + Unpin,
{
    let len = stream.read_u32_le().await? as usize;
    if len > DEFAULT_MAX_FRAME_LEN as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("raft frame too large: {len}"),
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    decode_envelope(&buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

// ── client protocol ───────────────────────────────────────────────────────────

/// Read one client request frame from `stream`.
///
/// Returns `(opcode, payload)`.
pub async fn read_client_frame<S>(stream: &mut S) -> std::io::Result<(u8, Vec<u8>)>
where
    S: AsyncReadExt + Unpin,
{
    let len = stream.read_u32_le().await? as usize;
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty client frame",
        ));
    }
    if len > DEFAULT_MAX_FRAME_LEN as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "client frame too large",
        ));
    }
    let opcode = stream.read_u8().await?;
    let payload_len = len - 1;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload).await?;
    }
    Ok((opcode, payload))
}

/// Write one client response frame to `stream`.
///
/// Frame: `frame_len(u32 LE) | status(u16 LE) | payload`.
pub async fn write_client_response<S>(
    stream: &mut S,
    status: u16,
    payload: &[u8],
) -> std::io::Result<()>
where
    S: AsyncWriteExt + Unpin,
{
    let frame_len = (2 + payload.len()) as u32;
    stream.write_u32_le(frame_len).await?;
    stream.write_u16_le(status).await?;
    if !payload.is_empty() {
        stream.write_all(payload).await?;
    }
    stream.flush().await?;
    Ok(())
}

/// Encode a client request frame into a byte buffer ready for writing to TCP.
///
/// Frame: `frame_len(u32 LE) | opcode(u8) | payload`.
pub fn encode_client_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let frame_len = (1 + payload.len()) as u32;
    let mut out = Vec::with_capacity(4 + 1 + payload.len());
    out.extend_from_slice(&frame_len.to_le_bytes());
    out.push(opcode);
    out.extend_from_slice(payload);
    out
}

/// Connect to a server, send one request frame, and read the response.
///
/// Returns `(status, payload)`.
pub async fn roundtrip(
    server_addr: SocketAddr,
    opcode: u8,
    payload: &[u8],
) -> std::io::Result<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect(server_addr).await?;
    let frame = encode_client_frame(opcode, payload);
    stream.write_all(&frame).await?;
    stream.flush().await?;
    // Read response
    let resp_len = stream.read_u32_le().await? as usize;
    if resp_len < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response frame too short",
        ));
    }
    let status = stream.read_u16_le().await?;
    let body_len = resp_len - 2;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        stream.read_exact(&mut body).await?;
    }
    Ok((status, body))
}

#[cfg(feature = "tls")]
pub async fn roundtrip_tls(
    server_addr: SocketAddr,
    opcode: u8,
    payload: &[u8],
    tls_config: &TlsConfig,
) -> std::io::Result<(u16, Vec<u8>)> {
    let root_store = if let Some(ca) = &tls_config.ca_path {
        let cas = tls_impl::load_certs(ca)?;
        let mut store = rustls::RootCertStore::empty();
        for c in cas {
            let _ = store.add(c);
        }
        store
    } else {
        // Fallback: empty, will fail for self-signed unless ca provided. For tests provide ca.
        rustls::RootCertStore::empty()
    };

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));
    let domain = rustls::pki_types::ServerName::try_from("localhost")
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid dns name"))?
        .to_owned();

    let tcp = TcpStream::connect(server_addr).await?;
    let mut tls_stream = connector.connect(domain, tcp).await?;

    let frame = encode_client_frame(opcode, payload);
    tls_stream.write_all(&frame).await?;
    tls_stream.flush().await?;

    let resp_len = tls_stream.read_u32_le().await? as usize;
    if resp_len < 2 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "response frame too short",
        ));
    }
    let status = tls_stream.read_u16_le().await?;
    let body_len = resp_len - 2;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        tls_stream.read_exact(&mut body).await?;
    }
    Ok((status, body))
}

#[cfg(feature = "tls")]
pub async fn send_envelopes_tls(
    envelopes: Vec<Envelope>,
    roster: &NodeRoster,
    tls_config: &TlsConfig,
) {
    if envelopes.is_empty() {
        return;
    }
    let mut by_dest: HashMap<NodeId, Vec<Envelope>> = HashMap::new();
    for env in envelopes {
        by_dest.entry(env.to).or_default().push(env);
    }

    for (node_id, envs) in by_dest {
        if let Some(addr) = roster.addr(node_id) {
            let tls_cfg = tls_config.clone();
            tokio::spawn(async move {
                let _ = send_to_addr_tls(addr, &envs, &tls_cfg).await;
            });
        }
    }
}

#[cfg(feature = "tls")]
async fn send_to_addr_tls(
    addr: SocketAddr,
    envs: &[Envelope],
    tls_config: &TlsConfig,
) -> std::io::Result<()> {
    let root_store = if let Some(ca) = &tls_config.ca_path {
        let cas = tls_impl::load_certs(ca)?;
        let mut store = rustls::RootCertStore::empty();
        for c in cas {
            let _ = store.add(c);
        }
        store
    } else {
        rustls::RootCertStore::empty()
    };

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let domain = rustls::pki_types::ServerName::try_from("localhost")
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad name"))?
        .to_owned();

    let tcp = TcpStream::connect(addr).await?;
    let mut tls_stream = connector.connect(domain, tcp).await?;

    for env in envs {
        let frame = encode_envelope(env);
        tls_stream.write_all(&frame).await?;
    }
    tls_stream.flush().await?;
    Ok(())
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

    fn duplex_pair(buffer: usize) -> (DuplexStream, DuplexStream) {
        tokio::io::duplex(buffer)
    }

    #[test]
    fn raft_listener_partition_flag_parameter_parity() {
        use std::sync::atomic::AtomicBool;
        use std::sync::Arc;
        type PartitionFlag = Option<Arc<AtomicBool>>;
        let _plain: fn(SocketAddr, mpsc::Sender<Envelope>, PartitionFlag) -> _ =
            start_raft_listener;
        #[cfg(feature = "tls")]
        {
            let _tls: fn(SocketAddr, mpsc::Sender<Envelope>, &TlsConfig, PartitionFlag) -> _ =
                start_raft_listener_tls;
        }
    }

    #[test]
    fn encode_client_frame_empty_payload() {
        let frame = encode_client_frame(5, &[]);
        assert_eq!(&frame[..4], &1u32.to_le_bytes());
        assert_eq!(frame[4], 5);
        assert_eq!(frame.len(), 5);
    }

    #[test]
    fn encode_client_frame_with_payload() {
        let frame = encode_client_frame(1, b"hello");
        assert_eq!(&frame[..4], &6u32.to_le_bytes());
        assert_eq!(frame[4], 1);
        assert_eq!(&frame[5..], b"hello");
    }

    #[tokio::test]
    async fn read_client_frame_valid() {
        let (mut client, mut server) = duplex_pair(64);
        let frame = encode_client_frame(2, b"key");
        client.write_all(&frame).await.unwrap();

        let (opcode, payload) = read_client_frame(&mut server).await.unwrap();
        assert_eq!(opcode, 2);
        assert_eq!(payload, b"key");
    }

    #[tokio::test]
    async fn read_client_frame_empty_rejected() {
        let (mut client, mut server) = duplex_pair(64);
        client.write_all(&0u32.to_le_bytes()).await.unwrap();

        let result = read_client_frame(&mut server).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_client_frame_oversized_rejected() {
        let (mut client, mut server) = duplex_pair(64);
        let huge_len: u32 = 65 * 1024 * 1024;
        client.write_all(&huge_len.to_le_bytes()).await.unwrap();

        let result = read_client_frame(&mut server).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn read_client_frame_truncated_payload() {
        let (mut client, mut server) = duplex_pair(64);
        client.write_all(&10u32.to_le_bytes()).await.unwrap();
        client.write_all(&[1u8]).await.unwrap();
        client.write_all(b"short").await.unwrap();
        drop(client);

        let result = read_client_frame(&mut server).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn write_and_read_response_roundtrip() {
        let (mut client, mut server) = duplex_pair(64);
        write_client_response(&mut server, STATUS_OK, b"ok")
            .await
            .unwrap();

        let resp_len = client.read_u32_le().await.unwrap() as usize;
        let status = client.read_u16_le().await.unwrap();
        let body_len = resp_len - 2;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.unwrap();

        assert_eq!(status, STATUS_OK);
        assert_eq!(body, b"ok");
    }

    #[tokio::test]
    async fn write_response_empty_payload() {
        let (mut client, mut server) = duplex_pair(64);
        write_client_response(&mut server, STATUS_NOT_FOUND, &[])
            .await
            .unwrap();

        let resp_len = client.read_u32_le().await.unwrap() as usize;
        let status = client.read_u16_le().await.unwrap();

        assert_eq!(resp_len, 2);
        assert_eq!(status, STATUS_NOT_FOUND);
    }

    #[tokio::test]
    async fn roundtrip_client_server() {
        let (mut client, mut server) = duplex_pair(128);
        let server_task = tokio::spawn(async move {
            let (opcode, _payload) = read_client_frame(&mut server).await.unwrap();
            assert_eq!(opcode, 5);
            write_client_response(&mut server, STATUS_OK, b"leader")
                .await
                .unwrap();
        });

        let frame = encode_client_frame(5, &[]);
        client.write_all(&frame).await.unwrap();
        let resp_len = client.read_u32_le().await.unwrap() as usize;
        let status = client.read_u16_le().await.unwrap();
        let body_len = resp_len - 2;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.unwrap();

        assert_eq!(status, STATUS_OK);
        assert_eq!(body, b"leader");
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn roundtrip_connection_refused() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let result = roundtrip(addr, 1, &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_client_frame_opcode_only() {
        let (mut client, mut server) = duplex_pair(64);
        let frame = encode_client_frame(5, &[]);
        client.write_all(&frame).await.unwrap();

        let (opcode, payload) = read_client_frame(&mut server).await.unwrap();
        assert_eq!(opcode, 5);
        assert!(payload.is_empty());
    }

    #[tokio::test]
    async fn read_client_frame_multiple() {
        let (mut client, mut server) = duplex_pair(128);
        let frame1 = encode_client_frame(1, b"put");
        let frame2 = encode_client_frame(2, b"get");
        client.write_all(&frame1).await.unwrap();
        client.write_all(&frame2).await.unwrap();

        let f1 = read_client_frame(&mut server).await.unwrap();
        let f2 = read_client_frame(&mut server).await.unwrap();
        assert_eq!(f1.0, 1);
        assert_eq!(f1.1, b"put");
        assert_eq!(f2.0, 2);
        assert_eq!(f2.1, b"get");
    }
}
