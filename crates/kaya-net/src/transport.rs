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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use kaya_raft::{Envelope, NodeId};

use crate::codec::{decode_envelope, encode_envelope};
use crate::roster::NodeRoster;

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
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        accept_raft_loop(listener, tx).await;
    });
    Ok(bound)
}

async fn accept_raft_loop(listener: TcpListener, tx: mpsc::Sender<Envelope>) {
    loop {
        tokio::select! {
            _ = tx.closed() => {
                break;
            }
            incoming = listener.accept() => {
                match incoming {
                    Ok((mut stream, _peer_addr)) => {
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
async fn read_raft_envelope(stream: &mut TcpStream) -> std::io::Result<Envelope> {
    let len = stream.read_u32_le().await? as usize;
    if len > 32 * 1024 * 1024 {
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
pub async fn read_client_frame(stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let len = stream.read_u32_le().await? as usize;
    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty client frame",
        ));
    }
    if len > 64 * 1024 * 1024 {
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
pub async fn write_client_response(
    stream: &mut TcpStream,
    status: u16,
    payload: &[u8],
) -> std::io::Result<()> {
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
