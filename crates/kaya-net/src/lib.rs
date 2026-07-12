pub mod codec;
pub mod roster;
pub mod transport;

pub use codec::{
    decode_admin_payload, decode_client_auth_payload, decode_envelope, decode_error_payload,
    decode_hello_request, decode_hello_response, decode_key_payload, decode_member_payload,
    decode_put_payload, decode_remove_member_payload, decode_scan_payload, decode_scan_response,
    decode_txn_begin_response, decode_txn_commit_response, decode_txn_id_payload,
    decode_txn_op_payload, decode_value_payload, encode_admin_payload, encode_client_auth_payload,
    encode_envelope, encode_error_payload, encode_hello_request, encode_hello_response,
    encode_key_payload, encode_member_payload, encode_put_payload, encode_remove_member_payload,
    encode_scan_payload, encode_scan_response, encode_txn_begin_response,
    encode_txn_commit_response, encode_txn_id_payload, encode_txn_op_payload, encode_value_payload,
    ADD_MEMBER_OPCODE, ADMIN_AUTH_PREFIX, CLIENT_AUTH_PREFIX, HELLO_OPCODE, PROTO_VERSION,
    REMOVE_MEMBER_OPCODE, TXN_BEGIN_OPCODE, TXN_COMMIT_OPCODE, TXN_OP_DELETE, TXN_OP_GET,
    TXN_OP_OPCODE, TXN_OP_PUT, TXN_ROLLBACK_OPCODE,
};
pub use roster::NodeRoster;
pub use transport::{
    encode_client_frame, read_client_frame, request_on_stream, roundtrip, send_envelopes,
    start_raft_listener, write_client_response, TlsConfig, STATUS_ERROR, STATUS_INVALID_ARGUMENT,
    STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK, STATUS_TXN_CONFLICT,
};

#[cfg(feature = "tls")]
pub use transport::{roundtrip_tls, send_envelopes_tls, start_raft_listener_tls};

use kaya_core::{KayaError, Result};

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 7379;
pub const DEFAULT_MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Hello = 0,
    Put = 1,
    Get = 2,
    Delete = 3,
    Scan = 4,
    Health = 5,
    Stats = 6,
    AddMember = 7,
    RemoveMember = 8,
    TxnBegin = 9,
    TxnOp = 10,
    TxnCommit = 11,
    TxnRollback = 12,
}

impl Opcode {
    pub fn from_wire(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Hello),
            1 => Ok(Self::Put),
            2 => Ok(Self::Get),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Scan),
            5 => Ok(Self::Health),
            6 => Ok(Self::Stats),
            7 => Ok(Self::AddMember),
            8 => Ok(Self::RemoveMember),
            9 => Ok(Self::TxnBegin),
            10 => Ok(Self::TxnOp),
            11 => Ok(Self::TxnCommit),
            12 => Ok(Self::TxnRollback),
            _ => Err(KayaError::invalid_argument(format!(
                "unknown protocol opcode: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub opcode: Opcode,
    pub payload: Vec<u8>,
}

pub fn validate_frame_len(frame_len: u32, max_frame_len: u32) -> Result<()> {
    if frame_len > max_frame_len {
        return Err(KayaError::invalid_argument(format!(
            "frame length {frame_len} exceeds max {max_frame_len}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzz_codec_no_panic() {
        let cases: &[&[u8]] = &[
            b"",
            &[0u8; 1],
            &[0u8; 8],
            &[0u8; 17],
            &[0xffu8; 100],
            b"\x00\x00\x00\x00\x00\x00\x00\x00garbage",
            b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ];
        for input in cases {
            let _ = decode_envelope(input);
            let _ = decode_hello_request(input);
            let _ = decode_hello_response(input);
            let _ = decode_put_payload(input);
            let _ = decode_key_payload(input);
            let _ = decode_scan_response(input);
            let _ = decode_error_payload(input);
            let _ = decode_value_payload(input);
            let _ = decode_txn_begin_response(input);
            let _ = decode_txn_op_payload(input);
            let _ = decode_txn_id_payload(input);
            let _ = decode_txn_commit_response(input);
        }
    }
}
