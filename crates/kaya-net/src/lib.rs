pub mod codec;
pub mod roster;
pub mod transport;

pub use codec::{
    decode_envelope, decode_error_payload, decode_key_payload, decode_put_payload,
    decode_scan_payload, decode_scan_response, decode_value_payload, encode_envelope,
    encode_error_payload, encode_key_payload, encode_put_payload, encode_scan_payload,
    encode_scan_response, encode_value_payload,
};
pub use roster::NodeRoster;
pub use transport::{
    encode_client_frame, read_client_frame, roundtrip, send_envelopes, start_raft_listener,
    write_client_response, STATUS_ERROR, STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
};

use kaya_core::{KayaError, Result};

pub const DEFAULT_HOST: &str = "127.0.0.1";
pub const DEFAULT_PORT: u16 = 7379;
pub const DEFAULT_MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Put = 1,
    Get = 2,
    Delete = 3,
    Scan = 4,
    Health = 5,
    Stats = 6,
}

impl Opcode {
    pub fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Get),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Scan),
            5 => Ok(Self::Health),
            6 => Ok(Self::Stats),
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
