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
