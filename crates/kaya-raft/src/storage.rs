use crate::types::{LogIndex, NodeId, Term};
use kaya_core::crc32c;

// Wire layout (64 bytes, little-endian):
//   [0..4]   magic "HSKR"
//   [4..8]   version
//   [8..16]  current_term
//   [16..24] voted_for (0 = none)
//   [24..32] last_included_index
//   [32..40] last_included_term
//   [40..60] reserved (zero)
//   [60..64] crc32c of bytes [0..60]
pub const RAFT_HARD_STATE_MAGIC: u32 = 0x484B_5352; // "HSKR" LE
pub const RAFT_HARD_STATE_VERSION: u32 = 1;
pub const RAFT_HARD_STATE_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}

pub fn encode_hard_state(hs: &HardState) -> [u8; RAFT_HARD_STATE_LEN] {
    let mut buf = [0u8; RAFT_HARD_STATE_LEN];
    buf[0..4].copy_from_slice(&RAFT_HARD_STATE_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&RAFT_HARD_STATE_VERSION.to_le_bytes());
    buf[8..16].copy_from_slice(&hs.current_term.0.to_le_bytes());
    buf[16..24].copy_from_slice(&hs.voted_for.map(|n| n.0).unwrap_or(0).to_le_bytes());
    buf[24..32].copy_from_slice(&hs.last_included_index.0.to_le_bytes());
    buf[32..40].copy_from_slice(&hs.last_included_term.0.to_le_bytes());
    let crc = crc32c(&buf[..60]);
    buf[60..64].copy_from_slice(&crc.to_le_bytes());
    buf
}

pub fn decode_hard_state(bytes: &[u8]) -> Result<HardState, String> {
    if bytes.len() != RAFT_HARD_STATE_LEN {
        return Err(format!("hard-state wrong len: {}", bytes.len()));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != RAFT_HARD_STATE_MAGIC {
        return Err(format!("bad hard-state magic: {magic:#x}"));
    }
    let ver = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if ver != RAFT_HARD_STATE_VERSION {
        return Err(format!("unsupported hard-state version: {ver}"));
    }
    let crc = u32::from_le_bytes(bytes[60..64].try_into().unwrap());
    if crc32c(&bytes[..60]) != crc {
        return Err("hard-state crc mismatch".into());
    }
    let voted_raw = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    Ok(HardState {
        current_term: Term(u64::from_le_bytes(bytes[8..16].try_into().unwrap())),
        voted_for: if voted_raw == 0 {
            None
        } else {
            Some(NodeId(voted_raw))
        },
        last_included_index: LogIndex(u64::from_le_bytes(bytes[24..32].try_into().unwrap())),
        last_included_term: Term(u64::from_le_bytes(bytes[32..40].try_into().unwrap())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_state_roundtrip() {
        let hs = HardState {
            current_term: Term(7),
            voted_for: Some(NodeId(2)),
            last_included_index: LogIndex(100),
            last_included_term: Term(6),
        };
        let enc = encode_hard_state(&hs);
        assert_eq!(decode_hard_state(&enc).unwrap(), hs);
    }

    #[test]
    fn hard_state_roundtrip_voted_for_none() {
        let hs = HardState {
            current_term: Term(3),
            voted_for: None,
            last_included_index: LogIndex(42),
            last_included_term: Term(2),
        };
        let enc = encode_hard_state(&hs);
        assert_eq!(decode_hard_state(&enc).unwrap(), hs);
    }

    #[test]
    fn hard_state_rejects_wrong_length() {
        let enc = encode_hard_state(&HardState {
            current_term: Term(1),
            voted_for: None,
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        });
        let short = &enc[..RAFT_HARD_STATE_LEN - 1];
        let err = decode_hard_state(short).unwrap_err();
        assert!(err.contains("wrong len"));
    }

    #[test]
    fn hard_state_rejects_bad_magic() {
        let mut enc = encode_hard_state(&HardState {
            current_term: Term(1),
            voted_for: None,
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        });
        enc[0] ^= 0xFF;
        let err = decode_hard_state(&enc).unwrap_err();
        assert!(err.contains("bad hard-state magic"));
    }

    #[test]
    fn hard_state_rejects_unsupported_version() {
        let mut enc = encode_hard_state(&HardState {
            current_term: Term(1),
            voted_for: None,
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        });
        enc[4..8].copy_from_slice(&2u32.to_le_bytes());
        let err = decode_hard_state(&enc).unwrap_err();
        assert!(err.contains("unsupported hard-state version"));
    }

    #[test]
    fn hard_state_rejects_bad_crc() {
        let mut enc = encode_hard_state(&HardState {
            current_term: Term(1),
            voted_for: None,
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
        });
        enc[10] ^= 0xFF;
        let err = decode_hard_state(&enc).unwrap_err();
        assert!(err.contains("crc mismatch"));
    }
}