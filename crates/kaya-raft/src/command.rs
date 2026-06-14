//! [`RaftCommand`]: replicated write operations stored in Raft log entries.
//!
//! Wire format:
//!
//! ```text
//! type      : u8       (1 = Put, 2 = Delete, 3 = ConfigChange)
//! key_len   : u32 LE
//! key       : bytes
//! [Put only]
//! value_len : u32 LE
//! value     : bytes
//! [ConfigChange]
//! phase     : u8
//! member_count : u32 LE
//! per member: id(u64) | raft_len(u32) | raft | client_len(u32) | client
//! ```

/// Phase of a membership configuration change (joint consensus).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChangePhase {
    /// Joint configuration: quorum required in both old and new voter sets.
    Joint = 1,
    /// Final configuration: only the new voter set remains.
    Final = 2,
}

/// A voting member with network endpoints (replicated in config-change entries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMember {
    pub id: crate::NodeId,
    pub raft_addr: String,
    pub client_addr: String,
}

impl ClusterMember {
    pub fn voter_ids(members: &[ClusterMember]) -> Vec<crate::NodeId> {
        members.iter().map(|m| m.id).collect()
    }
}

/// A replicated command stored in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftCommand {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    ConfigChange {
        phase: ConfigChangePhase,
        members: Vec<ClusterMember>,
    },
}

impl RaftCommand {
    /// Encode this command to bytes for storage in a Raft log entry.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            RaftCommand::Put { key, value } => {
                out.push(1u8);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
            RaftCommand::Delete { key } => {
                out.push(2u8);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
            }
            RaftCommand::ConfigChange { phase, members } => {
                out.push(3u8);
                out.push(*phase as u8);
                out.extend_from_slice(&(members.len() as u32).to_le_bytes());
                for member in members {
                    out.extend_from_slice(&member.id.0.to_le_bytes());
                    push_string(&mut out, &member.raft_addr);
                    push_string(&mut out, &member.client_addr);
                }
            }
        }
        out
    }

    /// Decode a command from bytes.
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        let mut cur = data;
        let cmd_type = next_u8(&mut cur)?;
        match cmd_type {
            1 => {
                let key = next_bytes(&mut cur)?;
                let value = next_bytes(&mut cur)?;
                Ok(RaftCommand::Put { key, value })
            }
            2 => {
                let key = next_bytes(&mut cur)?;
                Ok(RaftCommand::Delete { key })
            }
            3 => {
                let phase_byte = next_u8(&mut cur)?;
                let phase = match phase_byte {
                    1 => ConfigChangePhase::Joint,
                    2 => ConfigChangePhase::Final,
                    p => return Err(format!("unknown ConfigChangePhase: {p}")),
                };
                let count = next_u32(&mut cur)? as usize;
                if cur.len() == count * 8 {
                    // Legacy voters-only encoding (no address metadata).
                    let mut members = Vec::with_capacity(count);
                    for _ in 0..count {
                        members.push(ClusterMember {
                            id: crate::NodeId(next_u64(&mut cur)?),
                            raft_addr: String::new(),
                            client_addr: String::new(),
                        });
                    }
                    return Ok(RaftCommand::ConfigChange { phase, members });
                }
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    let id = crate::NodeId(next_u64(&mut cur)?);
                    let raft_addr = next_string(&mut cur)?;
                    let client_addr = next_string(&mut cur)?;
                    members.push(ClusterMember {
                        id,
                        raft_addr,
                        client_addr,
                    });
                }
                Ok(RaftCommand::ConfigChange { phase, members })
            }
            t => Err(format!("unknown RaftCommand type: {t}")),
        }
    }
}

fn push_string(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn next_string(cur: &mut &[u8]) -> Result<String, String> {
    let bytes = next_bytes(cur)?;
    String::from_utf8(bytes).map_err(|e| format!("invalid utf-8 in member address: {e}"))
}

fn next_u8(cur: &mut &[u8]) -> Result<u8, String> {
    if cur.is_empty() {
        return Err("unexpected EOF reading u8".to_owned());
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn next_u64(cur: &mut &[u8]) -> Result<u64, String> {
    if cur.len() < 8 {
        return Err("unexpected EOF reading u64".to_owned());
    }
    let v = u64::from_le_bytes([cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7]]);
    *cur = &cur[8..];
    Ok(v)
}

fn next_u32(cur: &mut &[u8]) -> Result<u32, String> {
    if cur.len() < 4 {
        return Err(format!(
            "unexpected EOF reading u32 (have {} bytes)",
            cur.len()
        ));
    }
    let v = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]);
    *cur = &cur[4..];
    Ok(v)
}

fn next_bytes(cur: &mut &[u8]) -> Result<Vec<u8>, String> {
    let len = next_u32(cur)? as usize;
    if cur.len() < len {
        return Err(format!(
            "truncated data: need {len} bytes, have {}",
            cur.len()
        ));
    }
    let bytes = cur[..len].to_vec();
    *cur = &cur[len..];
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_put() {
        let cmd = RaftCommand::Put {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn round_trip_delete() {
        let cmd = RaftCommand::Delete {
            key: b"some_key".to_vec(),
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn round_trip_config_change_with_addresses() {
        let cmd = RaftCommand::ConfigChange {
            phase: ConfigChangePhase::Joint,
            members: vec![
                ClusterMember {
                    id: crate::NodeId(1),
                    raft_addr: "127.0.0.1:7481".to_owned(),
                    client_addr: "127.0.0.1:7379".to_owned(),
                },
                ClusterMember {
                    id: crate::NodeId(4),
                    raft_addr: "127.0.0.1:7484".to_owned(),
                    client_addr: "127.0.0.1:7383".to_owned(),
                },
            ],
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn decode_legacy_voters_only_config_change() {
        let mut legacy = vec![3u8, ConfigChangePhase::Final as u8];
        legacy.extend_from_slice(&2u32.to_le_bytes());
        legacy.extend_from_slice(&1u64.to_le_bytes());
        legacy.extend_from_slice(&2u64.to_le_bytes());
        let decoded = RaftCommand::decode(&legacy).unwrap();
        assert!(matches!(
            decoded,
            RaftCommand::ConfigChange {
                phase: ConfigChangePhase::Final,
                members,
            } if members.len() == 2
                && members[0].id == crate::NodeId(1)
                && members[0].raft_addr.is_empty()
        ));
    }
}