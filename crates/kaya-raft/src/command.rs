//! [`RaftCommand`]: replicated write operations stored in Raft log entries.
//!
//! Wire format:
//!
//! ```text
//! type      : u8       (1 = Put, 2 = Delete, 3 = ConfigChange, 4 = TxnCommit)
//! key_len   : u32 LE
//! key       : bytes
//! [Put only]
//! value_len : u32 LE
//! value     : bytes
//! [ConfigChange]
//! phase     : u8
//! member_count : u32 LE
//! per member: id(u64) | raft_len(u32) | raft | client_len(u32) | client | [is_learner u8]
//!             (is_learner is trailing; omitted in legacy logs → voter)
//! [TxnCommit]
//! txn_id    : u64 LE
//! count     : u32 LE
//! per mutation:
//!   key_len u32 | key | has_value u8 (0/1) | [value_len u32 | value if has_value]
//! ```

/// Phase of a membership configuration change (joint consensus).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChangePhase {
    /// Joint configuration: quorum required in both old and new voter sets.
    Joint = 1,
    /// Final configuration: only the new voter set remains.
    Final = 2,
}

/// A cluster member with network endpoints (replicated in config-change entries).
///
/// Learners receive the log but do not vote or count toward quorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMember {
    pub id: crate::NodeId,
    pub raft_addr: String,
    pub client_addr: String,
    /// When `true`, the member is a non-voting learner replica.
    pub is_learner: bool,
}

impl ClusterMember {
    /// Voting member ids only (learners excluded).
    pub fn voter_ids(members: &[ClusterMember]) -> Vec<crate::NodeId> {
        members
            .iter()
            .filter(|m| !m.is_learner)
            .map(|m| m.id)
            .collect()
    }

    /// Convenience constructor for a voting member.
    pub fn voter(id: crate::NodeId, raft_addr: impl Into<String>, client_addr: impl Into<String>) -> Self {
        Self {
            id,
            raft_addr: raft_addr.into(),
            client_addr: client_addr.into(),
            is_learner: false,
        }
    }

    /// Convenience constructor for a non-voting learner.
    pub fn learner(
        id: crate::NodeId,
        raft_addr: impl Into<String>,
        client_addr: impl Into<String>,
    ) -> Self {
        Self {
            id,
            raft_addr: raft_addr.into(),
            client_addr: client_addr.into(),
            is_learner: true,
        }
    }
}

/// A replicated command stored in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftCommand {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    ConfigChange {
        phase: ConfigChangePhase,
        members: Vec<ClusterMember>,
    },
    /// Atomic multi-key transaction commit (type byte 4).
    ///
    /// Applied as one Raft log entry so all mutations become durable together
    /// w.r.t. other Raft applies (all-or-nothing at the consensus layer).
    /// `txn_id` is informational for logs/debugging; followers do not need an
    /// open local transaction to apply the mutations.
    TxnCommit {
        txn_id: u64,
        /// `(key, Some(value))` = put; `(key, None)` = delete.
        mutations: Vec<(Vec<u8>, Option<Vec<u8>>)>,
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
                    // Trailing learner flag (new format). Always written on encode.
                    out.push(if member.is_learner { 1u8 } else { 0u8 });
                }
            }
            RaftCommand::TxnCommit { txn_id, mutations } => {
                out.push(4u8);
                out.extend_from_slice(&txn_id.to_le_bytes());
                out.extend_from_slice(&(mutations.len() as u32).to_le_bytes());
                for (key, value) in mutations {
                    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    out.extend_from_slice(key);
                    match value {
                        Some(v) => {
                            out.push(1u8);
                            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                            out.extend_from_slice(v);
                        }
                        None => {
                            out.push(0u8);
                        }
                    }
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
                            is_learner: false,
                        });
                    }
                    return Ok(RaftCommand::ConfigChange { phase, members });
                }
                // Prefer new format (trailing is_learner u8 per member). Fall back
                // to address-only encoding used by older logs (all voters).
                let snapshot = cur;
                if let Ok(members) = decode_members_with_addrs(snapshot, count, true) {
                    return Ok(RaftCommand::ConfigChange { phase, members });
                }
                let members = decode_members_with_addrs(snapshot, count, false)?;
                Ok(RaftCommand::ConfigChange { phase, members })
            }
            4 => {
                let txn_id = next_u64(&mut cur)?;
                let count = next_u32(&mut cur)? as usize;
                let mut mutations = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = next_bytes(&mut cur)?;
                    let has_value = next_u8(&mut cur)?;
                    let value = match has_value {
                        0 => None,
                        1 => Some(next_bytes(&mut cur)?),
                        other => {
                            return Err(format!(
                                "invalid TxnCommit has_value flag: {other} (expected 0 or 1)"
                            ));
                        }
                    };
                    mutations.push((key, value));
                }
                Ok(RaftCommand::TxnCommit { txn_id, mutations })
            }
            t => Err(format!("unknown RaftCommand type: {t}")),
        }
    }
}

/// Decode `count` members from address-bearing wire form.
/// When `with_learner_flag` is true each member ends with a trailing u8 flag.
fn decode_members_with_addrs(
    data: &[u8],
    count: usize,
    with_learner_flag: bool,
) -> Result<Vec<ClusterMember>, String> {
    let mut cur = data;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        let id = crate::NodeId(next_u64(&mut cur)?);
        let raft_addr = next_string(&mut cur)?;
        let client_addr = next_string(&mut cur)?;
        let is_learner = if with_learner_flag {
            next_u8(&mut cur)? != 0
        } else {
            false
        };
        members.push(ClusterMember {
            id,
            raft_addr,
            client_addr,
            is_learner,
        });
    }
    // ConfigChange has no trailing payload after members; require full consume
    // so the with/without-flag probe can disambiguate formats.
    if !cur.is_empty() {
        return Err("trailing bytes after members".to_owned());
    }
    Ok(members)
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
    let v = u64::from_le_bytes([
        cur[0], cur[1], cur[2], cur[3], cur[4], cur[5], cur[6], cur[7],
    ]);
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
                ClusterMember::voter(
                    crate::NodeId(1),
                    "127.0.0.1:7481",
                    "127.0.0.1:7379",
                ),
                ClusterMember::voter(
                    crate::NodeId(4),
                    "127.0.0.1:7484",
                    "127.0.0.1:7383",
                ),
            ],
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn round_trip_config_change_with_learner() {
        let cmd = RaftCommand::ConfigChange {
            phase: ConfigChangePhase::Final,
            members: vec![
                ClusterMember::voter(crate::NodeId(1), "127.0.0.1:7481", "127.0.0.1:7379"),
                ClusterMember::learner(crate::NodeId(4), "127.0.0.1:7484", "127.0.0.1:7383"),
            ],
        };
        let encoded = cmd.encode();
        let decoded = RaftCommand::decode(&encoded).unwrap();
        assert_eq!(decoded, cmd);
        if let RaftCommand::ConfigChange { members, .. } = decoded {
            assert!(!members[0].is_learner);
            assert!(members[1].is_learner);
            assert_eq!(
                ClusterMember::voter_ids(&members),
                vec![crate::NodeId(1)]
            );
        }
    }

    #[test]
    fn decode_legacy_address_members_as_voters() {
        // Old wire: id | raft | client  (no trailing learner flag).
        let mut legacy = vec![3u8, ConfigChangePhase::Final as u8];
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&1u64.to_le_bytes());
        push_string(&mut legacy, "127.0.0.1:7481");
        push_string(&mut legacy, "127.0.0.1:7379");
        let decoded = RaftCommand::decode(&legacy).unwrap();
        match decoded {
            RaftCommand::ConfigChange { members, .. } => {
                assert_eq!(members.len(), 1);
                assert_eq!(members[0].id, crate::NodeId(1));
                assert_eq!(members[0].raft_addr, "127.0.0.1:7481");
                assert!(!members[0].is_learner, "legacy members decode as voters");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn voter_ids_excludes_learners() {
        let members = vec![
            ClusterMember::voter(crate::NodeId(1), "a", "b"),
            ClusterMember::learner(crate::NodeId(2), "c", "d"),
            ClusterMember::voter(crate::NodeId(3), "e", "f"),
        ];
        assert_eq!(
            ClusterMember::voter_ids(&members),
            vec![crate::NodeId(1), crate::NodeId(3)]
        );
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
                && !members[0].is_learner
        ));
    }

    #[test]
    fn round_trip_txn_commit_put_and_delete() {
        let cmd = RaftCommand::TxnCommit {
            txn_id: 42,
            mutations: vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), None),
                (b"c".to_vec(), Some(b"three".to_vec())),
            ],
        };
        let encoded = cmd.encode();
        assert_eq!(encoded[0], 4u8, "TxnCommit type byte must be 4");
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn round_trip_txn_commit_empty_mutations() {
        let cmd = RaftCommand::TxnCommit {
            txn_id: 7,
            mutations: vec![],
        };
        let encoded = cmd.encode();
        assert_eq!(encoded[0], 4u8);
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn txn_commit_decode_rejects_bad_has_value() {
        let mut bad = vec![4u8];
        bad.extend_from_slice(&1u64.to_le_bytes()); // txn_id
        bad.extend_from_slice(&1u32.to_le_bytes()); // count
        bad.extend_from_slice(&1u32.to_le_bytes()); // key_len
        bad.push(b'k');
        bad.push(2u8); // invalid has_value
        let err = RaftCommand::decode(&bad).unwrap_err();
        assert!(err.contains("has_value"), "err={err}");
    }
}
