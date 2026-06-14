use kaya_core::Lsn;

/// Raft term number. Increases monotonically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

/// Index into the Raft log. 1-based; `LogIndex(0)` means "no entry".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogIndex(pub u64);

/// Unique identity of a node within a Raft cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

/// A command committed by Raft that the storage engine should apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftApplyCommand {
    pub term: Term,
    pub index: LogIndex,
    pub engine_lsn_hint: Option<Lsn>,
}

impl RaftApplyCommand {
    /// Serialize to a single JSONL line for append-only persistence.
    ///
    /// Format: `{"term":N,"index":N,"lsn":N}` or without `lsn` when absent.
    pub fn to_jsonl(&self) -> String {
        match self.engine_lsn_hint {
            Some(lsn) => format!(
                "{{\"term\":{},\"index\":{},\"lsn\":{}}}\n",
                self.term.0,
                self.index.0,
                lsn.get()
            ),
            None => format!(
                "{{\"term\":{},\"index\":{}}}\n",
                self.term.0, self.index.0
            ),
        }
    }

    /// Parse a JSONL line produced by [`RaftApplyCommand::to_jsonl`].
    pub fn from_jsonl(line: &str) -> Result<Self, String> {
        let line = line.trim();
        if !line.starts_with('{') {
            return Err("expected JSON object".to_owned());
        }
        let term = parse_json_u64(line, "\"term\":")?;
        let index = parse_json_u64(line, "\"index\":")?;
        let lsn = parse_json_u64(line, "\"lsn\":").ok().map(Lsn::new);
        Ok(Self {
            term: Term(term),
            index: LogIndex(index),
            engine_lsn_hint: lsn,
        })
    }
}

fn parse_json_u64(json: &str, key: &str) -> Result<u64, String> {
    let start = json
        .find(key)
        .ok_or_else(|| format!("missing key {key}"))?
        + key.len();
    let tail = &json[start..];
    let end = tail
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(tail.len());
    if end == 0 {
        return Err(format!("no value for {key}"));
    }
    tail[..end]
        .parse()
        .map_err(|e| format!("invalid number for {key}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_apply_command_jsonl_round_trip_with_lsn() {
        let cmd = RaftApplyCommand {
            term: Term(3),
            index: LogIndex(42),
            engine_lsn_hint: Some(Lsn::new(9001)),
        };
        let parsed = RaftApplyCommand::from_jsonl(&cmd.to_jsonl()).unwrap();
        assert_eq!(parsed, cmd);
    }

    #[test]
    fn raft_apply_command_jsonl_round_trip_without_lsn() {
        let cmd = RaftApplyCommand {
            term: Term(1),
            index: LogIndex(1),
            engine_lsn_hint: None,
        };
        let parsed = RaftApplyCommand::from_jsonl(&cmd.to_jsonl()).unwrap();
        assert_eq!(parsed, cmd);
    }
}
