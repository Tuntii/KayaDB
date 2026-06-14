//! Static cluster membership: maps [`NodeId`] to a [`SocketAddr`].

use std::collections::HashMap;
use std::net::SocketAddr;

use kaya_raft::NodeId;

/// Static mapping from [`NodeId`] to network addresses.
///
/// The roster is established at startup and does not change while a node is
/// running (dynamic membership is a post-M8 concern).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RosterEntry {
    pub raft_addr: SocketAddr,
    pub client_addr: SocketAddr,
}

/// Static mapping from [`NodeId`] to network addresses.
///
/// The roster is established at startup and does not change while a node is
/// running (dynamic membership is a post-M8 concern).
#[derive(Debug, Clone)]
pub struct NodeRoster {
    entries: HashMap<NodeId, RosterEntry>,
}

impl NodeRoster {
    /// Build a roster from an iterator of `(node_id, raft_addr)` pairs (defaults client_addr to raft_addr).
    pub fn new(entries: impl IntoIterator<Item = (NodeId, SocketAddr)>) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(id, raft_addr)| {
                    (
                        id,
                        RosterEntry {
                            raft_addr,
                            client_addr: raft_addr,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Build a roster from an iterator of `(node_id, raft_addr, client_addr)` tuples.
    pub fn new_with_client(
        entries: impl IntoIterator<Item = (NodeId, SocketAddr, SocketAddr)>,
    ) -> Self {
        Self {
            entries: entries
                .into_iter()
                .map(|(id, raft_addr, client_addr)| {
                    (
                        id,
                        RosterEntry {
                            raft_addr,
                            client_addr,
                        },
                    )
                })
                .collect(),
        }
    }

    /// Look up the Raft network address for `id`.
    pub fn addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.entries.get(&id).map(|e| e.raft_addr)
    }

    /// Check if a NodeId exists in this roster.
    pub fn contains(&self, id: NodeId) -> bool {
        self.entries.contains_key(&id)
    }

    /// Look up the Client network address for `id`.
    pub fn client_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.entries.get(&id).map(|e| e.client_addr)
    }

    /// All node IDs present in this roster.
    pub fn all_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.entries.keys().copied().collect();
        ids.sort_by_key(|n| n.0);
        ids
    }

    /// All `(id, raft_addr)` pairs in this roster.
    pub fn all_entries(&self) -> Vec<(NodeId, SocketAddr)> {
        let mut v: Vec<_> = self
            .entries
            .iter()
            .map(|(&id, entry)| (id, entry.raft_addr))
            .collect();
        v.sort_by_key(|(id, _)| id.0);
        v
    }

    /// Number of nodes in the roster.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the roster is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert or replace a node's addresses.
    pub fn upsert(&mut self, id: NodeId, raft_addr: SocketAddr, client_addr: SocketAddr) {
        self.entries.insert(
            id,
            RosterEntry {
                raft_addr,
                client_addr,
            },
        );
    }

    /// Replace the roster with exactly `members`, parsing `host:port` strings.
    pub fn replace_from_members(
        &mut self,
        members: &[(NodeId, String, String)],
    ) -> Result<(), String> {
        let mut next = HashMap::new();
        for &(id, ref raft, ref client) in members {
            let raft_addr = raft
                .parse::<SocketAddr>()
                .map_err(|e| format!("invalid raft addr for node {}: {e}", id.0))?;
            let client_addr = client
                .parse::<SocketAddr>()
                .map_err(|e| format!("invalid client addr for node {}: {e}", id.0))?;
            next.insert(
                id,
                RosterEntry {
                    raft_addr,
                    client_addr,
                },
            );
        }
        self.entries = next;
        Ok(())
    }

    /// Keep only `voters` plus `self_id` in the roster (legacy config changes).
    pub fn retain_voters(
        &mut self,
        voters: &std::collections::BTreeSet<NodeId>,
        self_id: NodeId,
        self_raft: SocketAddr,
        self_client: SocketAddr,
    ) {
        self.entries
            .retain(|id, _| voters.contains(id) || *id == self_id);
        self.upsert(self_id, self_raft, self_client);
    }

    /// Merge members that carry non-empty address strings into the roster.
    pub fn merge_member_addresses(&mut self, members: &[(NodeId, String, String)]) {
        for &(id, ref raft, ref client) in members {
            if raft.is_empty() || client.is_empty() {
                continue;
            }
            if let (Ok(raft_addr), Ok(client_addr)) =
                (raft.parse::<SocketAddr>(), client.parse::<SocketAddr>())
            {
                self.upsert(id, raft_addr, client_addr);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn mk(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn lookup_present() {
        let r = NodeRoster::new([(NodeId(1), mk(7481)), (NodeId(2), mk(7482))]);
        assert_eq!(r.addr(NodeId(1)), Some(mk(7481)));
        assert_eq!(r.addr(NodeId(2)), Some(mk(7482)));
    }

    #[test]
    fn lookup_absent() {
        let r = NodeRoster::new([(NodeId(1), mk(7481))]);
        assert_eq!(r.addr(NodeId(99)), None);
    }

    #[test]
    fn all_ids_sorted() {
        let r = NodeRoster::new([
            (NodeId(3), mk(7483)),
            (NodeId(1), mk(7481)),
            (NodeId(2), mk(7482)),
        ]);
        assert_eq!(r.all_ids(), vec![NodeId(1), NodeId(2), NodeId(3)]);
    }
}
