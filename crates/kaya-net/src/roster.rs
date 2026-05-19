//! Static cluster membership: maps [`NodeId`] to a [`SocketAddr`].

use std::collections::HashMap;
use std::net::SocketAddr;

use kaya_raft::NodeId;

/// Static mapping from [`NodeId`] to network addresses.
///
/// The roster is established at startup and does not change while a node is
/// running (dynamic membership is a post-M8 concern).
#[derive(Debug, Clone)]
pub struct NodeRoster {
    entries: HashMap<NodeId, SocketAddr>,
}

impl NodeRoster {
    /// Build a roster from an iterator of `(node_id, socket_addr)` pairs.
    pub fn new(entries: impl IntoIterator<Item = (NodeId, SocketAddr)>) -> Self {
        Self {
            entries: entries.into_iter().collect(),
        }
    }

    /// Look up the Raft network address for `id`.
    pub fn addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.entries.get(&id).copied()
    }

    /// All node IDs present in this roster.
    pub fn all_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = self.entries.keys().copied().collect();
        ids.sort_by_key(|n| n.0);
        ids
    }

    /// All `(id, addr)` pairs in this roster.
    pub fn all_entries(&self) -> Vec<(NodeId, SocketAddr)> {
        let mut v: Vec<_> = self.entries.iter().map(|(&id, &addr)| (id, addr)).collect();
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
