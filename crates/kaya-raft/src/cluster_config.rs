//! Raft cluster configuration and joint-consensus helpers (Raft §4).

use std::collections::BTreeSet;

use crate::NodeId;

/// A set of voting members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfiguration {
    pub voters: BTreeSet<NodeId>,
}

impl ClusterConfiguration {
    pub fn from_voters<I: IntoIterator<Item = NodeId>>(voters: I) -> Self {
        Self {
            voters: voters.into_iter().collect(),
        }
    }

    pub fn quorum(&self) -> usize {
        self.voters.len().div_ceil(2)
    }

    pub fn peers_of(&self, self_id: NodeId) -> Vec<NodeId> {
        self.voters
            .iter()
            .copied()
            .filter(|&id| id != self_id)
            .collect()
    }
}

/// Effective configuration during normal or joint-consensus operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveConfig {
    Stable(ClusterConfiguration),
    Joint {
        outgoing: ClusterConfiguration,
        incoming: ClusterConfiguration,
    },
}

impl EffectiveConfig {
    pub fn stable(voters: BTreeSet<NodeId>) -> Self {
        Self::Stable(ClusterConfiguration { voters })
    }

    pub fn stable_config(&self) -> &ClusterConfiguration {
        match self {
            Self::Stable(c) => c,
            Self::Joint { incoming, .. } => incoming,
        }
    }

    /// All node ids relevant for replication in the current phase.
    pub fn all_voters(&self) -> BTreeSet<NodeId> {
        match self {
            Self::Stable(c) => c.voters.clone(),
            Self::Joint { outgoing, incoming } => outgoing
                .voters
                .iter()
                .chain(incoming.voters.iter())
                .copied()
                .collect(),
        }
    }

    /// Whether `match_index` counts satisfy commit quorum for the current config.
    pub fn commit_quorum_met(&self, match_counts: impl Fn(&NodeId) -> bool) -> bool {
        match self {
            Self::Stable(c) => {
                let met = c.voters.iter().filter(|id| match_counts(id)).count();
                met >= c.quorum()
            }
            Self::Joint { outgoing, incoming } => {
                let old_met = outgoing.voters.iter().filter(|id| match_counts(id)).count();
                let new_met = incoming.voters.iter().filter(|id| match_counts(id)).count();
                old_met >= outgoing.quorum() && new_met >= incoming.quorum()
            }
        }
    }

    pub fn election_quorum(&self) -> usize {
        match self {
            Self::Stable(c) => c.quorum(),
            // During joint consensus, elections use the joint voter set.
            Self::Joint { outgoing, incoming } => {
                outgoing.voters.len().max(incoming.voters.len()).div_ceil(2)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joint_commit_requires_both_majorities() {
        let old = ClusterConfiguration::from_voters([NodeId(1), NodeId(2), NodeId(3)]);
        let new = ClusterConfiguration::from_voters([NodeId(2), NodeId(3), NodeId(4)]);
        let joint = EffectiveConfig::Joint {
            outgoing: old,
            incoming: new,
        };
        // Old majority met (nodes 1+2) but incoming majority not (only node 2).
        let matched: BTreeSet<_> = [NodeId(1), NodeId(2)].into_iter().collect();
        assert!(!joint.commit_quorum_met(|id| matched.contains(id)));
        let matched: BTreeSet<_> = [NodeId(2), NodeId(3), NodeId(4)].into_iter().collect();
        assert!(joint.commit_quorum_met(|id| matched.contains(id)));
    }
}
