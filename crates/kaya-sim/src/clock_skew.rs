//! Logical clock skew injection for deterministic Raft cluster tests.
//!
//! Raft nodes in [`ClusterSim`] advance logical time via [`RaftNode::tick`].
//! Real clusters can experience clock skew where one node's election timer
//! fires earlier than peers expect. This module advances a single node's
//! logical clock without advancing others, reproducing that condition in sim.

use kaya_raft::NodeId;

use crate::cluster::ClusterSim;

/// Advance logical time for `node` by `extra_ticks` without ticking peers.
pub fn advance_node_clock(sim: &mut ClusterSim, node: NodeId, extra_ticks: u64) {
    for _ in 0..extra_ticks {
        sim.tick_node(node);
    }
    sim.deliver_network_rounds();
}

impl ClusterSim {
    /// Advance logical time for `node` by `extra_ticks` without ticking peers.
    pub fn advance_node_clock(&mut self, node: NodeId, extra_ticks: u64) {
        advance_node_clock(self, node, extra_ticks);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::SimNetworkConfig;

    fn no_fault_config() -> SimNetworkConfig {
        SimNetworkConfig {
            drop_percent: 0,
            dup_percent: 0,
            latency_ticks: 0,
            reorder_percent: 0,
        }
    }

    /// Election safety (RAFT-INV-001) holds when one node runs a fast logical clock.
    #[test]
    fn cluster_clock_skew_election_stable() {
        let mut sim = ClusterSim::new(3, 404, no_fault_config());
        sim.run_ticks(30);
        let leader = sim.current_leader().expect("initial leader");

        let skewed = (1u64..=3)
            .map(NodeId)
            .find(|&id| id != leader)
            .expect("follower");
        sim.advance_node_clock(skewed, 25);

        sim.run_ticks(120);

        let election_violations: Vec<_> = sim
            .violations()
            .iter()
            .filter(|v| v.contains("RAFT-INV-001"))
            .collect();
        assert!(
            election_violations.is_empty(),
            "election safety violated under clock skew: {:?}",
            sim.violations()
        );
        assert!(
            sim.current_leader().is_some(),
            "cluster should stabilize with a leader after skew"
        );
    }

    /// Skew on multiple followers still preserves at-most-one-leader invariant.
    #[test]
    fn cluster_clock_skew_multi_node_stable() {
        let mut sim = ClusterSim::new(5, 909, no_fault_config());
        sim.run_ticks(40);

        for id in (1u64..=5).map(NodeId) {
            if sim.current_leader() != Some(id) {
                sim.advance_node_clock(id, 15);
            }
        }

        sim.run_ticks(150);

        let election_violations: Vec<_> = sim
            .violations()
            .iter()
            .filter(|v| v.contains("RAFT-INV-001"))
            .collect();
        assert!(
            election_violations.is_empty(),
            "multi-node skew caused split brain: {:?}",
            sim.violations()
        );
    }
}
