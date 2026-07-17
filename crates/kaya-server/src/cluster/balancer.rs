//! Advisory store balancer: range-count heuristic (M22).
//!
//! Produces a [`RebalancePlan`] of suggested range moves. **Does not migrate
//! data or transfer leases** — operators / follow-on automation apply moves.

/// One suggested range placement change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMove {
    pub range_id: u64,
    pub from_node: u64,
    pub to_node: u64,
}

/// Ordered list of advisory moves that equalize range counts across nodes.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RebalancePlan {
    pub moves: Vec<RangeMove>,
}

/// Greedy range-count rebalance: while `max_count - min_count > 1`, move one
/// range from a richest node to a poorest node.
///
/// Tie-breaks (for determinism):
/// - richest: highest count, then highest `node_id`
/// - poorest: lowest count, then lowest `node_id`
/// - range taken from richest: highest `range_id` among that node's ranges
///
/// Empty input or a single node yields an empty plan. Nodes with empty
/// `range_ids` are included so ranges can move onto them.
pub fn plan_range_count(nodes: &[(u64 /* node */, Vec<u64> /* range_ids */)]) -> RebalancePlan {
    if nodes.len() < 2 {
        return RebalancePlan::default();
    }

    // Mutable assignment: node -> sorted range ids (desc for pop-from-rich).
    let mut assign: Vec<(u64, Vec<u64>)> = nodes
        .iter()
        .map(|(id, ranges)| {
            let mut r = ranges.clone();
            r.sort_unstable();
            r.dedup();
            (*id, r)
        })
        .collect();
    // Stable node order for scanning.
    assign.sort_by_key(|(id, _)| *id);

    let mut moves = Vec::new();
    // Bound iterations: at most total_ranges moves needed for count balance.
    let total: usize = assign.iter().map(|(_, r)| r.len()).sum();
    let max_iters = total.saturating_mul(2).max(1);

    for _ in 0..max_iters {
        let rich_idx = assign
            .iter()
            .enumerate()
            .max_by(|(_, (id_a, ra)), (_, (id_b, rb))| {
                ra.len().cmp(&rb.len()).then_with(|| id_a.cmp(id_b))
            })
            .map(|(i, _)| i)
            .expect("assign non-empty");
        let poor_idx = assign
            .iter()
            .enumerate()
            .min_by(|(_, (id_a, ra)), (_, (id_b, rb))| {
                ra.len().cmp(&rb.len()).then_with(|| id_a.cmp(id_b))
            })
            .map(|(i, _)| i)
            .expect("assign non-empty");

        let max_c = assign[rich_idx].1.len();
        let min_c = assign[poor_idx].1.len();
        if max_c.saturating_sub(min_c) <= 1 {
            break;
        }
        if rich_idx == poor_idx {
            break;
        }
        // Take highest range_id from richest.
        let Some(range_id) = assign[rich_idx].1.pop() else {
            break;
        };
        let from_node = assign[rich_idx].0;
        let to_node = assign[poor_idx].0;
        assign[poor_idx].1.push(range_id);
        assign[poor_idx].1.sort_unstable();
        moves.push(RangeMove {
            range_id,
            from_node,
            to_node,
        });
    }

    RebalancePlan { moves }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn balanced_cluster_needs_no_moves() {
        let nodes = vec![(1, vec![10, 11]), (2, vec![20, 21]), (3, vec![30, 31])];
        let plan = plan_range_count(&nodes);
        assert!(plan.moves.is_empty());
    }

    #[test]
    fn single_node_or_empty_is_noop() {
        assert!(plan_range_count(&[]).moves.is_empty());
        assert!(plan_range_count(&[(1, vec![1, 2, 3])]).moves.is_empty());
    }

    #[test]
    fn moves_from_rich_to_poor_until_diff_at_most_one() {
        // node1 has 4, node2 has 0 → end with 2/2
        let nodes = vec![(1, vec![1, 2, 3, 4]), (2, vec![])];
        let plan = plan_range_count(&nodes);
        assert_eq!(plan.moves.len(), 2);
        for m in &plan.moves {
            assert_eq!(m.from_node, 1);
            assert_eq!(m.to_node, 2);
        }
        // Highest range_ids move first (pop from sorted ascending → last).
        assert_eq!(plan.moves[0].range_id, 4);
        assert_eq!(plan.moves[1].range_id, 3);
    }

    #[test]
    fn three_nodes_equalizes_counts() {
        // 5 + 0 + 0 → 2, 2, 1 (diff ≤ 1)
        let nodes = vec![(10, vec![1, 2, 3, 4, 5]), (20, vec![]), (30, vec![])];
        let plan = plan_range_count(&nodes);
        let mut counts = std::collections::HashMap::new();
        counts.insert(10u64, 5usize);
        counts.insert(20, 0);
        counts.insert(30, 0);
        for m in &plan.moves {
            *counts.get_mut(&m.from_node).unwrap() -= 1;
            *counts.get_mut(&m.to_node).unwrap() += 1;
        }
        let vals: Vec<usize> = counts.values().copied().collect();
        let max = *vals.iter().max().unwrap();
        let min = *vals.iter().min().unwrap();
        assert!(max - min <= 1, "counts={counts:?} moves={:?}", plan.moves);
        assert_eq!(vals.iter().sum::<usize>(), 5);
    }

    #[test]
    fn already_off_by_one_is_ok() {
        let nodes = vec![(1, vec![1, 2]), (2, vec![3])];
        let plan = plan_range_count(&nodes);
        assert!(plan.moves.is_empty());
    }

    #[test]
    fn deterministic_tie_break_prefers_high_node_as_source() {
        // Both have 2; node 3 has 0. Richest tie: higher node_id (2) donates first.
        let nodes = vec![(1, vec![10, 11]), (2, vec![20, 21]), (3, vec![])];
        let plan = plan_range_count(&nodes);
        assert_eq!(plan.moves.len(), 1);
        assert_eq!(plan.moves[0].from_node, 2);
        assert_eq!(plan.moves[0].to_node, 3);
        assert_eq!(plan.moves[0].range_id, 21);
    }
}
