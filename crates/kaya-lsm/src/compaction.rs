use std::collections::HashSet;

use crate::TableMetadata;

/// Tables selected for a single compaction job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionCandidate {
    pub input_table_ids: Vec<u64>,
    pub output_level: u32,
}

/// Strategy for choosing which SSTables to compact.
pub trait CompactionPolicy {
    fn pick_compaction(
        &self,
        live_tables: &[TableMetadata],
        pinned: &HashSet<u64>,
    ) -> Option<CompactionCandidate>;
}

/// Current MVP behavior: merge all unpinned tables when at least two exist (L0-style).
#[derive(Debug, Clone, Copy, Default)]
pub struct L0MergePolicy;

impl CompactionPolicy for L0MergePolicy {
    fn pick_compaction(
        &self,
        live_tables: &[TableMetadata],
        pinned: &HashSet<u64>,
    ) -> Option<CompactionCandidate> {
        let eligible: Vec<u64> = live_tables
            .iter()
            .filter(|t| !pinned.contains(&t.table_id))
            .map(|t| t.table_id)
            .collect();
        if eligible.len() < 2 {
            return None;
        }
        Some(CompactionCandidate {
            input_table_ids: eligible,
            output_level: 0,
        })
    }
}

/// Leveled compaction: pick overlapping tables at the lowest non-empty level.
#[derive(Debug, Clone)]
pub struct LevelStrategy {
    pub level_count: u32,
    pub l0_compaction_trigger: usize,
}

impl Default for LevelStrategy {
    fn default() -> Self {
        Self {
            level_count: 7,
            l0_compaction_trigger: 4,
        }
    }
}

impl CompactionPolicy for LevelStrategy {
    fn pick_compaction(
        &self,
        live_tables: &[TableMetadata],
        pinned: &HashSet<u64>,
    ) -> Option<CompactionCandidate> {
        for level in 0..self.level_count {
            let at_level: Vec<&TableMetadata> = live_tables
                .iter()
                .filter(|t| t.level == level && !pinned.contains(&t.table_id))
                .collect();

            if level == 0 {
                if at_level.len() >= self.l0_compaction_trigger {
                    return Some(CompactionCandidate {
                        input_table_ids: at_level.iter().map(|t| t.table_id).collect(),
                        output_level: 1,
                    });
                }
                continue;
            }

            if at_level.len() >= 2 {
                return Some(CompactionCandidate {
                    input_table_ids: at_level.iter().map(|t| t.table_id).collect(),
                    output_level: level + 1,
                });
            }
        }
        None
    }
}

/// Size-tiered compaction: group similarly-sized adjacent tables at the same level.
#[derive(Debug, Clone)]
pub struct TierStrategy {
    pub min_tables: usize,
    pub size_ratio: f64,
}

impl Default for TierStrategy {
    fn default() -> Self {
        Self {
            min_tables: 4,
            size_ratio: 1.5,
        }
    }
}

impl CompactionPolicy for TierStrategy {
    fn pick_compaction(
        &self,
        live_tables: &[TableMetadata],
        pinned: &HashSet<u64>,
    ) -> Option<CompactionCandidate> {
        let mut by_level: std::collections::BTreeMap<u32, Vec<&TableMetadata>> =
            std::collections::BTreeMap::new();
        for table in live_tables {
            if pinned.contains(&table.table_id) {
                continue;
            }
            by_level.entry(table.level).or_default().push(table);
        }

        for (level, tables) in by_level {
            if tables.len() < self.min_tables {
                continue;
            }
            let mut sorted: Vec<&TableMetadata> = tables;
            sorted.sort_by_key(|t| t.file_size);

            let mut run: Vec<u64> = Vec::new();
            let mut run_max = 0u64;
            for table in sorted {
                if run.is_empty() {
                    run.push(table.table_id);
                    run_max = table.file_size;
                    continue;
                }
                let ratio = table.file_size as f64 / run_max.max(1) as f64;
                if ratio <= self.size_ratio {
                    run.push(table.table_id);
                    run_max = run_max.max(table.file_size);
                } else if run.len() >= self.min_tables {
                    return Some(CompactionCandidate {
                        input_table_ids: run,
                        output_level: level + 1,
                    });
                } else {
                    run = vec![table.table_id];
                    run_max = table.file_size;
                }
            }
            if run.len() >= self.min_tables {
                return Some(CompactionCandidate {
                    input_table_ids: run,
                    output_level: level + 1,
                });
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(id: u64, level: u32, size: u64) -> TableMetadata {
        TableMetadata {
            table_id: id,
            level,
            path: format!("sst/{id:016x}.sst"),
            smallest_key: vec![0],
            largest_key: vec![255],
            min_sequence: kaya_core::SequenceNumber::new(1),
            max_sequence: kaya_core::SequenceNumber::new(1),
            entry_count: 1,
            file_size: size,
            footer_checksum: 0,
        }
    }

    #[test]
    fn l0_merge_requires_two_eligible_tables() {
        let policy = L0MergePolicy;
        let tables = vec![table(1, 0, 100)];
        assert!(policy.pick_compaction(&tables, &HashSet::new()).is_none());

        let tables = vec![table(1, 0, 100), table(2, 0, 200)];
        let pick = policy.pick_compaction(&tables, &HashSet::new()).unwrap();
        assert_eq!(pick.input_table_ids, vec![1, 2]);
        assert_eq!(pick.output_level, 0);
    }

    #[test]
    fn l0_merge_skips_pinned_tables() {
        let policy = L0MergePolicy;
        let tables = vec![table(1, 0, 100), table(2, 0, 200)];
        let mut pinned = HashSet::new();
        pinned.insert(1);
        assert!(policy.pick_compaction(&tables, &pinned).is_none());
    }

    #[test]
    fn level_strategy_triggers_l0_at_threshold() {
        let policy = LevelStrategy {
            level_count: 4,
            l0_compaction_trigger: 3,
        };
        let tables = vec![table(1, 0, 10), table(2, 0, 10), table(3, 0, 10)];
        let pick = policy.pick_compaction(&tables, &HashSet::new()).unwrap();
        assert_eq!(pick.input_table_ids.len(), 3);
        assert_eq!(pick.output_level, 1);
    }

    #[test]
    fn tier_strategy_groups_similar_sizes() {
        let policy = TierStrategy {
            min_tables: 3,
            size_ratio: 2.0,
        };
        let tables = vec![
            table(1, 0, 100),
            table(2, 0, 120),
            table(3, 0, 150),
            table(4, 0, 10_000),
        ];
        let pick = policy.pick_compaction(&tables, &HashSet::new()).unwrap();
        assert_eq!(pick.input_table_ids, vec![1, 2, 3]);
        assert_eq!(pick.output_level, 1);
    }
}
