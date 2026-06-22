use std::collections::HashSet;

use kaya_core::{CompactionConfig, CompactionPolicyKind};
use kaya_lsm::{
    CompactionCandidate, CompactionPolicy, L0MergePolicy, LevelStrategy, TableMetadata,
    TierStrategy,
};

/// Compaction policy selected from engine configuration.
pub enum ConfiguredCompactionPolicy {
    L0Merge(L0MergePolicy),
    Leveled(LevelStrategy),
    Tiered(TierStrategy),
}

impl CompactionPolicy for ConfiguredCompactionPolicy {
    fn pick_compaction(
        &self,
        live_tables: &[TableMetadata],
        pinned: &HashSet<u64>,
    ) -> Option<CompactionCandidate> {
        match self {
            Self::L0Merge(policy) => policy.pick_compaction(live_tables, pinned),
            Self::Leveled(policy) => policy.pick_compaction(live_tables, pinned),
            Self::Tiered(policy) => policy.pick_compaction(live_tables, pinned),
        }
    }
}

pub fn compaction_policy_from_config(config: &CompactionConfig) -> ConfiguredCompactionPolicy {
    match config.policy {
        CompactionPolicyKind::L0Merge => ConfiguredCompactionPolicy::L0Merge(L0MergePolicy),
        CompactionPolicyKind::Leveled => ConfiguredCompactionPolicy::Leveled(LevelStrategy {
            level_count: config.leveled.level_count,
            l0_compaction_trigger: config.leveled.l0_compaction_trigger,
        }),
        CompactionPolicyKind::SizeTiered => ConfiguredCompactionPolicy::Tiered(TierStrategy {
            min_tables: config.tiered.min_tables,
            size_ratio: f64::from(config.tiered.ratio_x1000) / 1000.0,
        }),
    }
}
