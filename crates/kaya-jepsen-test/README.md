# kaya-jepsen-test

Jepsen-style correctness testing for KayaDB clusters: workloads, nemeses, history recording, and scenario registry.

## Bank workload (M17)

Transfers between accounts `acct:0..N-1` (decimal ASCII balances) using the Rust client SI transaction API. Invariant: **sum of all balances is constant**.

### Offline unit tests (always run)

```bash
cargo test -p kaya-jepsen-test --lib bank
cargo test -p kaya-jepsen-test --test bank_workload -- --nocapture
```

These cover the mock history sum checker (`BankModel` / `check_transfer_history`) without a cluster.

### Full bank scenario (cluster + kill/partition)

```bash
# In-process 3-node cluster (ignored by default)
cargo test -p kaya-jepsen-test --test bank_workload bank_scenario_cluster -- --ignored --nocapture

# Or short single-transfer integration
cargo test -p kaya-jepsen-test --test bank_workload bank_single_transfer -- --ignored --nocapture
```

Scenario id: `bank` (see `bank_scenario()` in `src/scenario.rs`). Nemesis: composite kill-node + partition. Verify mode: `BankSum` (live re-read of all accounts).

### Programmatic use

```rust,ignore
use kaya_jepsen_test::{bank_scenario, ClusterController, TestConfig, TestRunner};

let mut scenario = bank_scenario();
// optionally shorten duration for local runs
let result = TestRunner::new(TestConfig::from_scenario(&scenario, dir.path()))
    .run_scenario(&scenario, &mut cluster)
    .await?;
assert!(result.passed);
```
