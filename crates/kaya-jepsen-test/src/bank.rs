//! Bank transfer workload helpers and sum invariant (M17).
//!
//! Accounts are keys `acct:0..N-1` holding decimal ASCII integer balances.
//! Transfers use the client Snapshot Isolation txn API so both debit and
//! credit land in one commit (or neither does).

use kaya_client::KayaClient;
use kaya_core::KayaError;
use std::time::Duration;
use tokio::time::timeout;

/// Default number of bank accounts.
pub const BANK_NUM_ACCOUNTS: usize = 10;
/// Initial balance per account (decimal ASCII in the store).
pub const BANK_INITIAL_BALANCE: i64 = 100;
/// Key prefix for account keys: `acct:{i}`.
pub const BANK_KEY_PREFIX: &str = "acct:";

/// Expected constant total after seeding with defaults.
pub fn bank_expected_total(num_accounts: usize, initial: i64) -> i64 {
    (num_accounts as i64).saturating_mul(initial)
}

/// Wire key for account `i`.
pub fn bank_account_key(i: usize) -> Vec<u8> {
    format!("{BANK_KEY_PREFIX}{i}").into_bytes()
}

/// Encode balance as decimal ASCII (no leading zeros except for 0).
pub fn encode_balance(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// Parse a decimal ASCII balance value.
pub fn parse_balance(value: &[u8]) -> Result<i64, String> {
    let s = std::str::from_utf8(value).map_err(|e| format!("balance utf-8: {e}"))?;
    s.parse::<i64>()
        .map_err(|e| format!("balance parse '{s}': {e}"))
}

/// Invariant: sum of balances equals `expected`.
pub fn check_balances_sum(balances: &[i64], expected: i64) -> Result<(), String> {
    let sum: i64 = balances.iter().sum();
    if sum == expected {
        Ok(())
    } else {
        Err(format!(
            "bank sum invariant violated: sum={sum} expected={expected} balances={balances:?}"
        ))
    }
}

/// Offline bank model for unit-testing transfer histories without a cluster.
#[derive(Debug, Clone)]
pub struct BankModel {
    balances: Vec<i64>,
}

impl BankModel {
    pub fn new(num_accounts: usize, initial: i64) -> Self {
        Self {
            balances: vec![initial; num_accounts],
        }
    }

    pub fn balances(&self) -> &[i64] {
        &self.balances
    }

    pub fn total(&self) -> i64 {
        self.balances.iter().sum()
    }

    /// Apply a successful transfer (debit `from`, credit `to` by `amount`).
    ///
    /// Returns `Err` on invalid indices, non-positive amount, or insufficient funds.
    pub fn transfer(&mut self, from: usize, to: usize, amount: i64) -> Result<(), String> {
        if from == to {
            return Err("transfer requires distinct accounts".into());
        }
        if amount <= 0 {
            return Err("transfer amount must be positive".into());
        }
        if from >= self.balances.len() || to >= self.balances.len() {
            return Err(format!(
                "account out of range: from={from} to={to} n={}",
                self.balances.len()
            ));
        }
        if self.balances[from] < amount {
            return Err(format!(
                "insufficient funds: acct:{from} has {} need {amount}",
                self.balances[from]
            ));
        }
        self.balances[from] -= amount;
        self.balances[to] += amount;
        Ok(())
    }

    /// Check the constant-sum invariant against the seeding total.
    pub fn check_sum_invariant(&self, expected: i64) -> Result<(), String> {
        check_balances_sum(&self.balances, expected)
    }
}

/// A recorded transfer in a mock history (for offline invariant checks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BankTransfer {
    pub from: usize,
    pub to: usize,
    pub amount: i64,
    /// When false, the transfer was attempted but did not commit (conflict, funds, timeout).
    pub committed: bool,
}

/// Replay a transfer history on a fresh model and verify the sum never drifts.
pub fn check_transfer_history(
    num_accounts: usize,
    initial: i64,
    history: &[BankTransfer],
) -> Result<BankModel, String> {
    let expected = bank_expected_total(num_accounts, initial);
    let mut model = BankModel::new(num_accounts, initial);
    model.check_sum_invariant(expected)?;
    for (i, xfer) in history.iter().enumerate() {
        if !xfer.committed {
            continue;
        }
        model
            .transfer(xfer.from, xfer.to, xfer.amount)
            .map_err(|e| format!("history[{i}]: {e}"))?;
        model
            .check_sum_invariant(expected)
            .map_err(|e| format!("after history[{i}]: {e}"))?;
    }
    Ok(model)
}

/// Seed accounts `acct:0..n-1` with `initial` via plain puts (leader path).
pub async fn seed_bank_accounts(
    client: &mut KayaClient,
    num_accounts: usize,
    initial: i64,
) -> Result<(), String> {
    let value = encode_balance(initial);
    for i in 0..num_accounts {
        let key = bank_account_key(i);
        client
            .put(&key, &value)
            .await
            .map_err(|e| format!("seed acct:{i}: {e}"))?;
    }
    Ok(())
}

/// Read all account balances via GET.
pub async fn read_bank_balances(
    client: &mut KayaClient,
    num_accounts: usize,
) -> Result<Vec<i64>, String> {
    let mut out = Vec::with_capacity(num_accounts);
    for i in 0..num_accounts {
        let key = bank_account_key(i);
        match client.get(&key).await {
            Ok(Some(v)) => {
                out.push(parse_balance(&v).map_err(|e| format!("acct:{i}: {e}"))?);
            }
            Ok(None) => return Err(format!("missing account key acct:{i}")),
            Err(e) => return Err(format!("get acct:{i}: {e}")),
        }
    }
    Ok(out)
}

/// Transfer `amount` from `from` to `to` using a SI transaction.
///
/// - Insufficient funds → rollback and `Ok(false)` (no state change).
/// - Txn conflict → `Err(KayaError::TxnConflict)`.
/// - Success → `Ok(true)`.
pub async fn bank_transfer(
    client: &mut KayaClient,
    from: usize,
    to: usize,
    amount: i64,
) -> Result<bool, KayaError> {
    if from == to || amount <= 0 {
        return Ok(false);
    }
    let from_key = bank_account_key(from);
    let to_key = bank_account_key(to);

    let mut txn = client.begin_txn().await?;
    let from_bal = match txn.get(&from_key).await? {
        Some(v) => parse_balance(&v).map_err(KayaError::corruption)?,
        None => {
            let _ = txn.rollback().await;
            return Err(KayaError::invalid_argument(format!(
                "missing account acct:{from}"
            )));
        }
    };
    let to_bal = match txn.get(&to_key).await? {
        Some(v) => parse_balance(&v).map_err(KayaError::corruption)?,
        None => {
            let _ = txn.rollback().await;
            return Err(KayaError::invalid_argument(format!(
                "missing account acct:{to}"
            )));
        }
    };

    if from_bal < amount {
        txn.rollback().await?;
        return Ok(false);
    }

    txn.put(&from_key, &encode_balance(from_bal - amount))
        .await?;
    txn.put(&to_key, &encode_balance(to_bal + amount)).await?;
    txn.commit().await?;
    Ok(true)
}

/// Seed + read-back check of the sum invariant against a live client.
pub async fn verify_bank_sum_live(
    client: &mut KayaClient,
    num_accounts: usize,
    expected_total: i64,
    op_timeout: Duration,
) -> Result<(), String> {
    let balances = match timeout(op_timeout, read_bank_balances(client, num_accounts)).await {
        Ok(Ok(b)) => b,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err("timed out reading bank balances".into()),
    };
    check_balances_sum(&balances, expected_total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_keys_are_acct_prefix() {
        assert_eq!(bank_account_key(0), b"acct:0");
        assert_eq!(bank_account_key(9), b"acct:9");
    }

    #[test]
    fn balance_encode_parse_round_trip() {
        for n in [0i64, 1, 42, 100, 9999] {
            assert_eq!(parse_balance(&encode_balance(n)).unwrap(), n);
        }
    }

    #[test]
    fn sum_invariant_holds_for_seeded_model() {
        let model = BankModel::new(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE);
        let expected = bank_expected_total(BANK_NUM_ACCOUNTS, BANK_INITIAL_BALANCE);
        assert_eq!(model.total(), expected);
        model.check_sum_invariant(expected).unwrap();
    }

    #[test]
    fn transfer_preserves_sum() {
        let expected = bank_expected_total(5, 100);
        let mut model = BankModel::new(5, 100);
        model.transfer(0, 1, 30).unwrap();
        model.transfer(1, 2, 10).unwrap();
        model.transfer(4, 0, 5).unwrap();
        model.check_sum_invariant(expected).unwrap();
        assert_eq!(model.balances(), &[75, 120, 110, 100, 95]);
    }

    #[test]
    fn insufficient_funds_rejected() {
        let mut model = BankModel::new(2, 10);
        assert!(model.transfer(0, 1, 50).is_err());
        assert_eq!(model.balances(), &[10, 10]);
    }

    #[test]
    fn mock_history_preserves_sum() {
        let history = vec![
            BankTransfer {
                from: 0,
                to: 1,
                amount: 10,
                committed: true,
            },
            BankTransfer {
                from: 1,
                to: 2,
                amount: 5,
                committed: true,
            },
            BankTransfer {
                from: 0,
                to: 2,
                amount: 999,
                committed: false, // conflict / insufficient — ignored
            },
            BankTransfer {
                from: 2,
                to: 0,
                amount: 3,
                committed: true,
            },
        ];
        let model = check_transfer_history(3, 100, &history).unwrap();
        assert_eq!(model.total(), 300);
        assert_eq!(model.balances(), &[93, 105, 102]);
    }

    #[test]
    fn mock_history_detects_bad_committed_transfer() {
        // Manually craft an impossible history by double-applying without model
        // through check_balances_sum on a broken vector.
        let bad = vec![100i64, 100, 50]; // sum 250 != 300
        assert!(check_balances_sum(&bad, 300).is_err());
    }

    #[test]
    fn committed_history_with_overdraw_fails() {
        let history = vec![BankTransfer {
            from: 0,
            to: 1,
            amount: 500,
            committed: true,
        }];
        assert!(check_transfer_history(2, 100, &history).is_err());
    }
}
