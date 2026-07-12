---- MODULE TxnCommit ----
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* Minimal single-group Snapshot Isolation commit model (M17).             *)
(*                                                                         *)
(* Models abstract transaction lifecycle:                                  *)
(*   Open -> Committed | Aborted                                           *)
(*                                                                         *)
(* Captures:                                                               *)
(*   - write intents on keys while a txn is Open                           *)
(*   - write-write conflict: two Open txns cannot both hold intent on k    *)
(*   - commit atomicity at the abstract level: all intents of a txn become *)
(*     committed versions together, or none do (abort clears intents)      *)
(*                                                                         *)
(* Intentionally omits Raft log, WAL, multi-group, and network.            *)
(* Documentation-grade if TLC is unavailable; runnable when TLC is present.*)
(***************************************************************************)

CONSTANTS TxnIds, Keys, MaxTs

VARIABLES
    txnState,   \* TxnIds -> {"none", "open", "committed", "aborted"}
    readTs,     \* TxnIds -> 0..MaxTs  (snapshot / begin ts)
    intents,    \* subset of TxnIds \X Keys  (open write intents)
    committed,  \* Keys -> 0..MaxTs  (last commit_ts per key; 0 = never written)
    clock       \* global logical commit clock 0..MaxTs

vars == << txnState, readTs, intents, committed, clock >>

TypeOK ==
    /\ txnState \in [TxnIds -> {"none", "open", "committed", "aborted"}]
    /\ readTs \in [TxnIds -> 0..MaxTs]
    /\ intents \subseteq (TxnIds \X Keys)
    /\ committed \in [Keys -> 0..MaxTs]
    /\ clock \in 0..MaxTs

Init ==
    /\ txnState = [t \in TxnIds |-> "none"]
    /\ readTs = [t \in TxnIds |-> 0]
    /\ intents = {}
    /\ committed = [k \in Keys |-> 0]
    /\ clock = 0

\* ── helpers ──────────────────────────────────────────────────────────────

IsOpen(t) == txnState[t] = "open"

IntentsOf(t) == { k \in Keys : <<t, k>> \in intents }

KeyHasIntent(k) == \E t \in TxnIds : <<t, k>> \in intents

\* Write-write conflict at commit time: any written key has a later commit
\* than this txn's snapshot (classic SI first-committer-wins).
HasWwConflict(t) ==
    \E k \in IntentsOf(t) : committed[k] > readTs[t]

ClearIntents(t) == intents \ ({t} \X Keys)

\* ── actions ──────────────────────────────────────────────────────────────

Begin(t) ==
    /\ txnState[t] = "none"
    /\ clock < MaxTs
    /\ txnState' = [txnState EXCEPT ![t] = "open"]
    /\ readTs' = [readTs EXCEPT ![t] = clock]
    /\ UNCHANGED << intents, committed, clock >>

\* Stage a write intent. Fails if another open txn already holds the key.
StageWrite(t, k) ==
    /\ IsOpen(t)
    /\ ~KeyHasIntent(k) \/ <<t, k>> \in intents
    /\ intents' = intents \union {<<t, k>>}
    /\ UNCHANGED << txnState, readTs, committed, clock >>

\* Abstract commit: all intents materialize at one new commit_ts, or abort
\* on write-write conflict. Atomic at this model level.
Commit(t) ==
    /\ IsOpen(t)
    /\ IF HasWwConflict(t) \/ clock >= MaxTs THEN
           \* conflict or clock exhausted -> abort
           /\ txnState' = [txnState EXCEPT ![t] = "aborted"]
           /\ intents' = ClearIntents(t)
           /\ UNCHANGED << readTs, committed, clock >>
       ELSE
           /\ clock' = clock + 1
           /\ txnState' = [txnState EXCEPT ![t] = "committed"]
           /\ committed' =
                [k \in Keys |->
                    IF k \in IntentsOf(t) THEN clock' ELSE committed[k]]
           /\ intents' = ClearIntents(t)
           /\ UNCHANGED readTs

Abort(t) ==
    /\ IsOpen(t)
    /\ txnState' = [txnState EXCEPT ![t] = "aborted"]
    /\ intents' = ClearIntents(t)
    /\ UNCHANGED << readTs, committed, clock >>

Next ==
    \/ \E t \in TxnIds : Begin(t)
    \/ \E t \in TxnIds, k \in Keys : StageWrite(t, k)
    \/ \E t \in TxnIds : Commit(t)
    \/ \E t \in TxnIds : Abort(t)

Spec == Init /\ [][Next]_vars

\* ── invariants ───────────────────────────────────────────────────────────

\* TXN-3 style: at most one open intent holder per key.
AtMostOneIntentPerKey ==
    \A k \in Keys :
        Cardinality({ t \in TxnIds : <<t, k>> \in intents }) <= 1

\* Intents only belong to open transactions.
IntentsOnlyForOpen ==
    \A t \in TxnIds, k \in Keys :
        <<t, k>> \in intents => IsOpen(t)

\* Terminal states hold no intents (commit atomicity / abort cleanup).
NoIntentsWhenFinished ==
    \A t \in TxnIds :
        txnState[t] \in {"committed", "aborted"} => IntentsOf(t) = {}

\* Committed timestamps never exceed the logical clock.
CommittedWithinClock ==
    \A k \in Keys : committed[k] <= clock

\* Stickiness of finished states is by construction: Begin requires "none";
\* StageWrite/Commit/Abort require "open". No action rewrites a finished txn.

====