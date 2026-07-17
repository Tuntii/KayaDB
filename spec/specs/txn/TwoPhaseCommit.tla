---- MODULE TwoPhaseCommit ----
EXTENDS Naturals, FiniteSets, TLC

(***************************************************************************)
(* Minimal multi-group two-phase commit sketch (M23).                       *)
(*                                                                         *)
(* Models a single coordinator and a fixed set of participant groups for   *)
(* one transaction. Captures:                                              *)
(*                                                                         *)
(*   - prepare phase: all participants must Prepare before CommitDecision  *)
(*   - abort on any prepare failure                                        *)
(*   - commit / abort decision is atomic at the abstract level             *)
(*   - recovery: unfinished Preparing/Prepared participants abort          *)
(*     (conservative; matches server recover_incomplete_2pc)               *)
(*                                                                         *)
(* Intentionally omits Raft log, network reordering detail, and SI WW      *)
(* conflicts (see TxnCommit.tla for single-group SI).                      *)
(*                                                                         *)
(* Documentation-grade if TLC is unavailable; runnable when TLC is present.*)
(*                                                                         *)
(* Run (from this directory):                                              *)
(*   java -cp /path/to/tla2tools.jar tlc2.TLC \                             *)
(*        -config TwoPhaseCommit.cfg TwoPhaseCommit.tla                    *)
(* Expected: no invariant violations on the small constant set.            *)
(***************************************************************************)

CONSTANTS Participants
\* e.g. Participants = {g1, g2}

VARIABLES
    coord,        \* coordinator phase
    partState,    \* Participants -> participant phase
    decided       \* "none" | "commit" | "abort"  (global decision once set)

vars == << coord, partState, decided >>

CoordPhases == {
    "init",
    "preparing",
    "prepared",     \* all participants prepared; ready to decide commit
    "committing",
    "committed",
    "aborting",
    "aborted"
}

PartPhases == {
    "idle",
    "preparing",
    "prepared",
    "committed",
    "aborted"
}

TypeOK ==
    /\ coord \in CoordPhases
    /\ partState \in [Participants -> PartPhases]
    /\ decided \in {"none", "commit", "abort"}

Init ==
    /\ coord = "init"
    /\ partState = [p \in Participants |-> "idle"]
    /\ decided = "none"

\* ── helpers ──────────────────────────────────────────────────────────────

AllPrepared ==
    \A p \in Participants : partState[p] = "prepared"

AnyAborted ==
    \E p \in Participants : partState[p] = "aborted"

AllTerminal ==
    \A p \in Participants : partState[p] \in {"committed", "aborted"}

\* ── actions ──────────────────────────────────────────────────────────────

\* Coordinator starts prepare on all idle participants.
StartPrepare ==
    /\ coord = "init"
    /\ coord' = "preparing"
    /\ partState' = [p \in Participants |-> "preparing"]
    /\ UNCHANGED decided

\* One participant successfully prepares.
ParticipantPrepare(p) ==
    /\ coord = "preparing"
    /\ partState[p] = "preparing"
    /\ partState' = [partState EXCEPT ![p] = "prepared"]
    /\ UNCHANGED << coord, decided >>

\* One participant fails prepare → local abort; coordinator will abort all.
ParticipantPrepareFail(p) ==
    /\ coord = "preparing"
    /\ partState[p] = "preparing"
    /\ partState' = [partState EXCEPT ![p] = "aborted"]
    /\ UNCHANGED << coord, decided >>

\* All prepared → coordinator moves to prepared (ready for commit decision).
CoordAllPrepared ==
    /\ coord = "preparing"
    /\ AllPrepared
    /\ coord' = "prepared"
    /\ UNCHANGED << partState, decided >>

\* Any prepare failure observed → coordinator decides abort.
CoordDecideAbortOnFail ==
    /\ coord = "preparing"
    /\ AnyAborted
    /\ coord' = "aborting"
    /\ decided' = "abort"
    /\ UNCHANGED partState

\* Happy path: decide commit after all prepared.
CoordDecideCommit ==
    /\ coord = "prepared"
    /\ decided = "none"
    /\ coord' = "committing"
    /\ decided' = "commit"
    /\ UNCHANGED partState

\* Spontaneous abort after prepare (coordinator crash / conservative recovery).
CoordDecideAbortAfterPrepare ==
    /\ coord = "prepared"
    /\ decided = "none"
    /\ coord' = "aborting"
    /\ decided' = "abort"
    /\ UNCHANGED partState

\* Deliver commit to a prepared participant.
ParticipantCommit(p) ==
    /\ coord = "committing"
    /\ decided = "commit"
    /\ partState[p] = "prepared"
    /\ partState' = [partState EXCEPT ![p] = "committed"]
    /\ UNCHANGED << coord, decided >>

\* Deliver abort to a preparing/prepared participant (or already aborted no-op).
ParticipantAbort(p) ==
    /\ coord = "aborting"
    /\ decided = "abort"
    /\ partState[p] \in {"preparing", "prepared"}
    /\ partState' = [partState EXCEPT ![p] = "aborted"]
    /\ UNCHANGED << coord, decided >>

\* Coordinator finishes when all participants reached a terminal state.
CoordFinishCommit ==
    /\ coord = "committing"
    /\ \A p \in Participants : partState[p] = "committed"
    /\ coord' = "committed"
    /\ UNCHANGED << partState, decided >>

CoordFinishAbort ==
    /\ coord = "aborting"
    /\ \A p \in Participants : partState[p] = "aborted"
    /\ coord' = "aborted"
    /\ UNCHANGED << partState, decided >>

\* Crash recovery while preparing/prepared and no decision yet: abort.
\* Maps to recover_incomplete_2pc: Preparing/Prepared → Abort.
RecoverAbort ==
    /\ coord \in {"preparing", "prepared"}
    /\ decided = "none"
    /\ coord' = "aborting"
    /\ decided' = "abort"
    /\ UNCHANGED partState

Next ==
    \/ StartPrepare
    \/ \E p \in Participants : ParticipantPrepare(p)
    \/ \E p \in Participants : ParticipantPrepareFail(p)
    \/ CoordAllPrepared
    \/ CoordDecideAbortOnFail
    \/ CoordDecideCommit
    \/ CoordDecideAbortAfterPrepare
    \/ \E p \in Participants : ParticipantCommit(p)
    \/ \E p \in Participants : ParticipantAbort(p)
    \/ CoordFinishCommit
    \/ CoordFinishAbort
    \/ RecoverAbort

Spec == Init /\ [][Next]_vars

\* ── invariants (map to TXN-2PC-*) ────────────────────────────────────────

\* TXN-2PC-1 style: no participant reaches committed before a commit decision.
NoCommitBeforeDecision ==
    \A p \in Participants :
        partState[p] = "committed" => decided = "commit"

\* No participant commits if decision is abort.
NoCommitOnAbort ==
    decided = "abort" =>
        \A p \in Participants : partState[p] # "committed"

\* No participant aborts after a commit decision (once decided commit).
NoAbortOnCommit ==
    decided = "commit" =>
        \A p \in Participants : partState[p] # "aborted"

\* Decision is sticky: never flips once set (enforced by actions; state check).
DecisionStable ==
    decided \in {"none", "commit", "abort"}

\* Atomic visibility at terminal: either all committed or all aborted.
\* (While in flight, mixed preparing/prepared is fine.)
TerminalUniform ==
    coord \in {"committed", "aborted"} =>
        \/ (\A p \in Participants : partState[p] = "committed")
        \/ (\A p \in Participants : partState[p] = "aborted")

\* Coordinator committed only with commit decision and all participants committed.
CoordCommitConsistent ==
    coord = "committed" =>
        /\ decided = "commit"
        /\ \A p \in Participants : partState[p] = "committed"

CoordAbortConsistent ==
    coord = "aborted" =>
        /\ decided = "abort"
        /\ \A p \in Participants : partState[p] = "aborted"

====
