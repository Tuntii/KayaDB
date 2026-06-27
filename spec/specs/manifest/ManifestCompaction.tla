---- MODULE ManifestCompaction ----
EXTENDS Naturals, Sequences, TLC

(***************************************************************************)
(* Minimal manifest + compaction visibility model.                          *)
(*                                                                         *)
(* Live tables are referenced by manifest edits. Compaction replaces a     *)
(* set of input table IDs with one output table. Recovery must never       *)
(* expose a table that was deleted without its replacement being live.     *)
(***************************************************************************)

CONSTANT MaxTables

VARIABLES live, pendingOut, phase

vars == << live, pendingOut, phase >>

LiveSet(seq) == { seq[i] : i \in DOMAIN seq }

TypeOK ==
    /\ MaxTables \in Nat
    /\ live \in Seq(1..MaxTables)
    /\ pendingOut \in 0..MaxTables
    /\ phase \in {"running", "compacted"}

Init ==
    /\ live = <<1, 2>>
    /\ pendingOut = 0
    /\ phase = "running"

CreateTable ==
    /\ phase = "running"
    /\ Len(live) < MaxTables
    /\ live' = Append(live, Len(live) + 1)
    /\ UNCHANGED << pendingOut, phase >>

DeleteInputsAndStageOutput ==
    /\ phase = "running"
    /\ Len(live) >= 2
    /\ pendingOut = 0
    /\ live' = SubSeq(live, 1, Len(live) - 1)
    /\ pendingOut' = Len(live) + 1
    /\ UNCHANGED phase

PublishCompaction ==
    /\ phase = "running"
    /\ pendingOut > 0
    /\ live' = Append(live, pendingOut)
    /\ pendingOut' = 0
    /\ phase' = "compacted"

Next ==
    \/ CreateTable
    \/ DeleteInputsAndStageOutput
    \/ PublishCompaction

Spec ==
    Init /\ [][Next]_vars

NoDuplicateLiveIds ==
    \A i, j \in DOMAIN live :
        i # j => live[i] # live[j]

CompactionPublishesExactlyOnce ==
    phase = "compacted" => pendingOut = 0

LiveNeverEmptyAfterCompaction ==
    phase = "compacted" => Len(live) > 0

====