---- MODULE WalCrash ----
EXTENDS Naturals, Sequences, TLC

(***************************************************************************)
(* Minimal WAL durable-prefix model.                                        *)
(*                                                                         *)
(* The model captures the MVP strict durability rule:                       *)
(*                                                                         *)
(*   If a record is acknowledged, then after crash/recovery it must appear  *)
(*   in the recovered WAL prefix.                                           *)
(*                                                                         *)
(* This is intentionally small. It does not model bytes, checksums,         *)
(* segment rotation, or corruption classes. Those belong in tests and       *)
(* later refined models.                                                    *)
(***************************************************************************)

CONSTANT MaxRecords

VARIABLES log, stableLen, acked, recovered, phase

vars == << log, stableLen, acked, recovered, phase >>

Prefix(seq, n) ==
    IF n = 0 THEN <<>> ELSE SubSeq(seq, 1, n)

SeqElems(seq) ==
    { seq[i] : i \in DOMAIN seq }

TypeOK ==
    /\ MaxRecords \in Nat
    /\ log \in Seq(1..MaxRecords)
    /\ stableLen \in 0..MaxRecords
    /\ stableLen <= Len(log)
    /\ acked \subseteq 1..MaxRecords
    /\ recovered \in Seq(1..MaxRecords)
    /\ phase \in {"running", "recovered"}

Init ==
    /\ log = <<>>
    /\ stableLen = 0
    /\ acked = {}
    /\ recovered = <<>>
    /\ phase = "running"

AppendVolatile ==
    /\ phase = "running"
    /\ Len(log) < MaxRecords
    /\ log' = Append(log, Len(log) + 1)
    /\ UNCHANGED << stableLen, acked, recovered, phase >>

FsyncAndAck ==
    /\ phase = "running"
    /\ stableLen < Len(log)
    /\ stableLen' = Len(log)
    /\ acked' = 1..stableLen'
    /\ UNCHANGED << log, recovered, phase >>

CrashRecover ==
    /\ phase = "running"
    /\ recovered' = Prefix(log, stableLen)
    /\ phase' = "recovered"
    /\ UNCHANGED << log, stableLen, acked >>

Next ==
    \/ AppendVolatile
    \/ FsyncAndAck
    \/ CrashRecover

Spec ==
    Init /\ [][Next]_vars

StrictAckRecovered ==
    phase = "recovered" => acked \subseteq SeqElems(recovered)

RecoveredIsDurablePrefix ==
    phase = "recovered" => recovered = Prefix(log, stableLen)

RecoveredContainsNoFutureRecord ==
    phase = "recovered" => SeqElems(recovered) \subseteq 1..stableLen

StableWithinLog ==
    stableLen <= Len(log)

====
