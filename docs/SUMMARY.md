# Summary

## Introduction

* [Welcome to KayaDB](README.md)
* [KayaDB Explained](KayaDB_Explained.md) — The complete picture (what it does, why it exists, how everything works)
* [Why KayaDB?](README.md#project-status)

## Getting Started

* [Getting Started](getting-started.md)
* [Quick Start Examples](getting-started.md#run-a-single-node-server)
* [Embedded Usage (Rust)](getting-started.md#use-it-as-a-rust-library)

## Using KayaDB

* [CLI Reference (kayactl)](cli-reference.md)
  * [Global Flags & Modes](cli-reference.md#global-flags)
  * [Local (Embedded) Commands](cli-reference.md)
  * [Server / Cluster Mode](cli-reference.md)
  * [Inspect Commands](cli-reference.md#inspect-commands)
  * [Recovery & Diagnostics](cli-reference.md#recovery)
* [Client Library (kaya-client)](getting-started.md#tcp-client)
* [Client Protocol Specification](clients/client-protocol-spec.md) — Multi-language client behavior (leader redirection, errors, operations)
* [Client Wire Protocol](clients/client-wire-protocol.md) — Exact TCP framing & payloads (for client implementers)
* [Go Client Guide](clients/go-client.md) — Bootstrap a correct Go client (reference implementation + code)

## Architecture & Internals

* [Architecture Overview](architecture.md)
* [Design Principles](architecture.md#design-principles)
* [Crate Map & Responsibilities](architecture.md#crate-map)
* [Data Directory Layout](architecture.md#data-directory-layout)
* [Write & Read Paths](architecture.md#write-path)
* [Recovery Model](architecture.md#recovery-architecture)

## Core Components

* [Write-Ahead Log (WAL)](../spec/docs/wal-spec.md)
* [LSM Storage (Memtable, SSTable, Manifest)](../spec/docs/lsm-storage-format-spec.md)
* [Disk Abstraction & SimDisk](../spec/docs/disk-and-io-spec.md)
* [Crash Recovery & Idempotence](../spec/docs/recovery-spec.md)

## Distributed KayaDB

* [Raft Consensus](../spec/docs/raft-and-distributed-roadmap-spec.md)
* [Cluster & Server Protocol](../spec/docs/server-and-protocol-spec.md)
* [Jepsen-Style Testing & Failure Injection](jepsen-design.md)

## Correctness & Testing

* [Testing Philosophy](development.md#test-strategy)
* [Deterministic Simulation](development.md#simdisk-fault-injection)
* [Fuzz Testing](development.md#fuzzing)
* [Linearizability Checking](jepsen-design.md)
* [Development Workflow](development.md)

## Reference

* [Security & Safe Deployment](security.md)
* [Configuration](development.md)
* [Benchmarks & Performance](https://github.com/Tuntii/KayaDB/blob/main/BENCHMARKS.md)
* [Design Specifications](specifications.md)
  * [Spec Index](../spec/docs/00-spec-index.md)
  * [Key Specs (WAL, LSM, Recovery, Simulation...)](specifications.md#core-specifications)

## Contributing & Project

* [Contributing Guide](../CONTRIBUTING.md)
* [Development Guide](development.md)
* [Maintaining the Documentation](publishing.md)
* [Roadmap](../ROADMAP.md)
* [License](../README.md#license)

---

* [GitHub Repository](https://github.com/Tuntii/KayaDB)
* [Report an Issue](https://github.com/Tuntii/KayaDB/issues)