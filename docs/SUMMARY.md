# Summary

## Introduction

* [Documentation home](README.md)
* [KayaDB Explained](KayaDB_Explained.md) — what it is, why it exists, how every layer works
* [Productization north star](productization.md) — M13: prototype → deployable product

## Install & Run

* [Installation](installation.md) — crates.io, release binaries, build from source
* [Getting Started](getting-started.md) — first server, first commands, cluster quick-start
* [Releases & Versioning](releases.md) — tags, v0.1.46, upgrade notes
* [Deployment](deployment.md) — Docker Compose + Kubernetes
* [Deployment guide v2](deployment-guide-v2.md) — M22–M24 flags, ranges, encryption, ACL
* [SLO envelope](slo-envelope.md) — hard limits and design SLOs

## Using KayaDB

* [Usage scenarios (TR)](usage.md) — yerel test, küme, kurtarma, client, inceleme, otomasyon
* [CLI Reference (kayactl)](cli-reference.md)
  * [Global flags & modes](cli-reference.md#global-flags)
  * [Local (embedded) commands](cli-reference.md)
  * [Server / cluster mode](cli-reference.md)
  * [Inspect commands](cli-reference.md#inspect-commands)
  * [Recovery & diagnostics](cli-reference.md#recovery)
* [Client library (kaya-client)](getting-started.md#using-the-kaya-client-library)
* [Client protocol specification](clients/client-protocol-spec.md)
* [Client wire protocol](clients/client-wire-protocol.md)
* [Go client guide](clients/go-client.md)

## Architecture & Internals

* [Architecture overview](architecture.md)
* [Design principles](architecture.md#design-principles)
* [Crate map](architecture.md#crate-map)
* [Data directory layout](architecture.md#data-directory-layout)
* [Write & read paths](architecture.md#write-path)
* [Recovery model](architecture.md#recovery-architecture)

## Core Components

* [Write-Ahead Log (WAL)](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/wal-spec.md)
* [LSM storage](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/lsm-storage-format-spec.md)
* [Disk abstraction & SimDisk](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/disk-and-io-spec.md)
* [Crash recovery](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/recovery-spec.md)

## Distributed KayaDB

* [Raft & cluster spec](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/raft-and-distributed-roadmap-spec.md)
* [Server & protocol spec](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/server-and-protocol-spec.md)
* [Jepsen-style testing](jepsen-design.md)

## Runbooks

* [Add / remove node](runbooks/add-remove-node.md)
* [Decommission node](runbooks/decommission-node.md)
* [Rolling restart](runbooks/rolling-restart.md)
* [Backup & restore](runbooks/backup-restore.md)
* [Detecting split-brain](runbooks/detecting-split-brain.md)
* [mTLS sidecar](runbooks/mtls-sidecar.md)

## Correctness & Testing

* [Testing philosophy](development.md#test-strategy)
* [Deterministic simulation](development.md#simdisk-fault-injection)
* [Fuzz testing](development.md#fuzzing)
* [Linearizability](jepsen-design.md)
* [Development workflow](development.md)
* [CI & GitHub Actions](ci-and-actions.md)
* [Benchmarks](BENCHMARKS.md)

## Reference

* [Security & deployment](security.md)
* [Design specifications](specifications.md)
* [Publishing documentation](publishing.md)
* [Releases & versioning](releases.md)

## Contributing & Project

* [Contributing](CONTRIBUTING.md)
* [Roadmap](ROADMAP.md)
* [Changelog](CHANGELOG.md)
* [Code of Conduct](CODE_OF_CONDUCT.md)
* [License](https://github.com/Tuntii/KayaDB/blob/main/README.md#license)

---

* [GitHub repository](https://github.com/Tuntii/KayaDB)
* [Report an issue](https://github.com/Tuntii/KayaDB/issues)