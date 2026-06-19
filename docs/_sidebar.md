<!-- This file powers the sidebar when using GitHub Pages + Docsify -->
<!-- For GitBook, we still keep SUMMARY.md -->

- [Introduction](README.md)
  - [Welcome to KayaDB](README.md)
  - [Why KayaDB?](README.md#project-status)

- [Getting Started](getting-started.md)
  - [Quick Start Examples](getting-started.md#run-a-single-node-server)
  - [Embedded Usage (Rust)](getting-started.md#use-it-as-a-rust-library)

- [Using KayaDB](cli-reference.md)
  - [Kullanım Senaryoları](usage.md)
  - [CLI Reference (kayactl)](cli-reference.md)
  - [Client Library](getting-started.md#tcp-client)
  - [Client Protocol Spec](clients/client-protocol-spec.md)
  - [Wire Protocol](clients/client-wire-protocol.md)
  - [Go Client Guide](clients/go-client.md)

- [Architecture & Internals](architecture.md)
  - [Architecture Overview](architecture.md)
  - [Core Components](specifications.md)

- [Distributed Operation](jepsen-design.md)
  - [Raft & Cluster](jepsen-design.md)
  - [Jepsen-Style Testing](jepsen-design.md)

- [Correctness & Testing](development.md)
  - [Deterministic Simulation](development.md#simdisk-fault-injection)
  - [Fuzzing & Invariants](development.md#fuzzing)

- [Reference](security.md)
  - [Security & Deployment](security.md)
  - [Development Guide](development.md)

- [Design Specifications](specifications.md)
  - [Spec Index](https://github.com/Tuntii/KayaDB/blob/main/spec/docs/00-spec-index.md)
  - [Key Technical Specs](specifications.md#core-specifications)

- [Project](publishing.md)
  - [Productization north star](productization.md)
  - [Contributing](https://github.com/Tuntii/KayaDB/blob/main/CONTRIBUTING.md)
  - [Roadmap](https://github.com/Tuntii/KayaDB/blob/main/ROADMAP.md)
  - [Publishing Docs](publishing.md)

- [Runbooks](runbooks/rolling-restart.md)
  - [Add / Remove Node](runbooks/add-remove-node.md)
  - [Rolling Restart](runbooks/rolling-restart.md)
  - [Backup & Restore](runbooks/backup-restore.md)
  - [Detecting Split-Brain](runbooks/detecting-split-brain.md)
  - [mTLS Sidecar](runbooks/mtls-sidecar.md)