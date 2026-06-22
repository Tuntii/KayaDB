# Security Policy

## Supported Versions

KayaDB completed **M13 productization (2026-06-21)**. It is a correctness-first distributed KV engine with documented security controls and accepted deployment risks (see `docs/security.md` §7). Security fixes will be applied to the latest `main` branch and released as patch versions when appropriate.

| Version | Supported          |
| ------- | ------------------ |
| latest  | :white_check_mark: |
| < latest| :x:                |

## Reporting a Vulnerability

**Please do not open public GitHub issues for security vulnerabilities.**

Instead:

1. Use GitHub's **private vulnerability reporting** (preferred):
   - Go to the repository **Security** tab → **Report a vulnerability**.
2. Or email the maintainers privately at: tuntii@users.noreply.github.com (use subject starting with `[SECURITY]`).

### What to include

- Description of the issue and potential impact
- Steps to reproduce
- Affected versions / commits
- Any suggested mitigations

We will acknowledge receipt within **48 hours** and aim to provide a timeline for a fix.

## Scope

The following are in scope for security reports:

- Unauthenticated remote code execution or data exfiltration via the public client or Raft ports
- Crash / corruption that bypasses documented safety invariants
- Cryptographic issues in checksums or future TLS/auth code
- Supply chain / dependency issues with high severity

The following are **generally out of scope** (treat as defense-in-depth / operator responsibility):

- Running without firewall / private network (documented as insecure)
- Local data directory access by other processes on the same machine
- Denial of service via resource exhaustion on localhost-only deployments
- Issues requiring physical access or compromise of the host OS

Thank you for helping keep KayaDB and its users safe.
