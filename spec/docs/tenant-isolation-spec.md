# Tenant isolation spec (#29)

**Status:** Implemented (first version)  
**Scope:** Named tenants with exclusive key prefixes on top of PrefixAcl  
**Non-goals:** billing, full RBAC, resource quotas (residual; see ROADMAP)

---

## 1. Purpose

Per-prefix ACL (`--acl-file`) maps a key prefix to a token via longest-prefix match. That is key-space isolation, not tenancy: there is no tenant identity, prefixes may nest, and audit lines do not name a tenant.

This spec adds a **tenant layer on top of PrefixAcl**. It does not replace PrefixAcl or rewrite client auth.

## 2. Config

`--tenant-file PATH` / `KAYA_TENANT_FILE`. JSON:

```json
{
  "tenants": [
    {"id": "acme", "token": "tok-acme", "prefix": "acme/"},
    {"id": "globex", "token": "tok-globex", "prefix": "globex/"}
  ]
}
```

Load-time rules:

- each tenant has a unique non-empty `id`, a unique non-empty `token`, and a non-empty `prefix`
- prefixes are exclusive: no tenant prefix may be a prefix of another tenant's prefix (byte-wise; UTF-8 or `0x`/`hex:` like PrefixAcl)
- an empty `tenants` array is valid and denies every tenant-gated op
- malformed JSON or a missing `tenants` array fails server startup

`--tenant-file` and `--acl-file` are independent. Either, both, or neither may be set.

## 3. Authorization

Presented credential is the existing `CLIENT\x00` token.

| Ops | Rule |
|---|---|
| Keyed data-path: PUT / GET / DELETE / SCAN / TXN_OP | Token maps to exactly one tenant. The key (or SCAN prefix) **must** start with that tenant's prefix. Otherwise deny (`tenant denied`). |
| Keyless: TXN_BEGIN / TXN_COMMIT / TXN_ROLLBACK, CDC_POLL / CDC_CHECKPOINT, SPLIT_RANGE / MERGE_RANGE | Token must belong to some tenant (same shape as `PrefixAcl::authorize_token`). |
| HEALTH | Open (liveness probes). |
| STATS | Unchanged vs PrefixAcl (not tenant-gated). Admin opcodes stay on the operator-token path. |

Missing token, unknown token, or empty tenant list: deny.

### Combined with PrefixAcl

| Config | Gate |
|---|---|
| Only `--tenant-file` | TenantAcl is the ACL |
| Only `--acl-file` | PrefixAcl (M24, unchanged) |
| Both | **AND**: both must pass |
| Neither | Open, aside from `--client-token` if set |

`--client-token` (single shared token) still applies first when configured.

## 4. Audit

When a presented token maps to a tenant, the audit JSONL record includes `"tenant":"<id>"`. The field is omitted when no tenant is resolved (unknown token, no tenant config, or ops that do not carry a client token). Existing records without `tenant` remain valid.

## 5. Invariants

- **TENANT-1:** A tenant token cannot read or write a key outside its exclusive prefix.
- **TENANT-2:** Overlapping prefixes are rejected at load; two live tenants never share a key prefix.
- **TENANT-3:** A token maps to at most one tenant.
- **TENANT-4:** PrefixAcl, when also configured, cannot widen tenant access (AND).

## 6. Tests

- Unit: exclusive prefixes rejected; missing/unknown token deny; other tenant's key deny; AND with PrefixAcl.
- IT: `cross_tenant_access_denied` — two tenants, same-tenant GET OK, cross-tenant GET denied, tenant id on audit JSONL.

## 7. Future (not this version)

Resource quotas, RBAC / roles beyond one token per tenant, billing, a tenant control plane or UI.
