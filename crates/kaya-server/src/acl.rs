//! Per-prefix ACL (M24) and named-tenant isolation (#29).
//!
//! [`PrefixAcl`]: JSON object `prefix -> token`. Prefix keys may be UTF-8 text
//! or hex-encoded bytes (`0x…` / `hex:…`). When an ACL file is configured,
//! data-path ops (PUT/GET/DELETE/SCAN/TXN_*) authorize the presented client
//! token against the **longest** prefix that is a prefix of the request key.
//! Keyless ops (TXN_BEGIN/COMMIT/ROLLBACK, CDC_POLL/CHECKPOINT, SPLIT/MERGE)
//! accept any token that appears on at least one rule via
//! [`PrefixAcl::authorize_token`]. An empty ACL map denies every such op.
//!
//! [`TenantAcl`]: JSON `{ "tenants": [ { "id", "token", "prefix" }, ... ] }`.
//! Each tenant owns an exclusive key prefix. The presented token maps to at
//! most one tenant; keyed ops require the key to start with that tenant's
//! prefix. Keyless ops require the token to belong to some tenant. When both
//! PrefixAcl and TenantAcl are configured, both must pass (AND).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// Loaded prefix ACL: rules ordered longest-prefix first.
#[derive(Debug, Clone, Default)]
pub struct PrefixAcl {
    /// `(prefix_bytes, token)` sorted by `prefix_bytes.len()` descending.
    rules: Vec<(Vec<u8>, String)>,
}

impl PrefixAcl {
    /// Load ACL from a JSON file mapping `prefix_hex_or_utf8 -> token`.
    pub fn load_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("read ACL file {}: {e}", path.display()))?;
        Self::from_json(&raw)
    }

    /// Parse ACL from a JSON object string.
    pub fn from_json(raw: &str) -> Result<Self, String> {
        let map: HashMap<String, String> =
            serde_json::from_str(raw.trim()).map_err(|e| format!("parse ACL JSON: {e}"))?;
        Self::from_map(map)
    }

    /// Build ACL from an already-decoded map.
    pub fn from_map(map: HashMap<String, String>) -> Result<Self, String> {
        let mut rules = Vec::with_capacity(map.len());
        for (prefix_s, token) in map {
            if token.is_empty() {
                return Err(format!(
                    "ACL token for prefix {prefix_s:?} must be non-empty"
                ));
            }
            let prefix = parse_prefix_key(&prefix_s)?;
            rules.push((prefix, token));
        }
        // Longest prefix first; equal lengths keep a stable order by bytes.
        rules.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
        Ok(Self { rules })
    }

    /// Number of rules.
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    /// True when the ACL map is empty (deny-all for data ops).
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Longest-prefix match: key is allowed iff a rule's prefix is a prefix of
    /// `key` and that rule's token equals `token`. Empty ACL always denies.
    pub fn authorize(&self, key: &[u8], token: Option<&str>) -> bool {
        let Some(token) = token else {
            return false;
        };
        if self.rules.is_empty() {
            return false;
        }
        for (prefix, rule_tok) in &self.rules {
            if key.starts_with(prefix) {
                return rule_tok == token;
            }
        }
        false
    }

    /// Authorize when there is no key (TXN_BEGIN / TXN_COMMIT / TXN_ROLLBACK /
    /// CDC_POLL / CDC_CHECKPOINT / SPLIT_RANGE / MERGE_RANGE): any token that
    /// appears on at least one rule is accepted. Empty ACL denies.
    pub fn authorize_token(&self, token: Option<&str>) -> bool {
        let Some(token) = token else {
            return false;
        };
        if self.rules.is_empty() {
            return false;
        }
        self.rules.iter().any(|(_, t)| t == token)
    }
}

/// One named tenant: unique id, unique token, exclusive key prefix.
#[derive(Debug, Clone)]
struct TenantEntry {
    id: String,
    prefix: Vec<u8>,
}

/// Named-tenant isolation (#29): token → exclusive key prefix.
///
/// Loaded from JSON `{ "tenants": [ { "id", "token", "prefix" }, ... ] }`.
/// Prefixes may be UTF-8 text or hex (`0x…` / `hex:…`), same as [`PrefixAcl`].
#[derive(Debug, Clone, Default)]
pub struct TenantAcl {
    /// `token → (id, prefix_bytes)`. Tokens are unique by construction.
    by_token: HashMap<String, TenantEntry>,
}

impl TenantAcl {
    /// Load tenants from a JSON file.
    pub fn load_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let raw = fs::read_to_string(path)
            .map_err(|e| format!("read tenant file {}: {e}", path.display()))?;
        Self::from_json(&raw)
    }

    /// Parse tenants from a JSON object string.
    pub fn from_json(raw: &str) -> Result<Self, String> {
        let value: serde_json::Value =
            serde_json::from_str(raw.trim()).map_err(|e| format!("parse tenant JSON: {e}"))?;
        let Some(arr) = value.get("tenants").and_then(|v| v.as_array()) else {
            return Err("tenant file must be a JSON object with a \"tenants\" array".to_owned());
        };

        let mut by_token: HashMap<String, TenantEntry> = HashMap::new();
        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut prefixes: Vec<(String, Vec<u8>)> = Vec::with_capacity(arr.len());

        for (i, entry) in arr.iter().enumerate() {
            let obj = entry
                .as_object()
                .ok_or_else(|| format!("tenants[{i}] must be an object with id, token, prefix"))?;
            let id = obj
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("tenants[{i}].id must be a string"))?
                .to_owned();
            if id.is_empty() {
                return Err(format!("tenants[{i}].id must be non-empty"));
            }
            let token = obj
                .get("token")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("tenant {id:?} token must be a string"))?
                .to_owned();
            if token.is_empty() {
                return Err(format!("tenant {id:?} token must be non-empty"));
            }
            let prefix_s = obj
                .get("prefix")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("tenant {id:?} prefix must be a string"))?;
            let prefix = parse_prefix_key(prefix_s)?;
            if prefix.is_empty() {
                return Err(format!("tenant {id:?} prefix must be non-empty"));
            }
            if seen_ids.contains(&id) {
                return Err(format!("duplicate tenant id {id:?}"));
            }
            if by_token.contains_key(&token) {
                return Err(format!("duplicate tenant token (id {id:?})"));
            }
            seen_ids.insert(id.clone());
            prefixes.push((id.clone(), prefix.clone()));
            by_token.insert(token, TenantEntry { id, prefix });
        }

        assert_exclusive_prefixes(&prefixes)?;
        Ok(Self { by_token })
    }

    /// Number of tenants.
    pub fn len(&self) -> usize {
        self.by_token.len()
    }

    /// True when no tenants are configured (deny-all for tenant-gated ops).
    pub fn is_empty(&self) -> bool {
        self.by_token.is_empty()
    }

    /// Tenant id for a presented token, if any.
    pub fn tenant_id(&self, token: Option<&str>) -> Option<&str> {
        token.and_then(|t| self.by_token.get(t).map(|e| e.id.as_str()))
    }

    /// Keyed data-path authorize: token maps to one tenant and `key` starts
    /// with that tenant's exclusive prefix. Missing token or unknown token
    /// denies. Empty tenant list denies.
    pub fn authorize(&self, key: &[u8], token: Option<&str>) -> bool {
        let Some(token) = token else {
            return false;
        };
        let Some(entry) = self.by_token.get(token) else {
            return false;
        };
        key.starts_with(&entry.prefix)
    }

    /// Keyless authorize: token must belong to some tenant. Empty list denies.
    pub fn authorize_token(&self, token: Option<&str>) -> bool {
        let Some(token) = token else {
            return false;
        };
        self.by_token.contains_key(token)
    }
}

/// Combined PrefixAcl + TenantAcl gate. When both are set, both must pass
/// (AND). When only one is set, that layer is the ACL. When neither is set,
/// the call is open (the caller still applies `--client-token` separately).
pub fn authorize_key(
    acl: Option<&PrefixAcl>,
    tenants: Option<&TenantAcl>,
    key: &[u8],
    token: Option<&str>,
) -> bool {
    if let Some(tenants) = tenants {
        if !tenants.authorize(key, token) {
            return false;
        }
    }
    if let Some(acl) = acl {
        if !acl.authorize(key, token) {
            return false;
        }
    }
    true
}

/// Combined keyless authorize (TXN_BEGIN/COMMIT/ROLLBACK, CDC, SPLIT/MERGE).
pub fn authorize_token(
    acl: Option<&PrefixAcl>,
    tenants: Option<&TenantAcl>,
    token: Option<&str>,
) -> bool {
    if let Some(tenants) = tenants {
        if !tenants.authorize_token(token) {
            return false;
        }
    }
    if let Some(acl) = acl {
        if !acl.authorize_token(token) {
            return false;
        }
    }
    true
}

/// Reject overlapping tenant prefixes: no prefix may be a prefix of another.
fn assert_exclusive_prefixes(prefixes: &[(String, Vec<u8>)]) -> Result<(), String> {
    for (i, (id_a, pre_a)) in prefixes.iter().enumerate() {
        for (id_b, pre_b) in prefixes.iter().skip(i + 1) {
            if pre_a.starts_with(pre_b) || pre_b.starts_with(pre_a) {
                return Err(format!(
                    "tenant prefixes must be exclusive: {id_a:?} and {id_b:?} overlap"
                ));
            }
        }
    }
    Ok(())
}

/// Decode a JSON map key into raw prefix bytes.
///
/// * `0xDEAD` / `0Xdead` / `hex:dead` → hex decode
/// * anything else → UTF-8 bytes of the string as-is
fn parse_prefix_key(s: &str) -> Result<Vec<u8>, String> {
    let lower = s.to_ascii_lowercase();
    if let Some(hex) = lower
        .strip_prefix("0x")
        .or_else(|| lower.strip_prefix("hex:"))
    {
        return decode_hex(hex).map_err(|e| format!("ACL prefix {s:?}: {e}"));
    }
    Ok(s.as_bytes().to_vec())
}

fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err(format!("hex length must be even, got {}", hex.len()));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex digit {:?}", b as char)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn empty_acl_denies_all() {
        let acl = PrefixAcl::from_json("{}").unwrap();
        assert!(acl.is_empty());
        assert!(!acl.authorize(b"any", Some("tok")));
        assert!(!acl.authorize_token(Some("tok")));
        assert!(!acl.authorize(b"any", None));
    }

    #[test]
    fn longest_prefix_wins() {
        let mut map = HashMap::new();
        map.insert("a/".into(), "tok-a".into());
        map.insert("a/b/".into(), "tok-ab".into());
        map.insert("c/".into(), "tok-c".into());
        let acl = PrefixAcl::from_map(map).unwrap();

        assert!(acl.authorize(b"a/b/x", Some("tok-ab")));
        assert!(!acl.authorize(b"a/b/x", Some("tok-a")));
        assert!(acl.authorize(b"a/z", Some("tok-a")));
        assert!(!acl.authorize(b"a/z", Some("tok-ab")));
        assert!(acl.authorize(b"c/1", Some("tok-c")));
        assert!(!acl.authorize(b"d/1", Some("tok-a")));
        assert!(!acl.authorize(b"a/b/x", None));
    }

    #[test]
    fn hex_and_utf8_prefixes() {
        // "ab" in hex is 0x6162
        let json = r#"{ "0x6162": "hex-tok", "users/": "utf-tok" }"#;
        let acl = PrefixAcl::from_json(json).unwrap();
        assert!(acl.authorize(b"abc", Some("hex-tok"))); // b"ab" + b"c"
        assert!(acl.authorize(b"users/1", Some("utf-tok")));
        assert!(!acl.authorize(b"users/1", Some("hex-tok")));
    }

    #[test]
    fn authorize_token_any_rule() {
        let json = r#"{ "a/": "t1", "b/": "t2" }"#;
        let acl = PrefixAcl::from_json(json).unwrap();
        assert!(acl.authorize_token(Some("t1")));
        assert!(acl.authorize_token(Some("t2")));
        assert!(!acl.authorize_token(Some("t3")));
        assert!(!acl.authorize_token(None));
    }

    #[test]
    fn load_file_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "kaya_acl_ut_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("acl.json");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            write!(f, r#"{{"team/":"secret-a","other/":"secret-b"}}"#).unwrap();
        }
        let acl = PrefixAcl::load_file(&path).unwrap();
        assert_eq!(acl.len(), 2);
        assert!(acl.authorize(b"team/x", Some("secret-a")));
        assert!(!acl.authorize(b"team/x", Some("secret-b")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_token_rejected() {
        let err = PrefixAcl::from_json(r#"{ "a/": "" }"#).unwrap_err();
        assert!(err.contains("non-empty"), "{err}");
    }

    fn two_tenants() -> TenantAcl {
        TenantAcl::from_json(
            r#"{
                "tenants": [
                    {"id": "acme", "token": "tok-acme", "prefix": "acme/"},
                    {"id": "globex", "token": "tok-globex", "prefix": "globex/"}
                ]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn tenant_exclusive_prefixes_rejected() {
        let err = TenantAcl::from_json(
            r#"{
                "tenants": [
                    {"id": "acme", "token": "t1", "prefix": "acme/"},
                    {"id": "acme-east", "token": "t2", "prefix": "acme/east/"}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("exclusive"), "{err}");
    }

    #[test]
    fn tenant_equal_prefixes_rejected() {
        let err = TenantAcl::from_json(
            r#"{
                "tenants": [
                    {"id": "a", "token": "t1", "prefix": "shared/"},
                    {"id": "b", "token": "t2", "prefix": "shared/"}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("exclusive"), "{err}");
    }

    #[test]
    fn tenant_duplicate_id_rejected() {
        let err = TenantAcl::from_json(
            r#"{
                "tenants": [
                    {"id": "acme", "token": "t1", "prefix": "a/"},
                    {"id": "acme", "token": "t2", "prefix": "b/"}
                ]
            }"#,
        )
        .unwrap_err();
        assert!(err.contains("duplicate tenant id"), "{err}");
    }

    #[test]
    fn tenant_empty_token_rejected() {
        let err = TenantAcl::from_json(
            r#"{ "tenants": [ {"id": "acme", "token": "", "prefix": "acme/"} ] }"#,
        )
        .unwrap_err();
        assert!(err.contains("non-empty"), "{err}");
    }

    #[test]
    fn tenant_missing_token_denies() {
        let t = two_tenants();
        assert!(!t.authorize(b"acme/k", None));
        assert!(!t.authorize_token(None));
        assert!(!t.authorize(b"acme/k", Some("unknown")));
        assert!(!t.authorize_token(Some("unknown")));
        assert!(t.tenant_id(None).is_none());
        assert!(t.tenant_id(Some("unknown")).is_none());
    }

    #[test]
    fn tenant_denies_other_tenants_key() {
        let t = two_tenants();
        assert!(t.authorize(b"acme/k", Some("tok-acme")));
        assert!(!t.authorize(b"globex/k", Some("tok-acme")));
        assert!(t.authorize(b"globex/k", Some("tok-globex")));
        assert!(!t.authorize(b"acme/k", Some("tok-globex")));
        assert!(!t.authorize(b"other/k", Some("tok-acme")));
        assert_eq!(t.tenant_id(Some("tok-acme")), Some("acme"));
        assert_eq!(t.tenant_id(Some("tok-globex")), Some("globex"));
        assert!(t.authorize_token(Some("tok-acme")));
        assert!(t.authorize_token(Some("tok-globex")));
    }

    #[test]
    fn tenant_empty_list_denies() {
        let t = TenantAcl::from_json(r#"{ "tenants": [] }"#).unwrap();
        assert!(t.is_empty());
        assert!(!t.authorize(b"any", Some("tok")));
        assert!(!t.authorize_token(Some("tok")));
    }

    #[test]
    fn tenant_and_prefix_acl_both_must_pass() {
        let tenants = two_tenants();
        let mut map = HashMap::new();
        map.insert("acme/".into(), "tok-acme".into());
        map.insert("globex/".into(), "other-tok".into());
        let acl = PrefixAcl::from_map(map).unwrap();

        // Token is a tenant and matches PrefixAcl for acme/.
        assert!(authorize_key(
            Some(&acl),
            Some(&tenants),
            b"acme/k",
            Some("tok-acme")
        ));
        // Tenant ok for globex/, but PrefixAcl token is "other-tok" not "tok-globex".
        assert!(!authorize_key(
            Some(&acl),
            Some(&tenants),
            b"globex/k",
            Some("tok-globex")
        ));
        // Tenant-only: globex token is enough.
        assert!(authorize_key(
            None,
            Some(&tenants),
            b"globex/k",
            Some("tok-globex")
        ));
        // Neither layer configured: open (client-token is a separate gate).
        assert!(authorize_key(None, None, b"any", None));
        assert!(authorize_token(None, None, None));
        assert!(authorize_token(
            Some(&acl),
            Some(&tenants),
            Some("tok-acme")
        ));
        assert!(!authorize_token(
            Some(&acl),
            Some(&tenants),
            Some("tok-globex")
        ));
    }

    #[test]
    fn tenant_load_file_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "kaya_tenant_ut_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tenants.json");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            write!(
                f,
                r#"{{"tenants":[{{"id":"acme","token":"tok-acme","prefix":"acme/"}}]}}"#
            )
            .unwrap();
        }
        let t = TenantAcl::load_file(&path).unwrap();
        assert_eq!(t.len(), 1);
        assert!(t.authorize(b"acme/x", Some("tok-acme")));
        assert!(!t.authorize(b"other/x", Some("tok-acme")));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
