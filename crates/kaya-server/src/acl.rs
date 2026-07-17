//! Per-prefix ACL (M24): map key prefixes to client tokens.
//!
//! Config is a JSON object `prefix -> token`. Prefix keys may be UTF-8 text or
//! hex-encoded bytes (`0x…` / `hex:…`). When an ACL file is configured, data-path
//! ops (PUT/GET/DELETE/SCAN/TXN_*) authorize the presented client token against
//! the **longest** prefix that is a prefix of the request key. An empty ACL map
//! denies every such op.

use std::collections::HashMap;
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
        let map: HashMap<String, String> = serde_json::from_str(raw.trim())
            .map_err(|e| format!("parse ACL JSON: {e}"))?;
        Self::from_map(map)
    }

    /// Build ACL from an already-decoded map.
    pub fn from_map(map: HashMap<String, String>) -> Result<Self, String> {
        let mut rules = Vec::with_capacity(map.len());
        for (prefix_s, token) in map {
            if token.is_empty() {
                return Err(format!("ACL token for prefix {prefix_s:?} must be non-empty"));
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

    /// Authorize when there is no key (TXN_BEGIN / TXN_COMMIT / TXN_ROLLBACK):
    /// any token that appears on at least one rule is accepted. Empty ACL denies.
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

/// Decode a JSON map key into raw prefix bytes.
///
/// * `0xDEAD` / `0Xdead` / `hex:dead` → hex decode
/// * anything else → UTF-8 bytes of the string as-is
fn parse_prefix_key(s: &str) -> Result<Vec<u8>, String> {
    let lower = s.to_ascii_lowercase();
    if let Some(hex) = lower.strip_prefix("0x").or_else(|| lower.strip_prefix("hex:")) {
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
}
