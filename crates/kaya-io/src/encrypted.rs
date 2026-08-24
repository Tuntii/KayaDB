//! AES-256-GCM encryption-at-rest wrapper over any [`Disk`], with online
//! key-id rotation (KEK/DEK, #28).
//!
//! # On-disk layout (per file)
//!
//! Legacy v1 envelope (still readable, key id implicitly `0`):
//!
//! ```text
//! magic(8) = b"KAYAENC1"
//! plain_len(u64 LE)
//! nonce(12)
//! ciphertext || tag   // plain_len bytes of ciphertext + 16-byte GCM tag
//! ```
//!
//! v2 envelope, written once a [`Keyring`] carries more than the single
//! implicit id-0 key (i.e. any deployment that has rotated):
//!
//! ```text
//! magic(8) = b"KAYAENC2"
//! key_id(u32 LE)
//! plain_len(u64 LE)
//! nonce(12)
//! ciphertext || tag
//! ```
//!
//! `key_id` is bound into the AES-GCM AAD alongside the magic and length, so
//! tampering with the id in the header (to point a ciphertext at a different
//! key) fails authentication.
//!
//! Logical (plaintext) offsets are exposed through the [`Disk`] trait. Each
//! mutating content operation decrypts the whole file, applies the change, and
//! re-encrypts with a fresh nonce **under the active key**. Reads select the
//! decryption key from the id stored in the file's own header, so a rotation
//! opens a window in which old and new files coexist and are both readable —
//! see `docs/security.md` §7 for the operational rotation procedure and its
//! guarantees (there is no background re-encrypt in v1: old files upgrade to
//! the active key lazily, the next time something writes them).
//!
//! Concurrent content ops on the same instance (including clones) are serialized
//! via an internal lock so [`Disk::append`] atomicity holds after RMW.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::Path;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use kaya_core::{KayaError, Result};
use rand_core::{OsRng, RngCore};
use tokio::sync::Mutex;

use crate::{DirEntry, Disk, RelativePath};

/// On-disk magic identifying a legacy (single-key, pre-#28) AES-GCM sealed file.
/// Files carrying this magic have no key id in their header and are always
/// decrypted as key id `0`.
pub const ENC_MAGIC: &[u8; 8] = b"KAYAENC1";
/// On-disk magic identifying a keyed (post-#28) AES-GCM sealed file; the
/// header carries a `key_id` so the read path can pick the matching key.
pub const ENC_MAGIC2: &[u8; 8] = b"KAYAENC2";

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
/// magic + plain_len + nonce (legacy `KAYAENC1`).
const HEADER_LEN: usize = 8 + 8 + NONCE_LEN;
/// magic + key_id + plain_len + nonce (`KAYAENC2`).
const HEADER_LEN2: usize = 8 + 4 + 8 + NONCE_LEN;
/// Largest prefix any header-peek needs to read (covers both formats).
const HEADER_PEEK: usize = 20;

/// A set of AES-256 keys addressed by a small integer id, with one marked
/// active. The active key is always used to seal (encrypt) new/rewritten
/// files; any key in the ring can open (decrypt) a file whose header names
/// its id. Legacy `KAYAENC1` files carry no id and are treated as id `0`.
#[derive(Clone)]
pub struct Keyring {
    active_id: u32,
    keys: HashMap<u32, [u8; 32]>,
}

impl std::fmt::Debug for Keyring {
    // Deliberately omits key material: only ids are ever safe to print/log.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyring")
            .field("active_id", &self.active_id)
            .field("key_ids", &self.key_ids())
            .finish()
    }
}

impl Keyring {
    /// A keyring with a single active key. Equivalent to pre-#28 single-key
    /// mode when `active_id` is `0`.
    pub fn new(active_id: u32, active_key: [u8; 32]) -> Self {
        let mut keys = HashMap::new();
        keys.insert(active_id, active_key);
        Self { active_id, keys }
    }

    /// Add a previous-generation key: still usable to decrypt files sealed
    /// under it, never used to seal new ones.
    pub fn with_previous(mut self, id: u32, key: [u8; 32]) -> Self {
        self.keys.insert(id, key);
        self
    }

    /// Id of the key used for all new writes.
    pub fn active_id(&self) -> u32 {
        self.active_id
    }

    /// The active key's raw bytes (operators should not log this).
    pub fn active_key(&self) -> [u8; 32] {
        self.keys[&self.active_id]
    }

    /// Look up a key by id (active or previous).
    pub fn get(&self, id: u32) -> Option<&[u8; 32]> {
        self.keys.get(&id)
    }

    /// All key ids in the ring, ascending. Safe to log/print (no key material).
    pub fn key_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self.keys.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// True for a bare single-key-at-id-0 ring, i.e. a deployment that has
    /// never rotated. Files are sealed in the original `KAYAENC1` format so
    /// non-rotating deployments see zero on-disk format change.
    fn is_simple(&self) -> bool {
        self.active_id == 0 && self.keys.len() == 1
    }

    /// Rotate: `new_id` (must not already exist in the ring) becomes active
    /// with `new_key`; every existing key, including the previously-active
    /// one, is retained so files still sealed under them keep decrypting.
    pub fn rotate(&self, new_id: u32, new_key: [u8; 32]) -> Result<Self> {
        if self.keys.contains_key(&new_id) {
            return Err(KayaError::invalid_argument(format!(
                "key id {new_id} already exists in keyring"
            )));
        }
        let mut keys = self.keys.clone();
        keys.insert(new_id, new_key);
        Ok(Self {
            active_id: new_id,
            keys,
        })
    }
}

/// Disk wrapper that encrypts every file with AES-256-GCM.
#[derive(Clone)]
pub struct EncryptedDisk<D: Disk> {
    inner: D,
    keyring: Keyring,
    /// Serializes load-modify-store so concurrent appends never interleave.
    /// Async mutex so the guard is `Send` across `.await` points (tokio tasks).
    op_lock: Arc<Mutex<()>>,
}

impl<D: Disk> EncryptedDisk<D> {
    /// Wrap `inner` with a single 32-byte AES-256 key (id 0, no rotation).
    pub fn new(inner: D, key: [u8; 32]) -> Self {
        Self::with_keyring(inner, Keyring::new(0, key))
    }

    /// Wrap `inner` with a full [`Keyring`] (active + previous-generation keys).
    pub fn with_keyring(inner: D, keyring: Keyring) -> Self {
        Self {
            inner,
            keyring,
            op_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Load a raw 32-byte key from `path` and wrap `inner` (id 0, no rotation).
    pub fn from_key_file(inner: D, path: impl AsRef<Path>) -> Result<Self> {
        let key = load_key_file(path)?;
        Ok(Self::new(inner, key))
    }

    /// Load a [`Keyring`] from `path` and wrap `inner`.
    pub fn from_keyring_file(inner: D, path: impl AsRef<Path>) -> Result<Self> {
        let keyring = load_keyring_file(path)?;
        Ok(Self::with_keyring(inner, keyring))
    }

    /// Borrow the inner disk.
    pub fn inner(&self) -> &D {
        &self.inner
    }

    /// Borrow the keyring (operators should not log key material from it).
    pub fn keyring(&self) -> &Keyring {
        &self.keyring
    }

    async fn load_plain(&self, path: &RelativePath) -> Result<Vec<u8>> {
        let physical_len = match self.inner.file_len(path).await {
            Ok(n) => n,
            Err(KayaError::NotFound) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        if physical_len == 0 {
            return Ok(Vec::new());
        }
        if physical_len < HEADER_PEEK as u64 {
            return Err(KayaError::corruption(format!(
                "encrypted file {} is shorter than its header ({physical_len} bytes)",
                path.as_str()
            )));
        }

        let mut sealed = vec![0u8; physical_len as usize];
        let mut read = 0usize;
        while read < sealed.len() {
            let n = self
                .inner
                .read_at(path, read as u64, &mut sealed[read..])
                .await?;
            if n == 0 {
                return Err(KayaError::corruption(format!(
                    "unexpected EOF reading encrypted file {}",
                    path.as_str()
                )));
            }
            read += n;
        }
        open_sealed(&self.keyring, path, &sealed)
    }

    async fn store_plain(&self, path: &RelativePath, plain: &[u8]) -> Result<()> {
        let sealed = seal(&self.keyring, plain)?;
        let mut written = 0usize;
        while written < sealed.len() {
            let n = self
                .inner
                .write_at(path, written as u64, &sealed[written..])
                .await?;
            if n == 0 {
                return Err(KayaError::Io {
                    message: format!("short write sealing {}", path.as_str()),
                });
            }
            written += n;
        }
        self.inner.truncate(path, sealed.len() as u64).await?;
        Ok(())
    }
}

/// Read exactly 32 raw key bytes from a file.
pub fn load_key_file(path: impl AsRef<Path>) -> Result<[u8; 32]> {
    let path = path.as_ref();
    let bytes = std::fs::read(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            KayaError::NotFound
        } else {
            KayaError::from(e)
        }
    })?;
    if bytes.len() != 32 {
        return Err(KayaError::invalid_argument(format!(
            "encryption key file {} must be exactly 32 bytes, got {}",
            path.display(),
            bytes.len()
        )));
    }
    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Load a [`Keyring`] from a text file.
///
/// Format (blank lines and `#` comments ignored), one directive per line:
///
/// ```text
/// active <id>
/// key <id> <64 lowercase hex chars>
/// ```
///
/// Every `key` id referenced anywhere is retained so old envelopes keep
/// decrypting; `active` selects which one seals new writes. See
/// `docs/security.md` §7 and `docs/runbooks/key-rotation.md`.
pub fn load_keyring_file(path: impl AsRef<Path>) -> Result<Keyring> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            KayaError::NotFound
        } else {
            KayaError::from(e)
        }
    })?;
    parse_keyring(&text).map_err(|msg| {
        KayaError::invalid_argument(format!("keyring file {}: {msg}", path.display()))
    })
}

/// Write a [`Keyring`] to `path` in the format read by [`load_keyring_file`].
pub fn save_keyring_file(path: impl AsRef<Path>, keyring: &Keyring) -> Result<()> {
    let mut out = String::new();
    out.push_str("# KayaDB encryption keyring: key material, treat like a private key.\n");
    let _ = writeln!(out, "active {}", keyring.active_id());
    for id in keyring.key_ids() {
        let key = keyring.get(id).expect("id from key_ids() exists");
        let _ = writeln!(out, "key {id} {}", encode_hex_key(key));
    }
    std::fs::write(path, out).map_err(KayaError::from)
}

fn parse_keyring(text: &str) -> std::result::Result<Keyring, String> {
    let mut active_id: Option<u32> = None;
    let mut keys: HashMap<u32, [u8; 32]> = HashMap::new();
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let lineno = idx + 1;
        let mut parts = line.split_whitespace();
        match parts.next() {
            Some("active") => {
                let id = parts
                    .next()
                    .ok_or_else(|| format!("line {lineno}: 'active' missing id"))?;
                active_id = Some(
                    id.parse::<u32>()
                        .map_err(|e| format!("line {lineno}: bad active id: {e}"))?,
                );
            }
            Some("key") => {
                let id = parts
                    .next()
                    .ok_or_else(|| format!("line {lineno}: 'key' missing id"))?
                    .parse::<u32>()
                    .map_err(|e| format!("line {lineno}: bad key id: {e}"))?;
                let hex = parts
                    .next()
                    .ok_or_else(|| format!("line {lineno}: 'key' missing hex bytes"))?;
                let key = decode_hex_key(hex).map_err(|e| format!("line {lineno}: {e}"))?;
                if keys.insert(id, key).is_some() {
                    return Err(format!("line {lineno}: duplicate key id {id}"));
                }
            }
            Some(other) => return Err(format!("line {lineno}: unknown directive '{other}'")),
            None => {}
        }
    }
    let active_id = active_id.ok_or_else(|| "missing 'active <id>' line".to_owned())?;
    if keys.is_empty() {
        return Err("keyring has no 'key' lines".to_owned());
    }
    if !keys.contains_key(&active_id) {
        return Err(format!(
            "active key id {active_id} has no matching 'key {active_id} <hex>' line"
        ));
    }
    Ok(Keyring { active_id, keys })
}

fn decode_hex_key(hex: &str) -> std::result::Result<[u8; 32], String> {
    if hex.len() != 64 {
        return Err(format!(
            "key must be 64 hex chars (32 bytes), got {}",
            hex.len()
        ));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| "invalid hex digit in key".to_owned())?;
    }
    Ok(out)
}

fn encode_hex_key(key: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in key {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Generate a fresh random 32-byte AES-256 key from the OS CSPRNG. Used by
/// `kayactl encryption init`/`rotate` to provision new key material.
pub fn generate_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);
    key
}

/// Attempt to authenticate and decrypt a sealed blob with `keyring`, without
/// needing a [`Disk`]. Discards the plaintext on success. Used by `kayactl
/// encryption verify` to sanity-check a keyring against on-disk files (the
/// caller already knows and reports the real file path on failure).
pub fn verify_sealed(keyring: &Keyring, sealed: &[u8]) -> Result<()> {
    let path = RelativePath::new("file").expect("static literal is a valid RelativePath");
    open_sealed(keyring, &path, sealed).map(|_| ())
}

fn cipher_for(key: &[u8; 32]) -> Result<Aes256Gcm> {
    Aes256Gcm::new_from_slice(key)
        .map_err(|e| KayaError::invalid_argument(format!("invalid AES-256-GCM key: {e}")))
}

fn seal(keyring: &Keyring, plain: &[u8]) -> Result<Vec<u8>> {
    let mut nonce_bytes = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let plain_len = plain.len() as u64;
    let active_id = keyring.active_id();
    let cipher = cipher_for(&keyring.active_key())?;

    if keyring.is_simple() {
        // Byte-for-byte identical to pre-#28 output: non-rotating deployments
        // see no on-disk format change.
        let mut aad = [0u8; 16];
        aad[..8].copy_from_slice(ENC_MAGIC);
        aad[8..].copy_from_slice(&plain_len.to_le_bytes());
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plain,
                    aad: &aad,
                },
            )
            .map_err(|e| KayaError::internal(format!("AES-GCM encrypt failed: {e}")))?;

        let mut out = Vec::with_capacity(HEADER_LEN + ciphertext.len());
        out.extend_from_slice(ENC_MAGIC);
        out.extend_from_slice(&plain_len.to_le_bytes());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    } else {
        let mut aad = [0u8; 20];
        aad[..8].copy_from_slice(ENC_MAGIC2);
        aad[8..12].copy_from_slice(&active_id.to_le_bytes());
        aad[12..20].copy_from_slice(&plain_len.to_le_bytes());
        let ciphertext = cipher
            .encrypt(
                nonce,
                Payload {
                    msg: plain,
                    aad: &aad,
                },
            )
            .map_err(|e| KayaError::internal(format!("AES-GCM encrypt failed: {e}")))?;

        let mut out = Vec::with_capacity(HEADER_LEN2 + ciphertext.len());
        out.extend_from_slice(ENC_MAGIC2);
        out.extend_from_slice(&active_id.to_le_bytes());
        out.extend_from_slice(&plain_len.to_le_bytes());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }
}

/// Parsed envelope header, common to both `KAYAENC1` and `KAYAENC2`.
struct Header {
    key_id: u32,
    header_len: usize,
    plain_len: u64,
}

fn parse_header(sealed_prefix: &[u8]) -> Result<Header> {
    if sealed_prefix.len() < 8 {
        return Err(KayaError::corruption(
            "encrypted file too short to contain a magic",
        ));
    }
    if &sealed_prefix[..8] == ENC_MAGIC.as_slice() {
        if sealed_prefix.len() < 16 {
            return Err(KayaError::corruption("legacy encrypted header truncated"));
        }
        let plain_len = u64::from_le_bytes(sealed_prefix[8..16].try_into().unwrap());
        Ok(Header {
            key_id: 0,
            header_len: HEADER_LEN,
            plain_len,
        })
    } else if &sealed_prefix[..8] == ENC_MAGIC2.as_slice() {
        if sealed_prefix.len() < HEADER_PEEK {
            return Err(KayaError::corruption("keyed encrypted header truncated"));
        }
        let key_id = u32::from_le_bytes(sealed_prefix[8..12].try_into().unwrap());
        let plain_len = u64::from_le_bytes(sealed_prefix[12..20].try_into().unwrap());
        Ok(Header {
            key_id,
            header_len: HEADER_LEN2,
            plain_len,
        })
    } else {
        Err(KayaError::corruption(
            "encrypted file missing KAYAENC1/KAYAENC2 magic (wrong key mode or corrupt)",
        ))
    }
}

fn open_sealed(keyring: &Keyring, path: &RelativePath, sealed: &[u8]) -> Result<Vec<u8>> {
    let peek_len = HEADER_PEEK.min(sealed.len());
    let header = parse_header(&sealed[..peek_len])?;
    if sealed.len() < header.header_len + TAG_LEN {
        return Err(KayaError::corruption(
            "encrypted file truncated (shorter than header+tag)",
        ));
    }
    let plain_len = header.plain_len as usize;
    let nonce = Nonce::from_slice(&sealed[header.header_len - NONCE_LEN..header.header_len]);
    let ciphertext = &sealed[header.header_len..];
    if ciphertext.len() != plain_len + TAG_LEN {
        return Err(KayaError::corruption(format!(
            "encrypted file length mismatch: plain_len={plain_len}, ct={}",
            ciphertext.len()
        )));
    }

    let key = keyring.get(header.key_id).ok_or_else(|| {
        KayaError::invalid_argument(format!(
            "no key id {} in keyring to decrypt {} (see `kayactl encryption list`)",
            header.key_id,
            path.as_str()
        ))
    })?;
    let cipher = cipher_for(key)?;

    let aad: Vec<u8> = if header.header_len == HEADER_LEN {
        let mut aad = vec![0u8; 16];
        aad[..8].copy_from_slice(ENC_MAGIC);
        aad[8..].copy_from_slice(&header.plain_len.to_le_bytes());
        aad
    } else {
        let mut aad = vec![0u8; 20];
        aad[..8].copy_from_slice(ENC_MAGIC2);
        aad[8..12].copy_from_slice(&header.key_id.to_le_bytes());
        aad[12..20].copy_from_slice(&header.plain_len.to_le_bytes());
        aad
    };

    cipher
        .decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|_| {
            KayaError::corruption("AES-GCM authentication failed (wrong key or corrupt ciphertext)")
        })
}

/// Plaintext length from a sealed blob header, or `None` if not recognized.
fn plain_len_from_header(sealed_prefix: &[u8]) -> Option<u64> {
    parse_header(sealed_prefix).ok().map(|h| h.plain_len)
}

impl<D: Disk> Disk for EncryptedDisk<D> {
    async fn read_at(&self, path: &RelativePath, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let _guard = self.op_lock.lock().await;
        let plain = self.load_plain(path).await?;
        let start = offset as usize;
        if start >= plain.len() {
            return Ok(0);
        }
        let n = (plain.len() - start).min(buf.len());
        buf[..n].copy_from_slice(&plain[start..start + n]);
        Ok(n)
    }

    async fn write_at(&self, path: &RelativePath, offset: u64, buf: &[u8]) -> Result<usize> {
        let _guard = self.op_lock.lock().await;
        let mut plain = self.load_plain(path).await?;
        let start = offset as usize;
        let end = start + buf.len();
        if plain.len() < end {
            plain.resize(end, 0);
        }
        plain[start..end].copy_from_slice(buf);
        self.store_plain(path, &plain).await?;
        Ok(buf.len())
    }

    async fn append(&self, path: &RelativePath, buf: &[u8]) -> Result<u64> {
        let _guard = self.op_lock.lock().await;
        let mut plain = self.load_plain(path).await?;
        let offset = plain.len() as u64;
        plain.extend_from_slice(buf);
        self.store_plain(path, &plain).await?;
        Ok(offset)
    }

    async fn fsync_file(&self, path: &RelativePath) -> Result<()> {
        self.inner.fsync_file(path).await
    }

    async fn fsync_dir(&self, path: &RelativePath) -> Result<()> {
        self.inner.fsync_dir(path).await
    }

    async fn truncate(&self, path: &RelativePath, len: u64) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        let mut plain = self.load_plain(path).await?;
        let new_len = len as usize;
        if new_len > plain.len() {
            plain.resize(new_len, 0);
        } else {
            plain.truncate(new_len);
        }
        self.store_plain(path, &plain).await?;
        Ok(())
    }

    async fn rename(&self, from: &RelativePath, to: &RelativePath) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        self.inner.rename(from, to).await
    }

    async fn remove_file(&self, path: &RelativePath) -> Result<()> {
        let _guard = self.op_lock.lock().await;
        self.inner.remove_file(path).await
    }

    async fn list_dir(&self, path: &RelativePath) -> Result<Vec<DirEntry>> {
        let entries = self.inner.list_dir(path).await?;
        let mut out = Vec::with_capacity(entries.len());
        for mut entry in entries {
            if !entry.is_dir {
                // Best-effort: report plaintext length from header when present.
                if let Ok(physical) = self.inner.file_len(&entry.path).await {
                    if physical >= HEADER_PEEK as u64 {
                        let mut hdr = [0u8; HEADER_PEEK];
                        if self.inner.read_at(&entry.path, 0, &mut hdr).await.ok()
                            == Some(HEADER_PEEK)
                        {
                            if let Some(plain_len) = plain_len_from_header(&hdr) {
                                entry.len = plain_len;
                            }
                        }
                    }
                }
            }
            out.push(entry);
        }
        Ok(out)
    }

    async fn file_len(&self, path: &RelativePath) -> Result<u64> {
        let _guard = self.op_lock.lock().await;
        // Prefer header plain_len without full decrypt when layout is valid.
        let physical = self.inner.file_len(path).await?;
        if physical == 0 {
            return Ok(0);
        }
        if physical < HEADER_PEEK as u64 {
            return Err(KayaError::corruption(format!(
                "encrypted file {} is shorter than its header",
                path.as_str()
            )));
        }
        let mut hdr = [0u8; HEADER_PEEK];
        let n = self.inner.read_at(path, 0, &mut hdr).await?;
        if n < HEADER_PEEK {
            return Err(KayaError::corruption("encrypted file header truncated"));
        }
        match plain_len_from_header(&hdr) {
            Some(len) => Ok(len),
            None => Err(KayaError::corruption(
                "encrypted file missing KAYAENC1/KAYAENC2 magic",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SimDisk;
    use std::sync::Arc;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn test_key() -> [u8; 32] {
        *b"0123456789abcdef0123456789abcdef"
    }

    fn other_key() -> [u8; 32] {
        *b"fedcba9876543210fedcba9876543210"
    }

    #[test]
    fn roundtrip_write_read() {
        let disk = EncryptedDisk::new(SimDisk::new(), test_key());
        let path = RelativePath::new("enc/roundtrip.bin").unwrap();
        block_on(async {
            disk.write_at(&path, 0, b"secret-payload").await.unwrap();
            let mut buf = [0u8; 14];
            let n = disk.read_at(&path, 0, &mut buf).await.unwrap();
            assert_eq!(n, 14);
            assert_eq!(&buf, b"secret-payload");
            assert_eq!(disk.file_len(&path).await.unwrap(), 14);
        });
    }

    #[test]
    fn ciphertext_is_not_plaintext() {
        let inner = SimDisk::new();
        let disk = EncryptedDisk::new(inner.clone(), test_key());
        let path = RelativePath::new("enc/hidden.bin").unwrap();
        block_on(async {
            disk.append(&path, b"visible-if-leaked").await.unwrap();
            // Physical bytes on inner must not contain the plaintext.
            let phys_len = inner.file_len(&path).await.unwrap();
            let mut raw = vec![0u8; phys_len as usize];
            inner.read_at(&path, 0, &mut raw).await.unwrap();
            assert!(
                !raw.windows(b"visible-if-leaked".len())
                    .any(|w| w == b"visible-if-leaked"),
                "plaintext leaked into on-disk bytes"
            );
            assert_eq!(&raw[..8], ENC_MAGIC);
        });
    }

    #[test]
    fn wrong_key_fails_auth() {
        let path = RelativePath::new("enc/auth.bin").unwrap();
        let inner = SimDisk::new();
        let disk = EncryptedDisk::new(inner.clone(), test_key());
        block_on(async {
            disk.append(&path, b"auth-check").await.unwrap();
        });
        let mut bad_key = test_key();
        bad_key[0] ^= 0xff;
        let other = EncryptedDisk::new(inner, bad_key);
        let err = block_on(other.read_at(&path, 0, &mut [0u8; 16])).unwrap_err();
        assert!(
            matches!(err, KayaError::Corruption { .. }),
            "expected corruption, got {err:?}"
        );
    }

    #[test]
    fn load_key_file_requires_32_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.bin");
        std::fs::write(&path, [0u8; 16]).unwrap();
        let err = load_key_file(&path).unwrap_err();
        assert!(matches!(err, KayaError::InvalidArgument { .. }));

        std::fs::write(&path, [7u8; 32]).unwrap();
        let key = load_key_file(&path).unwrap();
        assert_eq!(key, [7u8; 32]);
    }

    #[test]
    fn append_returns_plaintext_offset() {
        let disk = Arc::new(EncryptedDisk::new(SimDisk::new(), test_key()));
        let path = RelativePath::new("enc/app.bin").unwrap();
        block_on(async {
            assert_eq!(disk.append(&path, b"aaa").await.unwrap(), 0);
            assert_eq!(disk.append(&path, b"bb").await.unwrap(), 3);
            let mut buf = [0u8; 5];
            disk.read_at(&path, 0, &mut buf).await.unwrap();
            assert_eq!(&buf, b"aaabb");
        });
    }

    // ── #28: key rotation ───────────────────────────────────────────────────

    #[test]
    fn simple_keyring_writes_legacy_format() {
        // A non-rotating deployment (single key, id 0) must produce byte-for-byte
        // the same envelope as pre-#28 (magic KAYAENC1, no key id field).
        let inner = SimDisk::new();
        let disk = EncryptedDisk::new(inner.clone(), test_key());
        let path = RelativePath::new("enc/legacy.bin").unwrap();
        block_on(disk.append(&path, b"payload")).unwrap();
        let phys_len = block_on(inner.file_len(&path)).unwrap();
        let mut raw = vec![0u8; phys_len as usize];
        block_on(inner.read_at(&path, 0, &mut raw)).unwrap();
        assert_eq!(&raw[..8], ENC_MAGIC);
    }

    #[test]
    fn rotated_keyring_reads_pre_rotation_files_and_writes_new_key() {
        let inner = SimDisk::new();
        let old_key = test_key();
        let new_key = other_key();

        // Write under the pre-rotation single key (legacy id 0).
        let path = RelativePath::new("enc/rotate.bin").unwrap();
        block_on(async {
            let disk = EncryptedDisk::new(inner.clone(), old_key);
            disk.append(&path, b"before-rotation").await.unwrap();
        });

        // Rotate: keyring now has id 1 active, id 0 retained as previous.
        let keyring = Keyring::new(0, old_key).rotate(1, new_key).unwrap();
        let rotated = EncryptedDisk::with_keyring(inner.clone(), keyring);

        // Old data, sealed under key id 0, is still readable through the window.
        let mut buf = [0u8; 15];
        block_on(rotated.read_at(&path, 0, &mut buf)).unwrap();
        assert_eq!(&buf, b"before-rotation");

        // Any write always re-seals under the new active key (id 1) and upgrades
        // the file to the keyed KAYAENC2 envelope.
        block_on(rotated.append(&path, b"-after")).unwrap();
        let phys_len = block_on(inner.file_len(&path)).unwrap();
        let mut raw = vec![0u8; phys_len as usize];
        block_on(inner.read_at(&path, 0, &mut raw)).unwrap();
        assert_eq!(&raw[..8], ENC_MAGIC2);
        let key_id = u32::from_le_bytes(raw[8..12].try_into().unwrap());
        assert_eq!(key_id, 1);

        // Readable again with only the new key still present (old key pruned)
        // once every file has rolled over.
        let mut check = [0u8; 21];
        block_on(rotated.read_at(&path, 0, &mut check)).unwrap();
        assert_eq!(&check, b"before-rotation-after");

        // The pre-rotation key alone can no longer decrypt the rewritten file.
        let old_only = EncryptedDisk::new(inner.clone(), old_key);
        let err = block_on(old_only.read_at(&path, 0, &mut [0u8; 4])).unwrap_err();
        assert!(matches!(err, KayaError::InvalidArgument { .. }));
    }

    #[test]
    fn rotate_rejects_duplicate_id() {
        let ring = Keyring::new(0, test_key());
        let err = ring.rotate(0, other_key()).unwrap_err();
        assert!(matches!(err, KayaError::InvalidArgument { .. }));
    }

    #[test]
    fn keyring_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keyring.txt");
        let ring = Keyring::new(0, test_key()).rotate(1, other_key()).unwrap();
        save_keyring_file(&path, &ring).unwrap();

        let loaded = load_keyring_file(&path).unwrap();
        assert_eq!(loaded.active_id(), 1);
        assert_eq!(loaded.key_ids(), vec![0, 1]);
        assert_eq!(*loaded.get(0).unwrap(), test_key());
        assert_eq!(*loaded.get(1).unwrap(), other_key());
    }

    #[test]
    fn keyring_file_rejects_missing_active_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.txt");
        std::fs::write(
            &path,
            "active 5\nkey 0 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n",
        )
        .unwrap();
        let err = load_keyring_file(&path).unwrap_err();
        assert!(matches!(err, KayaError::InvalidArgument { .. }));
    }
}
