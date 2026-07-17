//! AES-256-GCM encryption-at-rest wrapper over any [`Disk`].
//!
//! # On-disk layout (per file)
//!
//! ```text
//! magic(8) = b"KAYAENC1"
//! plain_len(u64 LE)
//! nonce(12)
//! ciphertext || tag   // plain_len bytes of ciphertext + 16-byte GCM tag
//! ```
//!
//! Logical (plaintext) offsets are exposed through the [`Disk`] trait. Each
//! mutating content operation decrypts the whole file, applies the change, and
//! re-encrypts with a fresh nonce. v1 uses a single 32-byte key as both KEK and
//! DEK; key rotation is a documented follow-on.
//!
//! Concurrent content ops on the same instance (including clones) are serialized
//! via an internal lock so [`Disk::append`] atomicity holds after RMW.

use std::path::Path;
use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use kaya_core::{KayaError, Result};
use rand_core::{OsRng, RngCore};
use tokio::sync::Mutex;

use crate::{DirEntry, Disk, RelativePath};

/// On-disk magic identifying an AES-GCM sealed KayaDB file.
pub const ENC_MAGIC: &[u8; 8] = b"KAYAENC1";

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
const HEADER_LEN: usize = 8 + 8 + NONCE_LEN; // magic + plain_len + nonce

/// Disk wrapper that encrypts every file with AES-256-GCM.
#[derive(Clone)]
pub struct EncryptedDisk<D: Disk> {
    inner: D,
    key: [u8; 32],
    /// Serializes load-modify-store so concurrent appends never interleave.
    /// Async mutex so the guard is `Send` across `.await` points (tokio tasks).
    op_lock: Arc<Mutex<()>>,
}

impl<D: Disk> EncryptedDisk<D> {
    /// Wrap `inner` with the given 32-byte AES-256 key.
    pub fn new(inner: D, key: [u8; 32]) -> Self {
        Self {
            inner,
            key,
            op_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Load a raw 32-byte key from `path` and wrap `inner`.
    pub fn from_key_file(inner: D, path: impl AsRef<Path>) -> Result<Self> {
        let key = load_key_file(path)?;
        Ok(Self::new(inner, key))
    }

    /// Borrow the inner disk.
    pub fn inner(&self) -> &D {
        &self.inner
    }

    /// Borrow the raw key bytes (operators should not log this).
    pub fn key(&self) -> &[u8; 32] {
        &self.key
    }

    fn cipher(&self) -> Result<Aes256Gcm> {
        Aes256Gcm::new_from_slice(&self.key)
            .map_err(|e| KayaError::invalid_argument(format!("invalid AES-256-GCM key: {e}")))
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
        if physical_len < HEADER_LEN as u64 + TAG_LEN as u64 {
            return Err(KayaError::corruption(format!(
                "encrypted file {} is shorter than header+tag ({physical_len} bytes)",
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
        open_sealed(&self.cipher()?, &sealed)
    }

    async fn store_plain(&self, path: &RelativePath, plain: &[u8]) -> Result<()> {
        let sealed = seal(&self.cipher()?, plain)?;
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

fn seal(cipher: &Aes256Gcm, plain: &[u8]) -> Result<Vec<u8>> {
    let mut nonce_bytes = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let plain_len = plain.len() as u64;
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
}

fn open_sealed(cipher: &Aes256Gcm, sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < HEADER_LEN + TAG_LEN {
        return Err(KayaError::corruption(
            "encrypted file truncated (shorter than header+tag)",
        ));
    }
    if &sealed[..8] != ENC_MAGIC.as_slice() {
        return Err(KayaError::corruption(
            "encrypted file missing KAYAENC1 magic (wrong key mode or corrupt)",
        ));
    }
    let plain_len = u64::from_le_bytes(sealed[8..16].try_into().unwrap()) as usize;
    let nonce = Nonce::from_slice(&sealed[16..HEADER_LEN]);
    let ciphertext = &sealed[HEADER_LEN..];
    if ciphertext.len() != plain_len + TAG_LEN {
        return Err(KayaError::corruption(format!(
            "encrypted file length mismatch: plain_len={plain_len}, ct={}",
            ciphertext.len()
        )));
    }

    let mut aad = [0u8; 16];
    aad[..8].copy_from_slice(ENC_MAGIC);
    aad[8..].copy_from_slice(&(plain_len as u64).to_le_bytes());

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

/// Plaintext length from a sealed blob header, or `None` if not a sealed file.
fn plain_len_from_header(sealed_prefix: &[u8]) -> Option<u64> {
    if sealed_prefix.len() < 16 || &sealed_prefix[..8] != ENC_MAGIC.as_slice() {
        return None;
    }
    Some(u64::from_le_bytes(sealed_prefix[8..16].try_into().ok()?))
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
                    if physical >= 16 {
                        let mut hdr = [0u8; 16];
                        if self.inner.read_at(&entry.path, 0, &mut hdr).await.ok() == Some(16) {
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
        if physical < HEADER_LEN as u64 + TAG_LEN as u64 {
            return Err(KayaError::corruption(format!(
                "encrypted file {} is shorter than header+tag",
                path.as_str()
            )));
        }
        let mut hdr = [0u8; 16];
        let n = self.inner.read_at(path, 0, &mut hdr).await?;
        if n < 16 {
            return Err(KayaError::corruption("encrypted file header truncated"));
        }
        match plain_len_from_header(&hdr) {
            Some(len) => Ok(len),
            None => Err(KayaError::corruption(
                "encrypted file missing KAYAENC1 magic",
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
}
