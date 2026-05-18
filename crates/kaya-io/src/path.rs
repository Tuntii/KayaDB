use std::fmt;
use std::path::Path;

use kaya_core::{KayaError, Result};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RelativePath(String);

impl RelativePath {
    pub fn new(path: impl AsRef<str>) -> Result<Self> {
        let raw = path.as_ref().replace('\\', "/");
        let trimmed = raw.trim();

        if trimmed.is_empty() || trimmed == "." {
            return Ok(Self(String::new()));
        }

        if Path::new(trimmed).is_absolute()
            || trimmed.starts_with('/')
            || trimmed.as_bytes().get(1) == Some(&b':')
        {
            return Err(KayaError::invalid_argument(format!(
                "relative path must not be absolute: {trimmed}"
            )));
        }

        let mut parts = Vec::new();
        for part in trimmed.split('/') {
            match part {
                "" | "." => continue,
                ".." => {
                    return Err(KayaError::invalid_argument(format!(
                        "relative path must not contain '..': {trimmed}"
                    )));
                }
                component => parts.push(component),
            }
        }

        Ok(Self(parts.join("/")))
    }

    pub fn root() -> Self {
        Self(String::new())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn join(&self, child: impl AsRef<str>) -> Result<Self> {
        let child = RelativePath::new(child)?;
        if self.is_root() {
            return Ok(child);
        }
        if child.is_root() {
            return Ok(self.clone());
        }
        Self::new(format!("{}/{}", self.0, child.0))
    }

    pub fn file_name(&self) -> Option<&str> {
        self.0.rsplit('/').next().filter(|name| !name.is_empty())
    }

    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/').filter(|part| !part.is_empty())
    }
}

impl TryFrom<&str> for RelativePath {
    type Error = KayaError;

    fn try_from(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

impl fmt::Display for RelativePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_root() {
            write!(f, ".")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal() {
        assert!(RelativePath::new("../wal").is_err());
        assert!(RelativePath::new("wal/../x").is_err());
        assert!(RelativePath::new("C:\\tmp\\db").is_err());
    }

    #[test]
    fn normalizes_simple_paths() {
        let path = RelativePath::new("./wal//0001.wal").expect("valid path");
        assert_eq!(path.as_str(), "wal/0001.wal");
    }
}
