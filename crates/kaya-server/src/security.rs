//! Production security boundary checks for cluster node startup.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// Returns true when `addr` binds to a loopback interface only.
pub fn is_loopback_bind(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}

/// Returns true when `addr` is an unrestricted wildcard bind.
pub fn is_wildcard_bind(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(v4) => v4 == Ipv4Addr::UNSPECIFIED,
        IpAddr::V6(v6) => v6 == Ipv6Addr::UNSPECIFIED,
    }
}

/// Validate that a listen address is permitted under the current policy.
///
/// By default only loopback binds are allowed (SEC-004). Public or wildcard
/// binds require `allow_public_bind`.
pub fn validate_bind_addr(addr: SocketAddr, allow_public_bind: bool) -> Result<(), String> {
    if is_loopback_bind(addr) {
        return Ok(());
    }
    if allow_public_bind {
        eprintln!(
            "warning: binding {addr} with --allow-public-bind; KayaDB has no auth/TLS"
        );
        return Ok(());
    }
    let kind = if is_wildcard_bind(addr) {
        "wildcard"
    } else {
        "non-loopback"
    };
    Err(format!(
        "refusing {kind} bind on {addr}: KayaDB has no authentication or TLS. \
         Use loopback (127.0.0.1) for local development, or pass --allow-public-bind \
         only on a trusted private network with external firewall controls."
    ))
}

/// Startup banner reminding operators of the trust model.
pub fn security_banner(allow_public_bind: bool) -> String {
    if allow_public_bind {
        "SECURITY: public bind enabled — no auth/TLS; restrict with firewall/mTLS wrapper"
            .to_owned()
    } else {
        "SECURITY: loopback-only bind — no auth/TLS in this build".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(host: &str, port: u16) -> SocketAddr {
        format!("{host}:{port}").parse().unwrap()
    }

    #[test]
    fn loopback_bind_allowed_without_flag() {
        assert!(validate_bind_addr(addr("127.0.0.1", 7379), false).is_ok());
    }

    #[test]
    fn public_bind_rejected_without_flag() {
        assert!(validate_bind_addr(addr("10.0.0.5", 7379), false).is_err());
    }

    #[test]
    fn wildcard_bind_rejected_without_flag() {
        assert!(validate_bind_addr(addr("0.0.0.0", 7379), false).is_err());
    }

    #[test]
    fn public_bind_allowed_with_flag() {
        assert!(validate_bind_addr(addr("10.0.0.5", 7379), true).is_ok());
    }
}