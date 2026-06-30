//! Client credential model and helpers for authenticated data-path operations.
//!
//! For M13: simple optional shared token used for PUT/GET/DELETE/SCAN/STATS.
//! The wire format (CLIENT prefix + optional token) is defined in kaya-net.
//!
//! This module is intentionally small and mirrors `operator_auth`.

pub use kaya_net::{decode_client_auth_payload, encode_client_auth_payload, CLIENT_AUTH_PREFIX};
