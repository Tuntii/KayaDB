//! Operator credential model and helpers for authenticated admin operations.
//!
//! For M13: simple optional shared token used only for ADD_MEMBER / REMOVE_MEMBER.
//! The wire format (ADMIN prefix + optional token) is defined in kaya-net.
//!
//! This module is intentionally small for Task 1 and will grow with enforcement logic.

pub use kaya_net::{
    decode_admin_payload, encode_admin_payload, ADMIN_AUTH_PREFIX, ADD_MEMBER_OPCODE,
    REMOVE_MEMBER_OPCODE,
};
