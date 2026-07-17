//! Operator credential model and helpers for authenticated admin operations.
//!
//! Optional shared token used for ADD_MEMBER / REMOVE_MEMBER / TRANSFER_LEADER /
//! PROMOTE_LEARNER / REBALANCE_PLAN. The wire format (ADMIN prefix + optional token)
//! is defined in kaya-net.
//!
//! This module is intentionally small for Task 1 and will grow with enforcement logic.

pub use kaya_net::{
    decode_admin_payload, encode_admin_payload, ADD_MEMBER_OPCODE, ADMIN_AUTH_PREFIX,
    PROMOTE_LEARNER_OPCODE, REBALANCE_PLAN_OPCODE, REMOVE_MEMBER_OPCODE, TRANSFER_LEADER_OPCODE,
};
