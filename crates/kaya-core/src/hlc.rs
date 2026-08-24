//! Hybrid Logical Clock (HLC) for M20 multi-raft / cross-shard timestamps.
//!
//! Packing for use as `commit_ts`: `(physical_ms << 16) | logical`.
//! Physical time is milliseconds; logical is a 16-bit counter that advances
//! when the wall clock stalls or when receiving a remote timestamp.
//!
//! # Uncertainty interval (v1 / #27)
//!
//! Plain `update`/`tick` still trust the merge rule (`max(local, wall,
//! remote)`) unconditionally; they are the low-level primitives and stay
//! infallible. Callers who receive a remote HLC sample (e.g.
//! `Engine::sync_clock`) should go through [`Hlc::checked_update`] instead,
//! which rejects a remote physical time that is implausibly far ahead of the
//! local wall clock (more than `max_offset_ms`, the configured uncertainty
//! bound — see `EngineConfig::max_clock_offset_micros`) rather than silently
//! pulling the local clock into the future.
//!
//! [`Hlc::lead_over_wall_ms`] reports how far a clock's physical component
//! currently leads real wall-clock time (nonzero right after a remote sample
//! genuinely ahead of local time was merged in, within the uncertainty
//! bound). The engine write path (`prepare_hlc_write_sequence`) waits out
//! that lead before a commit_ts derived from it is written/exposed, so a
//! commit is never observable before the wall clock has caught up to it.
//! See `spec/docs/transactions-spec.md` §17.7.

/// Hybrid logical clock: physical milliseconds + logical counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct Hlc {
    pub physical_ms: u64,
    pub logical: u16,
}

impl Hlc {
    /// Zero clock (epoch origin).
    pub fn zero() -> Self {
        Self {
            physical_ms: 0,
            logical: 0,
        }
    }

    /// Update with local wall clock and optional remote timestamp; return new HLC.
    ///
    /// Implements the standard HLC `update` rule (CockroachDB / PT / hybrid clocks):
    ///
    /// ```text
    /// pt' = max(local.pt, wall, remote.pt)
    /// if pt' == local.pt == remote.pt:  l' = max(local.l, remote.l) + 1
    /// else if pt' == local.pt:          l' = local.l + 1
    /// else if pt' == remote.pt:         l' = remote.l + 1
    /// else:                             l' = 0   // advanced solely by wall
    /// ```
    pub fn update(&mut self, now_ms: u64, remote: Option<Hlc>) -> Hlc {
        let remote = remote.unwrap_or_else(Hlc::zero);
        let pt = now_ms.max(self.physical_ms).max(remote.physical_ms);

        let logical = if pt == self.physical_ms && pt == remote.physical_ms {
            self.logical.max(remote.logical).saturating_add(1)
        } else if pt == self.physical_ms {
            self.logical.saturating_add(1)
        } else if pt == remote.physical_ms {
            remote.logical.saturating_add(1)
        } else {
            0
        };

        *self = Hlc {
            physical_ms: pt,
            logical,
        };
        *self
    }

    /// Advance using only the local wall clock (no remote sample).
    pub fn tick(&mut self, now_ms: u64) -> Hlc {
        self.update(now_ms, None)
    }

    /// Merge a remote HLC sample like [`Hlc::update`], but reject it when its
    /// physical component is more than `max_offset_ms` ahead of the local
    /// wall clock `now_ms` — the configured uncertainty bound. Guards
    /// against a single skewed or misbehaving peer dragging this node's
    /// clock arbitrarily far into the future.
    pub fn checked_update(
        &mut self,
        now_ms: u64,
        remote: Option<Hlc>,
        max_offset_ms: u64,
    ) -> Result<Hlc, ClockSkewExceeded> {
        if let Some(r) = remote {
            if r.physical_ms > now_ms.saturating_add(max_offset_ms) {
                return Err(ClockSkewExceeded {
                    local_now_ms: now_ms,
                    remote_physical_ms: r.physical_ms,
                    max_offset_ms,
                });
            }
        }
        Ok(self.update(now_ms, remote))
    }

    /// How far this clock's physical component leads real wall-clock time
    /// `now_ms`, in milliseconds. Zero in the common case; positive right
    /// after a `checked_update`/`update` merged in a remote sample genuinely
    /// ahead of local time (bounded by the uncertainty bound, since anything
    /// further ahead is rejected by `checked_update`). Callers wait out this
    /// lead before exposing a commit_ts derived from this clock, so it is
    /// never observable before the wall clock has actually caught up to it.
    pub fn lead_over_wall_ms(self, now_ms: u64) -> u64 {
        self.physical_ms.saturating_sub(now_ms)
    }

    /// Pack to `u64` for use as `commit_ts`: `(physical_ms << 16) | logical`.
    ///
    /// Physical milliseconds above `u64::MAX >> 16` are saturated so the shift
    /// does not overflow (far-future wall clocks).
    pub fn to_u64(self) -> u64 {
        let phys = self.physical_ms.min(u64::MAX >> 16);
        (phys << 16) | u64::from(self.logical)
    }

    /// Unpack from a `commit_ts` produced by [`Hlc::to_u64`].
    pub fn from_u64(v: u64) -> Self {
        Self {
            physical_ms: v >> 16,
            logical: (v & 0xFFFF) as u16,
        }
    }
}

/// A remote HLC observation's physical component was more than the
/// configured uncertainty bound ahead of the local wall clock, and was
/// rejected by [`Hlc::checked_update`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSkewExceeded {
    pub local_now_ms: u64,
    pub remote_physical_ms: u64,
    pub max_offset_ms: u64,
}

impl core::fmt::Display for ClockSkewExceeded {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "remote clock offset {}ms exceeds max_clock_offset_ms={} (local wall clock {}ms, remote physical {}ms)",
            self.remote_physical_ms.saturating_sub(self.local_now_ms),
            self.max_offset_ms,
            self.local_now_ms,
            self.remote_physical_ms,
        )
    }
}

impl std::error::Error for ClockSkewExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_and_order() {
        assert_eq!(Hlc::zero(), Hlc::default());
        let a = Hlc {
            physical_ms: 1,
            logical: 0,
        };
        let b = Hlc {
            physical_ms: 1,
            logical: 1,
        };
        assert!(a < b);
        assert!(
            b < Hlc {
                physical_ms: 2,
                logical: 0
            }
        );
    }

    #[test]
    fn tick_advances_physical_when_wall_moves() {
        let mut h = Hlc::zero();
        let t1 = h.tick(1000);
        assert_eq!(
            t1,
            Hlc {
                physical_ms: 1000,
                logical: 0
            }
        );
        let t2 = h.tick(1001);
        assert_eq!(
            t2,
            Hlc {
                physical_ms: 1001,
                logical: 0
            }
        );
    }

    #[test]
    fn tick_monotonic_when_now_ms_stalls() {
        let mut h = Hlc::zero();
        let a = h.tick(5000);
        let b = h.tick(5000);
        let c = h.tick(5000);
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a.physical_ms, 5000);
        assert_eq!(b.physical_ms, 5000);
        assert_eq!(c.physical_ms, 5000);
        assert_eq!(a.logical, 0);
        assert_eq!(b.logical, 1);
        assert_eq!(c.logical, 2);
        // Packed commit_ts also strictly increases.
        assert!(a.to_u64() < b.to_u64());
        assert!(b.to_u64() < c.to_u64());
    }

    #[test]
    fn tick_keeps_physical_when_wall_goes_backwards() {
        let mut h = Hlc {
            physical_ms: 10_000,
            logical: 3,
        };
        let t = h.tick(9_000);
        assert_eq!(t.physical_ms, 10_000);
        assert_eq!(t.logical, 4);
    }

    #[test]
    fn update_tracks_remote_ahead() {
        let mut h = Hlc {
            physical_ms: 100,
            logical: 2,
        };
        let remote = Hlc {
            physical_ms: 200,
            logical: 5,
        };
        // wall still behind remote
        let t = h.update(150, Some(remote));
        assert_eq!(
            t,
            Hlc {
                physical_ms: 200,
                logical: 6
            }
        );
    }

    #[test]
    fn update_same_physical_takes_max_logical_plus_one() {
        let mut h = Hlc {
            physical_ms: 100,
            logical: 7,
        };
        let remote = Hlc {
            physical_ms: 100,
            logical: 3,
        };
        let t = h.update(100, Some(remote));
        assert_eq!(
            t,
            Hlc {
                physical_ms: 100,
                logical: 8
            }
        );
    }

    #[test]
    fn pack_unpack_roundtrip() {
        let h = Hlc {
            physical_ms: 1_700_000_000_000,
            logical: 42,
        };
        assert_eq!(Hlc::from_u64(h.to_u64()), h);
    }

    #[test]
    fn pack_uses_low_16_for_logical() {
        let h = Hlc {
            physical_ms: 1,
            logical: 0xFFFF,
        };
        assert_eq!(h.to_u64(), (1u64 << 16) | 0xFFFF);
        let h2 = Hlc {
            physical_ms: 1,
            logical: 0,
        };
        assert_eq!(h2.to_u64(), 1u64 << 16);
    }

    // ── uncertainty interval (#27) ──────────────────────────────────────────

    #[test]
    fn checked_update_accepts_remote_within_bound() {
        let mut h = Hlc {
            physical_ms: 1_000,
            logical: 0,
        };
        let remote = Hlc {
            physical_ms: 1_400, // 400ms ahead of local wall clock (1000)
            logical: 0,
        };
        let t = h
            .checked_update(1_000, Some(remote), 500)
            .expect("within 500ms bound");
        assert_eq!(t.physical_ms, 1_400);
    }

    #[test]
    fn checked_update_rejects_remote_beyond_bound() {
        let mut h = Hlc {
            physical_ms: 1_000,
            logical: 0,
        };
        let remote = Hlc {
            physical_ms: 2_000, // 1000ms ahead of local wall clock (1000)
            logical: 0,
        };
        let before = h;
        let err = h
            .checked_update(1_000, Some(remote), 500)
            .expect_err("1000ms skew exceeds 500ms bound");
        assert_eq!(err.local_now_ms, 1_000);
        assert_eq!(err.remote_physical_ms, 2_000);
        assert_eq!(err.max_offset_ms, 500);
        // Rejected merge must not mutate the local clock.
        assert_eq!(h, before);
    }

    #[test]
    fn checked_update_boundary_is_inclusive() {
        let mut h = Hlc::zero();
        let remote = Hlc {
            physical_ms: 1_500,
            logical: 0,
        };
        // Exactly at the bound (1000 + 500 = 1500) is accepted.
        assert!(h.checked_update(1_000, Some(remote), 500).is_ok());
        let mut h2 = Hlc::zero();
        let remote2 = Hlc {
            physical_ms: 1_501,
            logical: 0,
        };
        assert!(h2.checked_update(1_000, Some(remote2), 500).is_err());
    }

    #[test]
    fn lead_over_wall_ms_zero_when_not_ahead() {
        let h = Hlc {
            physical_ms: 1_000,
            logical: 0,
        };
        assert_eq!(h.lead_over_wall_ms(1_000), 0);
        assert_eq!(h.lead_over_wall_ms(2_000), 0);
    }

    #[test]
    fn lead_over_wall_ms_reports_skew_after_remote_merge() {
        let mut h = Hlc {
            physical_ms: 1_000,
            logical: 0,
        };
        let remote = Hlc {
            physical_ms: 1_300,
            logical: 0,
        };
        let now_ms = 1_000;
        h.checked_update(now_ms, Some(remote), 500).unwrap();
        // Local clock is now 300ms ahead of the wall-clock sample that produced it.
        assert_eq!(h.lead_over_wall_ms(now_ms), 300);
        // Once real wall-clock time catches up, the lead shrinks to zero.
        assert_eq!(h.lead_over_wall_ms(1_300), 0);
        assert_eq!(h.lead_over_wall_ms(1_400), 0);
    }
}
