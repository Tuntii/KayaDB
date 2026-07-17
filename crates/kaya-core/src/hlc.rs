//! Hybrid Logical Clock (HLC) for M20 multi-raft / cross-shard timestamps.
//!
//! Packing for use as `commit_ts`: `(physical_ms << 16) | logical`.
//! Physical time is milliseconds; logical is a 16-bit counter that advances
//! when the wall clock stalls or when receiving a remote timestamp.
//!
//! # Uncertainty interval (v1 / M23)
//!
//! There is **no** `max_offset_ms` wait or clamp on tick/update. The merge rule
//! (`max(local, wall, remote)`) is trusted; operators should keep NTP skew well
//! under the intended SI freshness window. A future Cockroach-style uncertainty
//! interval (wait out max clock offset, or retry reads when `commit_ts` falls
//! inside the uncertainty window) would clamp here and on the engine write
//! path (`prepare_hlc_write_sequence`).

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
}
