/// Deterministic xorshift64 RNG.  No thread-local state, no wall-clock input.
pub(crate) struct SimRng {
    state: u64,
}

impl SimRng {
    pub(crate) fn new(seed: u64) -> Self {
        // xorshift64 requires a non-zero state.
        Self {
            state: if seed == 0 {
                0xcafe_babe_dead_beef
            } else {
                seed
            },
        }
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// Return a value in `0..max`.  Panics if `max == 0`.
    pub(crate) fn usize_below(&mut self, max: usize) -> usize {
        debug_assert!(max > 0);
        (self.next_u64() as usize) % max
    }

    /// Return `len` pseudo-random bytes.
    pub(crate) fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| (self.next_u64() & 0xff) as u8).collect()
    }

    /// Return the index of the selected bucket given integer weights.
    /// The sum of weights must be > 0.
    pub(crate) fn weighted_index(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|&w| w as u64).sum();
        debug_assert!(total > 0, "weights must not be all-zero");
        let r = self.next_u64() % total;
        let mut acc = 0u64;
        for (i, &w) in weights.iter().enumerate() {
            acc += w as u64;
            if r < acc {
                return i;
            }
        }
        weights.len() - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_instances() {
        let mut a = SimRng::new(42);
        let mut b = SimRng::new(42);
        for _ in 0..200 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn zero_seed_does_not_get_stuck() {
        let mut rng = SimRng::new(0);
        let v = rng.next_u64();
        assert_ne!(v, 0);
    }

    #[test]
    fn weighted_index_in_range() {
        let mut rng = SimRng::new(1);
        let weights = [10u32, 20, 30, 40];
        for _ in 0..1000 {
            let i = rng.weighted_index(&weights);
            assert!(i < weights.len());
        }
    }
}
