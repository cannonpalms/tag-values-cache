mod contains;
mod insert;
mod iter;

use std::ops::Range;

use criterion::{criterion_group, criterion_main};

criterion_main!(benches);
criterion_group!(benches, insert::bench, contains::bench, iter::bench);

/// Linear-feedback shift register based PRNG.
///
/// Generates 4,294,967,295 unique values before cycling.
#[derive(Debug, Clone)]
pub struct Lfsr(u32);

impl Default for Lfsr {
    fn default() -> Self {
        Self(42)
    }
}

impl Lfsr {
    pub fn new(seed: u32) -> Self {
        Self(seed)
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> u32 {
        let lsb = self.0 & 1;
        self.0 >>= 1;
        if lsb == 1 {
            self.0 ^= 0x80000057
        }
        assert_ne!(self.0, 42, "LFSR rollover");
        self.0
    }

    /// Return a valid [`Range`] with random bounds.
    pub fn next_range(&mut self) -> Range<u32> {
        let a = self.next();
        let b = self.next();
        Range {
            start: a.min(b),
            end: a.max(b),
        }
    }
}
