// 1. SeqId满足严格自增
// 2. 窗口限制同时处理的消息数目
pub struct AckWindow {
    start: i64,
    window_pointer: i64,
}

impl AckWindow {}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TinySet(u64);

impl TinySet {
    pub fn clear(&mut self) {
        self.0 = 0u64;
    }

    pub fn complement(self) -> TinySet {
        TinySet(!self.0)
    }

    pub fn contains(self, el: u32) -> bool {
        !self.intersect(TinySet::singleton(el)).is_empty()
    }

    pub fn intersect(self, other: TinySet) -> TinySet {
        TinySet(self.0 & other.0)
    }

    pub fn singleton(el: u32) -> TinySet {
        TinySet(1u64 << el as u64)
    }

    pub fn insert(self, el: u32) -> TinySet {
        self.union(TinySet::singleton(el))
    }

    pub fn insert_mut(&mut self, el: u32) -> bool {
        let old = *self;
        *self = old.insert(el);
        old != *self
    }

    pub fn remove(self, el: u32) -> TinySet {
        self.intersect(TinySet::singleton(el).complement())
    }

    pub fn remove_mut(&mut self, el: u32) -> bool {
        let old = *self;
        *self = old.remove(el);
        old != *self
    }

    pub fn union(self, other: TinySet) -> TinySet {
        TinySet(self.0 | other.0)
    }

    pub fn pop_lowest(&mut self) -> Option<u32> {
        if self.is_empty() {
            None
        } else {
            let lowest = self.0.trailing_zeros() as u32;
            self.0 ^= TinySet::singleton(lowest).0;
            Some(lowest)
        }
    }

    // if upper_bound=7 result is: 01111111
    pub fn range_lower(upper_bound: u32) -> TinySet {
        TinySet((1u64 << u64::from(upper_bound % 64u32)) - 1u64)
    }

    pub fn range_greater_or_equal(from_included: u32) -> TinySet {
        TinySet::range_lower(from_included).complement()
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0u64
    }

    pub fn empty() -> TinySet {
        TinySet(0u64)
    }

    pub fn full() -> TinySet {
        TinySet::empty().complement()
    }
}

pub struct BitSet {
    tinysets: Box<[TinySet]>,
    len: u64,
    max_value: u32,
}

fn num_buckets(max_val: u32) -> u32 {
    (max_val + 63u32) / 64u32
}

impl BitSet {
    pub fn with_max_value(max_value: u32) -> BitSet {
        let num_buckets = num_buckets(max_value);
        let tinybitsets = vec![TinySet::empty(); num_buckets as usize].into_boxed_slice();
        BitSet {
            tinysets: tinybitsets,
            len: 0,
            max_value,
        }
    }

    pub fn with_max_value_and_full(max_value: u32) -> BitSet {
        let num_buckets = num_buckets(max_value);
        let mut tinybitsets = vec![TinySet::full(); num_buckets as usize].into_boxed_slice();

        let lower = max_value % 64u32;
        if lower != 0 {
            tinybitsets[tinybitsets.len() - 1] = TinySet::range_lower(lower);
        }

        BitSet {
            tinysets: tinybitsets,
            len: max_value as u64,
            max_value,
        }
    }

    pub fn clear(&mut self) {
        for tinyset in self.tinysets.iter_mut() {
            *tinyset = TinySet::empty();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_range_lower() {
        let a = TinySet::range_lower(2);
        println!("{:b}", a.0);
    }
}
