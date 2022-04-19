use anyhow::Error as AnyError;

#[derive(Debug)]
pub enum WindowErr {
    Full,
}

#[derive(Debug)]
pub struct AckWindow {
    start: i64,
    bit_set: BitSet,
    size: u32,
    // current_max + start = current real seqid
    current_max: u32,
}

impl AckWindow {
    // size: 窗口最大开的大小
    pub fn new(size: u32) -> AckWindow {
        AckWindow {
            start: 0,
            bit_set: BitSet::with_max_value(size),
            size,
            current_max: 0,
        }
    }

    // When call send method, seq_id should sorted , or it will become
    //  unexpected behavior.
    pub fn send(&mut self, seq_id: i64) -> Result<(), WindowErr> {
        if self.start == 0 {
            self.start = seq_id;
        }
        if seq_id < self.start || seq_id >= self.start + i64::from(self.size) {
            return Err(WindowErr::Full);
        }
        // current_max cursor
        let offset = seq_id - self.start;
        if offset as u32 > self.current_max {
            self.current_max = offset as u32;
        }
        self.bit_set.insert((seq_id - self.start) as u32);
        Ok(())
    }

    pub fn have_vacancy(&self, seq_id: i64) -> bool {
        self.start == 0 || !(seq_id < self.start || seq_id >= self.start + i64::from(self.size))
    }

    fn ack(&mut self, seq_id: i64) -> Result<(), AnyError> {
        if seq_id < self.start || seq_id >= self.start + i64::from(self.size) {
            return Err(AnyError::msg("Invalid ack seqid, has exceeded the window"));
        }
        self.bit_set.remove((seq_id - self.start) as u32);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        // No one has done send operation
        if self.current_max == 0 {
            return true;
        }
        let bucket = self.bit_set.first_non_empty_bucket(0);
        match bucket {
            None => true,
            Some(v) => {
                let offset = self.bit_set.tinyset(v).lowest();
                match offset {
                    // Tinyset get from first non empty bucket, so logic will never come here.
                    None => unreachable!(),
                    Some(offset) => (offset + v * 64) > self.current_max,
                }
            }
        }
    }

    fn clear(&mut self) {
        self.start = 0;
        self.current_max = 0;
        self.bit_set.clear();
    }
}

#[derive(Clone, Copy, Eq, Debug, PartialEq)]
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

    pub fn lowest(&self) -> Option<u32> {
        if self.is_empty() {
            None
        } else {
            Some(self.0.trailing_zeros() as u32)
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

    pub fn len(self) -> u32 {
        self.0.count_ones()
    }
}

#[derive(Clone, Debug)]
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

    fn intersect_update_with_iter(&mut self, other: impl Iterator<Item = TinySet>) {
        self.len = 0;
        for (left, right) in self.tinysets.iter_mut().zip(other) {
            *left = left.intersect(right);
            self.len += left.len() as u64;
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn insert(&mut self, el: u32) {
        let higher = el / 64u32;
        let lower = el % 64u32;
        self.len += if self.tinysets[higher as usize].insert_mut(lower) {
            1
        } else {
            0
        }
    }

    pub fn remove(&mut self, el: u32) {
        let higher = el / 64u32;
        let lower = el % 64u32;
        self.len -= if self.tinysets[higher as usize].remove_mut(lower) {
            1
        } else {
            0
        }
    }

    pub fn contains(&self, el: u32) -> bool {
        self.tinyset(el / 64u32).contains(el % 64)
    }

    pub fn tinyset(&self, bucket: u32) -> TinySet {
        self.tinysets[bucket as usize]
    }

    pub fn first_non_empty_bucket(&self, bucket: u32) -> Option<u32> {
        self.tinysets[bucket as usize..]
            .iter()
            .cloned()
            .position(|tinyset| !tinyset.is_empty())
            .map(|delta_bucket| bucket + delta_bucket as u32)
    }

    pub fn max_value(&self) -> u32 {
        self.max_value
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
