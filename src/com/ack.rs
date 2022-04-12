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

    pub fn union(self, other: TinySet) -> TinySet {
        TinySet(self.0 | other.0)
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0u64
    }
}
