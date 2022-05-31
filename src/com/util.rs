// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ptr;
use std::time::Duration;

pub fn hash_set_with_capacity<T: Hash + Eq>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity(capacity)
}

struct Record<K> {
    prev: *mut Record<K>,
    next: *mut Record<K>,
    key: MaybeUninit<K>,
}

pub struct ValueEntry<K, V> {
    value: V,
    record: *mut Record<K>,
}

struct Trace<K> {
    head: Box<Record<K>>,
    tail: Box<Record<K>>,
    tick: usize,
    sample_mask: usize,
}

fn suture<K>(leading: &mut Record<K>, following: &mut Record<K>) {
    leading.next = following as *mut Record<K>;
    following.prev = leading as *mut Record<K>;
}

unsafe fn cut_out<K>(record: &mut Record<K>) {
    suture(record.prev.as_mut().unwrap(), record.next.as_mut().unwrap());
}

impl<K> Trace<K> {
    fn new(sample_mask: usize) -> Trace<K> {
        // 置于堆
        let mut head = Box::new(Record {
            prev: 1usize as *mut _,
            next: 1usize as *mut _,
            key: MaybeUninit::uninit(),
        });
        let mut tail = Box::new(Record {
            prev: 1usize as *mut _,
            next: 1usize as *mut _,
            key: MaybeUninit::uninit(),
        });
        suture(&mut head, &mut tail);

        Trace {
            head,
            tail,
            sample_mask,
            tick: 0,
        }
    }

    fn maybe_promote(&mut self, record: *mut Record<K>) {
        self.tick += 1;
        if self.tick & self.sample_mask == 0 {
            self.promote(record);
        }
    }

    fn promote(&mut self, record: *mut Record<K>) {
        unsafe {
            cut_out(record.as_mut().unwrap());
            suture(record.as_mut().unwrap(), self.head.next.as_mut().unwrap());
            suture(&mut self.head, record.as_mut().unwrap());
        }
    }

    fn delete(&mut self, record: *mut Record<K>) {
        unsafe {
            // 将记录从其中剪除
            cut_out(record.as_mut().unwrap());
            // 调用Record中key的destructor
            ptr::drop_in_place(Box::from_raw(record).key.as_mut_ptr());
        }
    }

    fn create(&mut self, key: K) -> *mut Record<K> {
        let record = Box::leak(Box::new(Record {
            prev: self.head.as_mut() as *mut Record<K>,
            next: self.head.next,
            key: MaybeUninit::new(key),
        }));
        unsafe {
            self.head.next.as_mut().unwrap().prev = record;
        }
        self.head.next = record;
        record
    }

    // 末尾的element放到头部,替换key
    fn reuse_tail(&mut self, key: K) -> (K, *mut Record<K>) {
        unsafe {
            let record = self.tail.prev;
            cut_out(record.as_mut().unwrap());
            suture(record.as_mut().unwrap(), self.head.next.as_mut().unwrap());
            suture(&mut self.head, record.as_mut().unwrap());
            let old_key = record.as_mut().unwrap().key.as_ptr().read();
            record.as_mut().unwrap().key = MaybeUninit::new(key);
            (old_key, record)
        }
    }

    fn clear(&mut self) {
        let mut cur = self.head.next;
        unsafe {
            while cur != self.tail.as_mut() as *mut Record<K> {
                let tmp = cur.as_mut().unwrap().next;
                ptr::drop_in_place(Box::from_raw(cur).key.as_mut_ptr());
                cur = tmp;
            }
            suture(&mut self.head, &mut self.tail);
        }
    }

    fn remove_tail(&mut self) -> K {
        unsafe {
            let record = self.tail.prev;
            cut_out(record.as_mut().unwrap());

            let r = Box::from_raw(record);
            return r.key.as_ptr().read();
        }
    }
}
pub trait SizePolicy<K, V> {
    fn current(&self) -> usize;
    fn on_insert(&mut self, key: &K, value: &V);
    fn on_remove(&mut self, key: &K, value: &V);
    fn on_reset(&mut self, val: usize);
}
pub struct CountTracker(usize);

impl Default for CountTracker {
    fn default() -> Self {
        Self(0)
    }
}

impl<K, V> SizePolicy<K, V> for CountTracker {
    fn current(&self) -> usize {
        self.0
    }

    fn on_insert(&mut self, _: &K, _: &V) {
        self.0 += 1;
    }

    fn on_remove(&mut self, _: &K, _: &V) {
        self.0 -= 1;
    }

    fn on_reset(&mut self, val: usize) {
        self.0 = val;
    }
}

pub struct LruCache<K, V, T> {
    map: HashMap<K, ValueEntry<K, V>>,
    trace: Trace<K>,
    capacity: usize,
    size_policy: T,
}

impl<K, V, T: SizePolicy<K, V>> LruCache<K, V, T>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
{
    pub fn with_capacity_sample_and_trace(
        mut capacity: usize,
        sample_mask: usize,
        size_policy: T,
    ) -> LruCache<K, V, T> {
        if capacity == 0 {
            capacity = 1;
        }
        LruCache {
            map: HashMap::default(),
            trace: Trace::new(sample_mask),
            capacity,
            size_policy,
        }
    }

    pub fn size(&self) -> usize {
        self.size_policy.current()
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.trace.clear();
        self.size_policy.on_reset(0);
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl<K, V> LruCache<K, V, CountTracker>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
{
    pub fn with_capacity(capacity: usize) -> LruCache<K, V, CountTracker> {
        LruCache::with_capacity_sample_and_trace(capacity, 0, CountTracker::default())
    }

    pub fn with_capacity_and_sample(
        capacity: usize,
        sample_task: usize,
    ) -> LruCache<K, V, CountTracker> {
        LruCache::with_capacity_sample_and_trace(capacity, sample_task, CountTracker::default())
    }
}

impl<K, V, T> LruCache<K, V, T>
where
    K: Eq + Hash + Clone + std::fmt::Debug,
    T: SizePolicy<K, V>,
{
    pub fn insert(&mut self, key: K, value: V) {
        let mut old_key: Option<K> = None;
        let current_size = SizePolicy::<K, V>::current(&self.size_policy);

        match self.map.entry(key) {
            HashMapEntry::Occupied(mut e) => {
                self.size_policy.on_remove(e.key(), &e.get().value);
                self.size_policy.on_insert(e.key(), &value);
                let mut entry = e.get_mut();
                self.trace.promote(entry.record);
                entry.value = value;
            }
            HashMapEntry::Vacant(v) => {
                let record = if self.capacity <= current_size {
                    let res = self.trace.reuse_tail(v.key().clone());
                    old_key = Some(res.0);
                    res.1
                } else {
                    self.trace.create(v.key().clone())
                };
                self.size_policy.on_insert(v.key(), &value);
                v.insert(ValueEntry { value, record });
            }
        }
        // 到达trace控制第容量上限，需要移出Trace最后一条
        if let Some(o) = old_key {
            let entry = self.map.remove(&o).unwrap();
            self.size_policy.on_remove(&o, &entry.value);
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(v) = self.map.remove(key) {
            self.trace.delete(v.record);
            self.size_policy.on_remove(key, &v.value);
            return Some(v.value);
        }
        None
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        match self.map.get(key) {
            Some(v) => {
                self.trace.maybe_promote(v.record);
                Some(&v.value)
            }
            None => None,
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        match self.map.get_mut(key) {
            Some(v) => {
                self.trace.maybe_promote(v.record);
                Some(&mut v.value)
            }
            None => None,
        }
    }

    pub fn iter(&self) -> Iter<K, ValueEntry<K, V>> {
        self.map.iter()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn resize(&mut self, mut new_cap: usize) {
        if new_cap == 0 {
            new_cap = 1;
        }
        if new_cap < self.capacity && self.size() > new_cap {
            while self.size() > new_cap {
                let key = self.trace.remove_tail();
                let entry = self.map.remove(&key).unwrap();
                self.size_policy.on_remove(&key, &entry.value);
            }
            self.map.shrink_to_fit();
        }
        self.capacity = new_cap;
    }
}

unsafe impl<K, V, T> Send for LruCache<K, V, T>
where
    K: Send,
    V: Send,
    T: Send + SizePolicy<K, V>,
{
}

pub trait CalcSleepTime {
    fn caculate_sleep_time(&mut self, time_unit: Duration) -> Duration;
}

impl CalcSleepTime for usize {
    fn caculate_sleep_time(&mut self, time_unit: Duration) -> Duration {
        *self += 1;
        if *self < 3 {
            return time_unit;
        } else {
            if *self < 100 {
                *self = 2;
            }
            time_unit.mul_f32(*self as f32)
        }
    }
}
