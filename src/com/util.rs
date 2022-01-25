// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::mem::MaybeUninit;
use std::ptr;

pub fn hash_set_with_capacity<T: Hash + Eq>(capacity: usize) -> HashSet<T> {
    HashSet::with_capacity(capacity)
}

struct Record<K> {
    prev: *mut Record<K>,
    next: *mut Record<K>,
    key: MaybeUninit<K>,
}

struct ValueEntry<K, V> {
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
    pub fn insert(&mut self, key: K, value: V) {}
}
