use std::collections::{linked_list::Iter, LinkedList};

use super::ack::DEFAULT_WIN_SIZE;

#[derive(Debug)]
pub struct RingQueue<T>
where
    T: std::fmt::Debug,
{
    start_id: i64,
    cur_id: i64,
    size: usize,
    data: LinkedList<Element<T>>,
}

impl<T> Default for RingQueue<T>
where
    T: std::fmt::Debug,
{
    fn default() -> Self {
        RingQueue::new(DEFAULT_WIN_SIZE as usize)
    }
}

pub struct RangeIter<'a, T>
where
    T: std::fmt::Debug,
{
    data: Iter<'a, Element<T>>,
    start_id: i64,
    end_id: i64,
}

#[derive(Debug)]
pub enum RingQueueError {
    // cur_id, start_id
    Full(i64, i64),
    // cur_id, start_id
    InvalidAckId(i64, i64),
}

impl std::fmt::Display for RingQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

#[derive(Debug)]
struct Element<T: std::fmt::Debug>(i64, T);

impl<T> RingQueue<T>
where
    T: std::fmt::Debug,
{
    pub fn new(size: usize) -> RingQueue<T> {
        RingQueue {
            start_id: 1,
            cur_id: 0,
            size,
            data: LinkedList::new(),
        }
    }

    pub fn allocate(&mut self) -> Result<i64, RingQueueError> {
        if self.cur_id != 0 && self.cur_id - self.start_id + 1 == self.size as i64 {
            return Err(RingQueueError::Full(self.cur_id, self.start_id));
        }
        self.cur_id += 1;
        return Ok(self.cur_id);
    }

    pub fn push(&mut self, el: T) -> Result<i64, RingQueueError> {
        let id = self.allocate()?;
        self.data.push_back(Element(id, el));
        return Ok(id);
    }

    pub fn ack(&mut self, ack_id: i64) -> Result<(), RingQueueError> {
        if ack_id > self.cur_id {
            return Err(RingQueueError::InvalidAckId(self.cur_id, self.start_id));
        }
        let poped_size = ack_id - self.start_id + 1;
        for _ in 0..poped_size {
            let _ = self.data.pop_front();
        }
        self.start_id = ack_id + 1;
        Ok(())
    }

    pub fn not_acked_len(&self) -> i64 {
        self.cur_id - self.start_id + 1
    }

    pub fn is_full(&self) -> bool {
        (self.cur_id - self.start_id + 1) == (self.size as i64)
    }

    pub fn range_iter(&self, range_start: i64) -> RangeIter<T> {
        let data = self.data.iter();
        RangeIter {
            data,
            start_id: range_start,
            end_id: self.cur_id,
        }
    }

    pub fn not_ack_iter(&self) -> RangeIter<T> {
        self.range_iter(self.start_id)
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn len(&self) -> usize {
        if self.cur_id == 0 {
            return 0;
        }
        (self.cur_id - self.start_id + 1) as usize
    }
}

impl<'a, T> Iterator for RangeIter<'a, T>
where
    T: std::fmt::Debug,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(el) = self.data.next() {
            if el.0 < self.start_id {
                continue;
            } else if el.0 > self.end_id {
                break;
            } else {
                return Some(&el.1);
            }
        }
        None
    }
}

#[cfg(test)]
mod ring1_test {
    use super::*;

    #[test]
    fn test_basics() {
        let mut queue = RingQueue::<i64>::new(10);
        let mut pushed_ids = vec![];

        // 1.Test Full
        for i in 1..11 {
            let id = queue.push(i).unwrap();
            pushed_ids.push(id);
        }
        assert!(queue.push(10).is_err());

        // 2.Test AckAll
        let _ = queue.ack(10);
        assert!(queue.not_acked_len() == 0);
        println!("queue is:{:?}", queue);

        // 3.Test Ack one by one
        for i in 1..100000 {
            let id = queue.push(i).unwrap();
            queue.ack(id).unwrap();
        }
        assert!(0 == queue.not_acked_len());

        // 4.Test Ack step 10
        let mut queue = RingQueue::<i64>::new(10);
        for i in 1..100001 {
            let id = queue.push(i).unwrap();
            if id % 4 == 0 {
                queue.ack(id).unwrap();
            }
        }
        println!("queue is:{:?}", queue);
        assert!(0 == queue.not_acked_len());

        let mut queue = RingQueue::<i64>::new(100);
        let mut collect = vec![];
        for i in 1..101 {
            let seq_id = queue.push(i).unwrap();
            collect.push(seq_id);
        }
        let iter = queue.range_iter(90);
        let a = iter.collect::<Vec<&i64>>();
        assert_eq!(a.len(), 11);

        let mut queue = RingQueue::<i64>::new(10);
        queue.push(1).unwrap();
        let id = queue.push(2).unwrap();
        queue.push(3).unwrap();

        let _ = queue.ack(id);
        println!("current queue is:{:?}", queue);
        println!("i64 max:{}", i64::MAX)
    }
}
