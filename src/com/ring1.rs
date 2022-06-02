use std::collections::{linked_list::Iter, LinkedList};

use anyhow::Error as AnyError;

#[derive(Debug)]
struct RingQueue<T>
where
    T: std::fmt::Debug,
{
    start_id: i32,
    cur_id: i32,
    size: usize,
    data: LinkedList<Element<T>>,
}

struct RangeIter<'a, T>
where
    T: std::fmt::Debug,
{
    data: Iter<'a, Element<T>>,
    start_id: i32,
    end_id: i32,
}

#[derive(Debug)]
struct Element<T: std::fmt::Debug>(i32, T);

impl<T> RingQueue<T>
where
    T: std::fmt::Debug,
{
    fn new(size: usize) -> RingQueue<T> {
        RingQueue {
            start_id: 1,
            cur_id: 0,
            size,
            data: LinkedList::new(),
        }
    }

    fn allocate(&mut self) -> Result<i32, AnyError> {
        if self.cur_id != 0 && self.cur_id - self.start_id + 1 == self.size as i32 {
            return Err(AnyError::msg("Full"));
        }
        self.cur_id += 1;
        return Ok(self.cur_id);
    }

    fn push(&mut self, el: T) -> Result<i32, AnyError> {
        let id = self.allocate()?;
        self.data.push_back(Element(id, el));
        return Ok(id);
    }

    fn ack(&mut self, ack_id: i32) -> Result<(), AnyError> {
        if ack_id > self.cur_id {
            return Err(AnyError::msg(format!(
                "Invalid ack_id:{}, bigger than current get max id{}",
                ack_id, self.cur_id,
            )));
        }
        let poped_size = ack_id - self.start_id + 1;
        for _ in 0..poped_size {
            let poped = self.data.pop_front();
        }
        self.start_id = ack_id;
        Ok(())
    }

    fn not_acked_len(&self) -> i32 {
        self.cur_id - self.start_id
    }

    fn range_iter(&self, range_start: i32) -> RangeIter<T> {
        let data = self.data.iter();
        RangeIter {
            data,
            start_id: range_start,
            end_id: self.cur_id,
        }
    }
}

impl<'a, T> Iterator for RangeIter<'a, T>
where
    T: std::fmt::Debug,
{
    type Item = &'a Element<T>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(el) = self.data.next() {
            if el.0 < self.start_id {
                continue;
            } else if el.0 > self.end_id {
                break;
            } else {
                return Some(el);
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
        let mut queue = RingQueue::<i32>::new(10);
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
        let mut queue = RingQueue::<i32>::new(10);
        for i in 1..100001 {
            let id = queue.push(i).unwrap();
            if id % 4 == 0 {
                queue.ack(id).unwrap();
            }
        }
        println!("queue is:{:?}", queue);
        assert!(0 == queue.not_acked_len());

        let mut queue = RingQueue::<i32>::new(100);
        let mut collect = vec![];
        for i in 1..101 {
            let seq_id = queue.push(i).unwrap();
            collect.push(seq_id);
        }
        let iter = queue.range_iter(90);
        let a = iter.collect::<Vec<&Element<i32>>>();
        assert_eq!(a.len(), 11);
    }
}
