use std::fmt::{Debug, Display};
use std::iter::Chain;
use std::slice::Iter;

use tracing::instrument;

pub trait SeqId {
    fn seq_id(&self) -> usize;
}

// RingQueue's length should be bigger than receiver window's length.
// We only care about data between ack position and send position
#[derive(Debug)]
pub struct RingQueue<E> {
    data: Box<[E]>,
    ack_pos: i32,
    send_pos: i32,
    size: i32,
    cur_id: i32,
    start_id: i32,
}

impl<E> Default for RingQueue<E>
where
    E: SeqId + Sized + Debug + Default + Clone,
{
    fn default() -> Self {
        RingQueue::new(64 * 100)
    }
}

pub enum RingIter<'a, T> {
    Chain(Chain<Iter<'a, T>, Iter<'a, T>>),
    Single(Iter<'a, T>),
    Empty,
}

pub type QueueLength = i32;
pub type CurId = i32;

#[derive(PartialEq, Debug)]
pub enum RingQueueError {
    SendNotOneByOne(CurId),
    QueueIsFull(CurId, QueueLength),
}

impl Display for RingQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl<'a, T> Iterator for RingIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Chain(c) => c.next(),
            Self::Single(i) => i.next(),
            _ => None,
        }
    }
}

impl<E> RingQueue<E>
where
    E: Sized + Debug + Default + Clone,
{
    pub fn new(size: usize) -> RingQueue<E> {
        let internal = vec![Default::default(); size];
        RingQueue {
            data: internal.into_boxed_slice(),
            ack_pos: -1,
            send_pos: -1,
            size: size as i32,
            cur_id: 0,
            start_id: -1,
        }
    }

    // When you get a QueueIsFull error, you should stop
    // sending new msg until [ack position] follow up [send position]
    pub fn send(&mut self, el: E) -> Result<i32, RingQueueError> {
        if self.is_full() {
            return Err(RingQueueError::QueueIsFull(self.cur_id, self.size));
        }
        self.send_pos += 1;
        self.cur_id += 1;

        if self.start_id == -1 {
            self.start_id = self.cur_id;
        }

        let _ = std::mem::replace(&mut self.data[self.send_pos as usize], el);
        Ok(self.cur_id)
    }

    #[instrument]
    pub fn check(&self, el: &E) -> Result<(), RingQueueError> {
        if self.is_full() {
            return Err(RingQueueError::QueueIsFull(self.cur_id, self.size));
        }
        return Ok(());
    }

    pub fn ack(&mut self, seq_id: i32) {
        // let offset = seq_id - self.start_id;
        // // Invalid seq_id
        // if offset >= (self.size - 1) {
        //     return;
        // }

        // self.start_id += offset;
        // self.ack_pos = offset;
        // self.start_id += offset;
        // if self.ack_pos == self.size - 1 {
        //     self.ack_pos = 0;
        //     self.send_pos = 0;
        //     if seq_id >= i32::MAX - 64 * 100 * 10 {
        //         self.start_id = 0;
        //         self.cur_id = 0;
        //     } else {
        //         self.start_id = self.cur_id;
        //     }
        // }
    }

    pub fn cur_seq_id(&self) -> i32 {
        self.cur_id
    }

    // send_pos is ack_pos's neighbour
    pub fn is_full(&self) -> bool {
        self.send_pos == self.size - 1
    }

    pub fn not_ack_iter(&self) -> RingIter<'_, E> {
        if self.ack_pos > self.send_pos {
            let start = (self.ack_pos + 1) % self.size;
            let left = self.data[start as usize..].iter();
            let right = self.data[0..self.send_pos as usize + 1].iter();
            RingIter::Chain(left.chain(right))
        } else if self.ack_pos == self.send_pos {
            RingIter::Empty
        } else {
            let start = (self.ack_pos + 1) % self.size;
            RingIter::Single(self.data[start as usize..self.send_pos as usize + 1].iter())
        }
    }

    #[allow(dead_code)]
    fn is_acked(&self) -> bool {
        (self.send_pos == 0) || (self.send_pos - 1) == self.ack_pos
    }
}

#[cfg(test)]
mod ring_tests {
    use super::*;

    #[derive(Debug, Default, Clone, PartialEq)]
    struct Element(i32, bool);

    // impl SeqId for Element {
    //     fn seq_id(&self) -> usize {
    //         self.0
    //     }
    // }

    impl From<i32> for Element {
        fn from(i: i32) -> Self {
            Element(i, false)
        }
    }

    #[test]
    fn test_one_by_one_ack() {
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        for _ in 0..100000 {
            let seq_id = ring.send(1i32.into()).unwrap();
            ring.ack(seq_id);
        }
        assert!(ring.is_acked());
    }

    #[test]
    fn test_hundred_by_hundred() {
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        for _ in 0..100000 {
            let mut max_id = 0;
            for _ in 0..100 {
                max_id = ring.send(1i32.into()).unwrap();
            }
            ring.ack(max_id);
        }
        assert!(ring.is_acked());
    }

    #[test]
    fn test_send_first_full_not_move_ack() {
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        for i in 0..100 {
            let _ = ring.send(i.into());
        }
        let r = ring.send(1i32.into());
        assert!(r.is_err());
    }

    #[test]
    fn test_send_and_then_ack() {
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        let mut seq_ids = Vec::new();
        for i in 0..2i32 {
            let seq_id = ring.send(i.into()).unwrap();
            seq_ids.push(seq_id);
        }
        for v in seq_ids {
            ring.ack(v);
        }
        assert!(ring.is_acked());
    }

    #[test]
    fn test_send_ack_iter_test_cross_vec_length() {
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        for i in 0..50 {
            let seq_id = ring.send(i.into()).unwrap();
            ring.ack(seq_id);
        }

        let mut sent_ids = vec![];
        let v: Vec<Element> = Vec::new();
        for i in 0..60 {
            let seq_id = ring.send(i.into()).unwrap();
            sent_ids.push(seq_id);
        }

        let i = ring.not_ack_iter();
        let mut iter = i.zip(v.into_iter());
        while let Some((e1, e2)) = iter.next() {
            assert!(*e1 == e2);
        }
    }

    #[test]
    fn test_send_ack_iter_not_cross_vec_length() {
        let start_element = 1i32;
        let mut ring: RingQueue<Element> = RingQueue::new(100);

        let bound1 = start_element + 50;
        for i in start_element..bound1 {
            let _ = ring.send(i.into());
            ring.ack(i);
        }
        for i in bound1..bound1 + 20 {
            let _ = ring.send(i.into());
        }
        let check_els = 51i32..71i32;
        let mut iter = ring.not_ack_iter().zip(check_els);
        while let Some((e1, e2)) = iter.next() {
            assert!(*e1 == <i32 as Into<Element>>::into(e2));
        }
    }
}
