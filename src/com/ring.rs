use std::fmt::Debug;
use std::iter::Chain;
use std::slice::Iter;

pub trait SeqId {
    fn seq_id(&self) -> usize;
}

pub trait BlankElement: PartialEq {
    type Item;
    fn is_blank(&self) -> bool;

    fn blank_val() -> Self::Item;
}

// RingQueue's length should be bigger than receiver window's length.
// We only care about data between ack position and send position
#[derive(Debug)]
pub struct RingQueue<E> {
    // data: Vec<E>,
    data: Box<[E]>,
    ack_pos: usize,
    send_pos: usize,
    size: usize,
    // current seqid
    cur_num: usize,
    start_num: usize,
}

pub enum RingIter<'a, T> {
    Chain(Chain<Iter<'a, T>, Iter<'a, T>>),
    Single(Iter<'a, T>),
    Empty,
}

#[derive(PartialEq, Debug)]
pub enum RingQueueError {
    SendNotOneByOne,
    QueueIsFull,
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

impl<E: Debug + Sized> RingQueue<E>
where
    E: SeqId + BlankElement<Item = E> + Debug + Clone + Sized,
{
    pub fn new(size: usize) -> RingQueue<E> {
        let internal = vec![E::blank_val(); size];
        RingQueue {
            data: internal.into_boxed_slice(),
            ack_pos: 0,
            send_pos: 0,
            size,
            cur_num: 0usize,
            start_num: 0usize,
        }
    }

    // When you get a QueueIsFull error, you should stop
    // sending new msg until [ack position] follow up [send position]
    pub fn send(&mut self, el: E) -> Result<(), RingQueueError> {
        if self.is_full() {
            return Err(RingQueueError::QueueIsFull);
        }
        if self.cur_num != 0 && el.seq_id() - self.cur_num != 1 {
            return Err(RingQueueError::SendNotOneByOne);
        }

        self.send_pos = (self.send_pos + 1) % self.size;
        if self.cur_num == 0 {
            self.cur_num = el.seq_id();
            self.start_num = el.seq_id();
        } else {
            self.cur_num += 1;
        }
        let _ = std::mem::replace(&mut self.data[self.send_pos], el);
        Ok(())
    }

    pub fn ack(&mut self, seq_id: usize) {
        if seq_id < self.cur_num || self.data.len() == 0 {
            return;
        }
        let offset = seq_id - self.cur_num;
        // ack seqid is send seqid
        if offset == 0 {
            self.ack_pos = self.send_pos;
        }
        if offset > self.size && offset / self.size == 1 {
            let p = self.ack_pos + (offset % self.size);
            if p <= self.send_pos {
                self.ack_pos = p;
            }
        }
    }

    // send_pos is ack_pos's neighbour
    pub fn is_full(&self) -> bool {
        // Only the first time, ack pos and send pos can be the same postion
        if self.cur_num != 0 && self.send_pos == self.ack_pos && self.send_pos == 0 {
            self.ack_pos == self.send_pos
        } else {
            if (self.send_pos + 1) / self.size == 1 {
                (self.send_pos + 1) % self.size == self.ack_pos
            } else {
                self.send_pos + 1 == self.ack_pos
            }
        }
    }

    pub fn not_ack_iter(&self) -> RingIter<'_, E> {
        if self.ack_pos > self.send_pos {
            println!("走Chain!");
            let start = (self.ack_pos + 1) % self.size;
            println!("Left is:{:?}", &self.data[start..]);
            let left = self.data[start..].iter();
            let right = self.data[0..self.send_pos as usize + 1].iter();
            RingIter::Chain(left.chain(right))
        } else if self.ack_pos == self.send_pos {
            RingIter::Empty
        } else {
            let start = (self.ack_pos + 1) % self.size;
            RingIter::Single(self.data[start..self.send_pos as usize + 1].iter())
        }
    }

    fn is_acked(&self) -> bool {
        self.send_pos == self.ack_pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct Element(usize, bool);

    impl SeqId for Element {
        fn seq_id(&self) -> usize {
            self.0
        }
    }

    impl BlankElement for Element {
        type Item = Element;

        fn is_blank(&self) -> bool {
            self.1
        }

        fn blank_val() -> Self::Item {
            Element(0, true)
        }
    }

    impl From<usize> for Element {
        fn from(i: usize) -> Self {
            Element(i, false)
        }
    }

    #[test]
    fn test_send_first_full_not_move_ack() {
        let start_element = 203234usize;
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        for i in start_element..start_element + 100 {
            let _ = ring.send(i.into());
        }
        let r = ring.send((start_element + 100 + 1).into());
        assert!(r.is_err());
        assert!(r.unwrap_err() == RingQueueError::QueueIsFull);
    }

    #[test]
    fn test_send_and_then_ack() {
        let start_element = 20234usize;
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        let mut seq_ids = Vec::new();
        for i in start_element..start_element + 2 {
            let _ = ring.send(i.into());
            seq_ids.push(i);
        }
        for v in seq_ids {
            ring.ack(v);
        }
        println!("{:?}", ring);
        assert!(ring.is_acked());
    }

    #[test]
    fn test_send_ack_iter_test_cross_vec_length() {
        let start_element = 1usize;
        let mut ring: RingQueue<Element> = RingQueue::new(100);
        let bound1 = start_element + 50;
        for i in 0..bound1 {
            let _ = ring.send(i.into());
            ring.ack(i);
        }

        let mut v: Vec<Element> = Vec::new();
        for i in bound1..bound1 + 60 {
            let _ = ring.send(i.into());
            v.push(i.into());
        }

        let i = ring.not_ack_iter();
        let mut iter = i.zip(v.into_iter());
        while let Some((e1, e2)) = iter.next() {
            assert!(*e1 == e2);
        }
    }

    #[test]
    fn test_send_ack_iter_not_cross_vec_length() {
        let start_element = 1usize;
        let mut ring: RingQueue<Element> = RingQueue::new(100);

        let bound1 = start_element + 50;
        for i in start_element..bound1 {
            let _ = ring.send(i.into());
            ring.ack(i);
        }
        for i in bound1..bound1 + 20 {
            let _ = ring.send(i.into());
        }
        let check_els = 51usize..71usize;
        let mut iter = ring.not_ack_iter().zip(check_els);
        while let Some((e1, e2)) = iter.next() {
            assert!(*e1 == <usize as Into<Element>>::into(e2));
        }
    }
}
