use std::iter::Chain;
use std::slice::Iter;

// RingQueue's length should be bigger
// We only care about data between ack position and send position
pub struct RingQueue<E> {
    data: Vec<E>,
    ack_pos: usize,
    send_pos: usize,
    size: usize,
}

enum RingIter<'a, T> {
    Chain(Chain<Iter<'a, T>, Iter<'a, T>>),
    Single(Iter<'a, T>),
    Empty,
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

impl<E> RingQueue<E> {
    fn new(size: usize) -> RingQueue<E> {
        RingQueue {
            data: Vec::new(),
            ack_pos: 0,
            send_pos: 0,
            size,
        }
    }

    fn insert(&mut self, el: E) -> bool {
        if self.is_full() {
            return false;
        }
        self.send_pos = self.send_pos + 1 % self.size;
        self.data.insert(self.send_pos, el);
        true
    }

    fn is_full(&self) -> bool {
        if self.send_pos < self.ack_pos {
            return self.send_pos + self.ack_pos == self.size;
        }
        false
    }

    fn not_ack_iter(&self) -> RingIter<'_, E> {
        if self.ack_pos > self.send_pos {
            let left = self.data.as_slice()[self.ack_pos..].iter();
            let right = self.data.as_slice()[0..self.send_pos].iter();
            RingIter::Chain(left.chain(right))
        } else if self.ack_pos == self.send_pos {
            RingIter::Empty
        } else {
            RingIter::Single(self.data.as_slice()[self.ack_pos..self.send_pos].iter())
        }
    }
}
