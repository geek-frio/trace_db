use std::iter::Chain;
use std::slice::Iter;

pub trait SeqId {
    fn seq_id(&self) -> u64;
}
// RingQueue's length should be bigger
// We only care about data between ack position and send position
pub struct RingQueue<E> {
    data: Vec<E>,
    ack_pos: usize,
    send_pos: usize,
    size: usize,
    start_num: u64,
}

enum RingIter<'a, T> {
    Chain(Chain<Iter<'a, T>, Iter<'a, T>>),
    Single(Iter<'a, T>),
    Empty,
}

enum RingQueueError {
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

impl<E> RingQueue<E>
where
    E: SeqId,
{
    fn new(size: usize, start_num: u64) -> RingQueue<E> {
        RingQueue {
            data: Vec::new(),
            ack_pos: 0,
            send_pos: 0,
            size,
            start_num,
        }
    }

    fn send(&mut self, el: E) -> Result<(), RingQueueError> {
        if self.is_full() {
            return Err(RingQueueError::QueueIsFull);
        }
        if el.seq_id() - self.start_num != 1 {
            return Err(RingQueueError::SendNotOneByOne);
        }
        self.send_pos = self.send_pos + 1 % self.size;
        self.data.insert(self.send_pos, el);
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.send_pos < self.ack_pos && self.send_pos + self.ack_pos == self.size
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_send_ack() {}
}
