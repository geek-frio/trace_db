use std::iter::Chain;
use std::slice::Iter;

pub trait SeqId {
    fn seq_id(&self) -> usize;
}

pub trait BlankElement {
    type Item;
    fn is_blank(&self) -> bool;

    fn blank_val() -> Self::Element;
}

// RingQueue's length should be bigger than receiver window's length.
// We only care about data between ack position and send position
pub struct RingQueue<E> {
    data: Vec<E>,
    ack_pos: usize,
    send_pos: usize,
    size: usize,
    // start seqid
    start_num: usize,
    // current seqid
    cur_num: usize,
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
    E: SeqId + BlankElement<Item = E>,
{
    fn new(size: usize, start_num: usize) -> RingQueue<E> {
        let mut internal = Vec::new();
        internal.push(<E as BlankElement>::blank_val());
        RingQueue {
            data: internal,
            ack_pos: 0,
            send_pos: 0,
            size,
            start_num,
            cur_num: start_num,
        }
    }

    fn send(&mut self, el: E) -> Result<(), RingQueueError> {
        if self.is_full() {
            return Err(RingQueueError::QueueIsFull);
        }
        if el.seq_id() - self.start_num != 1 {
            return Err(RingQueueError::SendNotOneByOne);
        }

        self.send_pos = (self.send_pos + 1) % self.size;
        self.cur_num += 1;
        self.data.insert(self.send_pos, el);
        Ok(())
    }

    fn ack(&mut self, seq_id: usize) {
        if seq_id <= self.cur_num || self.data.len() == 0 {
            return;
        }
        let offset = seq_id - self.cur_num;
        let l = self.data.len();
        if offset > l && offset / l == 1 {
            let p = self.ack_pos + (offset % l);
            if p <= self.send_pos {
                self.ack_pos = p;
            }
        }
    }

    fn is_full(&self) -> bool {
        self.send_pos < self.ack_pos && (self.send_pos + self.ack_pos) == self.size
    }

    fn not_ack_iter(&self) -> RingIter<'_, E> {
        if self.ack_pos > self.send_pos {
            let left = self.data.as_slice()[self.ack_pos..].iter();
            let right = self.data.as_slice()[0..self.send_pos as usize].iter();
            RingIter::Chain(left.chain(right))
        } else if self.ack_pos == self.send_pos {
            RingIter::Empty
        } else {
            RingIter::Single(self.data.as_slice()[self.ack_pos..self.send_pos as usize].iter())
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_send_ack() {}
}
