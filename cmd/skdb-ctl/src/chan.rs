use skdb::com::ack::{AckWindow, WindowErr};
use skdb::TOKIO_RUN;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::{SendError, TryRecvError, TrySendError};
use tokio::sync::mpsc::{self, unbounded_channel, UnboundedSender};
use tokio::sync::mpsc::{Receiver, Sender};

pub trait SeqIdFill {
    fn fill_seqid(&mut self, seq_id: i64);
}

pub struct SeqMail<T> {
    sender: IndexSender<T>,
}

fn chan<T: SeqIdFill>(buf_size: usize, win_size: usize) -> (IndexSender<T>, IndexReceiver<T>) {
    let (tx, rx) = mpsc::channel(buf_size);
    let seq_id = Arc::new(AtomicI64::new(1));
    (
        IndexSender::new(tx, win_size as u32, seq_id.clone()),
        IndexReceiver {
            seq_id: seq_id,
            receiver: rx,
        },
    )
}

enum MailSignal {
    Ack(i64),
    Shutdown,
}

pub enum SeqMailEvent {
    Tick,
}

impl<T: SeqIdFill + Send + 'static> SeqMail<T> {
    pub fn new(buf_size: usize) -> (SeqMail<T>, IndexReceiver<T>) {
        let (s, r) = chan::<T>(buf_size, 5000);
        (SeqMail { sender: s }, r)
    }

    pub async fn send_msg(&mut self, msg: T) -> Result<i64, IndexSendErr<T>> {
        let seq_id = self.sender.send(msg).await?;
        Ok(seq_id)
    }

    // Start the task, return event ref
    pub async fn start(buf_size: usize) -> UnboundedSender<T> {
        let (e_s, mut e_r) = unbounded_channel::<T>();
        let (seq_mail, r) = SeqMail::<T>::new(buf_size);

        TOKIO_RUN.spawn(async move {
            let event = e_r.recv().await;
        });
        e_s
    }
}

pub struct IndexReceiver<T> {
    seq_id: Arc<AtomicI64>,
    receiver: Receiver<T>,
}

pub enum IndexSendErr<T> {
    Win(WindowErr),
    Chan(ChanErr<T>),
}

pub enum ChanErr<T> {
    Send(SendError<T>),
    Try(TrySendError<T>),
}

#[derive(Clone, Debug)]
pub struct IndexSender<T> {
    seq_id: Arc<AtomicI64>,
    sender: Sender<T>,
    ack_win: AckWindow,
}

impl<T> From<SendError<T>> for IndexSendErr<T> {
    fn from(e: SendError<T>) -> Self {
        IndexSendErr::Chan(ChanErr::Send(e))
    }
}

impl<T> From<TrySendError<T>> for IndexSendErr<T> {
    fn from(e: TrySendError<T>) -> Self {
        IndexSendErr::Chan(ChanErr::Try(e))
    }
}

impl<T: SeqIdFill> IndexSender<T> {
    pub fn new(sender: Sender<T>, win_size: u32, seq_id: Arc<AtomicI64>) -> IndexSender<T> {
        IndexSender {
            seq_id,
            ack_win: AckWindow::new(win_size),
            sender,
        }
    }

    pub async fn send(&mut self, mut value: T) -> Result<i64, IndexSendErr<T>> {
        let current_id = self.seq_id.fetch_add(1, Ordering::Relaxed);
        if !self.ack_win.have_vacancy(current_id) {
            self.seq_id.fetch_sub(1, Ordering::Relaxed);
            return Err(IndexSendErr::Win(WindowErr::Full));
        }
        value.fill_seqid(current_id);
        return self
            .sender
            .send(value)
            .await
            .map(|_| {
                let _ = self.ack_win.send(current_id).map_err(|e| match e {
                    WindowErr::Full => {
                        self.seq_id.fetch_sub(1, Ordering::Relaxed);
                    }
                });
                current_id
            })
            .map_err(|e| {
                // When fail send, we need to sub 1
                self.seq_id.fetch_sub(1, Ordering::Relaxed);
                e.into()
            });
    }

    pub fn try_send(&mut self, mut value: T) -> Result<i64, IndexSendErr<T>> {
        let current_id = self.seq_id.fetch_add(1, Ordering::Relaxed);
        if !self.ack_win.have_vacancy(current_id) {
            return Err(IndexSendErr::Win(WindowErr::Full));
        }
        value.fill_seqid(current_id);
        return self
            .sender
            .try_send(value)
            .map(|_| {
                self.ack_win.send(current_id);
                current_id
            })
            .map_err(|e| {
                self.seq_id.fetch_sub(1, Ordering::Relaxed);
                e.into()
            });
    }
}

impl<T> IndexReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }
}

#[cfg(test)]
mod tests {}
