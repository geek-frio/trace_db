use futures::SinkExt;
use grpcio::Error as GrpcErr;
use grpcio::{ClientDuplexReceiver, StreamingCallSink, WriteFlags};
use skdb::com::ack::{AckWindow, WindowErr};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
pub trait SeqIdFill {
    fn fill_seqid(&mut self, seq_id: i64);
}

pub struct SeqMail<T> {
    sender: IndexSender<T>,
}

fn chan<T: SeqIdFill, Resp>(
    sink: StreamingCallSink<(T, WriteFlags)>,
    rpc_recv: ClientDuplexReceiver<Resp>,
    win_size: usize,
) -> (IndexSender<T>, IndexReceiver<T>) {
    let seq_id = Arc::new(AtomicI64::new(1));
    (
        IndexSender::new(sink, win_size as u32, seq_id.clone()),
        IndexReceiver {
            seq_id: seq_id,
            receiver: rpc_recv,
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

    pub async fn send_msg(&mut self, msg: T, flags: WriteFlags) -> Result<i64, IndexSendErr<T>> {
        let seq_id = self.sender.send(msg, flags).await?;
        Ok(seq_id)
    }

    // Start the task, return event ref
    pub async fn start(buf_size: usize) -> UnboundedSender<T> {
        let (e_s, mut e_r) = unbounded_channel::<T>();
        let (seq_mail, r) = SeqMail::<T>::new(buf_size);

        let event = e_r.recv().await;
        e_s
    }
}

pub struct IndexReceiver<T> {
    seq_id: Arc<AtomicI64>,
    receiver: ClientDuplexReceiver<T>,
}

pub enum IndexSendErr {
    Grpc(GrpcErr),
}

pub struct IndexSender<T> {
    seq_id: Arc<AtomicI64>,
    sender: StreamingCallSink<T>,
    ack_win: AckWindow,
}

impl From<GrpcErr> for IndexSendErr {
    fn from(e: GrpcErr) -> Self {
        IndexSendErr::Grpc(e)
    }
}

impl<T: SeqIdFill> IndexSender<T> {
    pub fn new(
        sender: StreamingCallSink<T>,
        win_size: u32,
        seq_id: Arc<AtomicI64>,
    ) -> IndexSender<T> {
        IndexSender {
            seq_id,
            ack_win: AckWindow::new(win_size),
            sender,
        }
    }

    pub async fn send(&mut self, mut value: T, flags: WriteFlags) -> Result<i64, IndexSendErr<T>> {
        let current_id = self.seq_id.fetch_add(1, Ordering::Relaxed);
        if !self.ack_win.have_vacancy(current_id) {
            self.seq_id.fetch_sub(1, Ordering::Relaxed);
            return Err(IndexSendErr::Win(WindowErr::Full));
        }
        value.fill_seqid(current_id);
        return self
            .sender
            .send((value, flags))
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

    pub fn try_send(&mut self, mut value: T) -> Result<i64, IndexSendErr> {
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
