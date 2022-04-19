use anyhow::Error as AnyError;
use futures::channel::mpsc::unbounded;
use futures::channel::mpsc::UnboundedSender;
use futures::future::{select, Either};
use futures::sink::SinkExt;
use futures::StreamExt;
use grpcio::Error as GrpcErr;
use grpcio::{ClientDuplexReceiver, StreamingCallSink, WriteFlags};
use skdb::com::ack::{AckWindow, WindowErr};
use skdb::TOKIO_RUN;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
pub trait SeqIdFill {
    fn fill_seqid(&mut self, seq_id: i64);
}

pub struct SeqMail<T, Resp> {
    sender: IndexSender<T>,
    pub receiver: ClientDuplexReceiver<Resp>,
}

impl<T: SeqIdFill + Send + 'static, Resp: 'static> SeqMail<T, Resp> {
    pub fn new(
        sink: StreamingCallSink<T>,
        rpc_recv: ClientDuplexReceiver<Resp>,
        win_size: usize,
    ) -> SeqMail<T, Resp> {
        SeqMail::new(sink, rpc_recv, win_size)
    }

    pub async fn send_msg(&mut self, msg: T, flags: WriteFlags) -> Result<i64, IndexSendErr> {
        let seq_id = self.sender.send(msg, flags).await?;
        Ok(seq_id)
    }

    //
    pub async fn start_task(
        sink: StreamingCallSink<T>,
        rpc_recv: ClientDuplexReceiver<Resp>,
        win_size: usize,
    ) -> Result<UnboundedSender<T>, AnyError> {
        let (e_s, mut e_r) = unbounded::<T>();
        let mut seq_mail = Self::new(sink, rpc_recv, win_size);

        TOKIO_RUN.spawn(async move {
            loop {
                match select(e_r.next(), seq_mail.receiver.next()).await {
                    Either::Left((value1, _)) => {}
                    Either::Right((value2, _)) => {}
                };
            }
        });

        Ok(e_s)
    }
}

pub enum IndexSendErr {
    Win(WindowErr),
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

    pub async fn send(&mut self, mut value: T, flags: WriteFlags) -> Result<i64, IndexSendErr> {
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
}
