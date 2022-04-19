use anyhow::Error as AnyError;
use core::time::Duration;
use futures::channel::mpsc::unbounded;
use futures::channel::mpsc::UnboundedSender;
use futures::future::{select, Either};
use futures::sink::SinkExt;
use futures::StreamExt;
use grpcio::Error as GrpcErr;
use grpcio::{ClientDuplexReceiver, StreamingCallSink, WriteFlags};
use skdb::com::ack::{AckWindow, WindowErr};
use skdb::TOKIO_RUN;
use skproto::tracing::SegmentData;
use skproto::tracing::SegmentRes;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::time::sleep;
pub trait SeqIdFill {
    fn fill_seqid(&mut self, seq_id: i64);
}

pub struct SeqMail<T, Resp> {
    sender: IndexSender<T>,
    pub receiver: ClientDuplexReceiver<Resp>,
}

impl SeqMail<SegmentData, SegmentRes> {
    pub fn new(
        sink: StreamingCallSink<SegmentData>,
        rpc_recv: ClientDuplexReceiver<SegmentRes>,
        win_size: usize,
    ) -> SeqMail<SegmentData, SegmentRes> {
        SeqMail::new(sink, rpc_recv, win_size)
    }

    pub async fn send_msg(
        &mut self,
        msg: SegmentData,
        flags: WriteFlags,
    ) -> Result<i64, IndexSendErr> {
        let seq_id = self.sender.send(msg, flags).await?;
        Ok(seq_id)
    }

    pub fn ack(&mut self, seqid: i64) -> Result<(), AnyError> {
        Ok(self.sender.ack_id(seqid)?)
    }

    pub async fn start_task(
        sink: StreamingCallSink<SegmentData>,
        rpc_recv: ClientDuplexReceiver<SegmentRes>,
        win_size: usize,
    ) -> UnboundedSender<SegmentData> {
        let (e_s, mut e_r) = unbounded::<SegmentData>();
        let mut seq_mail = Self::new(sink, rpc_recv, win_size);

        TOKIO_RUN.spawn(async move {
            let mut timer = None;
            loop {
                match select(e_r.next(), seq_mail.receiver.next()).await {
                    Either::Left((value1, _)) => {
                        if let Some(s) = value1 {
                            let r = seq_mail.send_msg(s, WriteFlags::default()).await;
                            match r {
                                Ok(_) => {}
                                Err(e) => match e {
                                    IndexSendErr::Win(e) => {
                                        match e {
                                            WindowErr::Full => {
                                                println!("Another Grpc segment receiver is full, slow down send speed");
                                                // 实现指数Full以后指数退避
                                                match timer {
                                                    None => {
                                                        sleep(Duration::from_secs(1)).await;
                                                        timer = Some((Instant::now(), 1));
                                                    }
                                                    Some((t, num )) => {
                                                        // 不到1s的时间内又发生了Full的情况, 等待时间指数退避
                                                        if t.elapsed().as_millis() < 1000 {
                                                            let hang_time= {
                                                                if num * 2 > 120 {
                                                                    120
                                                                } else {
                                                                    num
                                                                }
                                                            };
                                                            timer = Some((Instant::now(), hang_time));
                                                            sleep(Duration::from_secs(hang_time)).await;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    IndexSendErr::Grpc(e) => {
                                        println!("Send segmentdata failed! Has met serious grpc problem:e:{:?}", e);
                                    }
                                },
                            }
                        }
                    }
                    Either::Right((value2, _)) => match value2 {
                        Some(r) => match r {
                            Ok(resp) => {
                                let r = seq_mail.ack(resp.get_meta().seqId);
                                if let Err(e) = r {
                                    println!("Serious!!Ack window ack failed! e:{:?}", e);
                                }
                            }
                            Err(e) => match e {
                                GrpcErr::Codec(e) => {
                                    println!("Receiving AckMsg: Grpc codec problem!e:{:?}", e)
                                }
                                GrpcErr::InvalidMetadata(e) => {
                                    println!("Grpc invalid metadata problem!e:{:?}", e);
                                }
                                _ => {
                                    println!("Grpc send serious problem, quit!");
                                    return;
                                }
                            },
                        },
                        None => {
                            println!("Has got a none segment resp,continue loop!");
                            continue;
                        }
                    },
                };
            }
        });
        e_s
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

    pub fn ack_id(&mut self, seqid: i64) -> Result<(), AnyError> {
        Ok(self.ack_win.ack(seqid)?)
    }
}
