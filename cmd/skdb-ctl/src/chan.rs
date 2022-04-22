use anyhow::Error as AnyError;
use core::time::Duration;
use futures::channel::mpsc::unbounded;
use futures::channel::mpsc::UnboundedSender;
use futures::future::{select, Either};
use futures::sink::SinkExt;
use futures::StreamExt;
use grpcio::Error as GrpcErr;
use grpcio::{ClientDuplexReceiver, StreamingCallSink, WriteFlags};
use skdb::com::ring::BlankElement;
use skdb::com::ring::RingQueue;
use skdb::com::ring::RingQueueError;
use skdb::com::ring::SeqId;
use skdb::TOKIO_RUN;
use skproto::tracing::Meta_RequestType;
use skproto::tracing::SegmentData;
use skproto::tracing::SegmentRes;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::time::Instant;
use tokio::time::sleep;
use tracing::instrument;

pub struct SeqMail<T, W, Resp> {
    sender: IndexSender<T, W>,
    pub receiver: ClientDuplexReceiver<Resp>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct SegmentDataWrap(pub SegmentData);

impl BlankElement for SegmentDataWrap {
    type Item = SegmentDataWrap;

    fn is_blank(&self) -> bool {
        !self.0.has_meta()
    }

    fn blank_val() -> Self::Item {
        SegmentDataWrap(SegmentData::default())
    }
}

impl SeqId for SegmentDataWrap {
    fn seq_id(&self) -> usize {
        self.0.get_meta().get_seqId() as usize
    }
}

impl From<SegmentDataWrap> for SegmentData {
    fn from(s: SegmentDataWrap) -> Self {
        s.0
    }
}

impl SeqMail<SegmentData, SegmentDataWrap, SegmentRes> {
    pub fn new(
        sink: StreamingCallSink<SegmentData>,
        rpc_recv: ClientDuplexReceiver<SegmentRes>,
        win_size: usize,
    ) -> SeqMail<SegmentData, SegmentDataWrap, SegmentRes> {
        let sender = IndexSender::new(sink, win_size as u32);
        SeqMail {
            sender,
            receiver: rpc_recv,
        }
    }

    pub async fn send_msg(
        &mut self,
        msg: SegmentDataWrap,
        flags: WriteFlags,
    ) -> Result<(), IndexSendErr> {
        self.sender.send(msg, flags).await?;
        Ok(())
    }

    pub fn ack(&mut self, seqid: i64) -> Result<(), AnyError> {
        Ok(self.sender.ack_id(seqid)?)
    }

    pub async fn start_task(
        sink: StreamingCallSink<SegmentData>,
        rpc_recv: ClientDuplexReceiver<SegmentRes>,
        win_size: usize,
    ) -> UnboundedSender<SegmentDataWrap> {
        let (e_s, mut e_r) = unbounded::<SegmentDataWrap>();
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
                                            RingQueueError::QueueIsFull => {
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
                                            RingQueueError::SendNotOneByOne => {
                                                println!("Try to skip this msg, this should not be happen!");
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
                                // 需要处理SegmentRes的Meta类型
                                match resp.get_meta().get_field_type(){
                                    Meta_RequestType::HANDSHAKE => {
                                        println!("Handshake should not come here, do nothing~");
                                    },
                                    Meta_RequestType::TRANS_ACK=> {
                                        let r = seq_mail.ack(resp.get_meta().seqId);
                                        if let Err(e) = r {
                                            println!("Serious!!Ack window ack failed! e:{:?}", e);
                                        }
                                    },
                                    Meta_RequestType::NEED_RESEND => {
                                        let m = seq_mail.sender.ack_win.not_ack_iter();
                                        let segs = m.map(|w| {
                                            let mut s = w.clone().0;
                                            let m = s.mut_meta();
                                            // change 
                                            m.set_field_type(Meta_RequestType::NEED_RESEND);
                                            s
                                        }).collect::<Vec<SegmentData>>();
                                        segs.into_iter().for_each(|s| {
                                            // Retry data, we don't care the send status
                                            let _ = seq_mail.send_msg(SegmentDataWrap(s), WriteFlags::default());
                                        });
                                    }
                                    _ => {
                                        println!("Unexpected type, do nothing!");
                                    },
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
    Win(RingQueueError),
    Grpc(GrpcErr),
}

pub struct IndexSender<T, W> {
    sender: StreamingCallSink<T>,
    ack_win: RingQueue<W>,
    phatom: PhantomData<W>,
}

impl From<GrpcErr> for IndexSendErr {
    fn from(e: GrpcErr) -> Self {
        IndexSendErr::Grpc(e)
    }
}

impl<W: Debug + SeqId + Clone + BlankElement<Item = W> + Into<T>, T> IndexSender<T, W> {
    pub fn new(sender: StreamingCallSink<T>, win_size: u32) -> IndexSender<T, W> {
        IndexSender {
            ack_win: RingQueue::new(win_size as usize),
            sender,
            phatom: PhantomData,
        }
    }

    pub async fn send(&mut self, value: W, flags: WriteFlags) -> Result<(), IndexSendErr> {
        if self.ack_win.is_full() {
            return Err(IndexSendErr::Win(RingQueueError::QueueIsFull));
        }
        return self
            .sender
            .send((value.clone().into(), flags))
            .await
            .map(|_| {
                let _ = self.ack_win.send(value).map_err(|e| match e {
                    RingQueueError::QueueIsFull => {
                        unreachable!();
                    }
                    RingQueueError::SendNotOneByOne => {
                        println!("Value seqId should one by one");
                    }
                });
            })
            .map_err(|e| {
                // When fail send, we need to sub 1
                e.into()
            });
    }

    pub fn ack_id(&mut self, seqid: i64) -> Result<(), AnyError> {
        if seqid <= 0 {
            return Err(AnyError::msg(
                "Incorrect seqid, should not be negative or 0",
            ));
        }
        Ok(self.ack_win.ack(seqid as usize))
    }
}
