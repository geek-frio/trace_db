use anyhow::Error as AnyError;
use core::time::Duration;
use futures::channel::mpsc::unbounded;
use futures::channel::mpsc::UnboundedSender;
use futures::future::{select, Either};
use futures::sink::SinkExt;
use futures::StreamExt;
use grpcio::Error as GrpcErr;
use grpcio::{ClientDuplexReceiver, StreamingCallSink, WriteFlags};
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
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::trace;
use tracing::trace_span;
use tracing::warn;
use tracing::Instrument;

const MAX_SLEEP_SECS: u64 = 120;

pub struct SeqMail<T, W, Resp> {
    pub(crate) sender: IndexSender<T, W>,
    pub receiver: ClientDuplexReceiver<Resp>,
}

#[derive(PartialEq, Debug, Clone)]
pub struct SegmentDataWrap(pub SegmentData);


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
        self.sender
            .send(msg, flags)
            .instrument(trace_span!("send"))
            .await?;
        Ok(())
    }

    pub fn ack(&mut self, seqid: i64) -> Result<(), AnyError> {
        Ok(self.sender.ack_id(seqid)?)
    }

    pub async fn start_task(
        sink: StreamingCallSink<SegmentData>,
        rpc_recv: ClientDuplexReceiver<SegmentRes>,
        win_size: usize,
        conn_id: i32,
    ) -> UnboundedSender<SegmentDataWrap> {
        let (e_s, mut e_r) = unbounded::<SegmentDataWrap>();
        let mut seq_mail = Self::new(sink, rpc_recv, win_size);

        TOKIO_RUN.spawn(async move {
            let mut timer = None;
            loop {
                match select(e_r.next(), seq_mail.receiver.next()).await {
                    Either::Left((value1, _)) => {
                        if let Some(s) = value1 {
                            let r = seq_mail.send_msg(s, WriteFlags::default()).instrument(trace_span!("send_msg")).await;
                            if let Err(e) = r {
                                warn!(?e, "Send msg failed!");
                                match e {
                                    IndexSendErr::Win(e) => {
                                        match e {
                                            RingQueueError::QueueIsFull(cur_id, ring_queue_size) => {
                                                warn!(%cur_id, %ring_queue_size, ?seq_mail.sender.ack_win, "Ring queue is full");
                                                // 实现指数Full以后指数退避
                                                match timer {
                                                    None => {
                                                        warn!("First time sleep 1 second");
                                                        sleep(Duration::from_secs(1)).await;
                                                        timer = Some((Instant::now(), 1));
                                                    }
                                                    Some((t, num )) => {
                                                        // 不到1s的时间内又发生了Full的情况, 等待时间指数退避
                                                        if t.elapsed().as_millis() < 1000 {
                                                            let hang_time= {
                                                                if num * 2 > MAX_SLEEP_SECS{
                                                                    warn!(%num, "Has overseed the max sleep time in less one second after sleep");
                                                                   MAX_SLEEP_SECS 
                                                                } else {
                                                                    num
                                                                }
                                                            };
                                                            warn!(%hang_time, %num, "queue full, sleep again");
                                                            timer = Some((Instant::now(), hang_time));
                                                            sleep(Duration::from_secs(hang_time)).await;
                                                        }
                                                    }
                                                }
                                            }
                                            RingQueueError::SendNotOneByOne(cur_id) => {
                                                error!(%cur_id, ?seq_mail.sender.ack_win, "RingQueueError: not sent one by one! Try to skip this msg, this should not be happen!");
                                            }
                                        }
                                    }
                                    IndexSendErr::Grpc(e) => {
                                        error!(?e, "Send segmentdata failed! Has met serious grpc problem");
                                    }
                                }
                            }
                        }
                    }
                    Either::Right((value2, _)) => match value2 {
                        Some(r) => match r {
                            Ok(resp) => {
                                // 需要处理SegmentRes的Meta类型
                                match resp.get_meta().get_field_type(){
                                    Meta_RequestType::HANDSHAKE => {
                                        warn!("Handshake should not comme here, do nothing");
                                    },
                                    Meta_RequestType::TRANS_ACK=> {
                                        trace!(ack_id = resp.get_meta().get_seqId(), resp_meta = ?resp.get_meta(), "start to do trace seqId operation");
                                        let r = seq_mail.ack(resp.get_meta().seqId);
                                        if let Err(e) = r {
                                            error!(%e, "Serious!!Ack window ack failed!");
                                        }
                                    },
                                    Meta_RequestType::NEED_RESEND => {
                                        info!(resp = ?resp, "Have got need resend signal");
                                        let m = seq_mail.sender.ack_win.not_ack_iter();
                                        let segs = m.map(|w| {
                                            let mut s = w.clone().0;
                                            let m = s.mut_meta();
                                            // change 
                                            m.set_field_type(Meta_RequestType::NEED_RESEND);
                                            s
                                        }).collect::<Vec<SegmentData>>();
                                        trace!(resend_segs = ?segs, "Start to resend these segments");
                                        for seg in segs {
                                            let r = seq_mail.send_msg(SegmentDataWrap(seg), WriteFlags::default()).await;
                                            trace!(send_status = ?r, "resend segment");
                                        }
                                    }
                                    _ => {
                                        error!(resp_meta = ?resp.get_meta(), "Unexpected type, do nothing!");
                                    },
                                }
                           }
                            Err(e) => match e {
                                GrpcErr::Codec(e) => {
                                    error!(e = ?e, "Receiving AckMsg: Grpc codec problem! skip this message");
                                }
                                GrpcErr::InvalidMetadata(e) => {
                                    error!(e = ?e, "Grpc invalid metadata problem! skip this message");
                                }
                                _ => {
                                    error!(e = ?e, "Grpc send serious problem, quit!");
                                    return;
                                }
                            },
                        },
                        None => {
                            error!("Has got a none segment resp,continue loop!");
                            continue;
                        }
                    },
                };
            }
        }.instrument(info_span!("start_task", %conn_id)));
        e_s
    }
}

#[derive(Debug)]
pub enum IndexSendErr {
    Win(RingQueueError),
    Grpc(GrpcErr),
}

impl From<RingQueueError> for IndexSendErr {
    fn from(e: RingQueueError) -> Self {
        IndexSendErr::Win(e)
    }
}

pub struct IndexSender<T, W> {
    sender: StreamingCallSink<T>,
    pub(crate) ack_win: RingQueue<W>,
    phatom: PhantomData<W>,
}

impl From<GrpcErr> for IndexSendErr {
    fn from(e: GrpcErr) -> Self {
        IndexSendErr::Grpc(e)
    }
}

impl<W: Debug + SeqId + Clone + Into<T>, T> IndexSender<T, W> {
    pub fn new(sender: StreamingCallSink<T>, win_size: u32) -> IndexSender<T, W> {
        IndexSender {
            ack_win: RingQueue::new(win_size as usize),
            sender,
            phatom: PhantomData,
        }
    }

    pub async fn send(&mut self, value: W, flags: WriteFlags) -> Result<(), IndexSendErr> {
        self.ack_win.check(&value)?;
        return self
            .sender
            .send((value.clone().into(), flags))
            .await
            .map(|_| {
                trace!(msg = ?value, "Msg has been sent by IndexSender");
                let _ = self.ack_win.send(value);
                trace!("Sent msg has been sent by ring queue");
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
