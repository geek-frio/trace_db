use super::*;
use ack::*;
use chrono::prelude::*;
use chrono::Duration;

use crossbeam_channel::*;
use futures::SinkExt;
use futures::TryStreamExt;
use futures_util::{FutureExt as _, TryFutureExt as _};
use grpcio::*;
use skdb::com::mail::BasicMailbox;
use skdb::com::router::Either;
use skdb::com::router::Router;
use skdb::com::sched::NormalScheduler;
use skdb::tag::engine::TagWriteEngine;
use skdb::tag::fsm::TagFsm;
use skdb::*;
use skproto::tracing::*;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::thread;

#[derive(Clone)]
pub struct SkyTracingService {
    sender: Sender<SegmentData>,
    router: Router<TagFsm, NormalScheduler<TagFsm>>,
}

impl SkyTracingService {
    // do new and spawn two things
    pub fn new_spawn(router: Router<TagFsm, NormalScheduler<TagFsm>>) -> SkyTracingService {
        let (s, r) = unbounded::<SegmentData>();
        let m_router = router.clone();

        thread::spawn(move || loop {
            let res = r.recv();
            match res {
                Ok(seg_data) => {
                    // We don't need segment data before 30 days ago
                    let biz_timestamp = seg_data.biz_timestamp;
                    let now = Utc::now();
                    let d = Utc.timestamp_nanos(biz_timestamp as i64);

                    // We only need biz data 30 days ago
                    if now - Duration::days(30) > d {
                        continue;
                    }
                    let day = d.day();
                    let hour = d.hour();
                    let minute = d.minute() / 15;

                    // Only reserve last 30 days segment data
                    let s = format!("{}{:0>2}{:0>2}", day, hour, minute);
                    let addr = s.parse::<u64>().unwrap();

                    let res = m_router.send(addr, seg_data);
                    match res {
                        Either::Left(r) => {
                            if let Err(e) = r {
                                // TODO: Err process operation
                            }
                            continue;
                        }
                        Either::Right(msg) => {
                            // Mailbox not exists, so we regists a new one
                            let (s, r) = unbounded();
                            // TODO: use config struct
                            let mut engine = TagWriteEngine::new(addr, "/tmp");
                            // TODO: error process logic is emitted currently
                            let _ = engine.init();
                            let fsm = Box::new(TagFsm {
                                receiver: r,
                                mailbox: None,
                                engine: engine,
                            });
                            let state_cnt = Arc::new(AtomicUsize::new(0));
                            let mailbox = BasicMailbox::new(s, fsm, state_cnt);
                            let fsm = mailbox.take_fsm();
                            if let Some(mut f) = fsm {
                                f.mailbox = Some(mailbox.clone());
                                mailbox.release(f);
                            }
                            m_router.register(addr, mailbox);
                            m_router.send(addr, msg);
                        }
                    }
                }
                Err(e) => {}
            }
        });
        SkyTracingService { sender: s, router }
    }
}

impl SkyTracing for SkyTracingService {
    // Just for push msg test
    fn push_msgs(
        &mut self,
        ctx: ::grpcio::RpcContext,
        mut stream: ::grpcio::RequestStream<StreamReqData>,
        mut sink: ::grpcio::DuplexSink<StreamResData>,
    ) {
        let f = async move {
            let mut res_data = StreamResData::default();
            res_data.set_data("here comes response data".to_string());
            while let Some(data) = stream.try_next().await? {
                println!("Now we have the data:{:?}", data);
                sink.send((res_data.clone(), WriteFlags::default())).await?;
            }
            sink.close().await?;
            Ok(())
        }
        .map_err(|_: grpcio::Error| println!("xx"))
        .map(|_| ());

        ctx.spawn(f)
    }

    fn push_segments(
        &mut self,
        ctx: ::grpcio::RpcContext,
        mut stream: ::grpcio::RequestStream<SegmentData>,
        mut sink: ::grpcio::DuplexSink<SegmentRes>,
    ) {
        // Logic for handshake
        let handshake_exec = |_: SegmentData, mut sink: grpcio::DuplexSink<SegmentRes>| async {
            let conn_id = CONN_MANAGER.gen_new_conn_id();
            let mut resp = SegmentRes::new();
            let mut meta = Meta::new();
            meta.connId = conn_id;
            meta.field_type = Meta_RequestType::HANDSHAKE;
            resp.set_meta(meta);
            // We don't care handshake is success or not, client should retry for this
            let _ = sink.send((resp, WriteFlags::default())).await;
            println!("Has sent handshake response!");
            let _ = sink.flush().await;
            return sink;
        };

        // TODO: change a better name, process logic currently is not involved
        let mut ack_ctl = AckCtl::new();
        // Logic for processing segment datas
        let get_data_exec = async move {
            while let Some(data) = stream.try_next().await.unwrap() {
                if !data.has_meta() {
                    println!("Has no meta,quit");
                    continue;
                }
                match data.get_meta().get_field_type() {
                    Meta_RequestType::HANDSHAKE => {
                        sink = handshake_exec(data, sink).await;
                    }
                    Meta_RequestType::TRANS => {
                        ack_ctl.process_timely_ack_ctl(data, &mut sink).await;
                    }
                    _ => {
                        todo!();
                    }
                }
            }
        };
        TOKIO_RUN.spawn(get_data_exec);
    }
}
