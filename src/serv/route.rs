use anyhow::Error as AnyError;
use tokio::sync::mpsc::UnboundedReceiver;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc, Mutex}, marker::PhantomData, borrow::Cow,
};
use tantivy::{
    schema::{Schema, INDEXED, STORED, STRING, TEXT},
    Index,
};
use tracing::{error, info, trace, trace_span};

use crate::com::{index::{ConvertIndexAddr, MailKeyAddress}, router::Either, fsm::Fsm};
use crate::com::mail::BasicMailbox;
use crate::tag::engine::*;
use crate::tag::fsm::TagFsm;
use crate::{com::{config::GlobalConfig, router::RouteMsg}, tag::{fsm::SegmentDataCallback, engine::TracingTagEngine}};

pub struct LocalSegmentMsgConsumer<Router, Err> {
    router: Router,
    config: Arc<GlobalConfig>,
    index_map: Arc<Mutex<HashMap<i64, Index>>>,
    schema: Schema,
    receiver: UnboundedReceiver<SegmentDataCallback>,
    _err: PhantomData<Err>,
}

impl<Router, Err> LocalSegmentMsgConsumer<Router, Err> where Router: RouteMsg<Result<(), Err>, TagFsm, Addr = MailKeyAddress> {
    pub fn new(
        router: Router,
        config: Arc<GlobalConfig>,
        receiver: UnboundedReceiver<SegmentDataCallback>,
    ) -> LocalSegmentMsgConsumer<Router, Err> {
        let schema = Self::init_sk_schema();
        let index_map = Arc::new(Mutex::new(HashMap::default()));
        LocalSegmentMsgConsumer {
            router,
            config,
            index_map,
            schema,
            receiver,
            _err: PhantomData,
        }
    }

    fn route_msg(&self, seg: SegmentDataCallback) -> Result<(), SegmentDataCallback> {
        let mail_key_addr= seg.data.biz_timestamp.with_index_addr();
        let mail_key_addr= match mail_key_addr{
            Ok(path) => path,
            Err(_) => return Err(seg),
        };
        let trace_id = seg.data.get_trace_id();
        trace!(
            index_addr = ?mail_key_addr,
            seq_id = seg.data.get_meta().get_seqId(),
            trace_id = trace_id,
            "Has computed segment's address"
        );

        let index_path = Box::new(self.config.index_dir.clone());
        let send_stat = self.router.route_msg(mail_key_addr.into(), seg);
        match send_stat {
            Either::Right(msg) => {
                info!(
                    "Can't find addr's mailbox, create a new one"
                );
                let (s, r) = crossbeam_channel::unbounded();
                let mut engine =
                    TracingTagEngine::new(mail_key_addr, index_path.clone(), self.schema.clone());
                let res = engine.init();
                match res {
                    Ok(index) => {
                        self.index_map.lock().unwrap().insert(mail_key_addr.into(), index);
                        let fsm = Box::new(TagFsm::new(r, None, engine));
                        let state_cnt = Arc::new(AtomicUsize::new(0));
                        let mailbox = BasicMailbox::new(s, fsm, state_cnt); 
                        let fsm = mailbox.take_fsm();
                        if let Some(mut f) = fsm {
                            f.set_mailbox(Cow::Borrowed(&mailbox));
                            mailbox.release(f);
                        }
                        self.router.register(mail_key_addr.into(), mailbox);
                        self.router.route_msg(mail_key_addr.into(), msg);
                    }
                    // TODO: This error can not fix by retry, so we just ack this msg
                    // Maybe we should store this msg anywhere
                    Err(e) => {
                        error!("This error can not fix by retry, so we just ack this msg Maybe we should store this msg anywhere!");
                        error!(seq_id = msg.data.get_meta().get_seqId(), trace_id = ?msg.data.trace_id, "Init addr's TagEngine failed!Just callback this data.e:{:?}", e);
                        msg.callback.callback(msg.data.get_meta().get_seqId());
                    }
                }
            }
            Either::Left(Err(_)) => unreachable!("Receiver should never drop! Local unbounded channel mailbox, in logic we never drop receive!"),
            Either::Left(Ok(_)) => {
                trace!("Has routed successful");
            }
        }
        Ok(())
    }

    pub async fn loop_poll(&mut self) -> Result<(), AnyError> {
        loop {
            let segment_callback = self.receiver.recv().await;

            match segment_callback {
                Some(segment_callback) => {
                    let _consume_span = trace_span!(
                        "recv_and_process",
                        trace_id = ?segment_callback.data.trace_id
                    )
                    .entered();
                    trace!(parent: &segment_callback.span, "Has received the segment, try to route to mailbox.");
                    if let Err(seg) = self.route_msg(segment_callback) {
                        seg.callback.callback(seg.data.get_meta().get_seqId());
                    }
                }
                None => return Err(AnyError::msg("LocalSegmentMsgConsumer's sender has closed")),
            }
        }
    }

    pub fn init_sk_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field(ZONE, STRING);
        schema_builder.add_i64_field(API_ID, INDEXED);
        schema_builder.add_text_field(SERVICE, TEXT);
        schema_builder.add_u64_field(BIZTIME, STORED);
        schema_builder.add_text_field(TRACE_ID, STRING | STORED);
        schema_builder.add_text_field(SEGID, STRING);
        schema_builder.add_text_field(PAYLOAD, STRING);
        schema_builder.build()
    }
}