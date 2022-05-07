use anyhow::Error as AnyError;
use futures::channel::mpsc::Receiver;
use futures::stream::StreamExt;
use skdb::{
    com::{
        config::GlobalConfig,
        index::{IndexAddr, IndexPath},
        mail::BasicMailbox,
        router::{Either,  RouteMsg},
    },
    tag::{
        engine::*,
        fsm::{SegmentDataCallback, TagFsm},
    },
};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc, Mutex}, marker::PhantomData,
};
use tantivy::{
    schema::{Schema, INDEXED, STORED, STRING, TEXT},
    Index,
};
use tracing::{error, info, trace, trace_span};

pub(crate) struct LocalSegmentMsgConsumer<Router, Err> {
    router: Router,
    config: Arc<GlobalConfig>,
    index_map: Arc<Mutex<HashMap<i64, Index>>>,
    schema: Schema,
    receiver: Receiver<SegmentDataCallback>,
    _err: PhantomData<Err>,
}

impl<Router, Err> LocalSegmentMsgConsumer<Router, Err> where Router: RouteMsg<Result<(), Err>, SegmentDataCallback , TagFsm,  Addr = i64> {
    pub(crate) fn new(
        router: Router,
        config: Arc<GlobalConfig>,
        receiver: Receiver<SegmentDataCallback>,
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
        let path = IndexPath::compute_index_addr(seg.data.biz_timestamp as IndexAddr);
        let path = match path {
            Ok(path) => path,
            Err(_) => return Err(seg),
        };
        let trace_id = seg.data.get_trace_id();
        trace!(
            index_addr = path,
            seq_id = seg.data.get_meta().get_seqId(),
            trace_id = trace_id,
            "Has computed segment's address"
        );

        let index_path = Box::new(self.config.index_dir.clone());
        let send_stat = self.router.route_msg(path, seg);
        match send_stat {
            Either::Right(msg) => {
                info!(
                    index_addr = path,
                    "Can't find addr's mailbox, create a new one"
                );
                let (s, r) = crossbeam_channel::unbounded();
                // TODO: use config struct
                let mut engine =
                    TracingTagEngine::new(path, index_path.clone(), self.schema.clone());
                // TODO: error process logic is emitted currently
                let res = engine.init();
                match res {
                    Ok(index) => {
                        self.index_map.lock().unwrap().insert(path, index);
                        let fsm = Box::new(TagFsm::new(r, None, engine));
                        let state_cnt = Arc::new(AtomicUsize::new(0));
                        let mailbox = BasicMailbox::new(s, fsm, state_cnt); 
                        let fsm = mailbox.take_fsm();
                        if let Some(mut f) = fsm {
                            f.mailbox = Some(mailbox.clone());
                            mailbox.release(f);
                        }
                        self.router.register(path, mailbox);
                        self.router.route_msg(path, msg);
                    }
                    // TODO: This error can not fix by retry, so we just ack this msg
                    // Maybe we should store this msg anywhere
                    Err(e) => {
                        error!("This error can not fix by retry, so we just ack this msg Maybe we should store this msg anywhere!");
                        error!(index_addr = path, seq_id = msg.data.get_meta().get_seqId(), trace_id = ?msg.data.trace_id, "Init addr's TagEngine failed!Just callback this data.e:{:?}", e);
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

    async fn loop_poll(&mut self) -> Result<(), AnyError> {
        loop {
            let segment_callback = self.receiver.next().await;

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
