use super::{cluster::ClusterManager, RingServiceReqEvent};
use crate::com::{
    config::GlobalConfig,
    ring::RingQueueError,
    util::{CountTracker, LruCache},
};
use anyhow::Error as AnyError;
use crossbeam::channel::Sender;
use futures::future::Either;
use futures::stream::{Stream, StreamExt};
use grpcio::{Environment, ServerBuilder};
use rand::prelude::*;
use skproto::tracing::{Meta_RequestType, SegmentData, SegmentRes};
use skproto::tracing_grpc::SkyTracingClient;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};
use tower::{buffer::Buffer, Service};
use tower::{limit::RateLimit, ServiceExt};
use tracing::error;

use super::grpc_cli::WrapSegmentData;

enum BatchStatus {
    Created,       // Batch received from remote client
    ServiceCalled, // Request has been called waiting to be sent
    Acked,         // Batch has been acked
}

struct Batch<T> {
    id: i64,
    data: Vec<T>,
}

struct BatchIntoIter<T> {
    id: i64,
    data: std::vec::IntoIter<T>,
}

impl<T> Iterator for BatchIntoIter<T> {
    type Item = (i64, T);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.data.next();
        item.map(|a| (self.id, a))
    }
}

impl<T> IntoIterator for Batch<T> {
    type Item = (i64, T);

    type IntoIter = BatchIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        let iter = self.data.into_iter();
        BatchIntoIter {
            id: self.id,
            data: iter,
        }
    }
}

struct ProxyService<Req, S>
where
    S: Service<Req>,
{
    global_config: Arc<GlobalConfig>,
    cluster_manager: ClusterManager<Req, S>,
    marker: PhantomData<Req>,
}

impl<S> ProxyService<RingServiceReqEvent<WrapSegmentData>, S>
where
    S: Service<RingServiceReqEvent<WrapSegmentData>>,
{
    // fn new(
    //     config: Arc<GlobalConfig>,
    //     cluster_manager: ClusterManager<RingServiceReqEvent<WrapSegmentData>, S>,
    // ) -> ProxyService<
    //     // Buffer<
    //     //     RateLimit<RingService<TracingSinker<WrapSegmentData, SegmentData>, WrapSegmentData>>,
    //     //     RingServiceReqEvent<WrapSegmentData>,
    //     // >,
    //     WrapSegmentData,
    //     // TracingStreamer<SegmentRes>,
    //     SkyTracingClient,
    // > {
    //     ProxyService {
    //         global_config: config,
    //         // sinkers: Vec::new(),
    //         // streamers: Vec::new(),
    //         cluster_manager,
    //         marker: PhantomData,
    //     }
    // }

    async fn init(&mut self) {
        todo!();
    }

    fn check_is_msg_event(
        batch: &Batch<RingServiceReqEvent<WrapSegmentData>>,
    ) -> Result<(), AnyError> {
        for e in batch.data.iter() {
            match e {
                RingServiceReqEvent::NeedResend(_) | RingServiceReqEvent::Ack(_) => {
                    return Err(AnyError::msg("Invalid ring service event for shuffle call"))
                }
                _ => continue,
            }
        }
        Ok(())
    }

    async fn resend(&mut self, seg_res: &SegmentRes) {
        todo!();
    }

    // async fn shuffle_call(
    //     &mut self,
    //     batch: Batch<RingServiceReqEvent<WrapSegmentData>>,
    // ) -> Result<(), AnyError> {
    //     Self::check_is_msg_event(&batch)?;
    //     let mut rng = rand::thread_rng();
    //     let idx = rng.gen_range(0..self.sinkers.len());
    //     let batch_iter = batch.into_iter();

    //     // 1.first we sink these msgs to remote
    //     let sink = &mut self.sinkers[idx];
    //     let _ = sink.ready().await;
    //     let mut success_ids = Vec::new();
    //     for (_, req) in batch_iter {
    //         let call_res = sink.call(req).await;
    //         match call_res {
    //             Ok(ei) => {
    //                 if let Either::Right(id) = ei {
    //                     if let Some(id) = id {
    //                         success_ids.push(id);
    //                     }
    //                 }
    //             }
    //             Err(e) => {
    //                 let err = e.downcast::<RingServiceErr<SinkErr, RingQueueError>>();
    //                 if let Ok(e) = err {
    //                     if let RingServiceErr::Left(sink_err) = *e {
    //                         if let SinkErr::GrpcSinkErr(e) = sink_err {
    //                             error!("Grpc sink has met serious problem, we should drop current connection,e:{}", e);
    //                             return Err(AnyError::msg(
    //                                 "Serious error, grpc sink has met serious problem",
    //                             ));
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     // 2.then we start to listen msg ack msgs
    //     let stream = &mut self.streamers[idx];
    //     for s in stream.next().await {
    //         match s {
    //             Ok(seg_resp) => match seg_resp.get_meta().get_field_type() {
    //                 Meta_RequestType::TRANS_ACK => {
    //                     if seg_resp.get_meta().get_seqId() >= *success_ids.last().unwrap() {
    //                         return Ok(());
    //                     }
    //                 }
    //                 Meta_RequestType::NEED_RESEND => {
    //                     self.resend(&seg_resp).await;
    //                 }
    //                 _ => {
    //                     unreachable!("Remote gave a not permitted meta request type");
    //                 }
    //             },
    //             Err(e) => {
    //                 error!("Serious streaming problem, e:{:?}", e);
    //                 return Err(e);
    //             }
    //         }
    //     }
    //     return Err(AnyError::msg("Remote streamer has dropped"));
    // }
}
