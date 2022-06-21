use anyhow::Error as AnyError;
use skproto::tracing::{SegmentData, SegmentRes, SkyTracingClient};
use tracing::{error, trace};

use super::{Created, TracingConnection};

pub(crate) fn split_client(
    client: SkyTracingClient,
) -> Result<
    (
        TracingConnection<Created, SegmentData, SegmentRes>,
        SkyTracingClient,
    ),
    AnyError,
> {
    match client.push_segments() {
        Ok((sink, recv)) => {
            trace!("Push segments called success! sink:(StreamingCallSink) and receiver:(ClientDuplexReceiver is created!");
            Ok((TracingConnection::new(sink, recv), client))
        }
        Err(e) => {
            error!(?e, "connect push segment service failed!");
            Err(e.into())
        }
    }
}
