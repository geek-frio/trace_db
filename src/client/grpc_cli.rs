use anyhow::Error as AnyError;
use skproto::tracing::{SegmentData, SegmentRes, SkyTracingClient};
use tracing::error;

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
        Ok((sink, recv)) => Ok((TracingConnection::new(sink, recv), client)),
        Err(e) => {
            error!(?e, "connect push segment service failed!");
            Err(e.into())
        }
    }
}
