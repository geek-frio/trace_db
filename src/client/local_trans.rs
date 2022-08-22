use chrono::Local;
use futures::Stream;
use futures_sink::Sink;
use grpcio::WriteFlags;
use skproto::tracing::SegmentData;
use skproto::tracing::SegmentRes;
use std::task::Poll;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;

pub(crate) struct LocalSink {
    send: UnboundedSender<(SegmentData, WriteFlags)>,
}

impl LocalSink {
    pub fn new(send: UnboundedSender<(SegmentData, WriteFlags)>) -> LocalSink {
        LocalSink { send }
    }
}

impl Sink<(SegmentData, WriteFlags)> for LocalSink {
    type Error = ();

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: (SegmentData, WriteFlags),
    ) -> Result<(), Self::Error> {
        // It's a unbounded channel, we just send this item
        let _ = self.send.send(item);
        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // We will not close local channel
        Poll::Ready(Ok(()))
    }
}

pub(crate) struct RemoteStream {
    recv: UnboundedReceiver<(SegmentData, WriteFlags)>,
}

impl RemoteStream {
    pub fn new(recv: UnboundedReceiver<(SegmentData, WriteFlags)>) -> RemoteStream {
        RemoteStream { recv }
    }
}

impl Stream for RemoteStream {
    type Item = (SegmentData, WriteFlags);

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.recv.poll_recv(cx)
    }
}

pub(crate) struct LocalStream {
    recv: UnboundedReceiver<(SegmentRes, WriteFlags)>,
}

impl LocalStream {
    pub fn new(recv: UnboundedReceiver<(SegmentRes, WriteFlags)>) -> LocalStream {
        LocalStream { recv }
    }
}

impl Stream for LocalStream {
    type Item = Result<SegmentRes, grpcio::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let res = self.recv.poll_recv(cx);
        match res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(val) => Poll::Ready(val.map(|v| Ok(v.0))),
        }
    }
}

pub(crate) struct RemoteSink {
    send: UnboundedSender<(SegmentRes, WriteFlags)>,
}

impl futures_sink::Sink<(SegmentRes, WriteFlags)> for RemoteSink {
    type Error = (String, (SegmentRes, WriteFlags));

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: (SegmentRes, WriteFlags),
    ) -> Result<(), Self::Error> {
        let send_res = self.send.send(item);
        match send_res {
            Ok(_) => Ok(()),
            Err(e) => Err(("local sink peer is dropped".to_string(), e.0)),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
