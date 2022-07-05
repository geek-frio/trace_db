use crate::tag::engine::TagEngineError;
use skproto::tracing::SegmentData;
use tokio::sync::oneshot::Sender;

use super::pkt::PktHeader;

pub enum CallbackStat {
    Ok(PktHeader),
    IOErr(TagEngineError, PktHeader),
    ExpiredData(PktHeader),
    ShuttingDown(SegmentData),
}
pub struct AckCallback {
    sender: Sender<CallbackStat>,
}

impl AckCallback {
    pub fn new(sender: Sender<CallbackStat>) -> AckCallback {
        AckCallback { sender }
    }

    pub fn callback(self, stat: CallbackStat) {
        let _ = self.sender.send(stat);
    }
}
