use crate::tag::engine::TagEngineError;
use tokio::sync::oneshot::Sender;

pub(crate) enum CallbackStat {
    Ok(i64),
    IOErr(TagEngineError, i64),
    ExpiredData(i64),
    ShuttingDown,
}
pub struct AckCallback {
    sender: Sender<CallbackStat>,
}

impl AckCallback {
    pub fn new(sender: Sender<CallbackStat>) -> AckCallback {
        AckCallback { sender }
    }

    pub fn callback(&self, stat: CallbackStat) {
        let _ = self.sender.send(stat);
    }
}
