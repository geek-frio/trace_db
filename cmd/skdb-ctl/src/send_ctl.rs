use crate::chan::{IndexSendErr, IndexSender, SeqIdFill};

pub struct SenderCtlWrapper<T> {
    sender: IndexSender<T>,
}

impl<T: SeqIdFill> SenderCtlWrapper<T> {
    // win_size: max not ack sent msg
    fn new(sender: IndexSender<T>) -> SenderCtlWrapper<T> {
        SenderCtlWrapper { sender }
    }
    async fn send(&mut self, msg: T) -> Result<usize, IndexSendErr<T>> {
        let seq_id = self.sender.send(msg).await?;
        todo!()
    }

    fn send_ack_retry(self, msg: T) -> Result<usize, IndexSendErr<T>> {
        todo!()
    }
}
