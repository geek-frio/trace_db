use skproto::tracing::SegmentData;

#[derive(Debug)]
pub struct PktHeader {
    pub(crate) conn_id: i32,
    pub(crate) seq_id: i64,
}

impl Into<PktHeader> for SegmentData {
    fn into(self) -> PktHeader {
        let conn_id = self.get_meta().get_connId();
        let seq_id = self.get_meta().get_seqId();

        PktHeader { conn_id, seq_id }
    }
}
