use crate::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendError, TryRecvError, TrySendError};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::sleep;

pub trait SeqIdFill {
    fn fill_seqid(&mut self, seq_id: i64);
}

pub struct SeqMail<T, C> {
    seq_vals: Arc<Mutex<BTreeMap<i64, C>>>,
    sender: IndexSender<T>,
    cur_seqid: i64,
    ack_sender: Sender<MailSignal>,
}

fn chan<T>(buf_size: usize) -> (IndexSender<T>, IndexReceiver<T>) {
    let (tx, rx) = mpsc::channel(buf_size);
    let seq_id = Arc::new(AtomicI64::new(1));
    (
        IndexSender {
            seq_id: seq_id.clone(),
            sender: tx.clone(),
        },
        IndexReceiver {
            seq_id: seq_id.clone(),
            receiver: rx,
        },
    )
}

enum MailSignal {
    Ack(i64),
    Shutdown,
}

impl<T: SeqIdFill, C: Clone + Send + 'static> SeqMail<T, C> {
    pub fn new(buf_size: usize) -> (SeqMail<T, C>, IndexReceiver<T>) {
        let (s_t, mut s_r) = mpsc::channel(256);

        let map: Arc<Mutex<BTreeMap<i64, C>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let snap_map = map.clone();
        TOKIO_RUN.spawn(async move {
            loop {
                // TODO: add drain method, we only need the lastest ack number
                let ack_signal = s_r.recv().await;
                if let Some(o) = ack_signal {
                    match o {
                        MailSignal::Ack(seq_id) => {
                            let mut current_map_guard = snap_map.lock().await;
                            current_map_guard.iter().for_each(|(k, _c)| {
                                if *k <= seq_id {
                                    // TODO: callback logic
                                    println!("{} callback is invoked", k);
                                }
                            });
                            // iterate this map, remove lower seq id key value
                            current_map_guard.retain(|k, _| {
                                if *k < seq_id {
                                    return false;
                                }
                                true
                            });
                        }
                        MailSignal::Shutdown => {
                            // TODO: add callback invoke logic
                            return;
                        }
                    }
                }
                // Control frequency
                sleep(Duration::from_secs(1)).await;
            }
        });

        let (s, r) = chan::<T>(buf_size);
        (
            SeqMail {
                seq_vals: map,
                sender: s,
                cur_seqid: -1,
                ack_sender: s_t,
            },
            r,
        )
    }

    pub async fn try_send_msg(&self, msg: T, callback: C) -> Result<i64, TrySendError<T>> {
        let seq_id = self.sender.try_send(msg)?;
        let mut seq_ref = self.seq_vals.lock().await;
        seq_ref.insert(seq_id, callback);
        Ok(seq_id)
    }

    // Ack operation is incremented, never come back
    pub fn ack_id(&self, seq_id: i64) {
        if seq_id <= self.cur_seqid {
            return;
        }
        let _ = self.ack_sender.try_send(MailSignal::Ack(seq_id));
    }
}

pub struct IndexReceiver<T> {
    seq_id: Arc<AtomicI64>,
    receiver: Receiver<T>,
}

#[derive(Clone, Debug)]
pub struct IndexSender<T> {
    seq_id: Arc<AtomicI64>,
    sender: Sender<T>,
}

impl<T> IndexSender<T> {
    pub async fn send(&self, value: T) -> Result<i64, SendError<T>> {
        let current_id = self.seq_id.fetch_add(1, Ordering::Relaxed);
        return self
            .sender
            .send(value)
            .await
            .map(|_| current_id)
            .map_err(|e| {
                println!("Send error");
                e
            });
    }

    pub fn try_send(&self, value: T) -> Result<i64, TrySendError<T>> {
        let current_id = self.seq_id.fetch_add(1, Ordering::Relaxed);
        return self
            .sender
            .try_send(value)
            .map(|_| current_id)
            .map_err(|e| {
                println!("Try send error");
                e
            });
    }
}

impl<T> IndexReceiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }
}

#[cfg(test)]
mod tests {}
