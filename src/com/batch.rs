use super::fsm::Fsm;
use super::mail;
use crossbeam_channel::Receiver;
use std::borrow::Cow;
use std::ops::Deref;
use std::time::Duration;
use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
    thread::{current, ThreadId},
    time::Instant,
};

pub struct Batch<N> {
    normals: Vec<Option<NormalFsm<N>>>,
}

impl<N: Fsm> Batch<N> {
    pub fn with_capacity(cap: usize) -> Batch<N> {
        Batch {
            normals: Vec::with_capacity(cap),
        }
    }
    fn push(&mut self, fsm: FsmTypes<N>) -> bool {
        if let FsmTypes::Normal(f) = fsm {
            self.normals.push(Some(NormalFsm::new(f)));
            return true;
        } else {
            return false;
        }
    }

    fn is_empty(&self) -> bool {
        self.normals.is_empty()
    }

    fn clear(&mut self) {
        self.normals.clear();
    }

    fn release(&mut self, mut fsm: NormalFsm<N>, checked_len: usize) -> Option<NormalFsm<N>> {
        let mailbox = fsm.take_mailbox().unwrap();
        // 交换回NormalFsm手中的fsm
        mailbox.release(fsm.fsm);
        if mailbox.len() == checked_len {
            None
        } else {
            match mailbox.take_fsm() {
                None => None,
                Some(mut s) => {
                    s.set_mailbox(Cow::Owned(mailbox));
                    fsm.fsm = s;
                    Some(fsm)
                }
            }
        }
    }

    // pub fn schedule(&mut self, router: )
}

// 控制Fsm重新调度状态
enum ReschedulePolicy {
    Release(usize),
    Remove,
    Schedule,
}

pub struct NormalFsm<N> {
    fsm: Box<N>,
    timer: Instant,
    policy: Option<ReschedulePolicy>,
}

impl<N> Deref for NormalFsm<N> {
    type Target = N;

    fn deref(&self) -> &N {
        &self.fsm
    }
}

impl<N> DerefMut for NormalFsm<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fsm
    }
}

impl<N> NormalFsm<N> {
    fn new(fsm: Box<N>) -> NormalFsm<N> {
        NormalFsm {
            fsm,
            timer: Instant::now(),
            policy: None,
        }
    }
}

pub struct BatchSystem<N> {
    receiver: Receiver<N>,
    pool_size: usize,
    max_batch_size: usize,
}

impl<N> BatchSystem<N>
where
    N: Fsm + Send + 'static,
{
    // fn start_poller(&mut self, name: String) {}
}

pub enum FsmTypes<N> {
    Normal(Box<N>),
    Empty,
}

pub struct Poller<N, H> {
    pub fsm_receiver: Receiver<FsmTypes<N>>,
    pub handler: H,
    pub max_batch_size: usize,
    pub joinable_workers: Option<Arc<Mutex<Vec<ThreadId>>>>,
    pub reschedule_duration: Duration,
}

impl<N: Fsm, H: PollHandler<N>> Poller<N, H> {
    fn fetch_fsm(&mut self, batch: &mut Batch<N>) -> bool {
        if let Ok(fsm) = self.fsm_receiver.try_recv() {
            return batch.push(fsm);
        }
        if batch.is_empty() {
            self.handler.pause();
            // block wait
            if let Ok(fsm) = self.fsm_receiver.recv() {
                return batch.push(fsm);
            }
        }
        !batch.is_empty()
    }

    pub fn poll(&mut self) {
        let mut batch: Batch<N> = Batch::with_capacity(self.max_batch_size);
        let mut reschedule_fsms: Vec<usize> = Vec::with_capacity(self.max_batch_size);
        let mut to_skip_end = Vec::with_capacity(self.max_batch_size);
        let mut run = true;

        while run && self.fetch_fsm(&mut batch) {
            let max_batch_size = std::cmp::max(self.max_batch_size, batch.normals.len());
            self.handler.begin(max_batch_size);

            let mut hot_fsm_count = 0;
            for (i, p) in batch.normals.iter_mut().enumerate() {
                let p = p.as_mut().unwrap();
                let res = self.handler.handle(p);

                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(i);
                } else {
                    // 对于执行时间过长的fsm，说明其压力较大, 取其中第一半继续进行重新调度(重新fetch才能获取到)
                    // 目前看起来也就只有这里第
                    if p.timer.elapsed() >= self.reschedule_duration {
                        hot_fsm_count += 1;
                        if hot_fsm_count % 2 == 0 {
                            p.policy = Some(ReschedulePolicy::Schedule);
                            // 记录需要重新调度第fsm第下标
                            reschedule_fsms.push(i);
                        }
                    }
                    // 正常的fsm执行完毕以后都会进入这里
                    if let HandleResult::StopAt { progress, skip_end } = res {
                        // ?? 这里要确定progress第作用 ??
                        p.policy = Some(ReschedulePolicy::Release(progress));
                        reschedule_fsms.push(i);
                        if skip_end {
                            to_skip_end.push(i);
                        }
                    }
                }
            }
            // 起始下标从这里开始的原因是现在batch中的fsm已经处理过了
            // 只需要处理新增第fsm
            let mut fsm_cnt = batch.normals.len();
            while batch.normals.len() < max_batch_size {
                if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    // 如果收到了终止信号, 终止
                    run = batch.push(fsm);
                }
                // 没有获取到新第fsm,就break掉
                if !run || fsm_cnt >= batch.normals.len() {
                    break;
                }
                let p = batch.normals[fsm_cnt].as_mut().unwrap();
                let res = self.handler.handle(p);
                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(fsm_cnt);
                } else if let HandleResult::StopAt { progress, skip_end } = res {
                    p.policy = Some(ReschedulePolicy::Release(progress));
                    reschedule_fsms.push(fsm_cnt);
                    if skip_end {
                        to_skip_end.push(fsm_cnt);
                    }
                }
                fsm_cnt += 1;
            }
            self.handler.light_end(&mut batch.normals);
            for offset in &to_skip_end {
                todo!("重新schedual操作!");
                // 这里的操作会将batch中对应第fsm设置为None
            }
            to_skip_end.clear();
            self.handler.end(&mut batch.normals);
            while let Some(r) = reschedule_fsms.pop() {
                todo!("重新schedual操作!");
                // 这里的操作会将batch中对应第fsm设置为None
            }

            let left_fsm_cnt = batch.normals.len();
            if left_fsm_cnt > 0 {
                for i in 0..left_fsm_cnt {
                    let to_schedule = match batch.normals[i].take() {
                        Some(f) => f,
                        None => continue,
                    };
                    todo!("重新schedual操作!");
                }
            }
            batch.clear();
        }
    }
}

impl<N, H> Drop for Poller<N, H> {
    fn drop(&mut self) {
        if let Some(joinable_workers) = &self.joinable_workers {
            joinable_workers.lock().unwrap().push(current().id());
        }
    }
}

pub enum HandleResult {
    KeepProcessing,
    StopAt { progress: usize, skip_end: bool },
}

pub trait PollHandler<N: Fsm>: Send + 'static {
    fn begin(&mut self, batch_size: usize);
    fn handle(&mut self, normal: &mut impl DerefMut<Target = N>) -> HandleResult;
    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = N>>]);
    fn end(&mut self, _batch: &mut [Option<impl DerefMut<Target = N>>]);
    fn pause(&mut self);
}
