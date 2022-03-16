use super::fsm::Fsm;
use super::router::Router;
use super::sched::FsmScheduler;
use crate::tag::fsm::TagFsm;
use crossbeam_channel::{Receiver, TryRecvError};
use skproto::tracing::SegmentData;
use std::borrow::Cow;
use std::ops::Deref;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
    thread,
    thread::ThreadId,
    time::Instant,
};

pub struct Batch<N> {
    pub normals: Vec<Option<NormalFsm<N>>>,
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
        println!("FSM's mailbox is taken away in release() method!");
        let mailbox = fsm.take_mailbox().unwrap();
        // 交换回NormalFsm手中的fsm到mailbox之中
        mailbox.release(fsm.fsm);
        if mailbox.len() == checked_len {
            None
        } else {
            match mailbox.take_fsm() {
                None => None,
                Some(mut s) => {
                    println!("FSM's mailbox is set back in release() method!");
                    s.set_mailbox(Cow::Owned(mailbox));
                    fsm.fsm = s;
                    Some(fsm)
                }
            }
        }
    }

    // It will check the sender lenth of this fsm's mailbox
    // if there are no messsages waiting to be processed, fsm should be reset to mailbox
    // (It means this fsm is waiting for new schedualed operation)
    fn remove(&mut self, mut fsm: NormalFsm<N>) -> Option<NormalFsm<N>> {
        println!("FSM's mailbox is taken away in remove() method!");
        let mailbox = fsm.take_mailbox().unwrap();
        if mailbox.is_empty() {
            mailbox.release(fsm.fsm);
            None
        } else {
            println!("FSM' mailbox is set back in remove() method!");
            fsm.set_mailbox(Cow::Owned(mailbox));
            Some(fsm)
        }
    }

    pub fn schedule<S: FsmScheduler<F = N>>(
        &mut self,
        router: &Router<N, S>,
        index: usize,
        inplace: bool,
    ) {
        let to_schedule = match self.normals[index].take() {
            Some(f) => f,
            None => {
                // 是否保留占位
                if !inplace {
                    self.normals.swap_remove(index);
                }
                return;
            }
        };
        let mut res = match to_schedule.policy {
            //
            Some(ReschedulePolicy::Release(l)) => self.release(to_schedule, l),
            // The prerequisite for removing the fsm in the batch is that, the sender in the tag fsm's mailbox is empty
            // If it is not empty, we need to reschedual the batch
            Some(ReschedulePolicy::Remove) => self.remove(to_schedule),
            Some(ReschedulePolicy::Schedule) => {
                router.normal_scheduler.schedule(to_schedule.fsm);
                None
            }
            None => Some(to_schedule),
        };

        // 处理重新调度结果
        if let Some(f) = &mut res {
            f.policy.take();
            self.normals[index] = res;
        } else if !inplace {
            self.normals.swap_remove(index);
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

pub struct BatchSystem<N: Fsm, S> {
    receiver: Receiver<FsmTypes<N>>,
    pool_size: usize,
    max_batch_size: usize,
    router: Router<N, S>,
    joinable_workers: Arc<Mutex<Vec<ThreadId>>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    name_prefix: String,
}

impl<S> BatchSystem<TagFsm, S>
where
    S: FsmScheduler<F = TagFsm> + Clone + Send + 'static,
{
    pub fn new(
        router: Router<TagFsm, S>,
        receiver: Receiver<FsmTypes<TagFsm>>,
        pool_size: usize,
        max_batch_size: usize,
    ) -> BatchSystem<TagFsm, S> {
        BatchSystem {
            receiver,
            pool_size,
            max_batch_size,
            router,
            joinable_workers: Arc::new(Mutex::new(Vec::new())),
            workers: Arc::new(Mutex::new(Vec::new())),
            name_prefix: String::new(),
        }
    }

    pub fn start_poller(&mut self, name: String, max_batch_size: usize) {
        let handler = TagPollHandler {
            msg_buf: Vec::new(),
            counter: AtomicI32::new(0),
            last_time: None,
        };
        let mut poller = Poller::new(
            &self.receiver,
            handler,
            max_batch_size,
            self.router.clone(),
            self.joinable_workers.clone(),
        );

        let t = thread::Builder::new()
            .name(name)
            .spawn(move || {
                println!("Poller has started!");
                poller.poll();
            })
            .unwrap();
        self.workers.lock().unwrap().push(t);
    }

    pub fn spawn(&mut self, name_prefix: String) {
        self.name_prefix = name_prefix.clone();
        for i in 0..self.pool_size {
            self.start_poller(format!("{}-{}", name_prefix, i), self.max_batch_size);
        }
    }
}

pub enum FsmTypes<N> {
    Normal(Box<N>),
    Empty,
}

pub struct Poller<N: Fsm, H, S> {
    pub fsm_receiver: Receiver<FsmTypes<N>>,
    pub handler: H,
    pub max_batch_size: usize,
    pub joinable_workers: Arc<Mutex<Vec<ThreadId>>>,
    pub reschedule_duration: Duration,
    pub router: Router<N, S>,
}

impl<N: Fsm, H: PollHandler<N>, S: FsmScheduler<F = N>> Poller<N, H, S> {
    pub fn new(
        receiver: &Receiver<FsmTypes<N>>,
        handler: H,
        max_batch_size: usize,
        router: Router<N, S>,
        joinable_workers: Arc<Mutex<Vec<ThreadId>>>,
    ) -> Poller<N, H, S> {
        Poller {
            fsm_receiver: receiver.clone(),
            handler,
            max_batch_size,
            router,
            joinable_workers,
            reschedule_duration: Duration::from_secs(30),
        }
    }

    fn fetch_fsm(&mut self, batch: &mut Batch<N>) -> bool {
        if let Ok(fsm) = self.fsm_receiver.try_recv() {
            println!("Has pushed a new fsm");
            println!("batch length:{}", batch.normals.len());
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
            println!("Poller: Has fetched a fsm");
            let max_batch_size = std::cmp::max(self.max_batch_size, batch.normals.len());
            self.handler.begin(max_batch_size);

            let mut hot_fsm_count = 0;
            for (i, p) in batch.normals.iter_mut().enumerate() {
                let p = p.as_mut().unwrap();
                let res = self.handler.handle(p);

                println!("Poller: Handle process result is ok!");
                if p.is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    reschedule_fsms.push(i);
                } else {
                    // 判断这个fsm的处理时间，如果过长，需要重点关注
                    // 将batch发生了处理时间过长的fsm其中的一半进行重新调度处理
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
                        println!(
                            "Poller: Fsm's schedual logic become Release status, progress:{}, skip_end:{}",
                            progress, skip_end
                        );
                        p.policy = Some(ReschedulePolicy::Release(progress));
                        reschedule_fsms.push(i);
                        if skip_end {
                            println!("Poller: This fsm will be put into skip fsm");
                            to_skip_end.push(i);
                        }
                    }
                }
            }
            // 起始下标从这里开始的原因是现在batch中的fsm已经处理过了
            // 只需要处理新增第fsm
            let mut fsm_cnt = batch.normals.len();
            // 这里会把batch进行多次填充
            while batch.normals.len() < max_batch_size {
                if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    // 如果收到了终止信号, 终止
                    run = batch.push(fsm);
                }
                // 没有获取到新第fsm,就break掉
                if !run || fsm_cnt == batch.normals.len() {
                    println!("Poller: Has not got new batch fsm, break;");
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
                // 这里的操作会将batch中对应的fsm设置为None
                println!("Poller: Start to process fsm in to_skip_end set");
                batch.schedule(&self.router, *offset, true);
            }
            to_skip_end.clear();
            self.handler.end(&mut batch.normals);
            while let Some(r) = reschedule_fsms.pop() {
                batch.schedule(&self.router, r, false);
            }

            let left_fsm_cnt = batch.normals.len();
            if left_fsm_cnt > 0 {
                for i in 0..left_fsm_cnt {
                    let to_schedule = match batch.normals[i].take() {
                        Some(f) => f,
                        None => continue,
                    };
                    self.router.normal_scheduler.schedule(to_schedule.fsm);
                }
            }
            batch.clear();
        }
    }
}

impl<N: Fsm, H, S> Drop for Poller<N, H, S> {
    fn drop(&mut self) {
        // if let Some(joinable_workers) = &self.joinable_workers {
        //     joinable_workers.lock().unwrap().push(current().id());
        // }
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

struct TagPollHandler {
    msg_buf: Vec<SegmentData>,
    counter: AtomicI32,
    last_time: Option<Instant>,
}

impl TagPollHandler {
    fn new_spawn() -> TagPollHandler {
        TagPollHandler {
            msg_buf: Vec::new(),
            counter: AtomicI32::new(0),
            last_time: None,
        }
    }
}

impl PollHandler<TagFsm> for TagPollHandler {
    fn begin(&mut self, _batch_size: usize) {
        if self.last_time.is_none() {
            self.last_time = Some(Instant::now());
            return;
        }
        // TODO: currently do nothing
        println!("Begin is called, currently we don't need to do anything");
    }

    fn handle(&mut self, normal: &mut impl DerefMut<Target = TagFsm>) -> HandleResult {
        loop {
            match normal.receiver.try_recv() {
                Ok(msg) => {
                    self.msg_buf.push(msg);
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    println!("Channel disconnected");
                    break;
                }
            }
        }
        println!(
            "TagPollHandler: self.msg_buf length is:{}",
            self.msg_buf.len()
        );
        // batch got msg, batch consume
        normal.handle_tasks(&mut self.msg_buf);
        self.counter
            .fetch_add(self.msg_buf.len() as i32, Ordering::Relaxed);
        // clear msg_buf, wait for next process
        self.msg_buf.clear();
        HandleResult::StopAt {
            progress: 0,
            skip_end: false,
        }
    }

    fn light_end(&mut self, _batch: &mut [Option<impl DerefMut<Target = TagFsm>>]) {
        println!("Light end operation is called");
    }

    fn end(&mut self, _batch: &mut [Option<impl DerefMut<Target = TagFsm>>]) {
        println!("End operation is called!");
        if let Some(t) = &self.last_time {
            if t.elapsed().as_secs() > 5 {
                println!(
                    "TagPollHandler consuming rate is:{}, time window current is:{}",
                    self.counter.load(Ordering::Relaxed) / t.elapsed().as_secs() as i32,
                    t.elapsed().as_secs(),
                );
                self.counter.store(0, Ordering::Relaxed);
                self.last_time = Some(Instant::now());
            }
        }
    }

    // We just sleep to wait for more data to be processed.
    fn pause(&mut self) {}
}
