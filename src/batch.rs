use crate::fsm::Fsm;
use crate::router::Router;
use crate::sched::FsmScheduler;
use crate::tag::fsm::{FsmExecutor, TagFsm};
use crossbeam_channel::Receiver;
use std::borrow::Cow;
use std::thread::JoinHandle;
use std::time::Duration;
use std::{
    sync::{Arc, Mutex},
    thread,
    thread::ThreadId,
    time::Instant,
};
use tracing::{info, info_span, trace, trace_span, warn};

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
        let mailbox = fsm.as_mut().take_mailbox().unwrap();
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

    // It will check the sender lenth of this fsm's mailbox
    // if there are no messsages waiting to be processed, fsm should be reset to mailbox
    // (It means this fsm is waiting for new schedualed operation)
    fn remove(&mut self, mut fsm: NormalFsm<N>) -> Option<NormalFsm<N>> {
        let mailbox = fsm.as_mut().take_mailbox().unwrap();
        if mailbox.is_empty() {
            mailbox.release(fsm.fsm);
            None
        } else {
            fsm.as_mut().set_mailbox(Cow::Owned(mailbox));
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
                if !inplace {
                    self.normals.swap_remove(index);
                }
                return;
            }
        };
        let mut res = match to_schedule.policy {
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
        if let Some(f) = &mut res {
            f.policy.take();
            self.normals[index] = res;
        } else if !inplace {
            self.normals.swap_remove(index);
        }
    }
}

// 控制Fsm重新调度状态
#[derive(Debug)]
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

impl<N> AsRef<N> for NormalFsm<N> {
    fn as_ref(&self) -> &N {
        &self.fsm
    }
}

impl<N> AsMut<N> for NormalFsm<N> {
    fn as_mut(&mut self) -> &mut N {
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
        let _span = info_span!("Start_poller", %name).entered();
        let handler = TagPollHandler {
            msg_buf: Vec::new(),
            counter: 0,
            last_time: None,
            msg_cnt: 0,
        };
        let mut poller = Poller::new(
            &self.receiver,
            handler,
            max_batch_size,
            self.router.clone(),
            self.joinable_workers.clone(),
        );
        info!(%max_batch_size, %name, "Create poller");
        let t = thread::Builder::new()
            .name(name)
            .spawn(move || {
                poller.poll();
            })
            .unwrap();
        self.workers.lock().unwrap().push(t);
        info!("Poller is started");
    }

    pub fn spawn(&mut self, name_prefix: String) {
        let _span = info_span!("spawn", %name_prefix, pool_size = self.pool_size);
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
        let _poll_span = info_span!("poll").entered();
        let mut batch: Batch<N> = Batch::with_capacity(self.max_batch_size);
        let mut reschedule_fsms: Vec<usize> = Vec::with_capacity(self.max_batch_size);
        // 控制是否要在handler进行end之前release对应的fsm
        // 在调用end之前进行了schedual操作
        let mut to_skip_end = Vec::with_capacity(self.max_batch_size);
        let mut run = true;

        while run && self.fetch_fsm(&mut batch) {
            let max_batch_size = std::cmp::max(self.max_batch_size, batch.normals.len());
            trace!(batch_size = batch.normals.len(), "Has fetched a new fsm");
            self.handler.begin(max_batch_size);

            let mut hot_fsm_count = 0;
            trace!("Start to enumerate fsm and do handle logic");
            for (i, p) in batch.normals.iter_mut().enumerate() {
                let p = p.as_mut().unwrap();
                let res = self.handler.handle(p);

                trace!(handle_result = ?res, "Fsm handle logic execute complete");
                if p.as_ref().is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    warn!(policy = ?p.policy, "Fsm is stopped!Set fsm policy");
                    reschedule_fsms.push(i);
                } else {
                    if p.timer.elapsed() >= self.reschedule_duration {
                        hot_fsm_count += 1;
                        if hot_fsm_count % 2 == 0 {
                            info!(policy = ?p.policy, "Fsm execute too long");
                            p.policy = Some(ReschedulePolicy::Schedule);
                            // 记录需要重新调度第fsm第下标
                            reschedule_fsms.push(i);
                        }
                    }
                    if let HandleResult::StopAt { progress, skip_end } = res {
                        p.policy = Some(ReschedulePolicy::Release(progress));
                        reschedule_fsms.push(i);
                        if skip_end {
                            to_skip_end.push(i);
                        }
                    }
                }
            }

            let mut fsm_cnt = batch.normals.len();
            trace!("Start to fill new received fsm into batch");
            while batch.normals.len() < max_batch_size {
                if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    run = batch.push(fsm);
                }
                if !run || fsm_cnt == batch.normals.len() {
                    info!(%run, batch_size = batch.normals.len(), "break fill operation");
                    break;
                }
                let p = batch.normals[fsm_cnt].as_mut().unwrap();
                info!("Get new fsm and start to handle it");
                let res = self.handler.handle(p);
                if p.as_ref().is_stopped() {
                    p.policy = Some(ReschedulePolicy::Remove);
                    warn!(policy = ?p.policy, "Fsm is stopped!Set fsm policy");
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
            trace!(to_skip_end = ?to_skip_end, "Start to do skip end operation");
            for offset in &to_skip_end {
                // 这里的操作会将batch中对应的fsm设置为None
                batch.schedule(&self.router, *offset, true);
            }
            to_skip_end.clear();
            self.handler.end(&mut batch.normals);
            trace!(reschedule_fsms = ?reschedule_fsms, "Start to do reschedule fsms operation");
            while let Some(r) = reschedule_fsms.pop() {
                batch.schedule(&self.router, r, false);
            }

            let left_fsm_cnt = batch.normals.len();
            trace!(
                batch_size = batch.normals.len(),
                "One loop finally, start to do left fsm operation"
            );
            if left_fsm_cnt > 0 {
                for i in 0..left_fsm_cnt {
                    let to_schedule = match batch.normals[i].take() {
                        Some(f) => f,
                        None => continue,
                    };
                    trace!(idx = i, "normal_scheduler start to reschedule this fsm");
                    self.router.normal_scheduler.schedule(to_schedule.fsm);
                }
            }
            batch.clear();
        }
        info!("Batch system poll exit");
    }
}

impl<N: Fsm, H, S> Drop for Poller<N, H, S> {
    fn drop(&mut self) {}
}

#[derive(Debug, PartialEq)]
pub enum HandleResult {
    KeepProcessing,
    StopAt { progress: usize, skip_end: bool },
}

pub trait PollHandler<N>: Send + 'static
where
    N: Fsm,
{
    fn begin(&mut self, batch_size: usize);
    fn handle(&mut self, normal: &mut impl AsMut<N>) -> HandleResult;
    fn light_end(&mut self, batch: &mut [Option<impl AsMut<N>>]);
    fn end(&mut self, batch: &mut [Option<impl AsMut<N>>]);
    fn pause(&mut self);
}

struct TagPollHandler<N: FsmExecutor> {
    msg_buf: Vec<N::Msg>,
    counter: usize,
    last_time: Option<Instant>,
    msg_cnt: usize,
}

impl<N> PollHandler<N> for TagPollHandler<N>
where
    N: 'static + Fsm + FsmExecutor,
    N::Msg: Send,
{
    fn begin(&mut self, _batch_size: usize) {
        if self.last_time.is_none() {
            self.last_time = Some(Instant::now());
            return;
        }
    }

    fn handle(&mut self, normal: &mut impl AsMut<N>) -> HandleResult {
        let _poll_handler_span = trace_span!("handle");
        let normal = normal.as_mut();
        // batch got msg, batch consume
        let keep_process = normal.try_fill_batch(&mut self.msg_buf, &mut self.counter);
        normal.handle_tasks(&mut self.msg_buf, &mut self.msg_cnt);
        if normal.is_tick() {
            trace!(
                counter = self.counter,
                msg_cnt = self.msg_cnt,
                "We have got a Tick Event"
            );
            normal.commit(&mut self.msg_buf);
            self.msg_cnt = 0;
            self.msg_buf.clear();
            normal.untag_tick();
        }
        if keep_process {
            trace!("Fsm has unconsumed msgs so return KeepProcessing!");
            HandleResult::KeepProcessing
        } else {
            HandleResult::StopAt {
                progress: normal.remain_msgs(),
                skip_end: false,
            }
        }
    }

    fn light_end(&mut self, _batch: &mut [Option<impl AsMut<N>>]) {}

    fn end(&mut self, _batch: &mut [Option<impl AsMut<N>>]) {}

    // We just sleep to wait for more data to be processed.
    fn pause(&mut self) {}
}
#[cfg(test)]
mod test_batch_system {
    #[test]
    fn test() {}
}

#[cfg(test)]
mod test_poll_handler {
    use crate::tag::fsm::FsmExecutor;

    use super::*;

    #[allow(dead_code)]
    #[derive(Debug, Clone)]
    struct MockMsg {
        val: usize,
    }

    impl MockMsg {
        fn new(i: usize) -> MockMsg {
            Self { val: i }
        }
    }

    struct MockFsm {
        tick: bool,
        is_commit: bool,
    }

    impl Fsm for MockFsm {
        type Message = MockMsg;

        fn is_stopped(&self) -> bool {
            todo!()
        }

        fn set_mailbox(&mut self, _mailbox: Cow<'_, crate::com::mail::BasicMailbox<Self>>)
        where
            Self: Sized,
        {
        }

        fn take_mailbox(&mut self) -> Option<crate::com::mail::BasicMailbox<Self>>
        where
            Self: Sized,
        {
            return None;
        }

        fn tag_tick(&mut self) {
            self.tick = true;
        }

        fn untag_tick(&mut self) {
            self.tick = false;
        }

        fn is_tick(&self) -> bool {
            self.tick
        }
    }

    impl FsmExecutor for MockFsm {
        type Msg = MockMsg;

        fn try_fill_batch(&mut self, msg_buf: &mut Vec<Self::Msg>, counter: &mut usize) -> bool {
            for i in 0..100usize {
                *counter += 1;
                msg_buf.push(MockMsg::new(i));
            }
            true
        }

        fn handle_tasks(&mut self, msg_buf: &mut Vec<Self::Msg>, msg_cnt: &mut usize) {
            *msg_cnt += msg_buf.len();
        }

        fn commit(&mut self, _: &mut Vec<Self::Msg>) {
            self.is_commit = true;
        }

        fn remain_msgs(&self) -> usize {
            0
        }
    }

    fn init() -> (TagPollHandler<MockFsm>, NormalFsm<MockFsm>) {
        let poll_handler = TagPollHandler::<MockFsm> {
            msg_buf: Vec::new(),
            counter: 0,
            last_time: None,
            msg_cnt: 0,
        };
        let mock_fsm = MockFsm {
            tick: false,
            is_commit: false,
        };
        let normal_fsm = NormalFsm::new(Box::new(mock_fsm));
        (poll_handler, normal_fsm)
    }
    #[test]
    fn test_handle_keep_processing() {
        let (mut poll_handler, mut normal_fsm) = init();
        let handle_res = poll_handler.handle(&mut normal_fsm);
        assert!(handle_res == HandleResult::KeepProcessing);
    }

    #[test]
    fn test_tick() {
        let (mut poll_handler, mut normal_fsm) = init();
        poll_handler.msg_buf.push(MockMsg { val: 0 });
        normal_fsm.as_mut().tag_tick();
        let _ = poll_handler.handle(&mut normal_fsm);
        assert!(poll_handler.msg_buf.len() == 0);
        assert!(!normal_fsm.as_ref().is_tick());
        assert!(poll_handler.msg_cnt == 0);
        assert!(normal_fsm.as_mut().is_commit);
    }
}