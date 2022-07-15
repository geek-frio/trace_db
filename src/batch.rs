use crate::fsm::Fsm;
use crate::router::Router;
use crate::sched::FsmScheduler;
use crate::tag::fsm::{FsmExecutor, TagFsm};
use crossbeam_channel::Receiver;
use std::thread::JoinHandle;
use std::time::Duration;
use std::{
    sync::{Arc, Mutex},
    thread,
    thread::ThreadId,
    time::Instant,
};
use tracing::{info, info_span, trace, trace_span};

pub struct NormalFsm<N> {
    fsm: Box<N>,
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
        NormalFsm { fsm }
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
    }

    pub fn spawn(&mut self, name_prefix: String) {
        let _span = info_span!("spawn", %name_prefix, pool_size = self.pool_size);
        self.name_prefix = name_prefix.clone();
        for i in 0..self.pool_size {
            info!("Starting Index: {} poller", i);
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

    pub fn poll(&mut self) {
        // let mut batch: Batch<N> = Batch::with_capacity(self.max_batch_size);
        let batch = &mut Vec::with_capacity(self.max_batch_size);
        // Use flag to avoid waiting for long time to batch is ok
        let mut flag = true;

        'a: loop {
            info!("loop");

            for _ in 0..self.max_batch_size {
                let fsm = if let Ok(fsm) = self.fsm_receiver.try_recv() {
                    fsm
                } else if flag {
                    flag = false;

                    let res_fsm = self.fsm_receiver.recv();
                    match res_fsm {
                        Ok(fsm) => fsm,
                        Err(_e) => break 'a,
                    }
                } else {
                    flag = true;
                    break;
                };

                match fsm {
                    FsmTypes::Normal(fsm) => {
                        let mut normal_fsm = NormalFsm::new(fsm);
                        let res = self.handler.handle(&mut normal_fsm);

                        if normal_fsm.as_ref().is_stopped() && normal_fsm.as_ref().chan_msgs() > 0 {
                            self.router.normal_scheduler.schedule(normal_fsm.fsm);
                        } else if let HandleResult::StopAt {
                            remain_size: progress,
                        } = res
                        {
                            info!(
                                "remain_size:{}, progres:{}",
                                progress,
                                normal_fsm.as_ref().chan_msgs()
                            );
                            if normal_fsm.as_ref().chan_msgs() > progress {
                                self.router.normal_scheduler.schedule(normal_fsm.fsm);
                            } else {
                                batch.push(normal_fsm);
                            }
                        } else {
                            batch.push(normal_fsm);
                        }
                    }
                    FsmTypes::Empty => {
                        if self.fsm_receiver.len() > 0 {
                            self.router.normal_scheduler.shutdown();
                        } else {
                            break 'a;
                        }
                    }
                }
            }

            let mut fsm_vec = batch
                .into_iter()
                .map(|normal_fsm| &mut normal_fsm.fsm)
                .collect();
            self.handler.end(&mut fsm_vec);

            batch.clear();
        }
    }
}

impl<N: Fsm, H, S> Drop for Poller<N, H, S> {
    fn drop(&mut self) {}
}

#[derive(Debug, PartialEq)]
pub enum HandleResult {
    KeepProcessing,
    StopAt { remain_size: usize },
}

pub trait PollHandler<N>: Send + 'static
where
    N: Fsm,
{
    fn begin(&mut self, batch_size: usize);
    fn handle(&mut self, normal: &mut impl AsMut<N>) -> HandleResult;
    fn end(&mut self, batch: &mut Vec<impl AsMut<N>>);
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
                remain_size: normal.chan_msgs(),
            }
        }
    }

    fn end(&mut self, _batch: &mut Vec<impl AsMut<N>>) {}

    // We just sleep to wait for more data to be processed.
    fn pause(&mut self) {}
}
#[cfg(test)]
mod test_batch_system {
    #[test]
    fn test() {}
}

#[cfg(test)]
mod tests {
    use std::{
        borrow::Cow,
        panic, process,
        sync::{atomic::AtomicUsize, Once},
    };

    use crate::{log::init_console_logger, sched::NormalScheduler, tag::fsm::FsmExecutor};

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

    #[derive(Clone)]
    struct MockFsm {
        tick: bool,
        is_commit: bool,
        called_num: Arc<std::sync::atomic::AtomicI32>,
    }

    impl Fsm for MockFsm {
        type Message = MockMsg;

        fn is_stopped(&self) -> bool {
            false
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

        fn chan_msgs(&self) -> usize {
            10
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
            info!("Plus 1");
            self.called_num
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.is_commit = true;
        }

        fn remain_msgs(&self) -> usize {
            0
        }
    }

    fn init(
        counter: Arc<std::sync::atomic::AtomicI32>,
    ) -> (TagPollHandler<MockFsm>, NormalFsm<MockFsm>) {
        let poll_handler = TagPollHandler::<MockFsm> {
            msg_buf: Vec::new(),
            counter: 0,
            last_time: None,
            msg_cnt: 0,
        };
        let mock_fsm = MockFsm {
            tick: false,
            is_commit: false,
            called_num: counter,
        };
        let normal_fsm = NormalFsm::new(Box::new(mock_fsm));
        (poll_handler, normal_fsm)
    }

    #[test]
    fn test_handle_keep_processing() {
        let (mut poll_handler, mut normal_fsm) =
            init(Arc::new(std::sync::atomic::AtomicI32::new(0)));
        let handle_res = poll_handler.handle(&mut normal_fsm);
        assert!(handle_res == HandleResult::KeepProcessing);
    }

    #[test]
    fn test_tick() {
        let (mut poll_handler, mut normal_fsm) =
            init(Arc::new(std::sync::atomic::AtomicI32::new(0)));
        poll_handler.msg_buf.push(MockMsg { val: 0 });
        normal_fsm.as_mut().tag_tick();
        let _ = poll_handler.handle(&mut normal_fsm);
        assert!(poll_handler.msg_buf.len() == 0);
        assert!(!normal_fsm.as_ref().is_tick());
        assert!(poll_handler.msg_cnt == 0);
        assert!(normal_fsm.as_mut().is_commit);
    }

    macro_rules! poll_handler_create {
        ($end_fsm_size:expr, $remain_size:expr) => {
            fn init_poller(
                recevier: &Receiver<FsmTypes<MockFsm>>,
                sender: crossbeam_channel::Sender<FsmTypes<MockFsm>>,
                batch_size: usize,
            ) -> Poller<MockFsm, MockPollHandler, NormalScheduler<MockFsm>> {
                let mockpoll_handler = MockPollHandler { stop_times: 1 };

                let normal_sched = NormalScheduler { sender };
                let static_cnt = Arc::new(AtomicUsize::new(0));
                let router = Router::new(normal_sched, static_cnt, Default::default());

                Poller::new(
                    recevier,
                    mockpoll_handler,
                    batch_size,
                    router,
                    Arc::new(Mutex::new(vec![])),
                )
            }

            static CALL_ONCE: Once = Once::new();
            struct MockPollHandler {
                stop_times: usize,
            }

            impl PollHandler<MockFsm> for MockPollHandler {
                fn begin(&mut self, _batch_size: usize) {}

                fn handle(&mut self, normal: &mut impl AsMut<MockFsm>) -> HandleResult {
                    info!("Plus 1");
                    normal
                        .as_mut()
                        .called_num
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    let l = normal.as_mut().chan_msgs();
                    if $remain_size == 0 || self.stop_times == 0 {
                        HandleResult::StopAt { remain_size: l }
                    } else {
                        self.stop_times = 0;
                        HandleResult::StopAt {
                            remain_size: $remain_size,
                        }
                    }
                }

                fn end(&mut self, batch: &mut Vec<impl AsMut<MockFsm>>) {
                    CALL_ONCE.call_once(|| {
                        assert_eq!($end_fsm_size, batch.len());
                    });
                }

                fn pause(&mut self) {}
            }
        };
    }

    fn setup() {
        init_console_logger();

        let orig_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            orig_hook(panic_info);
            process::exit(1);
        }));
    }

    #[test]
    fn test_poller_basics() {
        setup();

        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        let (fsm_sender, fsm_receiver) = crossbeam_channel::unbounded();

        let sender = fsm_sender.clone();
        let (event_sender, event_receiver) = std::sync::mpsc::channel();

        poll_handler_create!(5, 0);

        let join = std::thread::spawn(move || {
            let mut poller = init_poller(&fsm_receiver, sender, 5);
            let _ = event_receiver.recv();
            poller.poll();
        });

        let counter = Arc::new(std::sync::atomic::AtomicI32::new(0));

        let m = MockFsm {
            tick: true,
            is_commit: true,
            called_num: counter,
        };

        for _ in 0..5 {
            let f = FsmTypes::Normal(Box::new(m.clone()));
            let _ = fsm_sender.send(f);
        }

        let _ = fsm_sender.send(FsmTypes::Empty);
        let _ = event_sender.send(());
        let _ = join.join();
    }

    #[test]
    fn test_remain_msgs_logic() {
        setup();

        let counter = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let (fsm_sender, fsm_receiver) = crossbeam_channel::unbounded();

        let sender = fsm_sender.clone();
        let (event_sender, event_receiver) = std::sync::mpsc::channel();

        poll_handler_create!(5, 1);

        let join = std::thread::spawn(move || {
            let mut poller = init_poller(&fsm_receiver, sender, 5);
            let _ = event_receiver.recv();
            poller.poll();
        });

        let b = Box::new(MockFsm {
            tick: true,
            is_commit: true,
            called_num: counter.clone(),
        });

        let f = FsmTypes::Normal(b);
        let _ = fsm_sender.send(f);
        let _ = fsm_sender.send(FsmTypes::Empty);
        let _ = event_sender.send(());
        let _ = join.join();

        assert_eq!(2, counter.load(std::sync::atomic::Ordering::Relaxed));
    }
}
