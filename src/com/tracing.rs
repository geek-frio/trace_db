use anyhow::Error as AnyError;
use async_trait::async_trait;
use chrono::Local;
use pin_project::pin_project;
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::task::Poll;
use tokio::fs::File;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;

use tracing::{error, info};
use tracing_subscriber::fmt::MakeWriter;

use crate::TOKIO_RUN;

pub struct RollingFileMaker {
    writer: RollingFileWriter,
}

struct MsgSender {
    file: Option<File>,
}

pub struct RollingFileWriter {
    sender: Sender<MsgEvent>,
}

impl RollingFileMaker {
    pub async fn init(
        name_prefix: String,
        log_path: PathBuf,
        rolling_size: usize,
        shut_notify: Sender<ReverseFileEvent>,
    ) -> Result<(RollingFileMaker, Sender<MsgEvent>), AnyError> {
        let (sender, mut receiver) = channel(5000);
        let writer = RollingFileWriter {
            sender: sender.clone(),
        };

        let mut tracing_log_consumer =
            TracingLogConsumer::new(name_prefix, log_path, rolling_size, shut_notify).await?;
        TOKIO_RUN.spawn(async move {
            loop {
                let msg_event = receiver.recv().await;
                match msg_event {
                    None => {
                        info!("Log receiver channel has been closed!")
                    }
                    Some(event) => match event {
                        MsgEvent::Flush => {
                            let _ = tracing_log_consumer.flush().await;
                            // Check if need rolling operation
                            if tracing_log_consumer.need_rolling() {
                                let r= tracing_log_consumer.recreate().await;
                                if let Err(e) = r {
                                    error!("Rolling executed failed!{:?}", e);
                                }
                            }
                        }
                        MsgEvent::Msg(m) => {
                            if let Err(e) = tracing_log_consumer.write(m.as_slice()).await {
                                if e.kind() == ErrorKind::Other {
                                    let _ = tracing_log_consumer.recreate().await;
                                    let r = tracing_log_consumer.write(m.as_slice()).await;
                                    if let Err(e) = r {
                                        error!(log_data = ?m.as_slice(), "Consume tracing log msgs failed!Recreate Writer can't save it {:?}", e);
                                    }
                                } else {
                                    error!(log_data = ?m.as_slice(), "Consume tracing log msgs failed!{:?}", e);
                                }
                            }
                        }
                        MsgEvent::Shutdown => {
                            while let Ok(r) = receiver.try_recv() {
                                if let MsgEvent::Msg(buf) = r {
                                    let _ = tracing_log_consumer.write(buf.as_slice()).await;
                                }
                            }
                            let _ = tracing_log_consumer.flush().await;
                            let _ = tracing_log_consumer.shut_notify.send(ReverseFileEvent::Shutdown).await;
                        }
                    },
                }
            }
        });
        Ok((RollingFileMaker { writer }, sender))
    }
}

#[derive(Debug)]
pub enum MsgEvent {
    Msg(Vec<u8>),
    Flush,
    Shutdown,
}

#[derive(Debug)]
pub enum ReverseFileEvent {
    LogFileCreate(PathBuf),
    Shutdown,
}

impl<'a> Write for &'a RollingFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.sender.try_send(MsgEvent::Msg(buf.to_owned()));
        match res {
            Ok(_) => return Ok(buf.len()),
            Err(e) => {
                return Err(std::io::Error::new(ErrorKind::Other, e));
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let res = self.sender.try_send(MsgEvent::Flush);
        match res {
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(std::io::Error::new(ErrorKind::Other, e));
            }
        }
    }
}

impl<'a> MakeWriter<'a> for RollingFileMaker {
    type Writer = &'a RollingFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        &self.writer
    }
}

#[async_trait]
trait RollingAbility {
    fn set_rolling_size(&mut self, rolling_size: usize);
    async fn recreate(&mut self) -> Result<(), AnyError>;
    fn need_rolling(&self) -> bool;
}

#[pin_project]
struct TracingLogConsumer {
    #[pin]
    writer: Option<BufWriter<File>>,
    log_dir: PathBuf,
    written_bytes: usize,
    flushed_bytes: usize,
    rolling_size: usize,
    file_num: usize,
    name_prefix: String,
    shut_notify: Sender<ReverseFileEvent>,
}

#[async_trait]
impl RollingAbility for TracingLogConsumer {
    fn set_rolling_size(&mut self, rolling_size: usize) {
        self.rolling_size = rolling_size;
    }

    async fn recreate(&mut self) -> Result<(), AnyError> {
        if let Some(ref mut w) = self.writer {
            self.written_bytes = 0;
            self.flushed_bytes = 0;
            w.flush().await?;
        }
        self.file_num += 1;
        let mut log_dir = self.log_dir.clone();
        self.writer = Self::create_writer(
            self.name_prefix.as_str(),
            &mut log_dir,
            self.file_num,
            &self.shut_notify,
        )
        .await?;
        Ok(())
    }

    fn need_rolling(&self) -> bool {
        self.flushed_bytes >= self.rolling_size
    }
}

impl AsyncWrite for TracingLogConsumer {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = self.project();
        let s = this.writer.as_pin_mut();
        match s {
            Some(w) => {
                *this.written_bytes = *this.written_bytes + buf.len();
                w.poll_write(cx, buf)
            }
            None => Poll::Ready(Err(std::io::Error::new(
                ErrorKind::Other,
                "No writer is init",
            ))),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        let this = self.project();
        let s = this.writer.as_pin_mut();
        match s {
            Some(w) => {
                let res = w.poll_flush(cx);
                match res {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(_) => {
                        *this.flushed_bytes = *this.written_bytes;
                        return Poll::Ready(Ok(()));
                    }
                }
            }
            None => Poll::Ready(Err(std::io::Error::new(
                ErrorKind::Other,
                "No writer is init",
            ))),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        // do nothing
        Self::poll_flush(self, cx)
    }
}

impl TracingLogConsumer {
    async fn new(
        name_prefix: String,
        log_dir: PathBuf,
        rolling_size: usize,
        shut_notify: Sender<ReverseFileEvent>,
    ) -> Result<TracingLogConsumer, AnyError> {
        let mut cur_log_dir = log_dir.clone();
        let writer =
            Self::create_writer(name_prefix.as_str(), &mut cur_log_dir, 1, &shut_notify).await?;
        Ok(TracingLogConsumer {
            writer,
            log_dir,
            written_bytes: 0,
            flushed_bytes: 0,
            rolling_size: rolling_size,
            file_num: 1,
            name_prefix,
            shut_notify,
        })
    }

    async fn create_writer(
        name_prefix: &str,
        path_buf: &mut PathBuf,
        file_num: usize,
        event_sender: &Sender<ReverseFileEvent>,
    ) -> Result<Option<BufWriter<File>>, AnyError> {
        let file_name = Self::gen_new_file_name(name_prefix, file_num);
        path_buf.push(file_name);
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path_buf)
            .await?;
        let _ = event_sender.try_send(ReverseFileEvent::LogFileCreate(path_buf.clone()));
        Ok(Some(BufWriter::new(file)))
    }

    fn gen_new_file_name(s: &str, file_num: usize) -> String {
        let local = Local::now();
        let mut final_name = String::new();
        final_name.push_str(s);
        final_name.push_str("-");
        let date_str = format!("{}", local.format("%Y%m%d-"));
        final_name.push_str(date_str.as_str());
        final_name.push_str(file_num.to_string().as_str());
        final_name.push_str("-");
        let time_str = format!("{}", local.format("%H%M%S-%f"));
        final_name.push_str(time_str.as_str());
        final_name.push_str(".log");
        return final_name;
    }
}

#[cfg(test)]
mod tracing_log {
    use super::*;
    use regex::Regex;
    use std::time::Duration;
    use tokio::{sync::mpsc::Receiver, time::sleep};

    const TEST_DIR: &'static str = "/tmp";
    const TEST_PREIFX: &'static str = "test_app";

    async fn create_test_rolling_file_maker(
        log_dir: &str,
        name_prefix: &str,
        rolling_size: usize,
    ) -> (
        RollingFileMaker,
        Receiver<ReverseFileEvent>,
        Sender<MsgEvent>,
    ) {
        let mut log_dir_buf = PathBuf::new();
        let (shut_notify, shut_recv) = channel(500);
        log_dir_buf.push(log_dir);
        let (maker, shutdown_sender) = RollingFileMaker::init(
            name_prefix.to_string(),
            log_dir_buf,
            rolling_size,
            shut_notify,
        )
        .await
        .expect("RollingFileMaker init failed!");
        (maker, shut_recv, shutdown_sender)
    }

    async fn collect_and_drop_files(
        shutdown_sender: Sender<MsgEvent>,
        log_event_receiver: &mut Receiver<ReverseFileEvent>,
        created_files: &mut Vec<PathBuf>,
    ) {
        let _ = shutdown_sender.send(MsgEvent::Shutdown).await;
        while let ReverseFileEvent::LogFileCreate(p) = log_event_receiver.recv().await.unwrap() {
            created_files.push(p);
        }
        for file in created_files {
            let r = tokio::fs::remove_file(file.as_path()).await;
            if let Err(e) = r {
                println!("Drop file error, e:{:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_rolling_init_one_line_test() -> Result<(), AnyError> {
        let buf = "abc";
        let (maker, mut shut_recv, shutdown_sender) =
            create_test_rolling_file_maker(TEST_DIR, TEST_PREIFX, 100).await;
        let mut w = maker.make_writer();
        w.write(buf.as_bytes()).unwrap();
        w.flush().unwrap();
        sleep(Duration::from_secs(1)).await;

        let mut created_files = vec![];
        let event = shut_recv.recv().await.unwrap();
        if let ReverseFileEvent::LogFileCreate(p) = event {
            let body = tokio::fs::read_to_string(p.as_path()).await.unwrap();
            assert!(body == buf);
            created_files.push(p);
        }
        collect_and_drop_files(shutdown_sender, &mut shut_recv, &mut created_files).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_lines() -> Result<(), AnyError> {
        let (maker, mut log_create_event_recv, shutdown_sender) =
            create_test_rolling_file_maker(TEST_DIR, TEST_PREIFX, 100).await;
        let mut w = maker.make_writer();
        let matches_num = 10;
        let mut s = String::new();
        let test_str = "1234567890";
        for _ in 0..10 {
            let _ = w.write(test_str.as_bytes());
            s.push_str(test_str);
        }
        let _ = w.flush();
        sleep(Duration::from_millis(500)).await;

        let mut created_files = vec![];
        let event = log_create_event_recv.recv().await.unwrap();
        if let ReverseFileEvent::LogFileCreate(p) = event {
            let body = tokio::fs::read_to_string(p.as_path()).await.unwrap();
            created_files.push(p);
            let re = Regex::new(r"1234567890").unwrap();
            let matches = re.find_iter(body.as_str());
            assert!(matches.count() == matches_num);
        }

        collect_and_drop_files(
            shutdown_sender,
            &mut log_create_event_recv,
            &mut created_files,
        )
        .await;
        Ok(())
    }

    #[tokio::test]
    async fn test_rolling_ability() -> Result<(), AnyError> {
        let (maker, mut log_create_recv, shutdown_notify) =
            create_test_rolling_file_maker(TEST_DIR, TEST_PREIFX, 100).await;
        let mut w = maker.make_writer();
        let matches_num = 100;
        let test_str = "1234567890";
        for _ in 0..matches_num {
            let _ = w.write(test_str.as_bytes());
            let _ = w.flush();
        }
        sleep(Duration::from_millis(500)).await;
        let _ = shutdown_notify.send(MsgEvent::Shutdown).await;
        let mut created_files = vec![];
        while let ReverseFileEvent::LogFileCreate(p) = log_create_recv.recv().await.unwrap() {
            created_files.push(p);
        }
        assert!(created_files.len() == 11);
        for file in created_files {
            let r = tokio::fs::remove_file(file.as_path()).await;
            if let Err(e) = r {
                println!("Drop file error, e:{:?}", e);
            }
        }
        Ok(())
    }
}
