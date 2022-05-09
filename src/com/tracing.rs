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
    ) -> Result<RollingFileMaker, AnyError> {
        // let (sender, receiver) = crossbeam_channel::bounded(5000);
        let (sender, mut receiver) = channel(5000);
        let writer = RollingFileWriter { sender };

        let mut tracing_log_consumer = TracingLogConsumer::new(name_prefix, log_path).await?;
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
                    },
                }
            }
        });
        Ok(RollingFileMaker { writer })
    }
}

#[derive(Debug)]
enum MsgEvent {
    Msg(Vec<u8>),
    Flush,
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
        self.writer =
            Self::create_writer(self.name_prefix.as_str(), &mut self.log_dir, self.file_num)
                .await?;
        Ok(())
    }

    fn need_rolling(&self) -> bool {
        self.flushed_bytes > self.rolling_size
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
        mut log_dir: PathBuf,
    ) -> Result<TracingLogConsumer, AnyError> {
        let writer = Self::create_writer(name_prefix.as_str(), &mut log_dir, 1).await?;
        Ok(TracingLogConsumer {
            writer,
            log_dir,
            written_bytes: 0,
            flushed_bytes: 0,
            rolling_size: 500 * 1024,
            file_num: 1,
            name_prefix,
        })
    }

    async fn create_writer(
        name_prefix: &str,
        path_buf: &mut PathBuf,
        file_num: usize,
    ) -> Result<Option<BufWriter<File>>, AnyError> {
        let file_name = Self::gen_new_file_name(name_prefix, file_num);
        path_buf.push(file_name);

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path_buf)
            .await?;
        Ok(Some(BufWriter::new(file)))
    }

    fn gen_new_file_name(s: &str, file_num: usize) -> String {
        let local = Local::now();
        let mut final_name = String::new();
        final_name.push_str(s);
        final_name.push_str("-");
        let time_str = format!("{}", local.format("%Y%m%d-%H%M%S"));
        final_name.push_str(time_str.as_str());
        final_name.push_str("-");
        final_name.push_str(file_num.to_string().as_str());
        return final_name;
    }
}
