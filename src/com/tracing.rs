use anyhow::Error as AnyError;
use chrono::Local;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::{
    fs::File,
    io::{ErrorKind, Write},
};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};

use tracing::info;
use tracing_subscriber::fmt::MakeWriter;

use crate::TOKIO_RUN;

struct RollingFileMaker {
    writer: RollingFileWriter,
}

struct MsgSender {
    file: Option<File>,
}

struct RollingFileWriter {
    sender: Sender<MsgEvent>,
}

impl RollingFileMaker {
    fn init(name_prefix: String, log_path: &Path) -> RollingFileMaker {
        // let (sender, receiver) = crossbeam_channel::bounded(5000);
        let (sender, mut receiver) = channel(5000);
        let writer = RollingFileWriter { sender };

        TOKIO_RUN.spawn(async move {
            let tracing_log_consumer = TracingLogConsumer::new(name_prefix);
            loop {
                let msg_event = receiver.recv().await;
                match msg_event {
                    None => {
                        info!("Log receiver channel has been closed!")
                    }
                    Some(event) => match event {
                        MsgEvent::Flush => {}
                        MsgEvent::Msg(m) => {}
                    },
                }
            }
        });
        RollingFileMaker { writer }
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

trait RollingAbility {
    fn set_rolling_size(&mut self, rolling_size: usize);
    fn recreate(&mut self);
}

struct TracingLogConsumer {
    writer: Option<BufWriter<File>>,
    written_bytes: usize,
    flushed_bytes: usize,
    rolling_size: usize,
    file_num: usize,
    name_prefix: String,
}

impl RollingAbility for TracingLogConsumer {
    fn set_rolling_size(&mut self, rolling_size: usize) {
        self.rolling_size = rolling_size;
    }

    fn recreate(&mut self) {}
}

impl TracingLogConsumer {
    fn new(name_prefix: String, path: &Path) -> Result<TracingLogConsumer, AnyError> {
        let file_name = Self::gen_new_file_name(name_prefix.as_str(), 1);
        let mut path_buf = PathBuf::new();
        path_buf.push(path);
        path_buf.push(file_name);

        let writer = Some(BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path_buf)?,
        ));

        Ok(TracingLogConsumer {
            writer,
            written_bytes: 0,
            flushed_bytes: 0,
            rolling_size: 500 * 1024,
            file_num: 1,
            name_prefix,
        })
    }
    fn gen_new_file_name(s: &str, file_num: usize) -> String {
        let local = Local::now();
        let final_name = String::new();
        final_name.push_str(s);
        final_name.push_str("-");
        let time_str = format!("{}", local.format("%Y%m%d-%H%M%S"));
        final_name.push_str(time_str.as_str());
        return final_name;
    }
}
