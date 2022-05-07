use std::{
    fs::File,
    io::{ErrorKind, Write},
};

use tracing_subscriber::fmt::MakeWriter;

use crate::TOKIO_RUN;

struct RollingFileMaker {
    writer: RollingFileWriter,
}

struct MsgSender {
    file: Option<File>,
}

struct RollingFileWriter {
    sender: crossbeam_channel::Sender<MsgEvent>,
}

impl RollingFileMaker {
    fn init() -> RollingFileMaker {
        let (sender, receiver) = crossbeam_channel::bounded(5000);
        let writer = RollingFileWriter { sender };

        TOKIO_RUN.spawn(async move {});
        RollingFileMaker { writer }
    }
}

enum MsgEvent {
    Msg(Vec<u8>),
    Flush,
}

impl<'a> Write for &'a RollingFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let res = self.sender.send(MsgEvent::Msg(buf.to_owned()));
        match res {
            Ok(_) => return Ok(buf.len()),
            Err(e) => {
                return Err(std::io::Error::new(ErrorKind::Other, e));
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let res = self.sender.send(MsgEvent::Flush);
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
        todo!()
    }
}
