use std::cell::RefCell;
use std::io::Write;
use std::sync::Mutex;

pub trait Logger: Sync + Send {
    fn append(&self, level: LoggingLevel, file: &str, line: u32, message: &str);
}

#[derive(Clone, Debug, PartialEq)]
pub enum LoggingLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LoggingLevel {
    pub fn to_string(&self) -> String {
        match self {
            LoggingLevel::Debug => "DEBUG",
            LoggingLevel::Info => "INFO",
            LoggingLevel::Warn => "WARN",
            LoggingLevel::Error => "Error",
        }.to_string()
    }
}

#[macro_export]
macro_rules! log_append {
    ($logger:expr, $level:expr, $($arg:tt)+) => {
        $logger.append($level, file!(), line!(), format!($($arg)+).as_str())
    };
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($arg:tt)+) => {
        $crate::log_append!($logger, $crate::log::LoggingLevel::Info, $($arg)+)
    }
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($arg:tt)+) => {
        $crate::log_append!($logger, $crate::log::LoggingLevel::Warn, $($arg)+)
    }
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($arg:tt)+) => {
        $crate::log_append!($logger, $crate::log::LoggingLevel::Error, $($arg)+)
    }
}

#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($arg:tt)+) => {
        $crate::log_append!($logger, $crate::log::LoggingLevel::Debug, $($arg)+)
    }
}

pub struct BlackHoleLogger;

impl Logger for BlackHoleLogger {
    fn append(&self, _level: LoggingLevel, _file: &str, _line: u32, _message: &str) {}
}

pub struct WriterLogger {
    writer: Mutex<Box<RefCell<dyn Write>>>
}

impl WriterLogger {
    pub fn new<T>(owns: T) -> Self
        where T: Write + 'static {
        Self { writer: Mutex::new( Box::new(RefCell::new(owns))) }
    }
}

unsafe impl Sync for WriterLogger {}
unsafe impl Send for WriterLogger {}

impl Logger for WriterLogger {
    fn append(&self, level: LoggingLevel, file: &str, line: u32, message: &str) {
        let write = self.writer.lock().unwrap();
        write!(write.borrow_mut(), "[{}:{}] {} {}\n", file, line, level.to_string(), message).unwrap();
        write.borrow_mut().flush().unwrap();
    }
}


#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use super::*;

    #[test]
    fn black_hole_log() {
        let log = Arc::new(BlackHoleLogger{});
        log_info!(log, "{}", 1);
    }

    #[test]
    fn writer_log() {
        let log = WriterLogger::new(io::stderr());
        log_warn!(log, "{} = {}", 2, 3);
    }
}