use chrono::Local;
use slog::{Drain, o};

pub const DEFAULT_LOGGING_TIME_FMT: &str = "%F %T:%S%.6f";

pub fn new_default_logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator)
        .use_file_location()
        .use_custom_timestamp(|wr|{
            let now = Local::now();
            write!(wr, "{}", now.format(DEFAULT_LOGGING_TIME_FMT))
        })
        .build()
        .fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    slog::Logger::root(drain, o!())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use slog::{debug, info};

    use super::*;

    #[test]
    fn std_logging() {

        let log = Arc::new(new_default_logger());
        info!(log, "ok={}", 1);
        debug!(log, "err={}", "fail");
    }
}