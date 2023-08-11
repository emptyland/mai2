#![allow(unused_variables)]
#![allow(dead_code)]

mod connection;

use std::io;
use std::sync::Arc;
use slog::{error, info};
use tokio::net::TcpListener;
use crate::exec::db::DB;
use crate::serve::connection::Connection;

#[tokio::test]
pub async fn echo() -> io::Result<()> {
    let db = DB::open("tests".into(), "dbs001".into()).unwrap();
    let log = db.logger.clone();
    let listener = TcpListener::bind("0.0.0.0:8888").await?;
    info!(log, "listen...");
    loop {
        let (socket, addr) = listener.accept().await?;
        info!(log, "accept...{addr} ok");

        let mut conn = Connection::new(socket, log.clone(), db.connect());
        tokio::spawn(async move {
            if let Err(e) = conn.handle_packet().await {
                error!(conn.logger, "network error: {e}");
            }
        });
    }
}

#[tokio::main]
pub async fn start(db: Arc<DB>, opts: ServerOptions) -> io::Result<()> {
    let log = db.logger.clone();
    let listener = TcpListener::bind(&opts.bind_addr).await?;
    info!(log, "server start listen...{}", opts.bind_addr);


    Ok(())
}

pub struct ServerOptions {
    pub bind_addr: String,
}

#[cfg(test)]
mod tests {
    use std::io;
    use super::*;

    #[test]
    fn example() -> io::Result<()> {
        echo()?;
        Ok(())
    }
}