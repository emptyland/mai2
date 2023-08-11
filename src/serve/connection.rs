use std::io;
use std::io::Cursor;
use std::sync::Arc;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream;
use exec::connection::Connection as DBConn;
use crate::{Arena, ArenaStr, exec};
use crate::mysql::Marshal;
use crate::mysql::protocol::{DEFAULT_CHARSET_CODE, DEFAULT_PROTOCOL_VERSION, FixedLengthString, HandshakeResponse41, HandshakeV10, LengthEncodedString};

pub struct Connection {
    socket: BufStream<TcpStream>,
    buf: BytesMut,
    pub logger: Arc<slog::Logger>,
    conn: Arc<DBConn>,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {

    pub fn new(socket: TcpStream, logger: Arc<slog::Logger>, conn: Arc<DBConn>) -> Self {
        Self {
            socket: BufStream::new(socket),
            buf: BytesMut::with_capacity(4096),
            logger,
            conn
        }
    }

    pub async fn handle_packet(&mut self) -> io::Result<()> {
        //self.socket.read_f64()
        todo!()
    }

    async fn handshake(&mut self) -> io::Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let packet = HandshakeV10 {
            protocol_version: DEFAULT_PROTOCOL_VERSION.into(),
            server_version: "mai-sql v0.0.1".into(),
            thread_id: 0.into(),
            auth_plugin_data_part_1: FixedLengthString::zero(),
            filter: 0.into(),
            capability_flags_1: 0.into(),
            character_set: DEFAULT_CHARSET_CODE.into(),
            status_flags: 0.into(),
            capability_flags_2: 0.into(),
            auth_plugin_data_len: 0.into(),
            reserved: FixedLengthString::zero(),
            auth_plugin_data_part_2: LengthEncodedString(ArenaStr::default()),
            auth_plugin_name: ArenaStr::default().into(),
        };
        let mut buf = vec![];
        packet.marshal(&mut buf).unwrap();
        self.socket.write_all(&buf).await?;

        self.socket.read_buf(&mut self.buf).await?;
        let mut buf = Cursor::new(&self.buf);
        let resp = HandshakeResponse41::unmarshal(&mut buf, &arena);
        todo!()
    }
}