use std::io;
use std::io::{BufRead, Read, Write};
use bytes::Buf;
use crate::{Arena, ArenaMut, ArenaStr, ArenaVec};
use crate::mysql::Marshal;

macro_rules! int {
    [1] => {
        FixedLengthInteger<u8, 1>
    };
    [2] => {
        FixedLengthInteger<u16, 2>
    };
    [3] => {
        FixedLengthInteger<u32, 3>
    };
    [4] => {
        FixedLengthInteger<u32, 4>
    };
    [lenenc] => {
        LengthEncodedInteger<u64>
    }
}

macro_rules! string {

    [nul] => {
        NullTerminatedString
    };
    [snul] => {
        NullTerminatedStaticRefStr
    };
    [lenenc] => {
        LengthEncodedString<ArenaStr>
    };
    [$n:expr] => {
        FixedLengthString<$n>
    };
}

pub const CHARSET_UTF8_MB4: u8 = 44;
pub const DEFAULT_CHARSET_CODE: u8 = CHARSET_UTF8_MB4;
pub const DEFAULT_PROTOCOL_VERSION: u8 = 10;


// Use the improved version of Old Password Authentication. More...
pub const CLIENT_LONG_PASSWORD: u32 = 1;
// Send found rows instead of affected rows in EOF_Packet. More...
pub const CLIENT_FOUND_ROWS: u32 = 2;
// Get all column flags. More...
pub const CLIENT_LONG_FLAG: u32 = 4;
// Database (schema) name can be specified on connect in Handshake Response Packet. More...
pub const CLIENT_CONNECT_WITH_DB: u32 = 8;
// DEPRECATED: Don't allow database.table.column. More...
pub const CLIENT_NO_SCHEMA: u32 =  16;
// Compression protocol supported. More...
pub const CLIENT_COMPRESS: u32 = 32;
// Special handling of ODBC behavior. More...
pub const CLIENT_ODBC: u32 = 64;
// Can use LOAD DATA LOCAL. More...
pub const CLIENT_LOCAL_FILES: u32 = 128;
// Ignore spaces before '('. More...
pub const CLIENT_IGNORE_SPACE: u32 = 256;
// New 4.1 protocol. More...
pub const CLIENT_PROTOCOL_41: u32 = 512;
// This is an interactive client. More...
pub const CLIENT_INTERACTIVE: u32 = 1024;
// Use SSL encryption for the session. More...
pub const CLIENT_SSL: u32 = 2048;
// Client only flag. More...
pub const CLIENT_IGNORE_SIGPIPE: u32 = 4096;
// Client knows about transactions. More...
pub const CLIENT_TRANSACTIONS: u32 = 8192;
// DEPRECATED: Old flag for 4.1 protocol More...
pub const CLIENT_RESERVED: u32 = 16384;
// DEPRECATED: Old flag for 4.1 authentication \ CLIENT_SECURE_CONNECTION. More...
pub const CLIENT_RESERVED2: u32 =  32768;
// Enable/disable multi-stmt support. More...
pub const CLIENT_MULTI_STATEMENTS: u32 = 1 << 16;
// Enable/disable multi-results. More...
pub const CLIENT_MULTI_RESULTS: u32 = 1 << 17;
// Multi-results and OUT parameters in PS-protocol. More...
pub const CLIENT_PS_MULTI_RESULTS: u32 = 1 << 18;
// Client supports plugin authentication. More...
pub const CLIENT_PLUGIN_AUTH: u32 = 1 << 19;
// Client supports connection attributes. More...
pub const CLIENT_CONNECT_ATTRS: u32 = 1 << 20;
// Enable authentication response packet to be larger than 255 bytes. More...
pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 1 << 21;
// Don't close the connection for a user account with expired password. More...
pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 1 << 22;
// Capable of handling server state change information. More...
pub const CLIENT_SESSION_TRACK: u32 = 1 << 23;
// Client no longer needs EOF_Packet and will use OK_Packet instead. More...
pub const CLIENT_DEPRECATE_EOF: u32 = 1 << 24;
// The client can handle optional metadata information in the resultset. More...
pub const CLIENT_OPTIONAL_RESULTSET_METADATA: u32 = 1 << 25;
// Compression protocol extended to support zstd compression method. More...
pub const CLIENT_ZSTD_COMPRESSION_ALGORITHM: u32 = 1 << 26;
// Support optional extension for query parameters into the COM_QUERY and COM_STMT_EXECUTE packets. More...
pub const CLIENT_QUERY_ATTRIBUTES: u32 = 1 << 27;
// Support Multi factor authentication. More...
pub const MULTI_FACTOR_AUTHENTICATION: u32 = 1 << 28;
// This flag will be reserved to extend the 32bit capabilities structure to 64bits. More...
pub const CLIENT_CAPABILITY_EXTENSION: u32 = 1 << 29;
// Verify server certificate. More...
pub const CLIENT_SSL_VERIFY_SERVER_CERT: u32 = 1 << 30;
// Don't reset the options after an unsuccessful connect. More...
pub const CLIENT_REMEMBER_OPTIONS: u32 = 1 << 31;

pub struct Packet {
    payload_length: int![3],
    sequence_id: int![1],
    payload: VariableLengthString,
}

impl Marshal for Packet {
    fn marshal(&self, _writer: &mut dyn Write) -> io::Result<usize> {
        todo!()
    }
}

pub struct OkPacket {
    header: int![1],
    affected_rows: int![lenenc],
    last_insert_id: int![lenenc],
    // begin CLIENT_PROTOCOL_41
    status_flags: int![2],
    warnings: int![2],
    // end   CLIENT_PROTOCOL_41

    info: LengthEncodedString<ArenaStr>,
    //session_state_info: LengthEncodedString,
}

impl OkPacket {
    pub fn is_ok(&self) -> bool { self.header.0 == 0x00 }
    pub fn is_eof(&self) -> bool { self.header.0 == 0xFE }
}

pub struct ErrPacket {
    header: int![1],
    err_code: int![2],
    // CLIENT_PROTOCOL_41 >
    sql_state_marker: string![1],
    sql_state: string![5],
    // CLIENT_PROTOCOL_41 <
    error_message: VariableLengthString,
}

/// Initial Handshake
/// Plain Handshake
pub struct HandshakeV9 {
    pub protocol_version: int![1],
    pub server_version: string![nul],
    pub thread_id: int![4],
    pub scramble: string![nul],
}

pub struct HandshakeResponse320 {
    pub client_flag: int![2],
    pub max_packet_size: int![3],
    pub username: string![nul],
    pub auth_response: string![nul],
    pub database: string![nul],
}

pub struct HandshakeV10 {
    pub protocol_version: int![1], // always 10
    pub server_version: string![snul],
    pub thread_id: int![4],
    pub auth_plugin_data_part_1: string![8],
    pub filter: int![1],
    pub capability_flags_1: int![2],
    pub character_set: int![1],
    pub status_flags: int![2],
    pub capability_flags_2: int![2],
    pub auth_plugin_data_len: int![1],
    pub reserved: string![10], // All 0s
    pub auth_plugin_data_part_2: string![lenenc],
    pub auth_plugin_name: string![nul]
}

impl Marshal for HandshakeV10 {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        let mut bytes = 0;
        bytes += self.protocol_version.marshal(writer)?;
        bytes += self.server_version.marshal(writer)?;
        bytes += self.thread_id.marshal(writer)?;
        bytes += self.auth_plugin_data_part_1.marshal(writer)?;
        bytes += self.filter.marshal(writer)?;
        bytes += self.capability_flags_1.marshal(writer)?;
        bytes += self.character_set.marshal(writer)?;
        bytes += self.status_flags.marshal(writer)?;
        bytes += self.capability_flags_2.marshal(writer)?;
        bytes += self.auth_plugin_data_len.marshal(writer)?;
        bytes += self.reserved.marshal(writer)?;
        bytes += self.auth_plugin_data_part_2.marshal(writer)?;
        Ok(bytes)
    }
}


pub struct HandshakeResponse41 {
    pub client_flag: int![4],
    pub max_packet_size: int![4],
    pub character_set: int![1],
    pub filter: string![32],
    pub username: string![nul],

    // !> CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA >
    pub auth_response_length: int![1],
    pub auth_response: string![lenenc],
    // !< CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA <

    // > CLIENT_CONNECT_WITH_DB
    pub database: string![nul],
    // < CLIENT_CONNECT_WITH_DB

    // > CLIENT_PLUGIN_AUTH
    pub client_plugin_name: string![nul],
    // < CLIENT_PLUGIN_AUTH

    // > CLIENT_ZSTD_COMPRESSION_ALGORITHM
    pub zstd_compression_level: int![1],
    // < CLIENT_ZSTD_COMPRESSION_ALGORITHM
}

impl HandshakeResponse41 {
    pub fn unmarshal<Reader: Buf>(rd: &mut Reader, arena: &ArenaMut<Arena>) -> Self {
        let client_flag = rd.get_u32();
        let max_packet_size = rd.get_u32();
        let character_set = rd.get_u8();
        let filter = FixedLengthString::<32>::unmarshal(rd);
        let username = NullTerminatedString::unmarshal(rd, arena);
        let auth_response_length;
        let auth_response;
        if (client_flag & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
            auth_response_length = 0;
            auth_response = LengthEncodedString::unmarshal(rd, arena);
        } else {
            auth_response_length = rd.get_u8();
            auth_response = LengthEncodedString::unmarshal_length(rd, auth_response_length as usize, arena);
        }

        let client_plugin_name;
        if (client_flag & CLIENT_PLUGIN_AUTH) != 0 {
            client_plugin_name = NullTerminatedString::unmarshal(rd, arena);
        } else {
            client_plugin_name = NullTerminatedString(ArenaStr::default());
        }

        let database;
        if (client_flag & CLIENT_CONNECT_WITH_DB ) != 0 {
            database = NullTerminatedString::unmarshal(rd, arena);
        } else {
            database = NullTerminatedString(ArenaStr::default());
        }

        let zstd_compression_level;
        if (client_flag & CLIENT_ZSTD_COMPRESSION_ALGORITHM) != 0 {
            zstd_compression_level = rd.get_u8();
        } else {
            zstd_compression_level = 0;
        }
        Self {
            client_flag: client_flag.into(),
            max_packet_size: max_packet_size.into(),
            character_set: character_set.into(),
            filter,
            username,
            auth_response_length: auth_response_length.into(),
            auth_response,
            database,
            client_plugin_name,
            zstd_compression_level: zstd_compression_level.into(),
        }
    }
}

// pub struct SessionStateInformation {
//     ty: FixedLengthInteger<u8, 1>,
//
// }

pub struct FixedLengthInteger<T, const N: usize>(T);

impl <const N: usize> From<u8> for FixedLengthInteger<u8, N> {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl <const N: usize> From<u16> for FixedLengthInteger<u16, N> {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl <const N: usize> From<u32> for FixedLengthInteger<u32, N> {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u8, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&[self.0])
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u16, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.0.to_le_bytes()[0..N])
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u32, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.0.to_le_bytes()[0..N])
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u64, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.0.to_le_bytes()[0..N])
    }
}

pub struct LengthEncodedInteger<T>(pub T);

impl LengthEncodedInteger<usize> {
    pub fn unmarshal<Reader: Buf>(rd: &mut Reader) -> Self {
        let this = LengthEncodedInteger::<u64>::unmarshal(rd);
        Self(this.0 as usize)
    }
}

impl LengthEncodedInteger<u64> {
    pub fn unmarshal<Reader: Buf>(rd: &mut Reader) -> Self {
        let leading = rd.get_u8();
        let mut buf: [u8; 8] = [0; 8];
        match leading {
            0xfc =>
                rd.reader().read_exact(&mut buf[0..2]).unwrap(),
            0xfd =>
                rd.reader().read_exact(&mut buf[0..3]).unwrap(),
            0xfe =>
                rd.reader().read_exact(&mut buf).unwrap(),
            _ => buf[0] = leading
        }
        Self(u64::from_le_bytes(buf))
    }
}

impl From<u64> for LengthEncodedInteger<u64> {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Marshal for LengthEncodedInteger<u64> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        let bytes = match self.0 {
            0..=251 => writer.write(&self.0.to_le_bytes()[0..1])?,
            252..=65536 => {
                let mut b = writer.write(&[0xfc])?;
                b += writer.write(&self.0.to_le_bytes()[0..2])?;
                b
            }
            65537..=16777216 => {
                let mut b = writer.write(&[0xfd])?;
                b += writer.write(&self.0.to_le_bytes()[0..3])?;
                b
            }
            16777217..=u64::MAX => {
                let mut b = writer.write(&[0xfe])?;
                b += writer.write(&self.0.to_le_bytes())?;
                b
            }
        };
        Ok(bytes)
    }
}

impl Marshal for LengthEncodedInteger<usize> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        LengthEncodedInteger::<u64>(self.0 as u64).marshal(writer)
    }
}

impl Marshal for ArenaStr {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(self.as_bytes())
    }
}

pub struct NullTerminatedString(ArenaStr);

impl NullTerminatedString {
    pub fn unmarshal<Reader: Buf>(rd: &mut Reader, arena: &ArenaMut<Arena>) -> Self {
        let mut buf = vec![];
        rd.reader().read_until(0, &mut buf).unwrap();
        Self(ArenaStr::new(std::str::from_utf8(&buf).unwrap(), arena.get_mut()))
    }
}

impl From<ArenaStr> for NullTerminatedString {
    fn from(value: ArenaStr) -> Self {
        Self(value)
    }
}

impl Marshal for NullTerminatedString {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        Ok(self.0.marshal(writer)? + writer.write(&[0])?)
    }
}

pub struct NullTerminatedStaticRefStr(&'static str);

impl From<&'static str> for NullTerminatedStaticRefStr {
    fn from(value: &'static str) -> Self {
        Self(value)
    }
}

impl Marshal for NullTerminatedStaticRefStr {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        Ok(writer.write(self.0.as_bytes())? + writer.write(&[0])?)
    }
}

pub struct VariableLengthString {

}

pub struct FixedLengthString<const N: usize>([u8; N]);


impl <const N: usize> FixedLengthString<N> {
    pub fn zero() -> Self { Self::fill(0) }
    pub fn fill(b: u8) -> Self { Self([b; N]) }

    pub fn unmarshal<Reader: Buf>(rd: &mut Reader) -> Self {
        let mut this = Self::zero();
        rd.reader().read_exact(&mut this.0[..]).unwrap();
        this
    }
}

impl <const N: usize> Marshal for FixedLengthString<N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.0)
    }
}

pub struct LengthEncodedString<T: Marshal>(pub T);

impl LengthEncodedString<ArenaStr> {
    pub fn unmarshal<Reader: Buf>(rd: &mut Reader, arena: &ArenaMut<Arena>) -> Self {
        let len = LengthEncodedInteger::<usize>::unmarshal(rd);
        Self::unmarshal_length(rd, len.0, arena)
    }

    pub fn unmarshal_length<Reader: Buf>(rd: &mut Reader, len: usize, arena: &ArenaMut<Arena>) -> Self {
        let mut buf = ArenaVec::with_capacity(len, arena);
        rd.reader().read_exact(&mut buf).unwrap();
        Self(ArenaStr::new(std::str::from_utf8(&buf).unwrap(), arena.get_mut()))
    }
}

impl <T: Marshal> Marshal for LengthEncodedString<T> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        let mut buf = Vec::new();
        self.0.marshal(&mut buf)?;

        let mut len = LengthEncodedInteger(buf.len()).marshal(writer)?;
        len += writer.write(&buf)?;
        Ok(len)
    }
}