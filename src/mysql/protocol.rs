use std::io;
use std::io::Write;
use crate::ArenaStr;
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
    [$n:expr] => {
        FixedLengthString<$n>
    };
    [lenenc] => {

    }
}


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
    pub fn is_ok(&self) -> bool { self.header.val == 0x00 }
    pub fn is_eof(&self) -> bool { self.header.val == 0xFE }
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
    protocol_version: int![1],
}



// pub struct SessionStateInformation {
//     ty: FixedLengthInteger<u8, 1>,
//
// }

pub struct FixedLengthInteger<T, const N: usize> {
    pub val: T,
}

impl <const N: usize> Marshal for FixedLengthInteger<u8, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&[self.val])
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u16, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.val.to_le_bytes()[0..N])
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u32, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.val.to_le_bytes()[0..N])
    }
}

impl <const N: usize> Marshal for FixedLengthInteger<u64, N> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(&self.val.to_le_bytes()[0..N])
    }
}

pub struct LengthEncodedInteger<T> {
    val: T
}

impl <T> LengthEncodedInteger<T> {
    pub fn new(val: T) -> Self {
        Self {val}
    }
}

impl Marshal for LengthEncodedInteger<u64> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        todo!()
    }
}

impl Marshal for LengthEncodedInteger<usize> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        todo!()
    }
}

impl Marshal for ArenaStr {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        writer.write(self.as_bytes())
    }
}

pub struct VariableLengthString {

}

pub struct FixedLengthString<const N: usize> {
    val: [u8;N]
}

pub struct LengthEncodedString<T: Marshal> {
    val: T
}

impl <T: Marshal> Marshal for LengthEncodedString<T> {
    fn marshal(&self, writer: &mut dyn Write) -> io::Result<usize> {
        let mut buf = Vec::new();
        self.val.marshal(&mut buf)?;

        let mut len = LengthEncodedInteger::new(buf.len()).marshal(writer)?;
        len += writer.write(&buf)?;
        Ok(len)
    }
}