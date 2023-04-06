use core::slice;
use std::cell::{Cell, Ref, RefCell};
use std::io;
use std::io::Write;
use std::mem::size_of;
use std::ptr::{addr_of, addr_of_mut};
use std::rc::Rc;
use std::sync::Arc;

use crate::env::{SequentialFile, WritableFile};
use crate::varint::{MAX_VARINT32_LEN, Varint, zig_zag32_encode};

pub trait Encode<T: Sized> {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize;
}

impl Encode<i32> for i32 {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        zig_zag32_encode(*self).write_to(buf)
    }
}

impl Encode<u32> for u32 {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        Varint::<u32>::encode(*self, buf)
    }
}

impl Encode<u64> for u64 {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        Varint::<u64>::encode(*self, buf)
    }
}

impl Encode<&[u8]> for &[u8] {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let mut n = (self.len() as u32).write_to(buf);
        n += buf.write(self).unwrap();
        n
    }
}

impl Encode<String> for String {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let mut n = (self.len() as u32).write_to(buf);
        n += buf.write(self.as_bytes()).unwrap();
        n
    }
}

impl Encode<Vec<u8>> for Vec<u8> {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let mut n = (self.len() as u32).write_to(buf);
        n += buf.write(self.as_slice()).unwrap();
        n
    }
}

pub trait Decode<T> {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<T>;
}

pub struct Decoder {
    offset: usize,
}

impl Decoder {
    pub fn new() -> Self {
        Self {
            offset: 0,
        }
    }

    pub fn offset(&self) -> usize { self.offset }

    fn checked_slice<'a>(&self, buf: &'a [u8], len: usize) -> io::Result<&'a [u8]> {
        if self.offset() + len > buf.len() {
            Err(io::Error::new(io::ErrorKind::InvalidData, "len not enough"))
        } else {
            Ok(&buf[self.offset..self.offset + len])
        }
    }

    pub fn take_slice<'a>(&mut self, buf: &'a [u8], len: usize) -> io::Result<&'a [u8]> {
        let rs = self.checked_slice(buf, len)?;
        self.offset += len;
        Ok(rs)
    }

    pub fn read_slice<'a>(&mut self, buf: &'a [u8]) -> io::Result<&'a [u8]> {
        let len: u32 = self.read_from(buf)?;
        self.take_slice(buf, len as usize)
    }
}

impl Decode<u8> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<u8> {
        let slice = self.take_slice(buf, 1)?;
        Ok(slice[0])
    }
}

impl Decode<i32> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<i32> {
        let (rs, n) = Varint::<i32>::decode(&buf[self.offset..])?;
        self.offset += n;
        Ok(rs)
    }
}

impl Decode<u32> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<u32> {
        let (rs, n) = Varint::<u32>::decode(&buf[self.offset..])?;
        self.offset += n;
        Ok(rs)
    }
}

impl Decode<i64> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<i64> {
        let (rs, n) = Varint::<i64>::decode(&buf[self.offset..])?;
        self.offset += n;
        Ok(rs)
    }
}

impl Decode<u64> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<u64> {
        let (rs, n) = Varint::<u64>::decode(&buf[self.offset..])?;
        self.offset += n;
        Ok(rs)
    }
}

impl Decode<String> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<String> {
        let len: u32 = self.read_from(buf)?;
        let data = self.take_slice(buf, len as usize)?;
        Ok(String::from_utf8_lossy(data).parse().unwrap())
    }
}

impl Decode<Vec<u8>> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> io::Result<Vec<u8>> {
        let len: u32 = self.read_from(buf)?;
        Ok(Vec::from(self.take_slice(buf, len as usize)?))
    }
}

pub struct FileWriter {
    file: Arc<RefCell<dyn WritableFile>>,
}

static DUMMY: [u8; 64] = [0; 64];

impl FileWriter {
    pub fn new(file: Arc<RefCell<dyn WritableFile>>) -> Self {
        Self { file }
    }

    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        self.file.borrow_mut().append(buf)
    }

    pub fn write_pad(&self, n: usize) -> io::Result<usize> {
        for _ in 0..n / DUMMY.len() {
            self.write(&DUMMY)?;
        }
        let remaining = n % DUMMY.len();
        if remaining > 0 {
            self.write(&DUMMY[0..remaining])?;
        }
        Ok(n)
    }

    pub fn write_byte(&self, b: u8) -> io::Result<usize> {
        self.write(&[b])
    }

    pub fn write_fixed_u16(&self, value: u16) -> io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    pub fn write_fixed_u32(&self, value: u32) -> io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    pub fn write_fixed_u64(&self, value: u64) -> io::Result<usize> {
        self.write(&value.to_le_bytes())
    }

    pub fn write_varint_u32(&self, value: u32) -> io::Result<usize> {
        let mut buf = Vec::new();
        Varint::<u32>::encode(value, &mut buf);
        self.write(buf.as_slice())
    }

    pub fn write_varint_u64(&self, value: u64) -> io::Result<usize> {
        let mut buf = Vec::new();
        Varint::<u64>::encode(value, &mut buf);
        self.write(buf.as_slice())
    }

    pub fn flush(&self) -> io::Result<()> {
        self.file.borrow_mut().flush()
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file.borrow_mut().sync()
    }

    pub fn truncate(&self, size: u64) -> io::Result<()> {
        self.file.borrow_mut().truncate(size)
    }

    pub fn file_size(&self) -> io::Result<u64> {
        let len = self.file.borrow().get_file_size()?;
        return Ok(len as u64)
    }
}

pub struct FileReader {
    file: Rc<RefCell<dyn SequentialFile>>,
    eof: Cell<bool>,
}

impl FileReader {
    pub fn new(file: Rc<RefCell<dyn SequentialFile>>) -> Self {
        Self { file, eof: Cell::new(false) }
    }

    pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        let read_in_bytes = self.file.borrow_mut().read(buf)?;
        if read_in_bytes < buf.len() {
            self.eof.set(true);
            Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        } else {
            Ok(read_in_bytes)
        }
    }

    pub fn read_byte(&self) -> io::Result<u8> {
        let mut buf: [u8; 1] = [0; 1];
        self.read(&mut buf)?;
        Ok(buf[0])
    }

    pub fn read_fixed_u16(&self) -> io::Result<u16> {
        let mut buf: [u8; 2] = [0; 2];
        self.read(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    pub fn read_fixed_u32(&self) -> io::Result<u32> {
        let mut buf: [u8; 4] = [0; 4];
        self.read(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    pub fn read_fixed_u64(&self) -> io::Result<u64> {
        let mut buf: [u8; 8] = [0; 8];
        self.read(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    pub fn skip(&self, n: usize) -> io::Result<u64> {
        self.file.borrow_mut().skip(n)
    }

    pub fn eof(&self) -> bool { self.eof.get() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let mut buf = Vec::<u8>::new();
        1u32.write_to(&mut buf);
        2u32.write_to(&mut buf);
        String::from("123456").write_to(&mut buf);

        assert_eq!([1, 2, 6, 49, 50, 51, 52, 53, 54], buf.as_slice());

        let rdb = buf.as_slice();
        let mut decoder = Decoder::new();
        assert_eq!(1u32, decoder.read_from(rdb).unwrap());
        assert_eq!(2u32, decoder.read_from(rdb).unwrap());
        assert_eq!(String::from("123456"), <Decoder as Decode<String>>::read_from(&mut decoder, rdb).unwrap());
    }
}