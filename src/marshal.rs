use core::slice;
use std::io::Write;
use std::mem::size_of;
use std::ptr::{addr_of, addr_of_mut};
use crate::varint::Varint;

pub trait Encode<T: Sized> {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize;
}

impl Encode<i32> for i32 {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        (self.clone() as u32).write_to(buf);
        size_of::<Self>()
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
    fn read_from(&mut self, buf: &[u8]) -> T;
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
}

impl Decode<i32> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> i32 {
        let parts = &buf[self.offset..self.offset + 4];
        self.offset += 4;
        let mut n: i32 = 0;
        let ptr = addr_of_mut!(n) as *mut u8;
        unsafe {
            slice::from_raw_parts_mut(ptr, 4).write(parts).unwrap();
        }
        n
    }
}

impl Decode<u32> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> u32 {
        let (rs, n) = Varint::<u32>::decode(&buf[self.offset..]);
        self.offset += n;
        rs
    }
}

impl Decode<u64> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> u64 {
        let (rs, n) = Varint::<u64>::decode(&buf[self.offset..]);
        self.offset += n;
        rs
    }
}

impl Decode<String> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> String {
        let len: u32 = self.read_from(buf);
        let data = &buf[self.offset..self.offset + len as usize];
        self.offset += len as usize;
        String::from_utf8_lossy(data).parse().unwrap()
    }
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
        assert_eq!(1u32, decoder.read_from(rdb));
        assert_eq!(2u32, decoder.read_from(rdb));
        assert_eq!(String::from("123456"), <Decoder as Decode<String>>::read_from(&mut decoder, rdb));
    }
}