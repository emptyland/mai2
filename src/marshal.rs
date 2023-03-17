use core::slice;
use std::io::Write;
use std::mem::size_of;
use std::ptr::{addr_of, addr_of_mut};

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
        let parts = [
            (self & 0xff) as u8,
            ((self & 0xff00) >> 8) as u8,
            ((self & 0xff0000) >> 16) as u8,
            ((self & 0xff000000) >> 24) as u8,
        ];
        buf.write(&parts).unwrap();
        size_of::<Self>()
    }
}

impl Encode<u64> for u64 {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        let parts = [
            (self & 0xff) as u8,
            ((self & 0xff00) >> 8) as u8,
            ((self & 0xff0000) >> 16) as u8,
            ((self & 0xff000000) >> 24) as u8,
            ((self & 0xff00000000) >> 32) as u8,
            ((self & 0xff0000000000) >> 40) as u8,
            ((self & 0xff000000000000) >> 48) as u8,
            ((self & 0xff00000000000000) >> 56) as u8,
        ];
        buf.write(&parts).unwrap();
        size_of::<Self>()
    }
}

impl Encode<String> for String {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        (buf.len() as u32).write_to(buf);
        buf.write(self.as_bytes()).unwrap();
        size_of::<u32>() + self.len()
    }
}

impl Encode<Vec<u8>> for Vec<u8> {
    fn write_to(&self, buf: &mut Vec<u8>) -> usize {
        (buf.len() as u32).write_to(buf);
        buf.write(self.as_slice()).unwrap();
        size_of::<u32>() + self.len()
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
        let parts = &buf[self.offset..self.offset + 4];
        self.offset += 4;
        (parts[0] as u32) |
            (parts[1] as u32) << 8 |
            (parts[2] as u32) << 16 |
            (parts[3] as u32) << 24
    }
}

impl Decode<u64> for Decoder {
    fn read_from(&mut self, buf: &[u8]) -> u64 {
        let parts = &buf[self.offset..self.offset + 8];
        self.offset += 8;
        (parts[0] as u64) |
            (parts[1] as u64) << 8 |
            (parts[2] as u64) << 16 |
            (parts[3] as u64) << 24 |
            (parts[4] as u64) << 32 |
            (parts[5] as u64) << 40 |
            (parts[6] as u64) << 48 |
            (parts[7] as u64) << 56
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
        1.write_to(&mut buf);
        2.write_to(&mut buf);
        String::from("123456").write_to(&mut buf);

        let rdb = buf.as_slice();
        let mut decoder = Decoder::new();
        assert_eq!(1, decoder.read_from(rdb));
        assert_eq!(2, decoder.read_from(rdb));
    }
}