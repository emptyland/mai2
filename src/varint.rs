use std::marker::PhantomData;
use std::ops::{BitOr, BitOrAssign, ShlAssign};

pub struct Varint<T> {
    data: PhantomData<T>,
}

impl Varint<u32> {
    pub fn encode(value: u32, buf: &mut Vec<u8>) -> usize {
        let old = buf.len();
        let mut i = 4;
        loop {
            let mut b = ((value & 0x7f << i * 7) >> i * 7) as u8;
            // dbg!(b);
            // dbg!(i);
            if b != 0 {
                for j in 0..i {
                    buf.push(b | 0x80u8);
                    b = ((value & 0x7f << j * 7) >> j * 7) as u8;
                }
                buf.push(b);
                return buf.len() - old;
            }
            i -= 1;
            if i <= 0 {
                break;
            }
        }
        let b = (value & 0x7f) as u8;
        buf.push(b);
        buf.len() - old
    }

    pub fn decode(buf: &[u8]) -> (u32, usize) {
        let mut rs = 0u32;
        let mut n = 0usize;
        for b in buf {
            n += 1;
            if *b < 0x80u8 {
                rs |= *b as u32;
                break;
            }
            rs |= (b & 0x7f) as u32;
            rs <<= 7;
        }
        (rs, n)
    }
}

impl Varint<u64> {
    pub fn encode(value: u64, buf: &mut Vec<u8>) -> usize {
        let old = buf.len();
        let mut i = 9;
        loop {
            let mut b = ((value & 0x7f << i * 7) >> i * 7) as u8;
            if b != 0 {
                for j in 0..i {
                    buf.push(b | 0x80u8);
                    b = ((value & 0x7f << j * 7) >> j * 7) as u8;
                }
                buf.push(b);
                return buf.len() - old;
            }
            i -= 1;
            if i <= 0 {
                break;
            }
        }
        let b = (value & 0x7f) as u8;
        buf.push(b);
        buf.len() - old
    }

    pub fn decode(buf: &[u8]) -> (u64, usize) {
        let mut rs = 0u64;
        let mut n = 0usize;
        for b in buf {
            n += 1;
            if *b < 0x80u8 {
                rs |= *b as u64;
                break;
            }
            rs |= (b & 0x7f) as u64;
            rs <<= 7;
        }
        (rs, n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let mut buf = Vec::<u8>::new();
        Varint::<u32>::encode(0, &mut buf);
        assert_eq!(1, buf.len());
        assert_eq!(&[0u8], buf.as_slice());

        buf.clear();
        Varint::<u32>::encode(1, &mut buf);
        assert_eq!(1, buf.len());
        assert_eq!(&[1], buf.as_slice());

        buf.clear();
        Varint::<u32>::encode(0x7f, &mut buf);
        assert_eq!(1, buf.len());
        assert_eq!(&[0x7fu8], buf.as_slice());

        buf.clear();
        Varint::<u32>::encode(0x80, &mut buf);
        assert_eq!(2, buf.len());
        assert_eq!(&[0x81, 0], buf.as_slice());
    }

    #[test]
    fn decoding() {
        let mut buf = Vec::<u8>::new();
        Varint::<u32>::encode(0x81, &mut buf);
        assert_eq!(2, buf.len());
        assert_eq!([0x81, 1], buf.as_slice());

        assert_eq!((0x81, 2), Varint::<u32>::decode(buf.as_slice()));

        buf.clear();
        Varint::<u32>::encode(0x80000u32, &mut buf);
        assert_eq!(3, buf.len());
        assert_eq!([0xa0, 0x80, 0], buf.as_slice());

        assert_eq!((0x80000u32, 3), Varint::<u32>::decode(buf.as_slice()));
    }
}