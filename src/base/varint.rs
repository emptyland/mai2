use std::io;
use std::marker::PhantomData;

pub struct Varint<T> {
    data: PhantomData<T>,
}

pub const MAX_VARINT32_LEN: usize = 5;
pub const MAX_VARINT64_LEN: usize = 10;

impl Varint<i32> {
    pub fn encode(value: i32, buf: &mut Vec<u8>) -> usize {
        Varint::<u32>::encode(zig_zag32_encode(value), buf)
    }

    pub fn decode(buf: &[u8]) -> io::Result<(i32, usize)> {
        let (v, n) = Varint::<u32>::decode(buf)?;
        Ok((zig_zag32_decode(v), n))
    }
}


impl Varint<u32> {
    pub fn encode(value: u32, buf: &mut Vec<u8>) -> usize {
        Varint::<u64>::encode(value as u64, buf)
    }

    pub fn decode(buf: &[u8]) -> io::Result<(u32, usize)> {
        let mut rs = 0u32;
        let mut n = 0usize;
        for b in buf {
            n += 1;
            if n >= MAX_VARINT32_LEN {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            if *b < 0x80u8 {
                rs |= *b as u32;
                break;
            }
            rs |= (b & 0x7f) as u32;
            rs <<= 7;
        }
        Ok((rs, n))
    }
}

impl Varint<i64> {
    pub fn encode(value: i64, buf: &mut Vec<u8>) -> usize {
        Varint::<u64>::encode(zig_zag64_encode(value), buf)
    }

    pub fn decode(buf: &[u8]) -> io::Result<(i64, usize)> {
        let (v, n) = Varint::<u64>::decode(buf)?;
        Ok((zig_zag64_decode(v), n))
    }
}

impl Varint<u64> {
    pub fn encode(value: u64, buf: &mut Vec<u8>) -> usize {
        let old = buf.len();
        let mut i = 9;
        loop {
            let mut b = ((value & 0x7f << i * 7) >> i * 7) as u8;
            if b != 0 {
                for j in (0..i).rev() {
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

    pub fn decode(buf: &[u8]) -> io::Result<(u64, usize)> {
        let mut rs = 0u64;
        let mut n = 0usize;
        for b in buf {
            n += 1;
            if n >= MAX_VARINT64_LEN {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            if *b & 0x80 == 0 {
                rs |= *b as u64;
                break;
            }

            rs |= (b & 0x7f) as u64;
            rs <<= 7;
        }
        Ok((rs, n))
    }

    // 11 0101011 0011111 1000000
    // 11000000 10011111 10101011 00000011
    // 10000011 11000000 10011111 00101011
    //
}

pub fn zig_zag32_encode(value: i32) -> u32 {
    let bit = if value < 0 { 1 } else { 0 };
    (value.unsigned_abs() << 1) | bit
}

pub fn zig_zag32_decode(value: u32) -> i32 {
    let signed = (value & 1) == 1;
    let i = i32::from_le_bytes((value >> 1).to_le_bytes());
    if signed { -i } else { i }
}

pub fn zig_zag64_encode(value: i64) -> u64 {
    let bit = if value < 0 { 1 } else { 0 };
    (value.unsigned_abs() << 1) | bit
}

pub fn zig_zag64_decode(value: u64) -> i64 {
    let signed = (value & 1) == 1;
    let i = i64::from_le_bytes((value >> 1).to_le_bytes());
    if signed { -i } else { i }
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
    fn signed_int() {
        let mut buf = Vec::<u8>::new();
        Varint::<i32>::encode(-1, &mut buf);
        assert_eq!(1, buf.len());
        assert_eq!(&[3], buf.as_slice());

        buf.clear();
        Varint::<i32>::encode(1, &mut buf);
        assert_eq!(1, buf.len());
        assert_eq!(&[2], buf.as_slice());
    }

    #[test]
    fn decoding() {
        let mut buf = Vec::<u8>::new();
        Varint::<u32>::encode(0x81, &mut buf);
        assert_eq!(2, buf.len());
        assert_eq!([0x81, 1], buf.as_slice());

        assert_eq!((0x81, 2), Varint::<u32>::decode(buf.as_slice()).unwrap());

        buf.clear();
        Varint::<u32>::encode(0x80000u32, &mut buf);
        assert_eq!(3, buf.len());
        assert_eq!([0xa0, 0x80, 0], buf.as_slice());

        assert_eq!((0x80000u32, 3), Varint::<u32>::decode(buf.as_slice()).unwrap());
    }

    #[test]
    fn big_number() -> io::Result<()> {
        let mut buf = Vec::<u8>::new();
        Varint::<u64>::encode(7000000, &mut buf);
        assert_eq!(4, buf.len());
        assert_eq!([131, 171, 159, 64], buf.as_slice());
        assert_eq!((7000000, 4), Varint::<u64>::decode(buf.as_slice())?);
        Ok(())
    }
}