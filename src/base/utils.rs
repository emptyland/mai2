use std::cmp::min;
use std::io::{Read, Write};
use std::num::Wrapping;
use std::ptr;
use std::ptr::{addr_of, addr_of_mut};

pub const BLOOM_FILTER_HASHES_ORDER: [fn(&[u8]) -> u32; 5] = [
    js_hash,
    sdbm_hash,
    rs_hash,
    elf_hash,
    bkdr_hash,
];

pub fn js_hash(input: &[u8]) -> u32 {
    let mut hash = Wrapping(1315423911u32);
    for b in input {
        hash ^= (hash << 5) + Wrapping(*b as u32) + (hash >> 2);
    }
    hash.0
}

pub fn sdbm_hash(input: &[u8]) -> u32 {
    let mut hash = Wrapping(0u32);
    for item in input {
        let c = Wrapping(*item as u32);
        hash = Wrapping(65599) * hash + c;
        hash = c + (hash << 6) + (hash << 16) - hash;
    }
    hash.0
}

pub fn rs_hash(input: &[u8]) -> u32 {
    let mut a = Wrapping(63689);
    let b = Wrapping(378551);
    let mut hash = Wrapping(0);
    for c in input {
        hash = hash * a + Wrapping(*c as u32);
        a *= b;
    }
    hash.0
}

pub fn elf_hash(input: &[u8]) -> u32 {
    let mut hash = Wrapping(0);
    let mut x = Wrapping(0);
    for c in input {
        hash = (hash << 4) + Wrapping(*c as u32);
        x = hash & Wrapping(0xF0000000);
        if x != Wrapping(0) {
            hash ^= x >> 24;
            hash &= !x;
        }
    }
    hash.0
}

pub fn bkdr_hash(input: &[u8]) -> u32 {
    let seed = Wrapping(131);
    let mut hash = Wrapping(0);
    for c in input {
        hash = hash * seed + Wrapping(*c as u32);
    }
    hash.0
}

pub fn round_down<T>(x: *mut T, m: i64) -> *mut T {
    (x as u64 & -m as u64) as *mut T
}

// return RoundDown<T>(static_cast<T>(x + m - 1), m);
pub fn round_up<T>(x: *mut T, m: i64) -> *mut T {
    round_down((x as i64 + m - 1) as *mut T, m)
}

pub fn to_ascii_lowercase<const N: usize>(input: &str) -> [u8;N] {
    let mut buf: [u8;N] = [0;N];
    debug_assert!(input.as_bytes().len() <= buf.len());
    unsafe {
        ptr::copy_nonoverlapping(addr_of!(input.as_bytes()[0]), addr_of_mut!(buf[0]),
                                 input.len());
    }
    buf[..input.len()].make_ascii_lowercase();
    buf
}

pub struct SliceReadWrapper<'a> {
    buf: &'a [u8],
    position: usize
}

impl <'a> SliceReadWrapper<'a> {
    fn readable_in_bytes(&self) -> usize {
        self.buf.len() - self.position
    }
}

impl <'a> From<&'a str> for SliceReadWrapper<'a> {
    fn from(value: &'a str) -> Self {
        Self {
            buf: value.as_bytes(),
            position: 0,
        }
    }
}

impl <'a> From<&'a String> for SliceReadWrapper<'a> {
    fn from(value: &'a String) -> Self {
        Self {
            buf: value.as_bytes(),
            position: 0
        }
    }
}

impl <'a> Read for SliceReadWrapper<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let len = min(buf.len(), self.readable_in_bytes());
        buf.write(&self.buf[self.position..self.position + len])?;
        self.position += len;
        Ok(len)
    }
}