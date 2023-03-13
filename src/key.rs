use std::alloc;
use std::alloc::{alloc, dealloc, Layout};
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::mem::size_of;
use std::ptr::NonNull;
use std::slice;

use crate::arena::Arena;

#[derive(Copy)]
pub struct KeyBundle {
    bundle: NonNull<KeyHeader>
}

struct KeyHeader {
    sequence_number: u64,
    tag: u16,
    key_len: u32,
    value_len: u32,
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum Tag {
    KEY,
    DELETION,
}

impl KeyBundle {
    pub fn for_key_value(arena: &mut Arena, sequence_number: u64, key: &[u8], value: &[u8]) -> KeyBundle {
        let kb = Self::for_uninitialized(arena, sequence_number, 0,
                                             key.len(), value.len());
        kb.key_mut().write(key).unwrap();
        kb.value_mut().write(value).unwrap();
        kb
    }

    pub fn for_deletion(arena: &mut Arena, sequence_number: u64, key: &[u8]) -> KeyBundle {
        let kb = Self::for_uninitialized(arena, sequence_number, 1,
                                             key.len(), 0);
        kb.key_mut().write(key).unwrap();
        kb
    }

    fn for_uninitialized(arena: &mut Arena, sequence_number: u64, tag: u16, key_size: usize, value_size: usize) -> KeyBundle {
        let mut header;
        unsafe {
            let chunk = arena.allocate(Self::build_layout(key_size + value_size));
            header = chunk.unwrap().as_ptr() as *mut KeyHeader;
            (*header).tag = tag;
            (*header).sequence_number = sequence_number;
            (*header).key_len = key_size as u32;
            (*header).value_len = value_size as u32;
        }
        KeyBundle { bundle: NonNull::new(header).unwrap() }
    }

    pub fn sequence_number(&self) -> u64 {
        unsafe {
            self.bundle.as_ref().sequence_number
        }
    }

    pub fn tag(&self) -> Tag {
        let tag_num;
        unsafe {
            tag_num = self.bundle.as_ref().tag;
        }
        match tag_num {
            0 => Tag::KEY,
            1 => Tag::DELETION,
            _ => panic!("Bad key tag number")
        }
    }

    pub fn key(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.payload_addr(self.key_offset()), self.key_len())
        }
    }

    fn key_mut(&self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.payload_addr_mut(self.key_offset()), self.key_len())
        }
    }

    pub fn key_len(&self) -> usize {
        unsafe {
            self.bundle.as_ref().key_len as usize
        }
    }

    const fn key_offset(&self) -> usize { 0 }

    pub fn value(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.payload_addr(self.value_offset()), self.value_len())
        }
    }

    fn value_mut(&self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.payload_addr_mut(self.value_offset()), self.value_len())
        }
    }

    pub fn value_len(&self) -> usize {
        unsafe {
            self.bundle.as_ref().value_len as usize
        }
    }

    fn value_offset(&self) -> usize {
        self.key_offset() + self.key_len()
    }

    const unsafe fn payload_addr(&self, offset: usize) -> *const u8 {
        (self.bundle.as_ptr() as *const u8).add(size_of::<KeyHeader>() + offset)
    }

    const unsafe fn payload_addr_mut(&self, offset: usize) -> *mut u8 {
        (self.bundle.as_ptr() as *mut u8).add(size_of::<KeyHeader>() + offset)
    }

    fn build_layout(payload_size: usize) -> Layout {
        let layout = Layout::new::<KeyHeader>();
        Layout::from_size_align(layout.size() + payload_size, layout.align())
            .expect("ok")
    }
}

impl Default for KeyBundle {
    fn default() -> Self {
        KeyBundle { bundle: NonNull::new(1 as *mut KeyHeader).unwrap() }
    }
}

impl Clone for KeyBundle {
    fn clone(&self) -> Self {
        KeyBundle { bundle: self.bundle }
    }
}

impl Debug for KeyBundle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyBundle")
            .field("sequence_number", &self.sequence_number())
            .field("tag", &self.tag())
            .field("key", &self.key())
            .field("value", &self.value())
            .finish()
    }
}

impl PartialEq<Self> for KeyBundle {
    fn eq(&self, other: &Self) -> bool {
        false
    }
}

impl PartialOrd for KeyBundle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let rs = self.key().partial_cmp(other.key());
        if matches!(rs, Some(Ordering::Equal)) {
            if self.sequence_number() > other.sequence_number() {
                Some(Ordering::Less)
            } else {
                Some(Ordering::Greater)
            }
        } else {
            rs
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let mut arena = Arena::new();
        let k = "hello";
        let v = "world";
        let bundle = KeyBundle::for_key_value(&mut arena, 1, k.as_bytes(),
                                              v.as_bytes());
        assert!(matches!(bundle.tag(), Tag::KEY));
        assert_eq!(1, bundle.sequence_number());
        assert_eq!(k.as_bytes(), bundle.key());
        assert_eq!(v.as_bytes(), bundle.value());
    }

    #[test]
    fn compare() {
        let mut arena = Arena::new();
        let b1 = KeyBundle::for_key_value(&mut arena, 1, "111".as_bytes(),
                                          "".as_bytes());
        let b2 = KeyBundle::for_key_value(&mut arena, 2, "112".as_bytes(),
                                          "".as_bytes());
        assert!(b1 < b2);
        assert!(b1 <= b2);

        let b3 = KeyBundle::for_key_value(&mut arena, 1, "222".as_bytes(),
                                          "".as_bytes());
        let b4 = KeyBundle::for_key_value(&mut arena, 2, "222".as_bytes(),
                                          "1".as_bytes());
        assert!(b3 > b4);
    }
}


