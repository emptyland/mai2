use std::alloc::Layout;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::mem::size_of;
use std::ptr::NonNull;
use std::rc::Rc;
use std::slice;

use crate::base::{Allocator};
use crate::storage::{BitwiseComparator, Comparator};

#[derive(Copy)]
pub struct KeyBundle {
    bundle: NonNull<KeyHeader>,
}

struct KeyHeader {
    len: u32,
    key_len: u32,
}

#[derive(Debug)]
#[derive(PartialEq)]
pub enum Tag {
    Key,
    Deletion,
}

impl Tag {
    pub fn to_flag(&self) -> u64 {
        match self {
            Tag::Key => 0,
            Tag::Deletion => 1u64 << 63
        }
    }

    pub fn to_byte(&self) -> u8 {
        match self {
            Tag::Key => 0,
            Tag::Deletion => 1
        }
    }
}

impl KeyBundle {
    pub fn new<A: Allocator + ?Sized>(arena: &mut A, tag: Tag, sequence_number: u64, key: &[u8], value: &[u8]) -> Self {
        let kb = Self::new_uninitialized(arena, sequence_number, tag, key.len(), value.len());
        kb.user_key_mut().write(key).unwrap();
        kb.value_mut().write(value).unwrap();
        kb
    }

    pub fn from_key_value<A: Allocator + ?Sized>(arena: &mut A, sequence_number: u64, key: &[u8], value: &[u8]) -> Self {
        let kb = Self::new_uninitialized(arena, sequence_number, Tag::Key,
                                         key.len(), value.len());
        kb.user_key_mut().write(key).unwrap();
        kb.value_mut().write(value).unwrap();
        kb
    }

    pub fn from_key<A: Allocator + ?Sized>(arena: &mut A, sequence_number: u64, key: &[u8]) -> Self {
        let kb = Self::new_uninitialized(arena, sequence_number, Tag::Key,
                                         key.len(), 0);
        kb.user_key_mut().write(key).unwrap();
        kb
    }

    pub fn from_deletion<A: Allocator + ?Sized>(arena: &mut A, sequence_number: u64, key: &[u8]) -> Self {
        let kb = Self::new_uninitialized(arena, sequence_number, Tag::Deletion,
                                         key.len(), 0);
        kb.user_key_mut().write(key).unwrap();
        kb
    }

    fn new_uninitialized<A: Allocator + ?Sized>(arena: &mut A, sequence_number: u64, tag: Tag, user_key_size: usize, value_size: usize) -> Self {
        let mut header;
        unsafe {
            let chunk = arena.allocate(Self::build_layout(user_key_size + TAG_SIZE + value_size));
            let base_ptr = chunk.unwrap().as_ptr() as *mut u8;
            header = base_ptr as *mut KeyHeader;
            (*header).key_len = (user_key_size + TAG_SIZE) as u32;
            (*header).len = (user_key_size + TAG_SIZE + value_size) as u32;

            *(base_ptr.add(size_of::<KeyHeader>() + user_key_size) as *mut u64) = sequence_number | tag.to_flag()
        }
        Self { bundle: NonNull::new(header).unwrap() }
    }

    pub fn sequence_number(&self) -> u64 {
        InternalKey::parse(self.key()).sequence_number
    }

    pub fn tag(&self) -> Tag {
        InternalKey::parse(self.key()).tag
    }

    pub fn user_key(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts(self.payload_addr(self.key_offset()), self.user_key_len())
        }
    }

    fn user_key_mut(&self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.payload_addr_mut(self.key_offset()), self.user_key_len())
        }
    }

    pub fn user_key_len(&self) -> usize { self.key_len() - TAG_SIZE }

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
            self.bundle.as_ref().len as usize - self.key_len()
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
        Layout::from_size_align(layout.size() + payload_size, layout.align()).unwrap()
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
            .field("key", &self.user_key())
            .field("value", &self.value())
            .finish()
    }
}

impl PartialEq<Self> for KeyBundle {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl PartialOrd for KeyBundle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let rs = self.user_key().partial_cmp(other.user_key());
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

pub const TAG_SIZE: usize = size_of::<u64>();

pub const MAX_SEQUENCE_NUMBER: u64 = 0x00ffffffffffffffu64;

#[derive(Debug, PartialEq)]
pub struct InternalKey<'a> {
    pub sequence_number: u64,
    pub tag: Tag,
    pub user_key: &'a [u8],
}

impl InternalKey<'_> {
    pub fn parse(key: &[u8]) -> Self {
        assert!(key.len() >= TAG_SIZE);

        let base_ptr = &key[0] as *const u8;
        let user_key = unsafe { slice::from_raw_parts(base_ptr, key.len() - TAG_SIZE) };
        let tail = unsafe {
            *(base_ptr.add(user_key.len()) as *const u64)
        };
        Self {
            sequence_number: tail & ((1u64 << 63) - 1),
            tag: if (tail & (1u64 << 63)) != 0 {
                Tag::Deletion
            } else {
                Tag::Key
            },
            user_key,
        }
    }

    pub fn from_str_key(user_key: &str, sequence_number: u64) -> Vec<u8> {
        Self::from_key(user_key.as_bytes(), sequence_number, Tag::Key)
    }

    pub fn from_key(user_key: &[u8], sequence_number: u64, tag: Tag) -> Vec<u8> {
        let mut key = Vec::default();
        key.append(&mut user_key.to_vec());
        let tail = (sequence_number | tag.to_flag()).to_le_bytes();
        key.append(&mut tail.to_vec());
        key
    }

    pub fn extract_user_key(key: &[u8]) -> &[u8] { &key[..key.len() - TAG_SIZE] }

    pub fn extract_tag(key: &[u8]) -> (Tag, u64) {
        let mut buf: [u8; 8] = [0; 8];
        (&mut buf[..]).write(&key[key.len() - TAG_SIZE..]).unwrap();
        let tail = u64::from_le_bytes(buf);
        let sequence_number = tail & ((1u64 << 63) - 1);
        if (tail & (1u64 << 63)) != 0 {
            (Tag::Deletion, sequence_number)
        } else {
            (Tag::Key, sequence_number)
        }
    }

    pub fn _test_expect(key: &[u8], user_key: &str, sequence_number: u64, tag: Tag) {
        let ik = Self::parse(key);
        assert_eq!(user_key.as_bytes(), ik.user_key);
        assert_eq!(sequence_number, ik.sequence_number);
        assert_eq!(tag, ik.tag);
    }
}

#[derive(Clone)]
pub struct InternalKeyComparator {
    pub user_cmp: Rc<dyn Comparator>,
}

impl InternalKeyComparator {
    pub fn new(user_cmp: Rc<dyn Comparator>) -> InternalKeyComparator {
        Self {
            user_cmp
        }
    }

    pub fn user_cmp(&self) -> Rc<dyn Comparator> {
        self.user_cmp.clone()
    }
}

impl Comparator for InternalKeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let lhs = InternalKey::parse(a);
        let rhs = InternalKey::parse(b);
        let ord = self.user_cmp.compare(lhs.user_key, rhs.user_key);
        if Ordering::Equal != ord {
            return ord;
        }
        assert_eq!(Ordering::Equal, ord);
        if lhs.sequence_number < rhs.sequence_number {
            Ordering::Greater
        } else if lhs.sequence_number > rhs.sequence_number {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }

    fn name(&self) -> String { String::from("internal-key-comparator") }
}

#[cfg(test)]
mod tests {
    use crate::base::Arena;
    use super::*;

    #[test]
    fn sanity() {
        let mut arena = Arena::new();
        let k = "hello";
        let v = "world";
        let bundle = KeyBundle::from_key_value(&mut arena, 1, k.as_bytes(),
                                               v.as_bytes());
        assert!(matches!(bundle.tag(), Tag::Key));
        assert_eq!(1, bundle.sequence_number());
        assert_eq!(k.as_bytes(), bundle.user_key());
        assert_eq!(v.as_bytes(), bundle.value());
    }

    #[test]
    fn compare() {
        let mut arena = Arena::new();
        let b1 = KeyBundle::from_key_value(&mut arena, 1, "111".as_bytes(),
                                           "".as_bytes());
        let b2 = KeyBundle::from_key_value(&mut arena, 2, "112".as_bytes(),
                                           "".as_bytes());
        assert!(b1 < b2);
        assert!(b1 <= b2);

        let b3 = KeyBundle::from_key_value(&mut arena, 1, "222".as_bytes(),
                                           "".as_bytes());
        let b4 = KeyBundle::from_key_value(&mut arena, 2, "222".as_bytes(),
                                           "1".as_bytes());
        assert!(b3 > b4);
    }

    #[test]
    fn internal_key_compare() {
        let mut arena = Arena::new();
        let b1 = KeyBundle::from_key_value(&mut arena, 1, "111".as_bytes(),
                                           "1".as_bytes());
        let b2 = KeyBundle::from_key_value(&mut arena, 2, "111".as_bytes(),
                                           "2".as_bytes());
        let user_cmp = Rc::new(BitwiseComparator {});
        let cmp = InternalKeyComparator::new(user_cmp);
        assert_eq!(Ordering::Greater, cmp.compare(b1.key(), b2.key()));
    }
}


