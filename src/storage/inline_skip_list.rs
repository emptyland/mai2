use std::{cmp, slice};
use std::alloc::Layout;
use std::cell::RefCell;
use std::io::Write;
use std::mem::size_of;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use rand::random;

use crate::base::{Allocator, Arena, ArenaMut};
use crate::storage::key::{InternalKey, Tag, TAG_SIZE};
use crate::storage::skip_list::{BRANCHING, Comparing, MAX_HEIGHT};

pub struct InlineSkipList<'a, Cmp> {
    arena: ArenaMut<Arena>,
    comparator: Cmp,
    max_height: AtomicU32,
    head: NonNull<InlineNode<'a>>,
}

impl<'a, Cmp: for<'b> Comparing<&'b [u8]>> InlineSkipList<'a, Cmp> {
    pub fn new(mut arena: ArenaMut<Arena>, comparator: Cmp) -> Self {
        let head = unsafe {
            InlineNode::new(MAX_HEIGHT, arena.deref_mut(), 0, 0)
        };
        Self {
            arena,
            comparator,
            head,
            max_height: AtomicU32::new(1),
        }
    }

    pub fn insert_deletion_key(&self, sequence_number: u64, key: &[u8]) {
        self.insert(Tag::Deletion, sequence_number, key, Default::default());
    }

    pub fn insert_key_value(&self, sequence_number: u64, key: &[u8], value: &[u8]) {
        self.insert(Tag::Key, sequence_number, key, value);
    }

    pub fn insert(&self, tag: Tag, sequence_number: u64, key: &[u8], value: &[u8]) {
        let mut arena = self.arena.clone();
        let node = InlineNode::with_key_value(MAX_HEIGHT,
                                              arena.deref_mut(),
                                              tag, sequence_number, key, value);
        self.put(node)
    }

    fn put(&self, node: NonNull<InlineNode<'a>>) {
        let mut prev: [Option<NonNull<InlineNode>>; MAX_HEIGHT] = [None; MAX_HEIGHT];
        //let internal_key = InternalKey::from_key(key, sequence_number, tag);
        let key = InlineNode::key(node.as_ptr());
        let mut x = self.find_greater_or_equal(key, &mut prev);
        assert!(x.is_none() || !self.equal(InlineNode::key(x.unwrap().as_ptr()), key));

        let height = self.random_height();
        if height > self.max_height() {
            for i in self.max_height()..height {
                prev[i] = Some(self.head.clone());
            }
            self.max_height.store(height as u32, Ordering::Relaxed);
        }

        x = Some(InlineNode::with_height(node.as_ptr(), height));
        for i in 0..height {
            InlineNode::no_barrier_set_next(x.unwrap().as_ptr(), i,
                                            InlineNode::no_barrier_next(prev[i].unwrap().as_ptr(), i));
            InlineNode::set_next(prev[i].unwrap().as_ptr(), i, x);
        }
    }

    pub fn iter(&self) -> IteratorImpl<'a, Cmp> {
        let mut it = IteratorImpl::new(self);
        it.seek_to_first();
        it
    }

    fn find_greater_or_equal<'b>(&self, key: &[u8], prev: &mut [Option<NonNull<InlineNode<'a>>>]) -> Option<NonNull<InlineNode<'a>>> {
        let mut x = self.head;
        let mut level = (self.max_height() - 1) as usize;
        loop {
            let next = InlineNode::next(x.as_ptr(), level);
            if self.key_is_after_node(key, next) {
                x = next.unwrap();
            } else {
                if prev.len() > 0 {
                    prev[level] = Some(x.clone());
                }
                if level == 0 {
                    return next;
                } else {
                    // Switch to next list
                    level -= 1;
                }
            }
        }
    }

    fn find_less_then(&self, key: &[u8]) -> NonNull<InlineNode<'a>> {
        let mut x = self.head;
        let mut level = self.max_height() - 1;
        loop {
            assert!(x == self.head || self.comparator.lt(InlineNode::key(x.as_ptr()), key));

            let next = InlineNode::next(x.as_ptr(), level);
            if next.is_none() || self.comparator.ge(InlineNode::key(next.unwrap().as_ptr()), key) {
                if level == 0 {
                    break;
                } else {
                    level -= 1;
                }
            } else {
                x = next.unwrap();
            }
        }
        x
    }

    fn find_last(&self) -> NonNull<InlineNode<'a>> {
        let mut x = self.head;
        let mut level = self.max_height() - 1;
        loop {
            let next = InlineNode::next(x.as_ptr(), level);
            if next.is_none() {
                if level == 0 {
                    break;
                } else {
                    level -= 1;
                }
            } else {
                x = next.unwrap();
            }
        }
        x
    }

    fn random_height(&self) -> usize {
        let mut height = 1usize;
        while height < MAX_HEIGHT && random::<usize>() % BRANCHING == 0 {
            height += 1;
        }
        assert!(height > 0usize);
        assert!(height <= MAX_HEIGHT);
        height
    }

    fn key_is_after_node(&self, key: &[u8], n: Option<NonNull<InlineNode<'a>>>) -> bool {
        match n {
            Some(node_ptr) =>
                self.comparator.lt(InlineNode::key(node_ptr.as_ptr()), key),
            None => false
        }
    }

    fn equal(&self, a: &[u8], b: &[u8]) -> bool {
        matches!(self.comparator.cmp(a, b), Some(cmp::Ordering::Equal))
    }

    fn max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed) as usize
    }
}

struct InlineNode<'a> {
    height: usize,
    key_len: usize,
    next: &'a mut [AtomicPtr<InlineNode<'a>>],
    internal_key: &'a [u8],
}

impl<'a> InlineNode<'a> {
    fn with_key_value(height: usize, arena: &mut dyn Allocator, tag: Tag, sequence_number: u64, key: &[u8], value: &[u8]) -> NonNull<Self> {
        let payload_len = key.len() + TAG_SIZE + value.len();
        let node = unsafe { Self::new(height, arena, payload_len, key.len() + TAG_SIZE) };
        let internal_key = unsafe { Self::internal_key_mut(node.as_ptr()) };
        assert_eq!(payload_len, internal_key.len());

        let mut offset = 0;
        (&mut internal_key[offset..offset + key.len()])
            .write(key).unwrap();
        offset += key.len();

        let tail = sequence_number | tag.to_flag();
        (&mut internal_key[offset..offset + TAG_SIZE])
            .write(&tail.to_le_bytes()).unwrap();
        offset += TAG_SIZE;

        (&mut internal_key[offset..offset + value.len()])
            .write(value).unwrap();
        node
    }

    fn with_deletion_key(height: usize, arena: &mut dyn Allocator, sequence_number: u64, key: &[u8]) -> NonNull<Self> {
        Self::with_key_value(height, arena, Tag::Deletion, sequence_number, key, Default::default())
    }

    unsafe fn new<A: Allocator + ?Sized>(height: usize, arena: &mut A, payload_len: usize, key_len: usize) -> NonNull<Self> {
        let next_len_in_bytes = size_of::<AtomicPtr<Self>>() * height;
        let layout = Self::layout(next_len_in_bytes + payload_len);
        let chunk = arena.allocate(layout).unwrap();
        assert_eq!(layout.size(), chunk.as_ref().len());
        let mut node = NonNull::new(chunk.as_ptr() as *mut Self).unwrap();
        node.as_mut().height = height;
        assert!(key_len <= payload_len);
        node.as_mut().key_len = key_len;

        let next_addr = (chunk.as_ptr() as *mut u8).add(size_of::<Self>());
        node.as_mut().next = slice::from_raw_parts_mut(next_addr as *mut AtomicPtr<Self>, height);
        // fill to 0 for initialization
        slice::from_raw_parts_mut(next_addr, next_len_in_bytes).fill(0);

        let payload_addr = next_addr.add(next_len_in_bytes);
        node.as_mut().internal_key = slice::from_raw_parts(payload_addr, payload_len);
        node
    }

    fn with_height(this: *mut Self, height: usize) -> NonNull<Self> {
        unsafe { (*this).height = height }
        NonNull::new(this).unwrap()
    }

    fn next(node: *const Self, i: usize) -> Option<NonNull<Self>> {
        let slice = unsafe { &(*node).next };
        NonNull::new(slice[i].load(Ordering::Acquire))
    }

    fn set_next(node: *mut Self, i: usize, next: Option<NonNull<Self>>) {
        let slice = unsafe { &(*node).next };
        match next {
            Some(ptr) => {
                slice[i].store(ptr.as_ptr(), Ordering::Release);
            }
            None => {
                slice[i].store(0 as *mut Self, Ordering::Release);
            }
        }
    }

    fn no_barrier_next(node: *const Self, i: usize) -> Option<NonNull<Self>> {
        let slice = unsafe { &(*node).next };
        NonNull::new(slice[i].load(Ordering::Relaxed))
    }

    fn no_barrier_set_next(node: *mut Self, i: usize, next: Option<NonNull<Self>>) {
        let slice = unsafe { &(*node).next };
        match next {
            Some(ptr) => {
                slice[i].store(ptr.as_ptr(), Ordering::Relaxed);
            }
            None => {
                slice[i].store(0 as *mut Self, Ordering::Relaxed);
            }
        }
    }

    fn internal_key(this: *const Self) -> &'a [u8] {
        unsafe { (*this).internal_key }
    }

    fn user_key(this: *const Self) -> &'a [u8] {
        &Self::key(this)[..Self::key_len(this) - TAG_SIZE]
    }

    fn key(this: *const Self) -> &'a [u8] {
        &Self::internal_key(this)[..Self::key_len(this)]
    }

    fn key_len(this: *const Self) -> usize {
        unsafe { (*this).key_len }
    }

    fn value(this: *const Self) -> &'a [u8] {
        &Self::internal_key(this)[Self::key_len(this)..]
    }

    unsafe fn internal_key_mut(this: *mut Self) -> &'a mut [u8] {
        let payload_addr = (this as *mut u8)
            .add(size_of::<Self>())
            .add((*this).height * size_of::<AtomicPtr<Self>>());
        slice::from_raw_parts_mut(payload_addr, Self::internal_key(this).len())
    }

    fn layout(total_len: usize) -> Layout {
        let header_layout = Layout::new::<Self>();
        Layout::from_size_align(header_layout.size() + total_len, header_layout.align()).unwrap()
    }
}

pub struct IteratorImpl<'a, Cmp> {
    owns: *const InlineSkipList<'a, Cmp>,
    node: Option<NonNull<InlineNode<'a>>>,
}

impl<'a, Cmp: for<'b> Comparing<&'b [u8]>> IteratorImpl<'a, Cmp> {
    pub fn new(owns: *const InlineSkipList<'a, Cmp>) -> Self {
        IteratorImpl {
            owns,
            node: None,
        }
    }

    pub fn valid(&self) -> bool {
        self.node.is_some()
    }

    pub fn key(&self) -> Option<&[u8]> {
        match self.node {
            Some(node_ptr) => {
                Some(InlineNode::key(node_ptr.as_ptr()))
            }
            None => None
        }
    }

    pub fn value(&self) -> Option<&'a [u8]> {
        match self.node {
            Some(node_ptr) => {
                Some(InlineNode::value(node_ptr.as_ptr()))
            }
            None => None
        }
    }

    pub fn next(&mut self) {
        self.node = InlineNode::next(self.node.unwrap().as_ptr(), 0);
        //dbg!(self.node);
    }

    pub fn prev(&mut self) {
        self.node = Some(self.owns().find_less_then(&self.key().unwrap()));
        if self.node == Some(self.owns().head) {
            self.node = None;
        }
    }

    pub fn seek(&mut self, key: &[u8]) {
        self.node = self.owns().find_greater_or_equal(key, &mut [None; 0]);
    }

    pub fn seek_to_first(&mut self) {
        self.node = InlineNode::next(self.owns().head.as_ptr(), 0);
        //dbg!(self.node);
    }

    pub fn seek_to_last(&mut self) {
        self.node = Some(self.owns().find_last());
        if self.node == Some(self.owns().head) {
            self.node = None;
        }
    }

    fn owns(&self) -> &InlineSkipList<'a, Cmp> {
        unsafe { &*self.owns }
    }
}

impl<'a, Cmp: for<'b> Comparing<&'b [u8]>> Iterator for IteratorImpl<'a, Cmp> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.node {
            Some(node_ptr) => unsafe {
                IteratorImpl::next(self);
                Some(InlineNode::key(node_ptr.as_ptr()).to_vec())
            }
            None => None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{BitwiseComparator, Comparator};
    use crate::storage::key::{InternalKey, InternalKeyComparator};

    use super::*;

    struct KeyComparator {
        internal_key_cmp: InternalKeyComparator,
    }

    impl Comparing<&[u8]> for KeyComparator {
        fn cmp(&self, lhs: &[u8], rhs: &[u8]) -> Option<cmp::Ordering> {
            Some(self.internal_key_cmp.compare(lhs, rhs))
        }
    }

    #[test]
    fn sanity() {
        let mut arena = Arena::new_ref();
        let map = InlineSkipList::new(arena.get_mut(), new_key_cmp());
        map.insert_key_value(1, "111".as_bytes(), "aaaaa".as_bytes());
        map.insert_key_value(2, "111".as_bytes(), "bbbbb".as_bytes());

        let mut iter = map.iter();
        assert!(iter.valid());
        let mut key = InternalKey::parse(iter.key().unwrap());
        assert_eq!("111".as_bytes(), key.user_key);
        assert_eq!("bbbbb".as_bytes(), iter.value().unwrap());
        assert_eq!(2, key.sequence_number);
        assert_eq!(Tag::Key, key.tag);

        iter.next();
        key = InternalKey::parse(iter.key().unwrap());
        assert!(iter.valid());
        assert_eq!("111".as_bytes(), key.user_key);
        assert_eq!("aaaaa".as_bytes(), iter.value().unwrap());
        assert_eq!(1, key.sequence_number);
        assert_eq!(Tag::Key, key.tag);

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn seek_keys() {
        let pairs = [("1", "a"), ("11", "b"), ("111", "c"), ("1111", "d")];
        let mut arena = Arena::new_ref();
        let mut map = InlineSkipList::new(arena.get_mut(), new_key_cmp());

        add_key_value_pairs(&mut map, 1, &pairs);

        let mut iter = map.iter();
        for (key, value) in pairs {
            let internal_key = InternalKey::from_key(key.as_bytes(), 5, Tag::Key);
            iter.seek(&internal_key);
            assert!(iter.valid());
            assert_eq!(value.as_bytes(), iter.value().unwrap());
        }
    }

    #[test]
    fn large_insertion() {
        let n = 10000;
        let mut arena = Arena::new_ref();
        let map = InlineSkipList::new(arena.get_mut(), new_key_cmp());

        let mut sn = 1;
        for i in 0..n {
            let key = format!("{}", i);
            let value = format!("v:{}", key);
            map.insert_key_value(sn, key.as_bytes(), value.as_bytes());
            sn += 1
        }

        let mut iter = map.iter();
        for i in 0..n {
            let key = format!("{}", i);
            let value = format!("v:{}", key);
            iter.seek(&InternalKey::from_key(key.as_bytes(), sn, Tag::Key));
            assert!(iter.valid());
            assert_eq!(value.as_bytes(), iter.value().unwrap());
        }
    }

    fn add_key_value_pairs(map: &mut InlineSkipList<KeyComparator>, sequence_number: u64,
                           pairs: &[(&str, &str)]) {
        let mut sn = sequence_number;
        for (key, value) in pairs {
            map.insert_key_value(sn, key.as_bytes(), value.as_bytes());
            sn += 1
        }
    }

    fn new_key_cmp() -> KeyComparator {
        KeyComparator {
            internal_key_cmp: InternalKeyComparator::new(Rc::new(BitwiseComparator {}))
        }
    }
}