use rand::prelude::*;
use std::{cmp, ptr};
use std::alloc::{alloc, Layout};
use std::cell::RefCell;
use std::mem::{align_of, size_of};
use std::ops::DerefMut;
use std::ptr::{addr_of, NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::rc::Rc;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use crate::arena::{Allocator, Arena};

//type Comparator = Box<dyn for<'a> Comparing<&'a [u8]>>;

const MAX_HEIGHT: usize = 12usize;
const BRANCHING: usize = 4usize;

pub struct SkipList<Key, Cmp> {
    arena: Rc<RefCell<Arena>>,
    comparator: Cmp,
    max_height: AtomicU32,
    head: NonNull<Node<Key>>,
}


pub trait Comparing<T> {
    fn cmp(&self, lhs: T, rhs: T) -> Option<cmp::Ordering>;

    fn lt(&self, lhs: T, rhs: T) -> bool {
        matches!(self.cmp(lhs, rhs), Some(cmp::Ordering::Less))
    }

    fn le(&self, lhs: T, rhs: T) -> bool {
        !matches!(self.cmp(lhs, rhs), Some(cmp::Ordering::Greater))
    }

    fn gt(&self, lhs: T, rhs: T) -> bool {
        matches!(self.cmp(lhs, rhs), Some(cmp::Ordering::Greater))
    }

    fn ge(&self, lhs: T, rhs: T) -> bool {
        !matches!(self.cmp(lhs, rhs), Some(cmp::Ordering::Less))
    }
}

impl<'a, Key: Default + Clone + Copy + 'a, Cmp: Comparing<&'a Key>> SkipList<Key, Cmp> {
    pub fn new(arena: Rc<RefCell<Arena>>, comparator: Cmp) -> SkipList<Key, Cmp> {
        let head = Node::new(arena.borrow_mut().deref_mut(), Key::default(), MAX_HEIGHT).unwrap();
        SkipList {
            arena,
            comparator,
            head,
            max_height: AtomicU32::new(1),
        }
    }

    pub fn insert(&self, key: &'a Key) {
        let mut prev: [Option<NonNull<Node<Key>>>; MAX_HEIGHT] = [None; MAX_HEIGHT];
        let mut x = self.find_greater_or_equal(key, &mut prev);

        unsafe {
            assert!(matches!(x, None) || !self.equal(&x.unwrap().as_ref().key, key));
        }
        let height = self.random_height();
        if height > self.max_height() {
            for i in self.max_height()..height {
                prev[i] = Some(self.head.clone());
            }
            self.max_height.store(height as u32, Ordering::Relaxed);
        }

        x = Node::new(self.arena.borrow_mut().deref_mut(), *key, height);
        for i in 0..height {
            Node::no_barrier_set_next(x.unwrap().as_ptr(), i,
                                      Node::no_barrier_next(prev[i].unwrap().as_ptr(), i));
            Node::set_next(prev[i].unwrap().as_ptr(), i, x);
        }
    }

    pub fn iter(&mut self) -> IteratorImpl<Key, Cmp> {
        let mut it = IteratorImpl::new(self);
        it.seek_to_first();
        it
    }

    fn find_greater_or_equal(&self, key: &'a Key, prev: &mut [Option<NonNull<Node<Key>>>]) -> Option<NonNull<Node<Key>>> {
        let mut x = self.head;
        let mut level = (self.max_height() - 1) as usize;
        loop {
            let next = Node::<Key>::next(x.as_ptr(), level);
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

    fn random_height(&self) -> usize {
        let mut height = 1usize;
        while height < MAX_HEIGHT && random::<usize>() % BRANCHING == 0 {
            height += 1;
        }
        assert!(height > 0usize);
        assert!(height <= MAX_HEIGHT);
        height
    }

    fn key_is_after_node(&self, key: &'a Key, n: Option<NonNull<Node<Key>>>) -> bool {
        match n {
            Some(node_ptr) => unsafe {
                self.comparator.lt(&node_ptr.as_ref().key, key)
            }
            None => false
        }
    }

    fn equal(&self, a: &'a Key, b: &'a Key) -> bool {
        matches!(self.comparator.cmp(a, b), Some(cmp::Ordering::Equal))
    }

    fn max_height(&self) -> usize {
        self.max_height.load(Ordering::Relaxed) as usize
    }
}

struct Node<Key> {
    key: Key,
    height: usize,
}


impl<Key> Node<Key> {
    fn new(arena: &mut dyn Allocator, key: Key, height: usize) -> Option<NonNull<Node<Key>>> {
        let size_in_bytes = size_of::<Self>() + size_of::<AtomicPtr<Self>>() * height + size_of::<usize>();
        //dbg!(size_in_bytes);
        let layout = Layout::from_size_align(size_in_bytes, align_of::<Self>()).unwrap();
        let node_ptr = unsafe {
            let node = arena.allocate(layout).unwrap().as_ptr() as *mut Self;
            (*node).key = key;
            (*node).height = height;
            for i in 0..height {
                Self::next_slice_mut(node)[i].store(0 as *mut Self, Ordering::Relaxed);
            }
            node
        };
        NonNull::new(node_ptr)
    }

    fn next(node: *const Self, i: usize) -> Option<NonNull<Self>> {
        let slice = unsafe { Self::next_slice(node) };
        NonNull::new(slice[i].load(Ordering::Acquire))
    }

    fn set_next(node: *mut Self, i: usize, next: Option<NonNull<Self>>) {
        let slice = unsafe { Self::next_slice_mut(node) };
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
        let slice = unsafe { Self::next_slice(node) };
        NonNull::new(slice[i].load(Ordering::Relaxed))
    }

    fn no_barrier_set_next(node: *mut Self, i: usize, next: Option<NonNull<Self>>) {
        let slice = unsafe { Self::next_slice_mut(node) };
        match next {
            Some(ptr) => {
                slice[i].store(ptr.as_ptr(), Ordering::Relaxed);
            }
            None => {
                slice[i].store(0 as *mut Self, Ordering::Relaxed);
            }
        }
    }

    unsafe fn next_slice<'a>(node: *const Self) -> &'a [AtomicPtr<Self>] {
        let base = (node as *const u8).add(size_of::<Self>()) as *const AtomicPtr<Self>;
        &*slice_from_raw_parts(base, (*node).height)
    }

    unsafe fn next_slice_mut<'a>(node: *mut Self) -> &'a mut [AtomicPtr<Self>] {
        let base = (node as *mut u8).add(size_of::<Self>()) as *mut AtomicPtr<Self>;
        &mut *slice_from_raw_parts_mut(base, (*node).height)
    }
}

pub struct IteratorImpl<Key, Cmp> {
    owns: *const SkipList<Key, Cmp>,
    node: Option<NonNull<Node<Key>>>,
}

impl<'a, Key: Default + Clone + Copy + 'a, Cmp: Comparing<&'a Key>> IteratorImpl<Key, Cmp> {
    pub fn new(owns: *const SkipList<Key, Cmp>) -> Self {
        IteratorImpl {
            owns,
            node: None,
        }
    }

    pub fn valid(&self) -> bool {
        !matches!(self.node, None)
    }

    pub fn key(&self) -> Option<&'a Key> {
        match self.node {
            Some(node_ptr) => unsafe {
                Some(&node_ptr.as_ref().key)
            }
            None => None
        }
    }

    pub fn next(&mut self) {
        self.node = Node::next(self.node.unwrap().as_ptr(), 0);
        //dbg!(self.node);
    }

    pub fn seek(&mut self, key: &'a Key) {
        self.node = self.owns().find_greater_or_equal(key, &mut [None; 0]);
    }

    pub fn seek_to_first(&mut self) {
        self.node = Node::next(self.owns().head.as_ptr(), 0);
        //dbg!(self.node);
    }

    fn owns(&self) -> &SkipList<Key, Cmp> {
        unsafe { &*self.owns }
    }
}

impl<'a, Key: Default + Clone + Copy + 'a, Cmp: Comparing<&'a Key>> Iterator for IteratorImpl<Key, Cmp> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        match self.node {
            Some(node_ptr) => unsafe {
                IteratorImpl::next(self);
                Some(node_ptr.as_ref().key)
            }
            None => None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fmt::format;
    use std::ops::{Deref, DerefMut};
    use std::rc::Rc;

    use crate::arena::Arena;
    use crate::key::{KeyBundle, Tag};

    use super::*;

    struct KeyComparator;

    impl Comparing<&KeyBundle> for KeyComparator {
        fn cmp(&self, lhs: &KeyBundle, rhs: &KeyBundle) -> Option<cmp::Ordering> {
            lhs.user_key().partial_cmp(rhs.user_key())
        }
    }

    struct IntComparator;

    impl Comparing<&i32> for IntComparator {
        fn cmp(&self, lhs: &i32, rhs: &i32) -> Option<cmp::Ordering> {
            lhs.partial_cmp(rhs)
        }
    }

    #[test]
    fn sanity() {
        let arena = Arena::new_rc();
        let key = KeyBundle::from_key_value(arena.borrow_mut().deref_mut(), 1,
                                            "111".as_bytes(), "a".as_bytes());
        let cmp = KeyComparator {};
        let map = SkipList::new(arena, cmp);
        map.insert(&key);
    }

    #[test]
    fn insert_keys() {
        let arena = Arena::new_rc();
        let cmp = KeyComparator {};
        let mut map = SkipList::new(arena.clone(), cmp);

        for i in 0..100 {
            //let mut borrowed = arena.borrow_mut();
            let s = format!("{:03}", i);
            let key = KeyBundle::from_key_value(arena.borrow_mut().deref_mut(), 1, s.as_bytes(), "a".as_bytes());
            map.insert(&key);
        }

        let mut i = 0;
        for bundle in map.iter() {
            assert_eq!(1, bundle.sequence_number());
            assert_eq!(Tag::Key, bundle.tag());

            let s = std::str::from_utf8(bundle.user_key()).unwrap();
            let z = format!("{:03}", i);
            assert_eq!(z.as_str(), s);

            let value = std::str::from_utf8(bundle.value()).unwrap();
            assert_eq!("a", value);

            i += 1;
        }

        let mut iter = IteratorImpl::new(&map);
        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!("000", std::str::from_utf8(iter.key().unwrap().user_key()).unwrap());
    }

    #[test]
    fn iterate() {
        let arena = Arena::new_rc();
        let cmp = IntComparator {};
        let map = SkipList::new(arena, cmp);
        map.insert(&1);

        let mut iter = IteratorImpl::new(&map);
        assert!(matches!(iter.key(), None));

        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!(Some(&1), iter.key());

        iter.next();
        assert!(!iter.valid());
    }

    #[test]
    fn iterate_more() {
        let arena = Arena::new_rc();
        let cmp = IntComparator {};
        let map = SkipList::new(arena, cmp);
        for i in 0..100 {
            map.insert(&i);
        }

        let mut iter = IteratorImpl::new(&map);
        iter.seek_to_first();
        let mut i = 0;
        while iter.valid() {
            assert_eq!(Some(&i), iter.key());
            iter.next();
            i += 1;
        }
        assert_eq!(100, i);
    }

    #[test]
    fn rust_iter() {
        let arena = Arena::new_rc();
        let cmp = IntComparator {};
        let mut map = SkipList::new(arena, cmp);
        for i in 0..100 {
            map.insert(&i);
        }

        let mut n = 0;
        for i in map.iter() {
            assert_eq!(n, i);
            n += 1;
        }
        assert_eq!(100, n);
    }
}