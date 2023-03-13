use std::alloc::{alloc, Layout};
use std::cmp;
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::{addr_of, NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use rand::prelude::*;

use crate::arena::Arena;
use crate::comparator::Comparing;

//type Comparator = Box<dyn for<'a> Comparing<&'a [u8]>>;

const MAX_HEIGHT: usize = 12usize;
const BRANCHING: usize = 4usize;

pub struct SkipList<'a, Key: 'a, Cmp> {
    arena: &'a mut Arena,
    comparator: Cmp,
    max_height: AtomicU32,
    head: NonNull<Node<Key>>,
}

impl<'a, Key: Default + Clone + Copy + 'a, Cmp: Comparing<&'a Key>> SkipList<'_, Key, Cmp> {
    pub fn new(arena: &mut Arena, comparator: Cmp) -> SkipList<'_, Key, Cmp> {
        let head = Node::new(arena, Key::default(), MAX_HEIGHT).unwrap();
        SkipList {
            arena,
            comparator,
            head,
            max_height: AtomicU32::new(1),
        }
    }

    // void Put(Key key) {
    //     // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
    //     // here since Insert() is externally synchronized.
    //     Node* prev[kMaxHeight];
    //     Node* x = FindGreaterOrEqual(key, prev);

    //     // Our data structure does not allow duplicate insertion
    //     DCHECK(x == NULL || !Equal(key, x->key));

    //     int height = RandomHeight();
    //     if (height > max_height()) {
    //         for (int i = max_height(); i < height; i++) {
    //             prev[i] = head_;
    //         }
    //         //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    //         // It is ok to mutate max_height_ without any synchronization
    //         // with concurrent readers.  A concurrent reader that observes
    //         // the new value of max_height_ will see either the old value of
    //         // new level pointers from head_ (NULL), or a new value set in
    //         // the loop below.  In the former case the reader will
    //         // immediately drop to the next level since NULL sorts after all
    //         // keys.  In the latter case the reader will use the new node.
    //         max_height_.store(height, std::memory_order_relaxed);
    //     }

    //     x = NewNode(key, height);
    //     for (int i = 0; i < height; i++) {
    //         // NoBarrier_SetNext() suffices since we will add a barrier when
    //         // we publish a pointer to "x" in prev[i].
    //         x->nobarrier_set_next(i, prev[i]->nobarrier_next(i));
    //         prev[i]->set_next(i, x);
    //     }
    // }

    pub fn put(&mut self, key: &'a Key) {
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

        x = Node::new(self.arena, *key, height);
        for i in 0..height {
            Node::no_barrier_set_next(x.unwrap().as_ptr(), i,
                                      Node::no_barrier_next(prev[i].unwrap().as_ptr(), i));
            Node::set_next(prev[i].unwrap().as_ptr(), i, x);
        }
    }

    // Node *FindGreaterOrEqual(Key key, Node** prev) const {
    //     auto x = head_;
    //     int level = max_height() - 1;
    //     while (true) {
    //         Node* next = x->next(level);
    //         if (KeyIsAfterNode(key, next)) {
    //             // Keep searching in this list
    //             x = next;
    //         } else {
    //             if (prev != NULL) prev[level] = x;
    //             if (level == 0) {
    //                 return next;
    //             } else {
    //                 // Switch to next list
    //                 level--;
    //             }
    //         }
    //     }
    // }
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

    // int RandomHeight() {
    //     int height = 1;

    //     while (height < kMaxHeight && (rand_() % kBranching) == 0) {
    //         height++;
    //     }

    //     DCHECK_GT(height, 0);
    //     DCHECK_LE(height, kMaxHeight);

    //     return height;
    // }
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
        matches!(self.comparator.compare(a, b), cmp::Ordering::Equal)
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
    fn new(arena: &mut Arena, key: Key, height: usize) -> Option<NonNull<Node<Key>>> {
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

struct IteratorImpl<'a, Key: 'a, Cmp> {
    own: &'a SkipList<'a, Key, Cmp>,
    node: Option<NonNull<Node<Key>>>
}

impl <'a, Key: Default + Clone + Copy + 'a, Cmp: Comparing<&'a Key>> IteratorImpl<'_, Key, Cmp> {
    pub fn new(own: &'a SkipList<'a, Key, Cmp>) -> IteratorImpl<'_, Key, Cmp> {
        IteratorImpl {
            own,
            node: None
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
        self.node = self.own.find_greater_or_equal(key, &mut [None; 0]);
    }

    pub fn seek_to_first(&mut self) {
        self.node = Node::next(self.own.head.as_ptr(), 0);
        //dbg!(self.node);
    }
}

impl <Key: Clone, Cmp> Iterator for IteratorImpl<'_, Key, Cmp> {
    type Item = Key;

    fn next(&mut self) -> Option<Self::Item> {
        match self.node {
            Some(node_ptr) => unsafe {
                Some(node_ptr.as_ref().key.clone())
            }
            None => None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::arena::Arena;
    use crate::key::KeyBundle;

    use super::*;

    struct KeyComparator;

    impl Comparing<&KeyBundle> for KeyComparator {
        fn compare(&self, lhs: &KeyBundle, rhs: &KeyBundle) -> cmp::Ordering {
            lhs.key().cmp(rhs.key())
        }
    }

    struct IntComparator;
    impl Comparing<&i32> for IntComparator {
        fn compare(&self, lhs: &i32, rhs: &i32) -> cmp::Ordering {
            lhs.cmp(rhs)
        }
    }

    #[test]
    fn sanity() {
        let mut arena = Arena::new();
        let key = KeyBundle::for_key_value(&mut arena, 1,
                                           "111".as_bytes(), "a".as_bytes());
        let cmp = KeyComparator {};
        let mut map = SkipList::new(&mut arena, cmp);
        map.put(&key);
    }

    #[test]
    fn iterate() {
        let mut arena = Arena::new();
        let cmp = IntComparator {};
        let mut map = SkipList::new(&mut arena, cmp);
        map.put(&1);

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
        let mut arena = Arena::new();
        let cmp = IntComparator {};
        let mut map = SkipList::new(&mut arena, cmp);
        for i in 0..100 {
            map.put(&i);
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
}