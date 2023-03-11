use std::alloc::{alloc, Layout};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::ptr::{addr_of, NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};

use rand::prelude::*;

use crate::arena::Arena;
use crate::comparator::{Comparator, Comparing};

//type Comparator = Box<dyn for<'a> Comparing<&'a [u8]>>;

const MAX_HEIGHT: usize = 12usize;
const BRANCHING: usize = 4usize;

pub struct SkipList<'a, Key> {
    arena: &'a mut Arena,
    comparator: Comparator,
    max_height: AtomicU32,
    head: NonNull<Node<Key>>,
}

impl<Key: Default + 'static> SkipList<'_, Key> {
    pub fn new(arena: &mut Arena, comparator: Comparator) -> SkipList<'_, Key> {
        let head = Node::new(arena, Key::default(), MAX_HEIGHT).unwrap();
        SkipList {
            arena,
            comparator,
            head,
            max_height: AtomicU32::new(0),
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
    fn find_greater_or_equal(&self, key: &Key, prev: Option<&mut [Option<NonNull<Node<Key>>>]>) -> Option<NonNull<Node<Key>>> {
        let mut x = &self.head;
        let mut level = (self.max_height() - 1) as usize;
        loop {
            let next = Node::<Key>::next(x.as_ptr(), level);
            if self.key_is_after_node(key, next) {
                x = &next.unwrap();
            } else {
                if let Some(prev_rv) = prev {
                    prev_rv[level] = Some(x.clone());
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
    const fn random_height(&self) -> usize {
        let mut height = 1usize;
        while height < MAX_HEIGHT && random::<usize>() % BRANCHING == 0 {
            height += 1;
        }
        assert!(height > 0usize);
        assert!(height <= MAX_HEIGHT);
        height
    }

    fn key_is_after_node(&self, key: &Key, n: Option<NonNull<Node<Key>>>) -> bool {
        match n {
            Some(&node_ptr) => {
                self.comparator.lt(node_ptr.as_ref().key, key)
            }
            None => false
        }
    }

    fn max_height(&self) -> u32 {
        self.max_height.load(Ordering::Relaxed)
    }
}

struct Node<Key> {
    key: Key,
    height: usize,
}


impl<Key: 'static> Node<Key> {
    fn new(arena: &mut Arena, key: Key, height: usize) -> Option<NonNull<Node<Key>>> {
        let size_in_bytes = size_of::<Node<Key>>() + size_of::<AtomicPtr<Node<Key>>>() * height;
        let layout = Layout::from_size_align(size_in_bytes, align_of::<Node<Key>>()).unwrap();
        let node_ptr = unsafe {
            let node = arena.allocate(layout).unwrap().as_ptr() as *mut Node<Key>;
            (*node).key = key;
            node
        };
        NonNull::new(node_ptr)
    }

    fn next(node: *const Self, i: usize) -> Option<NonNull<Self>> {
        let slice = unsafe { Self::next_slice(node) };
        NonNull::new(slice[i].load(Ordering::Acquire))
    }

    //fn set_next(node: *const Self, i: usize)

    fn no_barrier_next(node: *const Self, i: usize) -> Option<NonNull<Self>> {
        let slice = unsafe { Self::next_slice(node) };
        NonNull::new(slice[i].load(Ordering::Relaxed))
    }

    unsafe fn next_slice(node: *const Self) -> &'static [AtomicPtr<Self>] {
        let base = (node as *const u8).add(size_of::<Self>()) as *const AtomicPtr<Self>;
        &*slice_from_raw_parts(base, (*node).height)
    }

    unsafe fn next_slice_mut(node: *mut Self) -> &'static mut [AtomicPtr<Self>] {
        let base = (node as *mut u8).add(size_of::<Self>()) as *mut AtomicPtr<Self>;
        &mut *slice_from_raw_parts_mut(base, (*node).height)
    }
}