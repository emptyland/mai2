use std::alloc::{alloc, dealloc, Layout};
use std::cell::RefCell;
use std::{iter, ptr};
use std::fmt::{Debug, Display, Formatter};
use std::mem::size_of;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::ptr::{addr_of_mut, copy_nonoverlapping, NonNull, slice_from_raw_parts_mut, write};
use std::rc::Rc;

use crate::base::utils::round_up;

const NORMAL_PAGE_SIZE: usize = 16 * 1024;
const LARGE_PAGE_THRESHOLD_SIZE: usize = NORMAL_PAGE_SIZE / 2;

pub trait Allocator {
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()>;
}

#[derive(Debug)]
pub struct Arena {
    pages: Option<NonNull<NormalPage>>,
    large: Option<NonNull<LargePage>>,
    pub rss_in_bytes: usize,
    pub use_in_bytes: usize,
}

#[derive(Debug)]
struct NormalPage {
    next: Option<NonNull<NormalPage>>,
    free: *mut u8,
    remaining: usize,
    chunk: [u8; NORMAL_PAGE_SIZE],
}

#[derive(Debug)]
struct LargePage {
    next: Option<NonNull<LargePage>>,
    size: usize,
}

impl Arena {
    pub fn new() -> Self {
        Arena { pages: None, large: None, rss_in_bytes: 0, use_in_bytes: 0 }
    }

    pub fn new_rc() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new()))
    }

    pub fn normal_pages_count(&self) -> i32 {
        let mut head = &self.pages;
        let mut count = 0;
        loop {
            match head {
                Some(page) => unsafe {
                    count += 1;
                    head = &page.as_ref().next;
                }
                None => break count
            }
        }
    }

    fn allocate_large(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        self.large = unsafe { LargePage::new(self.large, layout) };
        self.use_in_bytes += layout.size();
        self.rss_in_bytes += layout.size() + size_of::<LargePage>();
        let free = {
            let chunk = self.large.unwrap().as_ptr() as *mut u8;
            let free = round_up(chunk, layout.align() as i64);
            NonNull::new(slice_from_raw_parts_mut(free, layout.size())).unwrap()
        };
        Ok(free)
    }
}

impl Allocator for Arena {
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        if layout.size() + layout.align() > LARGE_PAGE_THRESHOLD_SIZE {
            return self.allocate_large(layout.clone());
        }
        match self.pages {
            Some(mut head) => unsafe {
                let chunk = head.as_mut().allocate(layout);
                match chunk {
                    Some(ptr) => {
                        self.use_in_bytes += layout.size();
                        Ok(ptr)
                    }
                    None => {
                        self.pages = NormalPage::new(self.pages);
                        self.rss_in_bytes += size_of::<NormalPage>();
                        self.allocate(layout)
                    }
                }
            }
            None => {
                self.pages = NormalPage::new(self.pages);
                self.rss_in_bytes += size_of::<NormalPage>();
                self.allocate(layout)
            }
        }
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        loop {
            let head = self.pages;
            match head {
                Some(page) => unsafe {
                    self.pages = page.as_ref().next;
                    NormalPage::free(page)
                },
                None => break
            }
        }
        loop {
            let head = self.large;
            match head {
                Some(page) => unsafe {
                    self.large = page.as_ref().next;
                    LargePage::free(page)
                },
                None => break
            }
        }
    }
}

impl NormalPage {
    pub fn new(next: Option<NonNull<NormalPage>>) -> Option<NonNull<NormalPage>> {
        let layout = Layout::new::<NormalPage>();
        unsafe {
            let page = NonNull::new(alloc(layout) as *mut NormalPage);
            if let Some(none_null) = page {
                let mut naked_page = none_null.as_ptr();
                (*naked_page).next = next;
                (*naked_page).free = &mut (*naked_page).chunk[0] as *mut u8;
                (*naked_page).remaining = (*naked_page).chunk.len();
            }
            page
        }
    }

    pub unsafe fn free(page: NonNull<NormalPage>) {
        let layout = Layout::new::<NormalPage>();
        dealloc(page.as_ptr() as *mut u8, layout);
    }

    pub fn allocate(&mut self, layout: Layout) -> Option<NonNull<[u8]>> {
        let aligned = round_up(self.free, layout.align() as i64);
        let padding_size = (aligned as u64 - self.free as u64) as usize;

        if self.remaining < layout.size() + padding_size {
            return None;
        }
        unsafe {
            self.free = aligned.add(layout.size());

            let base = self as *mut Self as *mut u8;
            let limit = base.add(size_of::<Self>());
            assert!(self.free > base && self.free <= limit);
        }
        self.remaining -= layout.size() + padding_size;
        NonNull::new(slice_from_raw_parts_mut(aligned, layout.size()))
    }
}

impl LargePage {
    pub unsafe fn new(next: Option<NonNull<Self>>, payload_layout: Layout) -> Option<NonNull<Self>> {
        let page_layout = Layout::new::<Self>();
        let layout = Layout::from_size_align(page_layout.size() +
                                                 payload_layout.size() +
                                                 payload_layout.align(), page_layout.align()).unwrap();
        let page = NonNull::new(alloc(layout) as *mut LargePage);
        if let Some(none_null) = page {
            let mut naked_page = none_null.as_ptr();
            (*naked_page).next = next;
            (*naked_page).size = layout.size();
        }
        page
    }

    pub unsafe fn free(page: NonNull<Self>) {
        let page_layout = Layout::new::<Self>();
        let layout = Layout::from_size_align(page_layout.size() +
                                                 page.as_ref().size, page_layout.align()).unwrap();
        dealloc(page.as_ptr() as *mut u8, layout);
    }
}

pub struct ScopedMemory {
    block: [u8; 128],
    chunk: Vec<u8>,
}

impl ScopedMemory {
    pub fn new() -> Self {
        Self {
            block: [0; 128],
            chunk: Vec::new(),
        }
    }
}

impl Allocator for ScopedMemory {
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        let non_null = if layout.size() + layout.align() <= self.block.len() {
            let ptr = round_up(addr_of_mut!(self.block[0]), layout.align() as i64);
            NonNull::new(slice_from_raw_parts_mut(ptr, layout.size()))
        } else {
            self.chunk.extend(iter::repeat(0).take(layout.size() + layout.align()));
            let ptr = round_up(addr_of_mut!(self.chunk.as_mut_slice()[0]), layout.align() as i64);
            NonNull::new(slice_from_raw_parts_mut(ptr, layout.size()))
        };

        match non_null {
            Some(ptr) => Ok(ptr),
            None => Err(())
        }
    }
}

pub struct ArenaBox<T: ?Sized> {
    naked: NonNull<T>,
    owns: Rc<RefCell<Arena>>,
}

impl <T> ArenaBox<T> {
    pub fn new(data: T, owns: &Rc<RefCell<Arena>>) -> Self {
        let layout = Layout::new::<T>();
        let chunk = owns.borrow_mut().allocate(layout).unwrap();
        let naked = NonNull::new(chunk.as_ptr() as *mut T).unwrap();
        unsafe { write(naked.as_ptr(), data) }
        Self {
            naked,
            owns: owns.clone(),
        }
    }
}

impl<T: ?Sized> ArenaBox<T> {
    pub fn from_ptr(naked: NonNull<T>, owns: Rc<RefCell<Arena>>) -> Self {
        Self {
            naked, owns
        }
    }

    pub fn ptr(&self) -> NonNull<T> { self.naked }
    pub fn owns(&self) -> Rc<RefCell<Arena>> { self.owns.clone() }
}

impl<T: ?Sized> Deref for ArenaBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.naked.as_ref() }
    }
}

impl<T: ?Sized> DerefMut for ArenaBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.naked.as_mut() }
    }
}

impl <T: ?Sized> Clone for ArenaBox<T> {
    fn clone(&self) -> Self {
        Self {
            naked: self.naked.clone(),
            owns: self.owns.clone(),
        }
    }
}

impl <T: Debug> Debug for ArenaBox<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArenaBox<T>")
            .field(self.deref())
            .finish()
    }
}

pub struct ArenaStr {
    naked: NonNull<str>
}

impl ArenaStr {
    pub fn new(str: &str, alloc: &mut dyn Allocator) -> Self {
        let layout = Layout::from_size_align(str.len(), 4).unwrap();
        let mut chunk = alloc.allocate(layout).unwrap();
        let naked = unsafe {
            copy_nonoverlapping(str.as_ptr(), &mut chunk.as_mut()[0], str.len());
            NonNull::new(chunk.as_ptr() as *mut str).unwrap()
        };
        Self { naked }
    }

    pub fn from_string(str: &String, alloc: &mut dyn Allocator) -> Self {
        Self::new(str.as_str(), alloc)
    }

    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }

    pub fn as_str(&self) -> &str {
        unsafe { self.naked.as_ref() }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl Debug for ArenaStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArenaStr")
            .field(&self.to_string())
            .finish()
    }
}

impl PartialEq for ArenaStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Clone for ArenaStr {
    fn clone(&self) -> Self {
        Self { naked: self.naked }
    }
}

impl Display for ArenaStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}


pub struct ArenaVec<T> {
    naked: NonNull<[T]>,
    len: usize,
    owns: Rc<RefCell<Arena>>,
}

impl<T> ArenaVec<T> {
    pub fn new(owns: &Rc<RefCell<Arena>>) -> Self {
        let naked = unsafe {
            Self::new_uninitialized(owns.borrow_mut().deref_mut(), 1)
        };
        Self {
            naked,
            len: 0,
            owns: owns.clone(),
        }
    }

    pub fn push(&mut self, elem: T) {
        self.extend_if_needed(1);
        unsafe { write(&mut self.naked.as_mut()[self.len - 1], elem) }
    }

    pub fn len(&self) -> usize { self.len }

    pub fn capacity(&self) -> usize { unsafe { self.naked.as_ref() }.len() }

    pub fn as_slice(&self) -> &[T] {
        unsafe { &self.naked.as_ref()[..self.len] }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { &mut self.naked.as_mut()[..self.len] }
    }

    fn extend_if_needed(&mut self, incremental: usize) {
        if self.len() + incremental > self.capacity() {
            let new_cap = self.capacity() * 2 + 4;
            let naked = unsafe {
                Self::new_uninitialized(self.owns.borrow_mut().deref_mut(), new_cap)
            };
            let old_raw_vec = self.naked;
            self.naked = naked;
            unsafe {
                let src = &old_raw_vec.as_ref()[0] as *const T;
                let dst = &mut self.naked.as_mut()[0] as *mut T;
                ptr::copy_nonoverlapping(src, dst, self.len);
            }
        }

        self.len += incremental;
    }

    unsafe fn new_uninitialized(alloc: &mut dyn Allocator, capacity: usize) -> NonNull<[T]> {
        let elem_layout = Layout::new::<T>();
        let layout = Layout::from_size_align_unchecked(elem_layout.size() * capacity,
                                                       elem_layout.align());
        let chunk = alloc.allocate(layout).unwrap();
        let addr = chunk.as_ptr() as *mut T;
        NonNull::new(slice_from_raw_parts_mut(addr, capacity)).unwrap()
    }
}

impl <T> Index<usize> for ArenaVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl <T> IndexMut<usize> for ArenaVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

impl <T: Debug> Debug for ArenaVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut r = f.debug_list();
        for i in 0..self.len {
            r.entry(&self[i]);
        }
        r.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn sanity() {
        let mut arena = Arena::new();
        let s: [u8; 16] = [1; 16];
        let mut chunk = arena.allocate(Layout::from_size_align(16, 4).unwrap())
            .expect("TODO: panic message");
        unsafe {
            chunk.as_mut().write(&s).unwrap();
            assert_eq!(s, chunk.as_ref());
        }
        assert_eq!(1, arena.normal_pages_count());
    }

    #[test]
    fn allocation() {
        let mut arena = Arena::new();
        let s = "hello";
        let mut chunk = arena.allocate(Layout::from_size_align(s.len(), 4).unwrap()).unwrap();
        unsafe {
            chunk.as_mut().write(s.as_bytes()).unwrap();
            assert_eq!(s.as_bytes(), chunk.as_ref());
        }
    }

    #[test]
    fn arena_vec() {
        let arena = Arena::new_rc();
        let mut vec = ArenaVec::new(&arena);
        vec.push(1);
        vec.push(2);
        vec.push(3);
        assert_eq!(6, vec.capacity());
        assert_eq!(3, vec.len());
        assert_eq!(1, vec[0]);
        assert_eq!(2, vec[1]);
        assert_eq!(3, vec[2]);
    }

    #[test]
    fn arena_vec_large_push() {
        let arena = Arena::new_rc();
        let mut vec = ArenaVec::new(&arena);
        let n = 10000;

        for i in 0..n {
            vec.push(i);
        }
        assert_eq!(n, vec.len);
        for i in 0..n {
            assert_eq!(i, vec[i]);
        }
    }
}