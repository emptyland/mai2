use std::{iter, ptr, slice};
use std::alloc::{alloc, dealloc, Layout};
use std::cell::Cell;
use std::cmp::{min, Ordering};
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::mem::{align_of, size_of};
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::ptr::{addr_of, addr_of_mut, copy_nonoverlapping, NonNull, slice_from_raw_parts_mut, write};

use crate::base::utils::round_up;

const NORMAL_PAGE_SIZE: usize = 4 * 1024;
const LARGE_PAGE_THRESHOLD_SIZE: usize = NORMAL_PAGE_SIZE / 2;

pub trait Allocator {
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()>;
}

pub struct ArenaVal<T: Sized> {
    core: T,
    mut_count: Cell<usize>,
}

impl<T: Allocator + Sized> ArenaVal<T> {
    pub fn new(arena: T) -> Self {
        Self {
            core: arena,
            mut_count: Cell::new(0),
        }
    }

    pub fn get_mut(&self) -> ArenaMut<T> {
        let ptr = addr_of!(self.core) as *mut T;
        self.mut_count.set(self.mut_count.get() + 1);
        ArenaMut {
            shadow: NonNull::new(ptr).unwrap()
        }
    }
}

impl<T> Deref for ArenaVal<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target { &self.core }
}

pub struct ArenaRef<T> {
    core: NonNull<T>,
    mut_count: Cell<usize>,
}

impl<T: Allocator> ArenaRef<T> {
    pub fn new(arena: T) -> Self {
        let layout = Layout::for_value(&arena);
        let ptr = unsafe {
            let chunk = alloc(layout) as *mut T;
            write(chunk, arena);
            chunk
        };
        Self {
            core: NonNull::new(ptr).unwrap(),
            mut_count: Cell::new(0),
        }
    }

    pub fn get_mut(&self) -> ArenaMut<T> { ArenaMut::new(self) }
}

impl<T> Deref for ArenaRef<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { self.core.as_ref() }
    }
}

impl<T> Drop for ArenaRef<T> {
    fn drop(&mut self) {
        let layout = Layout::new::<T>();
        unsafe {
            drop(ptr::read(self.core.as_ptr()));
            dealloc(self.core.as_ptr() as *mut u8, layout);
        }
    }
}

#[derive(Debug)]
pub struct ArenaMut<T> {
    shadow: NonNull<T>,
}

impl<T: Allocator> ArenaMut<T> {
    fn new(this: &ArenaRef<T>) -> Self {
        this.mut_count.set(this.mut_count.get() + 1);
        Self { shadow: this.core.clone() }
    }

    pub fn get_ref(&self) -> &T { unsafe { self.shadow.as_ref() } }
    pub fn get_mut(&self) -> &mut T { unsafe { &mut *self.shadow.as_ptr() } }
}

impl<T> Deref for ArenaMut<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        unsafe { self.shadow.as_ref() }
    }
}

impl<T> DerefMut for ArenaMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.shadow.as_mut() }
    }
}

impl<T> Clone for ArenaMut<T> {
    fn clone(&self) -> Self {
        Self { shadow: self.shadow }
    }
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

    pub fn new_ref() -> ArenaRef<Self> {
        ArenaRef::new(Self::new())
    }

    pub fn new_val() -> ArenaVal<Self> {
        ArenaVal::new(Self::new())
    }

    pub fn dup_str(&mut self, origin: &str) -> Result<&str, ()> {
        let dest = unsafe {
            let buf = self.dup_uninitialized_array(origin.as_bytes())?;
            copy_nonoverlapping(origin.as_ptr(), buf, origin.len());
            slice::from_raw_parts(buf, origin.len())
        };
        Ok(std::str::from_utf8(dest).unwrap())
    }

    unsafe fn dup_uninitialized_array<T>(&mut self, origin: &[T]) -> Result<*mut T, ()> {
        let layout = Layout::from_size_align(size_of::<T>() * origin.len(),
                                             align_of::<T>()).unwrap();
        let chunk = self.allocate(layout)?;
        Ok(chunk.as_ptr() as *mut T)
    }

    pub fn contains_box<T>(&self, p: &ArenaBox<T>) -> bool {
        self.contains(p.naked.as_ptr())
    }

    pub fn contains<T>(&self, p: *const T) -> bool {
        let mut head = &self.pages;
        let exists = loop {
            match head {
                Some(page) => unsafe {
                    if page.as_ref().contains(p) {
                        break true;
                    }
                    head = &page.as_ref().next;
                }
                None => break false
            }
        };
        if exists {
            return true;
        }
        let mut head = &self.large;
        loop {
            match head {
                Some(page) => unsafe {
                    if page.as_ref().contains(p) {
                        break true;
                    }
                    head = &page.as_ref().next;
                }
                None => break false
            }
        }
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
        self.rss_in_bytes += layout.size() + layout.align() + size_of::<LargePage>();
        let free = {
            let chunk = unsafe { self.large.unwrap().as_ptr().add(1) as *mut u8 };
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
            assert!(self.free > base);
            assert!(self.free <= limit);
        }
        self.remaining -= layout.size() + padding_size;
        NonNull::new(slice_from_raw_parts_mut(aligned, layout.size()))
    }

    pub fn contains<T>(&self, i: *const T) -> bool {
        let p = i as *const u8;
        let begin = addr_of!(self.chunk[0]);
        p >= begin && p < self.free
    }
}

impl LargePage {
    pub unsafe fn new(next: Option<NonNull<Self>>, payload_layout: Layout) -> Option<NonNull<Self>> {
        let page_layout = Layout::new::<Self>();
        let layout = Layout::from_size_align(page_layout.size() + payload_layout.size() + payload_layout.align(),
                                               page_layout.align()).unwrap();
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

    pub fn contains<T>(&self, i: *const T) -> bool {
        unsafe {
            let begin = (addr_of!(*self) as *const u8).add(size_of::<Self>());
            let end = begin.add(self.size);
            let p = i as *const u8;
            p >= begin && p < end
        }
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
}

impl<T> ArenaBox<T> {
    pub fn new<A: Allocator + ?Sized>(data: T, arena: &mut A) -> Self {
        let layout = Layout::new::<T>();
        let chunk = arena.allocate(layout).unwrap();
        let naked = NonNull::new(chunk.as_ptr() as *mut T).unwrap();
        unsafe { write(naked.as_ptr(), data) }
        Self { naked }
    }
}

impl<T> From<&T> for ArenaBox<T> {
    fn from(value: &T) -> Self {
        Self {
            naked: NonNull::new(value as *const T as *mut T).unwrap()
        }
    }
}

impl<T: ?Sized> From<NonNull<T>> for ArenaBox<T> {
    fn from(value: NonNull<T>) -> Self {
        Self {
            naked: value
        }
    }
}

impl<T: ?Sized> From<&mut T> for ArenaBox<T> {
    fn from(value: &mut T) -> Self {
        Self {
            naked: NonNull::new(value as *mut T).unwrap()
        }
    }
}

impl<T: ?Sized> ArenaBox<T> {
    pub fn from_ptr(naked: NonNull<T>) -> Self {
        Self { naked }
    }

    pub fn ptr(&self) -> NonNull<T> { self.naked }
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

impl<T: ?Sized> Clone for ArenaBox<T> {
    fn clone(&self) -> Self {
        Self { naked: self.naked.clone() }
    }
}

impl<T: Debug> Debug for ArenaBox<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArenaBox<T>")
            .field(self.deref())
            .finish()
    }
}

pub struct ArenaStr {
    naked: NonNull<str>,
}

impl ArenaStr {
    pub fn new<A: Allocator + ?Sized>(str: &str, alloc: &mut A) -> Self {
        if str.is_empty() {
            return Self::default();
        }
        let layout = Layout::from_size_align(str.len(), 4).unwrap();
        let mut chunk = alloc.allocate(layout).unwrap();
        let naked = unsafe {
            if !str.is_empty() {
                copy_nonoverlapping(str.as_ptr(), &mut chunk.as_mut()[0], str.len());
            }
            NonNull::new(chunk.as_ptr() as *mut str).unwrap()
        };
        Self { naked }
    }

    pub fn from_string<A: Allocator + ?Sized>(str: &String, alloc: &mut A) -> Self {
        Self::new(str.as_str(), alloc)
    }

    pub fn from_arena(str: &str, arena: &ArenaMut<Arena>) -> Self {
        Self::new(str, arena.get_mut())
    }

    pub fn to_string(&self) -> String {
        self.as_str().to_string()
    }

    pub fn len(&self) -> usize { self.as_str().len() }

    pub fn is_empty(&self) -> bool { self.as_str().is_empty() }

    pub fn as_str(&self) -> &str {
        unsafe { self.naked.as_ref() }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }

    pub fn bytes_part(&self, start: usize, stop: usize) -> &[u8] {
        let end = min(stop, self.len());
        &self.as_bytes()[start..end]
    }
}

impl Default for ArenaStr {
    fn default() -> Self {
        static EMPTY_DUMMY: &str = "";
        Self {
            naked: NonNull::new(EMPTY_DUMMY as *const str as *mut str).unwrap()
        }
    }
}

impl Debug for ArenaStr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArenaStr")
            .field(&self.to_string())
            .finish()
    }
}

impl Hash for ArenaStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes())
    }
}

impl Eq for ArenaStr {

}

impl PartialEq for ArenaStr {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialOrd for ArenaStr {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
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


#[macro_export]
macro_rules! arena_vec {
    ($arena:expr, [$($x:expr),+ $(,)?]) => {
        {
            let mut tmp = $crate::base::ArenaVec::new($arena);
            $(tmp.push($x);)*
            tmp
        }
    };
    ($arena:expr) => { $crate::base::ArenaVec::new($arena) }
}

pub struct ArenaVec<T> {
    naked: NonNull<[T]>,
    len: usize,
    pub owns: ArenaMut<Arena>,
}

impl<T> ArenaVec<T> {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self::with_capacity(1, arena)
    }

    pub fn with_capacity(capacity: usize, arena: &ArenaMut<Arena>) -> Self {
        let mut owns = arena.clone();
        let naked = unsafe {
            Self::new_uninitialized(owns.deref_mut(), capacity)
        };
        Self {
            naked,
            len: 0,
            owns,
        }
    }

    pub fn of(elem: T, arena: &ArenaMut<Arena>) -> Self {
        let mut this = Self::new(arena);
        this.push(elem);
        this
    }

    pub fn with_init<Fn>(arena: &ArenaMut<Arena>, init: Fn, count: usize) -> Self
        where Fn: FnOnce(usize) -> T + Copy {
        let mut owns = arena.clone();
        let naked = unsafe {
            Self::new_uninitialized(owns.deref_mut(), count)
        };
        let mut this = Self {
            naked,
            len: 0,
            owns,
        };
        for i in 0..count {
            this.push(init(i));
        }
        this
    }

    pub fn push(&mut self, elem: T) {
        self.extend_if_needed(1);
        unsafe { write(&mut self.naked.as_mut()[self.len - 1], elem) }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            None
        } else {
            unsafe {
                let addr = addr_of!(self.naked.as_ref()[self.len() - 1]);
                self.len -= 1;
                Some(ptr::read(addr))
            }
        }
    }

    pub fn front(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self[0])
        }
    }

    pub fn back(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            Some(&self[self.len() - 1])
        }
    }

    pub fn len(&self) -> usize { self.len }

    pub fn capacity(&self) -> usize { unsafe { self.naked.as_ref() }.len() }

    pub fn as_slice(&self) -> &[T] {
        unsafe { &self.naked.as_ref()[..self.len] }
    }

    pub fn as_slice_mut(&mut self) -> &mut [T] {
        unsafe { &mut self.naked.as_mut()[..self.len] }
    }

    pub fn iter(&self) -> ArenaVecIter<T> {
        ArenaVecIter {
            cursor: 0,
            items: self.as_slice(),
        }
    }

    pub fn iter_mut(&mut self) -> ArenaVecIterMut<T> {
        ArenaVecIterMut {
            cursor: 0,
            owns: self,
        }
    }

    pub fn replace_more(&mut self, i: usize, other: &mut Self) -> usize {
        if other.is_empty() {
            return 0;
        }
        if i >= self.len() {
            panic!("i out of range for replace_more");
        }
        let incremental_len = other.len() - 1;
        self.extend_if_needed(incremental_len);
        unsafe {
            let src = addr_of!(self.naked.as_ref()[i + 1]);
            let dst = addr_of_mut!(self.naked.as_mut()[i + other.len()]);
            ptr::copy(src, dst, self.len() - i - 1);
        }
        unsafe {
            let src = addr_of!(other.naked.as_ref()[0]);
            let dst = addr_of_mut!(self.naked.as_mut()[i]);
            ptr::copy_nonoverlapping(src, dst, other.len());
        }
        other.len = 0;
        incremental_len
    }

    pub fn append(&mut self, other: &mut Self) {
        let dest = self.extend_if_needed(other.len());
        debug_assert_eq!(dest.len(), other.len());
        unsafe {
            ptr::copy_nonoverlapping(addr_of!(other.as_slice()[0]),
                                     addr_of_mut!(dest[0]),
                                     other.len());
        }
        other.clear();
    }

    pub fn clear(&mut self) { self.len = 0; }

    pub fn truncate(&mut self, want: usize) {
        if want <= self.len() {
            self.len = want;
        } else {
            unreachable!()
        }
    }

    pub unsafe fn raw_ptr(&self) -> NonNull<[T]> { self.naked }

    fn extend_if_needed(&mut self, incremental: usize) -> &mut [T] {
        if self.len() + incremental > self.capacity() {
            let new_cap = self.capacity() * 2 + 4 + incremental;
            let naked = unsafe {
                Self::new_uninitialized(self.owns.deref_mut(), new_cap)
            };
            let old_raw_vec = self.naked;
            self.naked = naked;
            unsafe {
                let src = &old_raw_vec.as_ref()[0] as *const T;
                let dst = &mut self.naked.as_mut()[0] as *mut T;
                ptr::copy_nonoverlapping(src, dst, self.len);
            }
        }
        let rv = &mut unsafe { self.naked.as_mut() }[self.len..self.len + incremental];
        self.len += incremental;
        rv
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

impl<T: Clone> ArenaVec<T> {
    pub fn with_data(arena: &ArenaMut<Arena>, data: &[T]) -> Self {
        Self::with_init(arena, |i| {
            data[i].clone()
        }, data.len())
    }
}

impl<T: Clone> Clone for ArenaVec<T> {
    fn clone(&self) -> Self { self.dup(&self.owns) }
}

impl<T: Clone> ArenaVec<T> {
    pub fn to_vec(&self) -> Vec<T> {
        Vec::from(self.as_slice())
    }

    pub fn dup(&self, arena: &ArenaMut<Arena>) -> Self {
        let mut other = Self::with_capacity(self.capacity(), arena);
        for elem in self {
            other.push(elem.clone());
        }
        other
    }
}

impl<T> Deref for ArenaVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target { self.as_slice() }
}

impl<T> DerefMut for ArenaVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_slice_mut()
    }
}

impl Hash for ArenaVec<u8> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_slice())
    }
}

impl<T: PartialEq> PartialEq for ArenaVec<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.len() == other.len() {
            for i in 0..self.len() {
                if self[i] != other[i] {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}

impl<T: Eq> Eq for ArenaVec<T> {}

impl<T> Index<usize> for ArenaVec<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T> IndexMut<usize> for ArenaVec<T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.as_slice_mut()[index]
    }
}

impl<'a, T> IntoIterator for &'a ArenaVec<T> {
    type Item = &'a T;
    type IntoIter = ArenaVecIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

pub struct ArenaVecIter<'a, T> {
    cursor: usize,
    items: &'a [T],
}

impl<'a, T> Iterator for ArenaVecIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.items.len() {
            None
        } else {
            let index = self.cursor;
            self.cursor += 1;
            Some(&self.items[index])
        }
    }
}

pub struct ArenaVecIterMut<'a, T> {
    cursor: usize,
    owns: &'a mut ArenaVec<T>,
}

impl<'a, T> Iterator for ArenaVecIterMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.owns.len() {
            None
        } else {
            let index = self.cursor;
            self.cursor += 1;
            let slice_mut = unsafe { self.owns.naked.as_mut() };
            Some(&mut slice_mut[index])
        }
    }
}

impl Write for ArenaVec<u8> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut dst = self.extend_if_needed(buf.len());
        dst.write(buf).unwrap();
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

impl<T: Debug> Debug for ArenaVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut r = f.debug_list();
        for i in 0..self.len {
            r.entry(&self[i]);
        }
        r.finish()
    }
}

pub mod map {
    use super::*;
    use std::collections::hash_map::{DefaultHasher, RandomState};
    use std::marker::PhantomData;
    use std::mem;

    pub struct ArenaMap<K, V, S = RandomState> {
        arena: ArenaMut<Arena>,
        slots: ArenaBox<[*mut Entry<K, V>]>,
        len: usize,
        conflict_factor: f32,
        _holder: PhantomData<S>,
    }

    struct Entry<K, V> {
        pair: (K, V),
        hash_code: u64,
        next: *mut Self,
    }

    impl <K, V> Entry<K, V> {
        pub fn new<A: Allocator>(k: K, v: V, hash_code: u64, next: *mut Self, arena: &mut A) -> *mut Self {
            let layout = Layout::new::<Self>();
            let chunk = arena.allocate(layout).unwrap();
            let ptr = chunk.as_ptr() as *mut Self;
            unsafe {
                let this = &mut *ptr;
                //this.pair = (k, v);
                ptr::write(&mut this.pair, (k, v));
                this.hash_code = hash_code;
                this.next = next;
            }
            ptr
        }
    }

    impl <K: Eq + Hash, V> ArenaMap<K, V, RandomState> {

        pub fn new(arena: &ArenaMut<Arena>) -> Self {
            Self {
                arena: arena.clone(),
                slots: Self::zero_slots(),
                len: 0,
                conflict_factor: 0.7,
                _holder: Default::default(),
            }
        }

        pub fn from_pairs<const N: usize>(arena: &ArenaMut<Arena>, value: [(K, V); N]) -> Self {
            let mut this = Self::new(arena);
            for (k, v) in value {
                this.insert(k, v);
            }
            this
        }

        pub fn capacity(&self) -> usize { self.slots.len() }

        pub fn insert(&mut self, k: K, v: V) -> Option<V> {
            match self.locate_entry_mut(&k) {
                Some(entry) => {
                    let origin = mem::replace(&mut entry.pair.1, v);
                    Some(origin)
                }
                None => {
                    self.rehash_if_needed(1);
                    let (hash_code, slot) = self.locate_slot(&k);
                    let node = Entry::new(k, v, hash_code, ptr::null_mut(), self.arena.get_mut());

                    let n = unsafe { &mut *node };
                    n.next = self.slots[slot];
                    self.slots[slot] = node;
                    self.len += 1;
                    None
                }
            }
        }

        pub fn get(&self, k: &K) -> Option<&V> {
            if self.slots.is_empty() {
                return None;
            }
            let (hash_code, slot) = self.locate_slot(&k);
            let mut p = self.slots[slot];
            while p != ptr::null_mut() {
                let n = unsafe {&*p};
                if n.hash_code == hash_code && n.pair.0.eq(k) {
                    return Some(&n.pair.1);
                }
                p = n.next;
            }
            None
        }

        pub fn get_mut(&self, k: &K) -> Option<&mut V> {
            if self.slots.is_empty() {
                return None;
            }
            let (hash_code, slot) = self.locate_slot(&k);
            let mut p = self.slots[slot];
            while p != ptr::null_mut() {
                let n = unsafe {&mut *p};
                if n.hash_code == hash_code && n.pair.0.eq(k) {
                    return Some(&mut n.pair.1);
                }
                p = n.next;
            }
            None
        }

        pub fn remove(&mut self, k: &K) -> Option<V> {
            let (hash_code, slot) = self.locate_slot(&k);
            let mut prev = ptr::null_mut() as *mut Entry<K, V>;
            let mut x = self.slots[slot];
            while x != ptr::null_mut() {
                let n = unsafe {&mut *x };
                if n.hash_code == hash_code && n.pair.0.eq(k) {
                    if prev == ptr::null_mut() {
                        self.slots[slot] = ptr::null_mut();
                    } else {
                        let mut p = unsafe { &mut *prev };
                        p.next = n.next;
                    }
                    self.len -= 1;
                    return Some(unsafe { ptr::read(x).pair.1 })
                }

                prev = x;
                x = n.next;
            }
            None
        }

        pub fn is_empty(&self) -> bool { self.len == 0 }

        pub fn len(&self) -> usize { self.len }

        pub fn clear(&mut self) {
            for slot in self.slots.iter_mut() {
                let mut p = *slot;
                while p != ptr::null_mut() {
                    let n = unsafe { &*p };
                    drop(unsafe { ptr::read(&n.pair) });
                    p = n.next;
                }
                *slot = ptr::null_mut();
            }
            self.len = 0;
        }

        pub fn finalize(&mut self) {
            self.clear();
            self.slots = Self::zero_slots();
        }

        pub fn iter(&self) -> Iter<'_, K, V> {
            let mut it = Iter {
                slots: self.slots.clone(),
                index: self.slots.len(),
                current: ptr::null_mut(),
                _holder: Default::default(),
            };
            for i in 0..self.slots.len() {
                if self.slots[i] != ptr::null_mut() {
                    it.index = i;
                    it.current = self.slots[i];
                    break
                }
            }
            it
        }

        fn rehash_if_needed(&mut self, increment: usize) {
            let factor = (self.len + increment) as f32 / self.capacity() as f32;
            if factor <= self.conflict_factor {
                return;
            }

            let slots = Self::allocate_slots(&self.arena, (self.capacity() << 1) + 4);
            let mut original_slots = mem::replace(&mut self.slots, slots);
            for original_slot in original_slots.iter_mut() {
                while *original_slot != ptr::null_mut() {
                    let p = *original_slot;
                    *original_slot = unsafe{ &*p }.next;

                    let n = unsafe { &mut *p };
                    let i = n.hash_code as usize % self.capacity();
                    n.next = self.slots[i];
                    self.slots[i] = p;
                }
            }
        }

        fn locate_entry_mut(&mut self, k: &K) -> Option<&mut Entry<K, V>> {
            if self.capacity() == 0 {
                return None;
            }
            let (hash_code, slot) = self.locate_slot(k);
            let mut p = self.slots[slot];
            while p != ptr::null_mut() {
                let n = unsafe { &mut *p };
                if n.hash_code == hash_code && n.pair.0.eq(k) {
                    return Some(n)
                }
                p = n.next;
            }
            None
        }

        fn locate_slot(&self, k: &K) -> (u64, usize) {
            let mut s = DefaultHasher::default();
            k.hash(&mut s);
            let hash_code = s.finish();
            (hash_code, hash_code as usize % self.capacity())
        }

        fn zero_slots() -> ArenaBox<[*mut Entry<K, V>]> {
            static ZERO_DUMMY: [u8;8] = [0;8];
            let ptr = addr_of!(ZERO_DUMMY[0]) as *mut *mut Entry<K, V>;
            ArenaBox::from_ptr(NonNull::new(slice_from_raw_parts_mut(ptr, 0)).unwrap())
        }

        fn allocate_slots(arena: &ArenaMut<Arena>, len: usize) -> ArenaBox<[*mut Entry<K, V>]> {
            let atomic = Layout::new::<*mut Entry<K, V>>();
            let layout = Layout::from_size_align(atomic.size() * len, atomic.align()).unwrap();
            let chunk: NonNull<[u8]> = arena.get_mut().allocate(layout).unwrap();
            let addr = chunk.as_ptr() as *mut *mut Entry<K, V>;
            let mut slots = ArenaBox::from_ptr(NonNull::new(slice_from_raw_parts_mut(addr, len)).unwrap());
            for i in 0..slots.len() {
                slots[i] = ptr::null_mut();
            }
            slots
        }
    }

    pub struct Iter<'a, K, V> {
        slots: ArenaBox<[*mut Entry<K, V>]>,
        index: usize,
        current: *mut Entry<K, V>,
        _holder: PhantomData<&'a K>,
    }

    impl <'a, K: 'a, V: 'a> Iterator for Iter<'a, K, V> {
        type Item = (&'a K, &'a V);

        fn next(&mut self) -> Option<Self::Item> {
            if self.index >= self.slots.len() && self.current == ptr::null_mut() {
                return None;
            }
            if self.current == ptr::null_mut() {
                loop {
                    self.index += 1;
                    if self.index >= self.slots.len() {
                        return None;
                    }
                    if self.slots[self.index] != ptr::null_mut() {
                        self.current = self.slots[self.index];
                        break;
                    }
                }
            }

            let n = unsafe { &*self.current };
            self.current = n.next;
            Some((&n.pair.0, &n.pair.1))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use crate::map::ArenaMap;

    use super::*;

    #[test]
    fn sanity() {
        let mut arena = Arena::new();
        let s: [u8; 16] = [1; 16];
        let mut chunk = arena.allocate(Layout::from_size_align(16, 4).unwrap()).unwrap();
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
        let arena = Arena::new_ref();
        let mut vec = ArenaVec::new(&arena.get_mut());
        vec.push(1);
        vec.push(2);
        vec.push(3);
        assert_eq!(7, vec.capacity());
        assert_eq!(3, vec.len());
        assert_eq!(1, vec[0]);
        assert_eq!(2, vec[1]);
        assert_eq!(3, vec[2]);
    }

    #[test]
    fn arena_vec_large_push() {
        let arena = Arena::new_ref();
        let mut vec = ArenaVec::new(&arena.get_mut());
        let n = 10000;

        for i in 0..n {
            vec.push(i);
        }
        assert_eq!(n, vec.len);
        for i in 0..n {
            assert_eq!(i, vec[i]);
        }
    }

    #[test]
    fn arena_vec_replace_more() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut a = arena_vec!(&arena, [1, 2, 3]);
        let mut b = arena_vec!(&arena, [6, 5, 4]);

        a.replace_more(1, &mut b);
        assert_eq!(5, a.len());
        assert_eq!(0, b.len());
        assert_eq!([1, 6, 5, 4, 3], a.as_slice());

        let mut a = arena_vec!(&arena, [1, 2, 3]);
        let mut b = arena_vec!(&arena, [6, 5, 4]);
        a.replace_more(2, &mut b);
        assert_eq!(5, a.len());
        assert_eq!(0, b.len());
        assert_eq!([1, 2, 6, 5, 4], a.as_slice());

        let mut a = arena_vec!(&arena, [1, 2, 3]);
        let mut b = arena_vec!(&arena, [6, 5, 4]);
        a.replace_more(0, &mut b);
        assert_eq!(5, a.len());
        assert_eq!(0, b.len());
        assert_eq!([6, 5, 4, 2, 3], a.as_slice());
    }

    #[test]
    fn arena_map_sanity() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut map = ArenaMap::new(&arena);
        assert_eq!(None, map.insert("aaa", 1));
        assert_eq!(4, map.capacity());
        assert_eq!(1, map.len());

        assert_eq!(None, map.insert("bbb", 2));
        assert_eq!(4, map.capacity());
        assert_eq!(2, map.len());

        assert_eq!(None, map.remove(&"ccc"));
        assert_eq!(4, map.capacity());
        assert_eq!(2, map.len());

        assert_eq!(Some(2), map.remove(&"bbb"));
        assert_eq!(4, map.capacity());
        assert_eq!(1, map.len());
    }

    #[test]
    fn arena_map_large_insertion() {
        const N: i32 = 1000;

        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut map = ArenaMap::new(&arena);
        for i in 0..N {
            let s = format!("k.{i}");
            map.insert(s, (i + 1) * 100);
        }
        assert_eq!(N as usize, map.len());

        for i in 0..N {
            let s = format!("k.{i}");
            assert_eq!(Some((i + 1) * 100), map.get(&s).cloned());
        }

        map.finalize()
    }

    #[test]
    fn arena_map_iterate() {
        const N: i32 = 1000;

        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut map = ArenaMap::new(&arena);
        for i in 0..N {
            let s = format!("k.{i}");
            map.insert(s, (i + 1) * 100);
        }
        assert_eq!(N as usize, map.len());

        let mut i = 0;
        for (_, v) in map.iter() {
            i += 1;
            assert_eq!(0, v % 100);
        }
        assert_eq!(N, i);

        map.finalize();
    }

    #[test]
    fn arena_map_remove() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut map = ArenaMap::from_pairs(&arena, [
            ("aaa", 1),
            ("bbb", 2),
            ("ccc", 3)
        ]);
        assert_eq!(Some(2), map.remove(&"bbb"));
        assert_eq!(None, map.remove(&"ddd"));
    }

    #[test]
    fn arena_fuzz_insertion() {
        const N: i32 = 10000;

        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut map = ArenaMap::new(&arena);
        for i in 0..N {
            let s = format!("key-{}", rand::random::<i32>());
            map.insert(s, i);
        }
    }
}
