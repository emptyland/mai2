use std::alloc::{alloc, dealloc, Layout};
use std::cell::RefCell;
use std::iter;
use std::mem::size_of;
use std::ptr::{addr_of_mut, NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::rc::Rc;

use crate::utils::round_up;

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
}
