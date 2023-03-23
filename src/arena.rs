use std::alloc::{alloc, dealloc, Layout};
use std::cell::RefCell;
use std::iter;
use std::ptr::{addr_of, addr_of_mut, NonNull, slice_from_raw_parts_mut};
use std::rc::Rc;

pub trait Allocator {
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()>;
}

#[derive(Debug)]
pub struct Arena {
    pages: Option<NonNull<Page>>,
}

#[derive(Debug)]
struct Page {
    next: Option<NonNull<Page>>,
    free: *mut u8,
    remaining: usize,
    chunk: [u8; 16 * 1024],
}

impl Arena {
    pub fn new() -> Self {
        Arena { pages: None }
    }

    pub fn new_rc() -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new()))
    }

    pub fn pages_count(&self) -> i32 {
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
}

impl Allocator for Arena {
    fn allocate(&mut self, layout: Layout) -> Result<NonNull<[u8]>, ()> {
        match self.pages {
            Some(mut head) => unsafe {
                let chunk = head.as_mut().allocate(layout);
                match chunk {
                    Some(ptr) => Ok(ptr),
                    None => {
                        self.pages = Page::new(self.pages);
                        self.allocate(layout)
                    }
                }
            }
            None => {
                self.pages = Page::new(self.pages);
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
                    Page::free(page)
                },
                None => break
            }
        }
    }
}

impl Page {
    pub fn new(next: Option<NonNull<Page>>) -> Option<NonNull<Page>> {
        let layout = Layout::new::<Page>();
        unsafe {
            let page = NonNull::new(alloc(layout) as *mut Page);
            if let Some(none_null) = page {
                let mut naked_page = none_null.as_ptr();
                (*naked_page).next = next;
                (*naked_page).free = &mut (*naked_page).chunk[0] as *mut u8;
                (*naked_page).remaining = (*naked_page).chunk.len();
            }
            page
        }
    }

    pub unsafe fn free(page: NonNull<Page>) {
        let layout = Layout::new::<Page>();
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
        }
        NonNull::new(slice_from_raw_parts_mut(aligned, layout.size()))
    }
}

pub struct ScopedMemory {
    block: [u8;128],
    chunk: Vec<u8>
}

impl ScopedMemory {
    pub fn new() -> Self {
        Self {
            block: [0;128],
            chunk: Vec::new()
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

pub fn round_down<T>(x: *mut T, m: i64) -> *mut T {
    (x as u64 & -m as u64) as *mut T
}

// return RoundDown<T>(static_cast<T>(x + m - 1), m);
pub fn round_up<T>(x: *mut T, m: i64) -> *mut T {
    round_down((x as i64 + m - 1) as *mut T, m)
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
        assert_eq!(1, arena.pages_count());
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
