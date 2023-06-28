use std::alloc::{alloc, dealloc, Layout};
use std::any::Any;
use std::ptr;
use std::ptr::NonNull;
use std::sync::{Arc, Weak};
use crate::{log_warn, Logger};
use crate::storage::db::DBImpl;

use crate::storage::Snapshot;

pub struct SnapshotSet {
    logger: Arc<dyn Logger>,
    snapshots: NonNull<SnapshotShadow>,
}

impl SnapshotSet {
    pub fn new(logger: &Arc<dyn Logger>) -> SnapshotSet {
        Self {
            logger: logger.clone(),
            snapshots: unsafe { SnapshotShadow::dummy() },
        }
    }

    pub fn new_snapshot(&mut self, owns: &Weak<DBImpl>, sequence_number: u64, ts: u64) -> Arc<dyn Snapshot> {
        let shadow = unsafe {
            SnapshotShadow::new(ptr::null_mut(), ptr::null_mut(), sequence_number, ts)
        };
        self.snapshots_mut().insert_tail(shadow);
        Arc::new(SnapshotImpl {
            owns: owns.clone(),
            shadow
        })
    }

    pub fn remove_snapshot(&mut self, snapshot_impl: &SnapshotImpl) {
        unsafe {
            let mut ptr = snapshot_impl.shadow;
            ptr.as_mut().unlink();
            SnapshotShadow::delete(ptr);
        }
    }

    pub fn oldest(&self) -> u64 {
         self.snapshots().head().sequence_number
    }

    pub fn is_empty(&self) -> bool {
        self.snapshots().is_empty()
    }

    fn snapshots(&self) -> &SnapshotShadow {
        unsafe {
            self.snapshots.as_ref()
        }
    }

    fn snapshots_mut(&mut self) -> &mut SnapshotShadow {
        unsafe {
            self.snapshots.as_mut()
        }
    }
}

impl Drop for SnapshotSet {
    fn drop(&mut self) {
        if !self.snapshots().is_empty() {
            log_warn!(self.logger, "snapshot not released: {}", self.snapshots().len());
        }
        while !self.snapshots().is_empty() {
            let x = self.snapshots_mut().head_mut();
            x.unlink();
            unsafe { SnapshotShadow::delete(NonNull::new(x).unwrap()); }
        }
    }
}

struct SnapshotShadow {
    next: *mut SnapshotShadow,
    prev: *mut SnapshotShadow,
    sequence_number: u64,
    ts: u64
}

impl SnapshotShadow {
    unsafe fn dummy() -> NonNull<Self> {
        let mut ptr = Self::new(ptr::null_mut(), ptr::null_mut(), 0, 0);
        let shadow = ptr.as_mut();
        shadow.next = ptr.as_ptr();
        shadow.prev = ptr.as_ptr();
        ptr
    }

    unsafe fn new(next: *mut SnapshotShadow,
           prev: *mut SnapshotShadow,
           sequence_number: u64,
           ts: u64) -> NonNull<Self> {
        let layout = Layout::new::<Self>();
        let chunk = alloc(layout) as *mut Self;
        let shadow = &mut *chunk;
        shadow.next = next;
        shadow.prev = prev;
        shadow.sequence_number = sequence_number;
        shadow.ts = ts;
        NonNull::new(shadow as *mut Self).unwrap()
    }

    unsafe fn delete(this: NonNull<Self>) {
        let layout = Layout::new::<Self>();
        let chunk = this.as_ptr() as *mut u8;
        dealloc(chunk, layout);
    }

    fn is_empty(&self) -> bool {
        self.next as *const Self == self as *const Self && self.prev as *const Self == self as *const Self
    }

    fn head(&self) -> &SnapshotShadow {
        unsafe {
            &*self.next
        }
    }

    fn head_mut(&mut self) -> &mut SnapshotShadow {
        unsafe { &mut *self.next }
    }

    fn len(&self) -> usize {
        let mut n = 0;
        let mut x = self.next;
        while x as *const Self != self as *const Self {
            n += 1;
            unsafe { x = (&*x).next }
        }
        n
    }

    fn insert_tail(&mut self, mut x: NonNull<Self>) {
        unsafe {
            x.as_mut().next = self;
            let prev = self.prev;
            x.as_mut().prev = prev;
            (&mut *prev).next = x.as_ptr();
            self.prev = x.as_ptr();
        }
    }

    fn unlink(&mut self) {
        unsafe {
            (&mut *self.prev).next = self.next;
            (&mut *self.next).prev = self.prev;
        }
    }
}

pub struct SnapshotImpl {
    owns: Weak<DBImpl>,
    shadow: NonNull<SnapshotShadow>,
}

impl SnapshotImpl {
    pub fn from(handle: &Arc<dyn Snapshot>) -> &Self {
        handle.as_any().downcast_ref::<Self>().unwrap()
    }
    pub fn sequence_number(&self) -> u64 { self.shadow().sequence_number }
    pub fn ts(&self) -> u64 { self.shadow().ts }

    fn shadow(&self) -> &SnapshotShadow { unsafe { self.shadow.as_ref() } }
}

impl Snapshot for SnapshotImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Drop for SnapshotImpl {
    fn drop(&mut self) {
        if let Some(db) = self.owns.upgrade() {
            //log_debug!(db.logger, "release snapshot: {} at {}", self.sequence_number, self.ts);
            db.snapshots.borrow_mut().remove_snapshot(self);
        }
    }
}