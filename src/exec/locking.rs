use std::alloc::Layout;
use std::array::from_fn;
use std::collections::{HashMap, HashSet};
use std::num::Wrapping;
use std::ops::DerefMut;
use std::ptr;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::thread::yield_now;

use crate::base::{Allocator, Arena, utils};

pub struct LockingManagement {
    lock_instances: RwLock<HashMap<u64, Arc<LockingInstance>>>
}

impl LockingManagement {
    pub fn new() -> Self {
        Self {
            lock_instances: RwLock::default(),
        }
    }

    pub fn install(&self, id: u64) {
        let mut locks = self.lock_instances.write().unwrap();
        locks.insert(id, Arc::new(LockingInstance::new()));
    }

    pub fn uninstall(&self, id: u64) {
        let mut locks = self.lock_instances.write().unwrap();
        locks.remove(&id);
    }

    pub fn instance(&self, id: u64) -> Option<Arc<LockingInstance>> {
        let locks = self.lock_instances.read().unwrap();
        locks.get(&id).cloned()
    }
}

pub struct LockingInstance {
    lock_stripes: [AtomicPtr<LockingStripe>; 1024],
    available_stripes: AtomicUsize,
    arena: Mutex<Arena>
}

unsafe impl Sync for LockingInstance {}
unsafe impl Send for LockingInstance {}

impl LockingInstance {
    const NOT_INIT: *mut LockingStripe = ptr::null_mut();
    const PENDING_MASK: *mut LockingStripe = 1 as *mut LockingStripe;
    const CREATE_MASK: usize = !1;

    pub fn new() -> Self {
        Self {
            lock_stripes: from_fn(|_|{AtomicPtr::new(ptr::null_mut())}),
            available_stripes: AtomicUsize::new(0),
            arena: Mutex::new(Arena::new())
        }
    }

    pub fn group(&self) -> LockingGroup {
        LockingGroup {
            owns: self,
            stripes: HashSet::new()
        }
    }

    pub fn shared_lock(&self, key: &[u8]) -> RwLockReadGuard<u64> {
        let stripe = self.get_stripe(key);
        stripe.lock_count.fetch_add(1, Ordering::Relaxed);
        stripe.lock.read().unwrap()
    }

    pub fn exclusive_lock(&self, key: &[u8]) -> RwLockWriteGuard<u64> {
        let stripe = self.get_stripe(key);
        stripe.lock_count.fetch_add(1, Ordering::Relaxed);
        stripe.lock.write().unwrap()
    }

    pub fn available_stripes(&self) -> usize { self.available_stripes.load(Ordering::Acquire) }

    fn get_stripe(&self, key: &[u8]) -> &LockingStripe {
        let index = self.get_stripe_index(key);
        self.stripe_at_or_lazy_init(index)
    }

    fn get_stripe_index(&self, key: &[u8]) -> usize {
        let hash_code = Wrapping(utils::js_hash(key));
        (hash_code * hash_code).0 as usize % self.lock_stripes.len()
    }

    fn stripe_at_or_lazy_init(&self, i: usize) -> &LockingStripe {
        //&self.lock_stripes[i].lo
        let inst = &self.lock_stripes[i];
        if inst.load(Ordering::Acquire) as usize & Self::CREATE_MASK == 0 && Self::need_init(inst) {
            self.install(inst);
        }
        unsafe { &*inst.load(Ordering::Relaxed) }
    }

    fn need_init(inst: &AtomicPtr<LockingStripe>) -> bool {
        if inst.compare_exchange(Self::NOT_INIT, Self::PENDING_MASK,
                                 Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return true;
        }
        while inst.load(Ordering::Acquire) == Self::PENDING_MASK {
            yield_now();
        }
        return false;
    }

    fn install(&self, inst: &AtomicPtr<LockingStripe>) {
        let mut arena = self.arena.lock().unwrap();
        let ptr = unsafe { LockingStripe::new(arena.deref_mut()) };
        inst.store(ptr, Ordering::Release);
        self.available_stripes.fetch_add(1, Ordering::AcqRel);
    }
}

pub struct LockingGroup<'a> {
    owns: &'a LockingInstance,
    stripes: HashSet<usize>,
}

impl LockingGroup<'_> {
    pub fn add(&mut self, key: &[u8]) -> &mut Self {
        self.stripes.insert(self.owns.get_stripe_index(key));
        self
    }

    pub fn shared_lock_all(&self) -> Vec<RwLockReadGuard<u64>> {
        self.stripes().iter().map(|x|{
            x.lock_count.fetch_add(1, Ordering::Relaxed);
            x.lock.read().unwrap()
        }).collect()
    }

    pub fn exclusive_lock_all(&self) -> Vec<RwLockWriteGuard<u64>> {
        self.stripes().iter().map(|x|{
            x.lock_count.fetch_add(1, Ordering::Relaxed);
            x.lock.write().unwrap()
        }).collect()
    }

    fn stripes(&self) -> Vec<&LockingStripe> {
        self.stripes.iter().map(|x|{
            self.owns.stripe_at_or_lazy_init(*x)
        }).collect()
    }
}


impl Drop for LockingInstance {
    fn drop(&mut self) {
        for inst in &self.lock_stripes {
            match NonNull::new(inst.load(Ordering::Relaxed)) {
                Some(ptr) => unsafe {
                    drop(ptr::read(ptr.as_ptr()));
                },
                None => ()
            }
        }
    }
}

struct LockingStripe {
    lock: RwLock<u64>,
    lock_count: AtomicU64,
}

impl LockingStripe {
    unsafe fn new(arena: &mut dyn Allocator) -> *mut Self {
        let layout = Layout::new::<Self>();
        let chunk = arena.allocate(layout).unwrap();
        let ptr = chunk.as_ptr() as *mut Self;
        ptr::write(ptr, Self {
            lock: RwLock::new(0),
            lock_count: AtomicU64::default(),
        });
        ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let locks = LockingManagement::new();
        locks.install(0);
        let rs = locks.instance(0);
        assert!(rs.is_some());
        let inst = rs.unwrap().clone();
        let _lk1 = inst.shared_lock("aaa".as_bytes());
        let _lk2 = inst.shared_lock("bbb".as_bytes());
        assert_eq!(2, inst.available_stripes());
    }
}