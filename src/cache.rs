use std::alloc::Layout;
use std::hash::Hash;
use std::iter;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::ptr::{NonNull, slice_from_raw_parts};
use std::sync::{Arc, Mutex, MutexGuard};
use lru::LruCache;
use crate::arena::{Allocator, Arena};
use crate::env::{Env, RandomAccessFile};
use crate::marshal::Decoder;

pub struct TableCache {
    abs_db_path: PathBuf,
    env: Arc<dyn Env>,
    //block_cache: BlockCache<'static>,
}

pub struct BlockCache {
    limit_in_bytes: usize,
    stripes: Vec<Mutex<CacheShard<(u64, u64)>>>,
}

impl BlockCache {
    pub fn new(n_stripes: usize, limit_in_bytes: usize) -> Self {
        let mut stripes = Vec::new();
        for _ in 0..n_stripes {
            let cache = CacheShard::<(u64, u64)>::new(limit_in_bytes, 1024);
            stripes.push(Mutex::new(cache));
        }
        Self {
            limit_in_bytes,
            stripes,
        }
    }

    pub fn get(&mut self, file: &mut dyn RandomAccessFile, file_number: u64, offset: u64, checksum_verify: bool) -> &[u8] {
        let shard = self.get_shard(file_number);
        let mut lru = shard.lock().unwrap();
        let chunk = lru.get_or_insert(&(file_number, offset), |arena| {
            let layout = Layout::from_size_align(1000, 4).unwrap();
            let mut chunk = arena.allocate(layout).unwrap();
            unsafe {
                file.positioned_read(offset, chunk.as_mut()).unwrap();
            }
            chunk
        });
        unsafe { chunk.as_ref() }
    }

    pub fn get_shard(&mut self, file_number: u64) -> &Mutex<CacheShard<(u64, u64)>> {
        let len = self.stripes.len();
        &mut self.stripes[file_number as usize % len]
    }
}


pub struct CacheShard<K> {
    capacity_in_bytes: usize,
    used_in_bytes: usize,
    lru: LruCache<K, NonNull<[u8]>>,
    arena: Arena,
}

impl<T: Clone + Hash + Eq> CacheShard<T> {
    pub fn new(capacity_in_bytes: usize, capacity_in_keys: usize) -> Self {
        let cap = NonZeroUsize::new(capacity_in_keys).unwrap();
        Self {
            capacity_in_bytes,
            used_in_bytes: 0,
            lru: LruCache::new(cap),
            arena: Arena::new(),
        }
    }

    pub fn get(&mut self, key: &T) -> Option<NonNull<[u8]>> {
        match self.lru.get(key) {
            Some(block) => Some(block.clone()),
            None => None
        }
    }

    pub fn get_or_insert<F>(&mut self, key: &T, mut load: F) -> NonNull<[u8]>
        where F: FnMut(&mut Arena) -> NonNull<[u8]> {
        self.lru.get_or_insert(key.clone(), || { load(&mut self.arena) }).clone()
    }
}