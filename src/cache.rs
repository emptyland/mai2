use std::alloc::{alloc, dealloc, Layout};
use std::hash::Hash;
use std::{io, iter};
use std::mem::{size_of, size_of_val};
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::ptr::{addr_of_mut, NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, Ordering};
use crc::{Crc, CRC_32_ISCSI};
use lru::LruCache;
use crate::env::{Env, RandomAccessFile};
use crate::marshal::{Decoder, FixedDecode, VarintDecode};
use crate::varint::MAX_VARINT32_LEN;

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

    pub fn get(&mut self, file: &mut dyn RandomAccessFile, file_number: u64, offset: u64, checksum_verify: bool) -> io::Result<Block> {
        let shard = self.get_shard(file_number);
        let mut lru = shard.lock().unwrap();
        let mut rs: Option<io::Result<Block>> = None;
        let block = lru.get_or_insert(&(file_number, offset), || {
            match Self::load_block(file, offset, checksum_verify) {
                Err(e) => {
                    rs = Some(Err(e));
                    Block::new(0)
                }
                Ok(block) => block
            }
        });
        if rs.is_some() { rs.unwrap() } else { Ok(block) }
    }

    fn load_block(file: &mut dyn RandomAccessFile, offset: u64, checksum_verify: bool) -> io::Result<Block> {
        let mut header = Vec::from_iter(iter::repeat(0u8).take(offset as usize + MAX_VARINT32_LEN));
        file.positioned_read(offset, &mut header)?;
        let (checksum, len, delta) = {
            let mut decoder = Decoder::new();
            let checksum: u32 = decoder.read_fixed(&header)?;
            let len: u32 = decoder.read_from(&header)?;
            (checksum, len as usize, decoder.offset() as u64)
        };

        let mut block = Block::new(len);
        file.positioned_read(offset + delta, block.payload_mut())?;

        if checksum_verify {
            let crc = Crc::<u32>::new(&CRC_32_ISCSI);
            let mut digest = crc.digest();
            digest.update(block.payload());
            if digest.finalize() != checksum {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "incorrect checksum"));
            }
        }
        Ok(block)
    }

    pub fn get_shard(&mut self, file_number: u64) -> &Mutex<CacheShard<(u64, u64)>> {
        let len = self.stripes.len();
        &mut self.stripes[file_number as usize % len]
    }
}


pub struct CacheShard<K> {
    capacity_in_bytes: usize,
    used_in_bytes: usize,
    lru: LruCache<K, Block>,
}

impl<T: Clone + Hash + Eq> CacheShard<T> {
    pub fn new(capacity_in_bytes: usize, capacity_in_keys: usize) -> Self {
        let cap = NonZeroUsize::new(capacity_in_keys).unwrap();
        Self {
            capacity_in_bytes,
            used_in_bytes: 0,
            lru: LruCache::new(cap),
        }
    }

    pub fn get(&mut self, key: &T) -> Option<Block> {
        match self.lru.get(key) {
            Some(block) => Some(block.clone()),
            None => None
        }
    }

    pub fn get_or_insert<F>(&mut self, key: &T, mut load: F) -> Block
        where F: FnMut() -> Block {
        self.lru.get_or_insert(key.clone(), || { load() }).clone()
    }
}

pub struct Block {
    naked: NonNull<BlockHeader>
}

impl Block {
    pub fn new(len: usize) -> Self {
        let naked = unsafe {
            BlockHeader::new(len)
        };
        Self { naked }
    }

    pub fn ref_count(&self) -> u64 {
        self.refs.load(Ordering::Relaxed)
    }

    fn from_naked(naked: NonNull<BlockHeader>) -> Self {
        Self { naked }
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        let new_one = Block { naked: self.naked };
        unsafe { new_one.naked.as_ref() }
            .refs
            .fetch_add(1, Ordering::Relaxed);
        new_one
    }
}

impl Deref for Block {
    type Target = BlockHeader;

    fn deref(&self) -> &Self::Target {
        unsafe { self.naked.as_ref() }
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.naked.as_mut() }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        dbg!(self.refs.load(Ordering::Relaxed));
        if self.refs.fetch_sub(1, Ordering::Relaxed) == 1 {
            unsafe { BlockHeader::free(self.naked) };
        }
    }
}

pub struct BlockHeader {
    refs: AtomicU64,
    payload: NonNull<[u8]>,
}

impl BlockHeader {

    unsafe fn new(len: usize) -> NonNull<BlockHeader> {
        let chunk = alloc(Self::make_layout(len));
        let mut header = NonNull::<Self>::new(chunk as *mut Self).unwrap();
        header.as_mut().refs.store(1, Ordering::Relaxed);
        let payload_addr = chunk.add(size_of::<Self>());
        header.as_mut().payload = NonNull::new(slice_from_raw_parts_mut(payload_addr, len)).unwrap();
        header
    }

    unsafe fn free(this: NonNull<BlockHeader>) {
        let payload_size = this.as_ref().payload().len();
        dealloc(this.as_ptr() as *mut u8, Self::make_layout(payload_size));
    }

    fn rss_size(&self) -> usize {
        size_of_val(self) + self.payload().len()
    }

    pub fn payload(&self) -> &[u8] {
        unsafe { self.payload.as_ref() }
    }

    pub fn payload_mut(&mut self) -> &mut [u8] {
        unsafe { self.payload.as_mut() }
    }

    fn make_layout(payload_size: usize) -> Layout {
        let header_layout = Layout::new::<Self>();
        Layout::from_size_align(header_layout.size() + payload_size, header_layout.align()).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let block = Block::new(0);
        assert_eq!(0, block.payload().len());
        assert_eq!(24, block.rss_size());
    }
}