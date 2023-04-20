use std::{io, iter, slice};
use std::alloc::{alloc, dealloc, Layout};
use std::cell::RefCell;
use std::fmt::Error;
use std::hash::Hash;
use std::io::Write;
use std::mem::{size_of, size_of_val};
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::ptr::{addr_of, addr_of_mut, NonNull, slice_from_raw_parts, slice_from_raw_parts_mut};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicU64, Ordering};

use crc::{Crc, CRC_32_ISCSI};
use lru::LruCache;

use crate::column_family::ColumnFamilyImpl;
use crate::env::{Env, RandomAccessFile};
use crate::key::Tag;
use crate::mai2;
use crate::mai2::{from_io_result, PinnableValue, ReadOptions};
use crate::marshal::{Decoder, FixedDecode, VarintDecode};
use crate::sst_builder::{BlockHandle, TableProperties};
use crate::sst_reader::{KeyBloomFilter, SSTReader};
use crate::varint::MAX_VARINT32_LEN;

pub struct TableCache {
    abs_db_path: PathBuf,
    env: Arc<dyn Env>,
    pub block_cache: Arc<BlockCache>,
    lru: Mutex<LruCache<u64, Rc<TableEntry>>>,
}

struct TableEntry {
    cf_id: u32,
    file_path: PathBuf,
    file: Rc<RefCell<dyn RandomAccessFile>>,
    table: Arc<SSTReader>,
}

impl TableCache {
    pub fn new(abs_db_path: &Path, env: &Arc<dyn Env>, max_open_files: usize,
               block_cache_capacity: usize) -> Self {
        Self {
            abs_db_path: abs_db_path.to_path_buf(),
            env: env.clone(),
            block_cache: Arc::new(BlockCache::new(7, block_cache_capacity)),
            lru: Mutex::new(LruCache::new(NonZeroUsize::new(max_open_files).unwrap())),
        }
    }

    pub fn get(&self, read_opts: &ReadOptions, cfi: &ColumnFamilyImpl, file_number: u64, key: &[u8])
               -> mai2::Result<(PinnableValue, Tag)> {
        let entry = from_io_result(self.get_or_insert(cfi, file_number, 0))?;
        entry.table.get(read_opts, &cfi.internal_key_cmp, key)
    }

    pub fn get_reader(&self, cfi: &ColumnFamilyImpl, file_number: u64) -> io::Result<Arc<SSTReader>> {
        let entry = self.get_or_insert(cfi, file_number, 0)?;
        Ok(entry.table.clone())
    }

    fn get_or_insert(&self, cfi: &ColumnFamilyImpl,
                     file_number: u64,
                     file_size: u64) -> io::Result<Rc<TableEntry>> {
        let mut lru = self.lru.lock().unwrap();

        let rv = lru.get(&file_number);
        if let Some(entry) = rv {
            Ok(entry.clone())
        } else {
            let entry = Rc::new(self.load(cfi, file_number, file_size)?);
            lru.push(file_number, entry.clone());
            Ok(entry)
        }
    }

    fn load(&self, cfi: &ColumnFamilyImpl, file_number: u64, mut file_size: u64) -> io::Result<TableEntry> {
        let file_path = cfi.get_table_file_path(&self.env, file_number);
        let file = self.env.new_random_access_file(dbg!(&file_path))?;
        if file_size == 0 {
            file_size = file.borrow().get_file_size()? as u64;
        }
        let table = SSTReader::new(file.clone(), file_number, file_size,
                                   true, self.block_cache.clone())?;
        Ok(TableEntry {
            cf_id: 0,
            file_path,
            file,
            table: Arc::new(table),
        })
    }
}

pub struct BlockCache {
    limit_in_bytes: usize,
    stripes: Vec<Mutex<CacheShard<(u64, u64)>>>,
}

impl BlockCache {
    pub fn new(n_stripes: usize, limit_in_bytes: usize) -> Self {
        let mut stripes = Vec::new();
        for _ in 0..n_stripes {
            let cache = CacheShard::<(u64, u64)>::new(limit_in_bytes, 64);
            stripes.push(Mutex::new(cache));
        }
        Self {
            limit_in_bytes,
            stripes,
        }
    }

    pub fn get(&self, file: &mut dyn RandomAccessFile, file_number: u64, offset: u64, checksum_verify: bool) -> io::Result<Block> {
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

    pub fn invalidate(&self, file_number: u64) {
        let shard = self.get_shard(file_number);
        let mut lru = shard.lock().unwrap();
        lru.lru.clear();
    }

    fn load_block(file: &mut dyn RandomAccessFile, offset: u64, checksum_verify: bool) -> io::Result<Block> {
        let mut header = Vec::from_iter(iter::repeat(0u8)
            .take(4/*crc32*/ + MAX_VARINT32_LEN/*len*/));
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
                let message = format!("Incorrect checksum, block offset={}", offset);
                return Err(io::Error::new(io::ErrorKind::InvalidData, message));
            }
        }
        Ok(block)
    }

    pub fn get_shard(&self, file_number: u64) -> &Mutex<CacheShard<(u64, u64)>> {
        let len = self.stripes.len();
        &self.stripes[file_number as usize % len]
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
    naked: NonNull<BlockHeader>,
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
        //dbg!(self.refs.load(Ordering::Relaxed));
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

    pub fn restarts(&self) -> &[u32] {
        let restarts_len = {
            let mut buf: [u8; 4] = [0; 4];
            let src = &self.payload()[self.payload().len() - 4..self.payload().len()];
            (&mut buf[..]).write(src).unwrap();
            u32::from_le_bytes(buf)
        } as usize;
        unsafe {
            let end = addr_of!(self.payload()[self.payload().len() - 1]).add(1);
            let restarts_addr = end.sub(4).sub(restarts_len * 4) as *const u32;
            slice::from_raw_parts(restarts_addr, restarts_len)
        }
    }

    pub fn data(&self) -> &[u8] {
        let restarts = self.restarts();
        let end_pos = self.payload().len() - (restarts.len() * 4 + 4);
        &self.payload()[..end_pos]
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

    #[test]
    fn block_cache() {
        let cache = BlockCache::new(7, 10000);
        assert_eq!(7, cache.stripes.len());
        assert_eq!(10000, cache.limit_in_bytes);
    }
}