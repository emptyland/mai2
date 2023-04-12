use std::cell::RefCell;
use std::{io, iter};
use std::cmp::Ordering::Equal;
use std::ffi::c_void;
use std::io::Write;
use std::iter::Iterator;
use std::mem::size_of_val;
use std::ops::DerefMut;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use crc::{Crc, CRC_32_ISCSI};
use crate::cache::{Block, BlockCache};
use crate::comparator::Comparator;
use crate::env::RandomAccessFile;
use crate::{iterator, mai2, utils};
use crate::iterator::{Direction, Iterator as MaiIterator};
use crate::key::{InternalKey, InternalKeyComparator, KeyBundle, Tag};
use crate::mai2::{from_io_result, PinnableValue, ReadOptions};
use crate::marshal::{Decoder, RandomAccessFileReader, VarintDecode};
use crate::sst_builder::{BlockHandle, SST_MAGIC_NUMBER, TableProperties};
use crate::status::{Corrupting, Status};
use crate::varint::{MAX_VARINT32_LEN, Varint};

pub struct SSTReader {
    //this: Weak<SSTReader>,
    file: Rc<RefCell<dyn RandomAccessFile>>,
    file_number: u64,
    file_size: u64,
    checksum_verify: bool,
    block_cache: Arc<BlockCache>,
    table_properties: TableProperties,
    keys_filter: Option<KeyBloomFilter>,
    // TODO:
}

impl SSTReader {
    pub fn new(file: Rc<RefCell<dyn RandomAccessFile>>, file_number: u64,
               file_size: u64, checksum_verify: bool, block_cache: Arc<BlockCache>) -> io::Result<Self> {
        Self {
            file,
            file_number,
            file_size,
            checksum_verify,
            block_cache,
            table_properties: Default::default(),
            keys_filter: None,
        }.prepare()
    }

    fn prepare(mut self) -> io::Result<Self> {
        if self.file_size < 12 {
            Err(io::Error::new(io::ErrorKind::InvalidData, "file size too small"))?;
        }

        let reader = RandomAccessFileReader::new(self.file.clone());
        let magic_number = reader.read_fixed32(self.file_size - 4)?;
        if magic_number != SST_MAGIC_NUMBER {
            Err(io::Error::new(io::ErrorKind::InvalidData, "incorrect file magic number"))?;
        }

        let props_pos = reader.read_fixed64(self.file_size - 12)?;
        if props_pos >= self.file_size - 12 {
            Err(io::Error::new(io::ErrorKind::InvalidData, "incorrect table properties position"))?;
        }

        let props_handle = BlockHandle::new(props_pos, self.file_size - 12 - props_pos);
        let props_block = self.read_block(props_handle)?;
        self.table_properties = TableProperties::from_unmarshal(&props_block);

        // index
        if self.checksum_verify {
            self.read_block(self.table_properties.index_handle())?;
        }

        // filter
        let filter_bits = self.read_block(self.table_properties.filter_handle())?;
        assert_eq!(filter_bits.len() % 4, 0);
        let mut filter_buckets = Vec::new();
        for i in 0..filter_bits.len() / 4 {
            let mut buf: [u8; 4] = [0; 4];
            (&mut buf[..]).write(&filter_bits[i * 4..i * 4 + 4]).unwrap();
            filter_buckets.push(u32::from_le_bytes(buf));
        }
        drop(filter_bits);

        self.keys_filter = Some(KeyBloomFilter::new(filter_buckets));
        Ok(self)
    }

    pub fn keys_filter(&self) -> &KeyBloomFilter {
        self.keys_filter.as_ref().unwrap()
    }

    pub fn get(&self, _read_opts: &ReadOptions, internal_key_cmp: &InternalKeyComparator,
               target: &[u8]) -> mai2::Result<(PinnableValue, Tag)> {
        let mut index_iter = from_io_result(self.new_index_iter(internal_key_cmp))?;
        index_iter.seek(target);
        if index_iter.status() != Status::Ok {
            Err(index_iter.status())?;
        }
        if !index_iter.valid() {
            Err(Status::NotFound)?;
        }
        let handle = BlockHandle::from_unmarshal(index_iter.value());
        let mut block_iter = from_io_result(self.new_block_iter(internal_key_cmp, handle))?;
        block_iter.seek(target);
        if block_iter.status() != Status::Ok {
            Err(block_iter.status())?;
        }
        if !block_iter.valid() {
            Err(Status::NotFound)?;
        }

        let internal_key = InternalKey::parse(block_iter.key());
        // dbg!(&internal_key.user_key);
        // dbg!(InternalKey::extract_user_key(target));
        if internal_key_cmp
            .user_cmp
            .compare(internal_key.user_key, InternalKey::extract_user_key(target)) != Equal {
            Err(Status::NotFound)?;
        }
        let value = PinnableValue::from_block(&block_iter.block, block_iter.value());
        Ok((value, internal_key.tag))
    }

    pub fn new_iterator(this: &Rc<Self>, read_opts: &ReadOptions,
                        internal_key_cmp: &InternalKeyComparator) -> io::Result<IteratorImpl> {
        let iter = IteratorImpl::new(internal_key_cmp,
                                     this.new_index_iter(internal_key_cmp)?,
                                     read_opts.verify_checksum,
                                     this);
        Ok(iter)
    }

    fn new_index_iter(&self, internal_key_cmp: &InternalKeyComparator) -> io::Result<BlockIterator> {
        let block = self.block_cache.get(self.file.borrow_mut().deref_mut(),
                                         self.file_number,
                                         self.table_properties.index_position,
                                         self.checksum_verify)?;
        Ok(BlockIterator::new(internal_key_cmp.clone(), block))
    }

    fn new_block_iter(&self, internal_key_cmp: &InternalKeyComparator, handle: BlockHandle)
                      -> io::Result<BlockIterator> {
        let block = self.block_cache.get(self.file.borrow_mut().deref_mut(),
                                         self.file_number,
                                         handle.offset,
                                         self.checksum_verify)?;
        Ok(BlockIterator::new(internal_key_cmp.clone(), block))
    }

    fn read_block(&self, handle: BlockHandle) -> io::Result<Vec<u8>> {
        //let mut buf = Vec::from_iter(iter::repeat(0).take(handle.size as usize));
        let crc32sum = {
            let mut buf: [u8; 4] = [0; 4];
            self.file.borrow_mut().positioned_read(handle.offset, &mut buf[..])?;
            u32::from_le_bytes(buf)
        };

        let (len, delta) = {
            let mut buf: [u8; MAX_VARINT32_LEN] = [0; MAX_VARINT32_LEN];
            self.file.borrow_mut().positioned_read(handle.offset + 4, &mut buf[..])?;
            Varint::<u32>::decode(&buf)?
        };
        let mut buf = Vec::from_iter(iter::repeat(0).take(len as usize));
        self.file.borrow_mut().positioned_read(handle.offset + 4 + delta as u64, &mut buf)?;
        if self.checksum_verify {
            let crc = Crc::<u32>::new(&CRC_32_ISCSI);
            let mut digest = crc.digest();
            digest.update(&buf);

            if crc32sum != digest.finalize() {
                Err(io::Error::new(io::ErrorKind::InvalidData, "crc32 checksum fail"))?;
            }
        }
        Ok(buf)
    }
}

impl Drop for SSTReader {
    fn drop(&mut self) {
        self.block_cache.invalidate(self.file_number);
    }
}

pub struct KeyBloomFilter {
    bits: Vec<u32>,
    n_bits: usize,
}

impl KeyBloomFilter {
    pub fn new(bits: Vec<u32>) -> Self {
        let n_bits = bits.len() * 32;
        Self {
            bits,
            n_bits,
        }
    }

    pub fn may_exists(&self, key: &[u8]) -> bool {
        for hash in utils::BLOOM_FILTER_HASHES_ORDER {
            let h = hash(key) as usize;
            if !self.test_bit(h % self.n_bits) {
                return false;
            }
        }
        return true;
    }

    pub fn ensure_not_exists(&self, key: &[u8]) -> bool { !self.may_exists(key) }

    pub fn used_memory_in_bytes(&self) -> usize {
        size_of_val(self) + self.bits.len() * 4
    }

    fn test_bit(&self, i: usize) -> bool {
        self.bits[i / 32] & (1u32 << (i % 32)) != 0
    }
}

pub struct IteratorImpl {
    owns: Rc<SSTReader>,
    internal_key_cmp: InternalKeyComparator,
    index_iter: BlockIterator,
    checksum_verify: bool,
    block_iter: Option<BlockIterator>,
    saved_key: Vec<u8>,
    direction: Direction,
    status: Status,
}

impl IteratorImpl {
    pub fn new(internal_key_cmp: &InternalKeyComparator, index_iter: BlockIterator,
               checksum_verify: bool, owns: &Rc<SSTReader>) -> Self {
        Self {
            owns: owns.clone(),
            internal_key_cmp: internal_key_cmp.clone(),
            index_iter,
            checksum_verify,
            block_iter: None,
            saved_key: Default::default(),
            direction: Direction::Forward,
            status: Status::Ok,
        }
    }

    fn seek_to_block(&mut self, handle: BlockHandle, to_first: bool) {
        let rs = self.owns.new_block_iter(&self.internal_key_cmp, handle);
        if let Err(e) = rs {
            self.status = Status::corrupted(e.to_string());
            return;
        }
        self.block_iter = Some(rs.unwrap());

        if to_first {
            self.block_iter_mut().seek_to_first();
        } else {
            self.block_iter_mut().seek_to_last();
        }
    }

    fn save_key_if_needed(&mut self) {
        if self.valid() && self.owns.table_properties.last_level {
            self.saved_key = InternalKey::from_key(self.block_iter().key(), 0, Tag::Key);
        }
    }

    fn block_iter(&self) -> &BlockIterator {
        self.block_iter.as_ref().unwrap()
    }

    fn block_iter_mut(&mut self) -> &mut BlockIterator {
        self.block_iter.as_mut().unwrap()
    }
}

impl iter::Iterator for IteratorImpl {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl iterator::Iterator for IteratorImpl {
    fn valid(&self) -> bool {
        self.status == Status::Ok &&
            self.index_iter.valid() &&
            self.block_iter.is_some() &&
            self.block_iter().valid()
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.direction = Direction::Forward;

        if self.index_iter.valid() {
            let handle = BlockHandle::from_unmarshal(self.index_iter.value());
            self.seek_to_block(handle, true);
        }
        self.save_key_if_needed();
    }

    fn seek_to_last(&mut self) {
        self.index_iter.seek_to_last();
        self.direction = Direction::Reserve;

        if self.index_iter.valid() {
            let handle = BlockHandle::from_unmarshal(self.index_iter.value());
            self.seek_to_block(handle, false);
        }
    }

    fn seek(&mut self, key: &[u8]) {
        self.direction = Direction::Forward;
        self.index_iter.seek(key);
        if !self.index_iter.valid() {
            return;
        }

        let handle = BlockHandle::from_unmarshal(self.index_iter.value());
        self.seek_to_block(handle, true);
        if !self.block_iter().valid() {
            self.status = Status::NotFound;
        }
        self.save_key_if_needed();
    }

    fn move_next(&mut self) {
        assert!(self.valid());
        self.direction = Direction::Forward;
        //iterator::Iterator::next(self.block_iter_mut());
        self.block_iter_mut().move_next();

        if !self.block_iter().valid() {
            //iterator::Iterator::next(&mut self.index_iter);
            self.index_iter.move_next();
            if self.index_iter.valid() {
                let handle = BlockHandle::from_unmarshal(self.index_iter.value());
                self.seek_to_block(handle, true);
            }
        }
        self.save_key_if_needed();
    }

    fn move_prev(&mut self) {
        assert!(self.valid());
        self.direction = Direction::Reserve;
        self.block_iter_mut().move_prev();

        if !self.block_iter().valid() {
            self.index_iter.move_prev();
            if self.index_iter.valid() {
                let handle = BlockHandle::from_unmarshal(self.index_iter.value());
                self.seek_to_block(handle, false);
            }
        }
        self.save_key_if_needed();
    }

    fn key(&self) -> &[u8] {
        assert!(self.valid());
        if self.owns.table_properties.last_level {
            &self.saved_key
        } else {
            self.block_iter().key()
        }
    }

    fn value(&self) -> &[u8] {
        assert!(self.valid());
        self.block_iter().value()
    }

    fn status(&self) -> Status { self.status.clone() }
}

pub struct BlockIterator {
    internal_key_cmp: InternalKeyComparator,
    block: Block,
    //restarts: &'a [u32],
    current_restart: isize,
    current_local: isize,
    local: Vec<(Vec<u8>, *const [u8])>,
    status: Status,
}

impl BlockIterator {
    pub fn new(internal_key_cmp: InternalKeyComparator, block: Block) -> Self {
        Self {
            internal_key_cmp,
            //restarts: block.restarts(),
            block,
            //restarts,
            current_local: 0,
            current_restart: 0,
            local: Default::default(),
            //status: Cell::new(Status::Ok),
            status: Status::Ok,
        }
    }

    fn restart(&self, i: usize) -> usize {
        self.block.restarts()[i] as usize
    }

    fn prepare_read(&mut self, i: usize) {
        let restarts = self.block.restarts();
        assert!(i < restarts.len());

        self.local.clear();
        let mut p = restarts[i] as usize;
        let end = if i == restarts.len() - 1 {
            self.block.data().len()
        } else {
            restarts[i + 1] as usize
        };

        let mut last_key = Vec::default();
        while p < end {
            let rs = self.read(&last_key, p);
            if let Err(e) = rs {
                self.status = Status::corrupted(e.to_string());
                break;
            }
            let (key, value, next_pos) = rs.unwrap();
            p = next_pos;

            last_key = key.clone();
            self.local.push((key, value as *const [u8]));
        }
    }

    fn read(&self, prev_key: &[u8], position: usize) -> io::Result<(Vec<u8>, &[u8], usize)> {
        let buf = &self.block.payload()[position..];
        let mut decoder = Decoder::new();
        let shared_len: u32 = decoder.read_from(buf)?;
        //let private_len: u32 = decoder.read_from(buf)?;

        // shared part of key
        let mut key = Vec::from(&prev_key[0..shared_len as usize]);
        // private part of key
        let private_part = decoder.read_slice(buf)?;
        private_part.iter().for_each(|x| { key.push(*x) });

        let value = decoder.read_slice(buf)?;
        Ok((key, value, position + decoder.offset()))
    }
}

impl iter::Iterator for BlockIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

impl iterator::Iterator for BlockIterator {
    fn valid(&self) -> bool {
        return self.status == Status::Ok &&
            self.current_restart >= 0 && self.current_restart < self.block.restarts().len() as isize &&
            self.current_restart >= 0 && self.current_local < self.local.len() as isize;
    }

    fn seek_to_first(&mut self) {
        self.prepare_read(0);
        self.current_restart = 0;
        self.current_local = 0;
    }

    fn seek_to_last(&mut self) {
        self.prepare_read(self.block.restarts().len() - 1);
        self.current_restart = self.block.restarts().len() as isize - 1;
        self.current_local = self.local.len() as isize - 1;
    }

    fn seek(&mut self, target: &[u8]) {
        let mut count = self.block.restarts().len();
        let mut first = 0;
        while count > 0 {
            let mut it = first;
            let step = count / 2;
            it += step;

            let rs = self.read(&[], self.restart(it));
            if let Err(e) = rs {
                self.status = Status::corrupted(e.to_string());
                return;
            }

            let (key, _, _) = rs.unwrap();
            if !self.internal_key_cmp.lt(target, &key) {
                it += 1;
                first = it;
                count -= step + 1;
            } else {
                count = step;
            }
        }
        if first != 0 {
            first -= 1;
        } else if first >= self.block.restarts().len() {
            first = 0;
        }

        for i in first..self.block.restarts().len() {
            if i > first + 1 {
                break;
            }
            self.prepare_read(i);
            if self.status != Status::Ok {
                break;
            }

            for idx in 0..self.local.len() {
                if !self.internal_key_cmp.lt(&self.local[idx].0, target) {
                    self.current_local = idx as isize;
                    self.current_restart = first as isize;
                    return;
                }
            }
        }
        self.status = Status::NotFound;
    }

    fn move_next(&mut self) {
        if self.current_local >= self.local.len() as isize - 1 {
            if self.current_restart < self.block.restarts().len() as isize - 1 {
                self.current_restart += 1;
                self.prepare_read(self.current_restart as usize);
            } else {
                self.current_restart += 1;
            }
            self.current_local = 0;
            return;
        }
        self.current_local += 1;
    }

    fn move_prev(&mut self) {
        if self.current_local == 0 {
            if self.current_restart > 0 {
                self.current_restart -= 1;
                self.prepare_read(self.current_restart as usize);
            } else {
                self.current_restart -= 1;
            }
            self.current_local = self.local.len() as isize - 1;
            return;
        }
        self.current_local -= 1;
    }

    fn key(&self) -> &[u8] {
        &self.local[self.current_local as usize].0
    }

    fn value(&self) -> &[u8] {
        unsafe { &*self.local[self.current_local as usize].1 }
    }

    fn status(&self) -> Status { self.status.clone() }
}

#[cfg(test)]
mod tests {
    use crate::arena::Arena;
    use crate::env::{MemoryRandomAccessFile, MemoryWritableFile};
    use super::*;
    use crate::sst_builder::tests::*;

    #[test]
    fn sanity() -> io::Result<()> {
        let chunk = build_sst_memory_chunk(&[
            ("aaa", "1"),
            ("bbb", "2"),
            ("ccc", "3")
        ], 1)?;
        let reader = new_sst_memory_reader(chunk)?;

        assert_eq!(512, reader.table_properties.block_size);
        assert_eq!(3, reader.table_properties.n_entries);
        assert_eq!(3, reader.table_properties.last_version);
        assert!(!reader.table_properties.last_level);

        let rd_opts = ReadOptions::default();
        let internal_key_cmp = internal_key_cmp();
        let rv = reader.get(&rd_opts, &internal_key_cmp,
                            &InternalKey::from_str_key("bbb", 3));
        assert!(rv.is_ok());
        assert_eq!("2".as_bytes(), rv.unwrap().0.value());
        Ok(())
    }

    #[test]
    fn get_keys() -> io::Result<()> {
        let chunk = build_sst_memory_chunk(&[
            ("aaa", "1"),
            ("bbb", "2"),
            ("ccc", "3"),
        ], 1)?;
        let reader = new_sst_memory_reader(chunk)?;

        let rd_opts = ReadOptions::default();
        let internal_key_cmp = internal_key_cmp();

        {
            let key = InternalKey::from_str_key("aaa", 1);
            let (value, tag) = reader.get(&rd_opts, &internal_key_cmp,
                                          &key).unwrap();
            assert_eq!("1".as_bytes(), value.value());
            assert_eq!(Tag::Key, tag);
        }

        {
            let key = InternalKey::from_str_key("aaa", 4);
            let (value, tag) = reader.get(&rd_opts, &internal_key_cmp,
                                          &key).unwrap();
            assert_eq!("1".as_bytes(), value.value());
            assert_eq!(Tag::Key, tag);
        }

        {
            let key = InternalKey::from_str_key("bbb", 4);
            let (value, tag) = reader.get(&rd_opts, &internal_key_cmp,
                                          &key).unwrap();

            assert_eq!("2".as_bytes(), value.value());
            assert_eq!(Tag::Key, tag);
        }
        Ok(())
    }

    #[test]
    fn iterate_move_next() -> io::Result<()> {
        let chunk = build_sst_memory_chunk(&[
            ("aaa", "1"),
            ("bbb", "2"),
            ("ccc", "3"),
            ("ddd", "4"),
            ("eee", "5"),
        ], 1)?;
        let reader = Rc::new(new_sst_memory_reader(chunk)?);
        let rd_opts = ReadOptions::default();
        let internal_key_cmp = internal_key_cmp();
        let mut iter = SSTReader::new_iterator(&reader, &rd_opts, &internal_key_cmp)?;
        iter.seek_to_first();
        assert!(iter.valid());
        assert_eq!("aaa".as_bytes(), InternalKey::extract_user_key(iter.key()));
        assert_eq!("1".as_bytes(), iter.value());

        iter.move_next();
        assert!(iter.valid());
        assert_eq!("bbb".as_bytes(), InternalKey::extract_user_key(iter.key()));
        assert_eq!("2".as_bytes(), iter.value());

        iter.move_next();
        assert!(iter.valid());
        assert_eq!("ccc".as_bytes(), InternalKey::extract_user_key(iter.key()));
        assert_eq!("3".as_bytes(), iter.value());

        iter.move_next();
        assert!(iter.valid());
        assert_eq!("ddd".as_bytes(), InternalKey::extract_user_key(iter.key()));
        assert_eq!("4".as_bytes(), iter.value());

        iter.move_next();
        assert!(iter.valid());
        assert_eq!("eee".as_bytes(), InternalKey::extract_user_key(iter.key()));
        assert_eq!("5".as_bytes(), iter.value());

        iter.move_next();
        assert!(!iter.valid());

        Ok(())
    }

    #[test]
    fn iterate_move_prev() -> io::Result<()> {
        let chunk = build_sst_memory_chunk(&[
            ("aaa", "1"),
            ("bbb", "2"),
            ("ccc", "3"),
        ], 1)?;
        let reader = Rc::new(new_sst_memory_reader(chunk)?);
        let rd_opts = ReadOptions::default();
        let internal_key_cmp = internal_key_cmp();
        let mut iter = SSTReader::new_iterator(&reader, &rd_opts, &internal_key_cmp)?;
        {
            iter.seek_to_last();
            assert!(iter.valid());
            let key = InternalKey::parse(iter.key());
            assert_eq!("ccc".as_bytes(), key.user_key);
            assert_eq!(3, key.sequence_number);
            assert_eq!(Tag::Key, key.tag);
            assert_eq!("3".as_bytes(), iter.value());
        }

        {
            iter.move_prev();
            assert!(iter.valid());
            let key = InternalKey::parse(iter.key());
            assert_eq!("bbb".as_bytes(), key.user_key);
            assert_eq!(2, key.sequence_number);
            assert_eq!(Tag::Key, key.tag);
            assert_eq!("2".as_bytes(), iter.value());
        }

        {
            iter.move_prev();
            assert!(iter.valid());
            let key = InternalKey::parse(iter.key());
            assert_eq!("aaa".as_bytes(), key.user_key);
            assert_eq!(1, key.sequence_number);
            assert_eq!(Tag::Key, key.tag);
            assert_eq!("1".as_bytes(), iter.value());
        }

        iter.move_prev();
        assert!(!iter.valid());
        Ok(())
    }

    fn new_sst_memory_reader(chunk: Vec<u8>) -> io::Result<SSTReader> {
        let cache = BlockCache::new(7, 10000);
        let file_size = chunk.len() as u64;
        let file = MemoryRandomAccessFile::new_rc(chunk);
        SSTReader::new(file, 1, file_size, true, Arc::new(cache))
    }

    fn build_sst_memory_chunk(kvs: &[(&str, &str)], sequence_number: u64) -> io::Result<Vec<u8>> {
        let internal_key_cmp = internal_key_cmp();
        let mut builder = new_memory_builder(&internal_key_cmp, kvs.len());
        let mut arena = Arena::new();
        add_keys(&mut builder, kvs, sequence_number, &mut arena)?;
        builder.finish()?;

        let file = builder.test_owns_file().clone();
        let borrowed_file = file.borrow();
        let mem = borrowed_file.as_any().downcast_ref::<MemoryWritableFile>().unwrap();
        Ok(mem.buf().clone())
    }
}