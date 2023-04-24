use std::{io, iter, slice};
use std::cell::RefCell;
use std::cmp::min;
use std::io::Write;
use std::mem::{replace, size_of, size_of_val};
use std::ptr::{addr_of, slice_from_raw_parts};
use std::sync::Arc;

use crc::{Crc, CRC_32_ISCSI};

use crate::base;
use crate::storage::config;
use crate::storage::Comparator;
use crate::storage::WritableFile;
use crate::storage::key::{InternalKey, InternalKeyComparator};
use crate::base::{Decoder, FileWriter, FixedEncode, VarintDecode, VarintEncode};

pub const SST_MAGIC_NUMBER: u32 = 0x74737300;

pub struct SSTBuilder<'a> {
    internal_key_cmp: &'a InternalKeyComparator,
    writer: FileWriter,
    written_in_bytes: u64,
    block_size: u64,
    n_restart: usize,
    approximated_n_entries: usize,

    has_seen_first_key: bool,
    is_last_level: bool,
    properties: TableProperties,
    block_builder: DataBlockBuilder,
    index_builder: DataBlockBuilder,
    filter_builder: FilterBlockBuilder,
}

#[derive(Clone, Debug, Default)]
pub struct TableProperties {
    pub last_level: bool,
    pub block_size: u64,
    pub n_entries: u32,
    pub index_position: u64,
    pub index_size: u64,
    pub filter_position: u64,
    pub filter_size: u64,
    pub last_version: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

impl TableProperties {
    pub fn new(block_size: u64) -> Self {
        let mut this = Self::default();
        this.block_size = block_size;
        this
    }

    pub fn from_unmarshal(buf: &[u8]) -> Self {
        let mut this = Self::default();
        this.unmarshal(buf);
        this
    }

    pub fn marshal_to(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(if self.last_level { 1 } else { 0 });
        self.block_size.write_to(&mut buf);
        self.n_entries.write_to(&mut buf);
        self.index_position.write_to(&mut buf);
        self.index_size.write_to(&mut buf);
        self.filter_position.write_to(&mut buf);
        self.filter_size.write_to(&mut buf);
        self.last_version.write_to(&mut buf);
        self.smallest_key.write_to(&mut buf);
        self.largest_key.write_to(&mut buf);
        buf
    }

    pub fn unmarshal(&mut self, buf: &[u8]) {
        let mut decoder = Decoder::new();
        let last_level: u8 = decoder.read_from(buf).unwrap();
        self.last_level = last_level != 0;
        self.block_size = decoder.read_from(buf).unwrap();
        self.n_entries = decoder.read_from(buf).unwrap();
        self.index_position = decoder.read_from(buf).unwrap();
        self.index_size = decoder.read_from(buf).unwrap();
        self.filter_position = decoder.read_from(buf).unwrap();
        self.filter_size = decoder.read_from(buf).unwrap();
        self.last_version = decoder.read_from(buf).unwrap();
        self.smallest_key = decoder.read_from(buf).unwrap();
        self.largest_key = decoder.read_from(buf).unwrap();
    }

    pub fn index_handle(&self) -> BlockHandle {
        BlockHandle::new(self.index_position, self.index_size)
    }

    pub fn filter_handle(&self) -> BlockHandle {
        BlockHandle::new(self.filter_position, self.filter_size)
    }
}

impl<'a> SSTBuilder<'a> {
    pub fn new(internal_key_cmp: &'a InternalKeyComparator, file: Arc<RefCell<dyn WritableFile>>,
               block_size: u64, n_restart: usize, approximated_n_entries: usize) -> Self {
        Self {
            internal_key_cmp,
            writer: FileWriter::new(file),
            written_in_bytes: 0,
            block_size,
            n_restart,
            approximated_n_entries,
            has_seen_first_key: false,
            is_last_level: false,
            properties: TableProperties::new(block_size),
            block_builder: DataBlockBuilder::new(n_restart as i32),
            index_builder: DataBlockBuilder::new(n_restart as i32),
            filter_builder: FilterBlockBuilder::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if self.filter_builder.is_empty() {
            let size_in_bytes = FilterBlockBuilder::compute_boom_filter_size(self.approximated_n_entries,
                                                                             self.block_size as usize,
                                                                             5);
            self.filter_builder.reset(size_in_bytes as u32);
        }

        let internal_key = InternalKey::parse(key);
        if !self.has_seen_first_key {
            if internal_key.sequence_number == 0 {
                self.is_last_level = true;
            }
            self.properties.smallest_key = Vec::from(key);
            self.properties.largest_key = Vec::from(key);

            self.has_seen_first_key = true;
        }

        if self.is_last_level {
            self.block_builder.add(internal_key.user_key, value);
        } else {
            self.block_builder.add(key, value);
        }
        self.filter_builder.add_key(internal_key.user_key);

        if self.block_builder.current_size_estimate() >= self.block_size as usize {
            let block = self.block_builder.finish().unwrap();
            let handle = self.write_block(&block)?;
            self.index_builder.add(&self.block_builder.last_key, &handle.marshal_to());
            self.block_builder.reset();
        }

        if self.internal_key_cmp.lt(key, &self.properties.smallest_key) {
            self.properties.smallest_key = Vec::from(key);
        }
        if self.internal_key_cmp.gt(key, &self.properties.largest_key) {
            self.properties.largest_key = Vec::from(key);
        }
        if internal_key.sequence_number > self.properties.last_version {
            self.properties.last_version = internal_key.sequence_number;
        }
        self.properties.n_entries += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> io::Result<()> {
        if !self.block_builder.is_empty() {
            let last_block = self.block_builder.finish().unwrap();
            let handle = self.write_block(&last_block)?;
            self.index_builder.add(&self.block_builder.last_key, &handle.marshal_to());
        }

        let filter_handle = {
            let bits = self.filter_builder.finish();
            assert!(!bits.is_empty());
            let ptr = addr_of!(bits[0]) as *const u8;
            let buf = unsafe {
                slice::from_raw_parts(ptr, bits.len() * size_of_val(&bits[0]))
            };
            self.write_block(buf)?
        };
        let index_handle = {
            let buf = self.index_builder.finish().unwrap();
            self.write_block(&buf)?
        };
        let props_handle = self.write_properties(index_handle, filter_handle)?;
        self.write_footer(props_handle)?;

        self.writer.flush()?;
        Ok(())
    }

    pub fn abandon(&mut self) -> io::Result<()> {
        self.block_builder.reset();
        self.index_builder.reset();
        self.filter_builder.reset(0);

        self.has_seen_first_key = false;
        self.is_last_level = false;
        self.properties = TableProperties::new(self.block_size);

        self.writer.truncate(0)?;
        self.written_in_bytes = 0;
        Ok(())
    }

    pub fn file_size(&self) -> io::Result<u64> {
        self.writer.file_size()
    }

    fn write_footer(&mut self, props_handle: BlockHandle) -> io::Result<()> {
        self.written_in_bytes += self.writer.write_fixed_u64(props_handle.offset)? as u64;
        self.written_in_bytes += self.writer.write_fixed_u32(SST_MAGIC_NUMBER)? as u64;
        Ok(())
    }

    fn write_properties(&mut self, index: BlockHandle, filter: BlockHandle) -> io::Result<BlockHandle> {
        self.properties.index_position = index.offset;
        self.properties.index_size = index.size;

        self.properties.filter_position = filter.offset;
        self.properties.filter_size = filter.size;

        self.write_block(&self.properties.marshal_to())
    }

    fn write_block(&mut self, block: &[u8]) -> io::Result<BlockHandle> {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();

        digest.update(block);

        let checksum = digest.finalize();
        let offset = self.written_in_bytes; //self.writer
        let handle = BlockHandle { offset, size: 4 + block.len() as u64 };
        self.written_in_bytes += self.writer.write_fixed_u32(checksum)? as u64;
        self.written_in_bytes += self.writer.write_varint_u32(block.len() as u32)? as u64;
        self.written_in_bytes += self.writer.write(block)? as u64;
        Ok(handle)
    }

    pub fn test_owns_file(&self) -> Arc<RefCell<dyn WritableFile>> {
        self.writer.file.clone()
    }
}

#[derive(Debug, Default)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn new(offset: u64, size: u64) -> Self {
        Self { offset, size }
    }

    pub fn from_unmarshal(buf: &[u8]) -> Self {
        let mut decoder = Decoder::new();
        Self {
            offset: decoder.read_from(buf).unwrap(),
            size: decoder.read_from(buf).unwrap(),
        }
    }

    pub fn marshal_to(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.offset.write_to(&mut buf);
        self.size.write_to(&mut buf);
        buf
    }
}

#[derive(Default)]
struct DataBlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    n_restart: i32,
    count: i32,
    has_finished: bool,
}

impl DataBlockBuilder {
    pub fn new(n_restart: i32) -> Self {
        let mut this = Self::default();
        this.n_restart = n_restart;
        this
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.last_key.clear();
        self.count = 0;
        self.has_finished = false;
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.has_finished);
        if self.count == 0 {
            self.restarts.push(self.buf.len() as u32);

            // shared-len
            0u32.write_to(&mut self.buf);
            // private-len and key
            key.write_to(&mut self.buf);
        } else {
            let shared_size = self.extract_prefix(key);
            // shared-len
            (shared_size as u32).write_to(&mut self.buf);
            // private-len
            ((key.len() - shared_size) as u32).write_to(&mut self.buf);
            // key
            self.buf.write(&key[shared_size..]).unwrap();
        }
        // value
        value.write_to(&mut self.buf);

        self.last_key.clear();
        self.last_key.write_all(key).unwrap();
        self.count = (self.count + 1) % self.n_restart;
    }

    pub fn finish(&mut self) -> Option<Vec<u8>> {
        if self.buf.is_empty() {
            return None;
        }
        for offset in self.restarts.iter() {
            (*offset).write_fixed(&mut self.buf);
        }
        (self.restarts.len() as u32).write_fixed(&mut self.buf);
        self.has_finished = true;
        Some(replace(&mut self.buf, Default::default()))
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4 + size_of::<u32>() * 2
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty() && self.restarts.is_empty() && self.last_key.is_empty()
    }

    fn extract_prefix(&self, input: &[u8]) -> usize {
        let n = min(input.len(), self.last_key.len());
        for i in 0..n {
            if self.last_key[i] != input[i] {
                return i;
            }
        }
        n
    }
}

const BLOOM_FILTER_SIZE_LIMIT: usize = 10 * config::MB;

struct FilterBlockBuilder {
    pub bits: Vec<u32>,
}

impl FilterBlockBuilder {
    pub fn new() -> Self {
        Self {
            bits: Default::default()
        }
    }

    pub fn reset(&mut self, size_in_bytes: u32) {
        assert_eq!(0, size_in_bytes % size_of::<u32>() as u32);
        let capacity = size_in_bytes as usize / size_of::<u32>();
        self.bits = Vec::from_iter(iter::repeat(0).take(capacity));
    }

    pub fn finish(&mut self) -> Vec<u32> {
        replace(&mut self.bits, Default::default())
    }

    pub fn is_empty(&self) -> bool { self.bits.is_empty() }

    pub fn add_key(&mut self, key: &[u8]) {
        for hash in base::BLOOM_FILTER_HASHES_ORDER {
            self.set_bit(hash(key) % self.size_in_bits());
        }
    }

    pub fn maybe_exists(&self, key: &[u8]) -> bool {
        for hash in base::BLOOM_FILTER_HASHES_ORDER {
            if !self.test_bit(hash(key) % self.size_in_bits()) {
                return false;
            }
        }
        true
    }

    pub fn ensure_not_exists(&self, key: &[u8]) -> bool {
        !self.maybe_exists(key)
    }

    fn compute_boom_filter_size(approximated_n_entries: usize, alignment: usize, bits: usize) -> usize {
        if approximated_n_entries == 0 {
            alignment * 2
        } else {
            let mut rv = (approximated_n_entries * (bits * 2) + 7) / 8;
            rv = config::round_up(rv, alignment as i64) + alignment;
            rv
        }
    }

    fn size_in_bits(&self) -> u32 { self.size_in_bytes() * 8 }

    fn size_in_bytes(&self) -> u32 { (self.bits.len() * size_of::<u32>()) as u32 }

    fn set_bit(&mut self, index: u32) {
        let i = index as usize;
        self.bits[i / 32] |= 1 << (i % 32);
    }

    fn test_bit(&self, index: u32) -> bool {
        let i = index as usize;
        self.bits[i / 32] & (1 << (i % 32)) != 0
    }
}

#[cfg(test)]
pub mod tests {
    use std::rc::Rc;

    use crate::base::Arena;
    use crate::storage::BitwiseComparator;
    use crate::storage::MemoryWritableFile;
    use crate::storage::key::KeyBundle;
    use crate::base::*;

    use super::*;

    #[test]
    fn hashs() {
        let input = "hello".as_bytes();
        assert_ne!(js_hash(input), sdbm_hash(input));
        assert_ne!(rs_hash(input), sdbm_hash(input));
        assert_ne!(rs_hash(input), elf_hash(input));
        assert_ne!(bkdr_hash(input), elf_hash(input));
    }

    #[test]
    fn sanity() -> io::Result<()> {
        let internal_key_cmp = internal_key_cmp();
        let mut builder = new_memory_builder(&internal_key_cmp, 3);
        let mut arena = Arena::new();
        add_keys(&mut builder,
                 &[("a", "1"), ("b", "2"), ("c", "3"), ],
                 1, &mut arena)?;
        builder.finish()?;

        let file = builder.test_owns_file().clone();
        let borrowed_file = file.borrow();
        let mem = borrowed_file.as_any().downcast_ref::<MemoryWritableFile>().unwrap();
        assert_eq!(1157, mem.buf().len());
        //dbg!(mem.buf());
        Ok(())
    }

    pub fn add_keys(builder: &mut SSTBuilder, kvs: &[(&str, &str)], sequence_number: u64, arena: &mut Arena) -> io::Result<()> {
        let mut sn = sequence_number;
        for (k, v) in kvs {
            let internal_key = KeyBundle::from_key(arena, sn, k.as_bytes());
            builder.add(internal_key.key(), v.as_bytes())?;
            sn += 1;
        }
        Ok(())
    }

    pub fn new_memory_builder(internal_key_cmp: &InternalKeyComparator, n_entries: usize) -> SSTBuilder {
        let file = MemoryWritableFile::new();

        SSTBuilder::new(internal_key_cmp,
                        Arc::new(RefCell::new(file)),
                        512,
                        4,
                        n_entries)
    }

    pub fn internal_key_cmp() -> InternalKeyComparator {
        let cmp = Rc::new(BitwiseComparator {});
        InternalKeyComparator::new(cmp)
    }
}