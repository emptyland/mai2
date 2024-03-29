use std::{io, iter};
use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::mem::size_of;
use std::ops::Deref;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use crate::{Corrupting, new_default_logger, Result, Status};
use crate::base::{Decoder, VarintDecode, VarintEncode};
use crate::storage::{BitwiseComparator, Comparator};
use crate::storage::{Env, EnvImpl};
use crate::storage::cache::Block;
use crate::storage::IteratorArc;
use crate::storage::key::Tag;
use crate::storage::memory_table::MemoryTable;

pub const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

pub struct ColumnFamilyDescriptor {
    pub name: String,
    pub options: ColumnFamilyOptions,
}

impl Default for ColumnFamilyDescriptor {
    fn default() -> Self {
        ColumnFamilyDescriptor {
            name: String::from("cf"),
            options: ColumnFamilyOptions::default(),
        }
    }
}

pub struct ColumnFamilyOptionsBuilder {
    opts: ColumnFamilyOptions,
}

impl ColumnFamilyOptionsBuilder {
    pub fn dir(&mut self, dir: String) -> &mut Self {
        self.opts.dir = dir;
        self
    }

    pub fn temporary(&mut self, temporary: bool) -> &mut Self {
        self.opts.temporary = temporary;
        self
    }

    pub fn write_buf_size(&mut self, len: usize) -> &mut Self {
        self.opts.write_buf_size = len;
        self
    }

    pub fn build(&mut self) -> ColumnFamilyOptions {
        self.opts.clone()
    }
}

#[derive(Clone)]
pub struct ColumnFamilyOptions {
    pub user_comparator: Rc<dyn Comparator>,
    pub block_size: u64,
    pub write_buf_size: usize,
    pub block_restart_interval: usize,
    pub dir: String,
    pub temporary: bool,
}

impl ColumnFamilyOptions {
    pub fn with() -> ColumnFamilyOptionsBuilder {
        ColumnFamilyOptionsBuilder { opts: Self::default() }
    }

    pub fn with_modify(&self) -> ColumnFamilyOptionsBuilder {
        ColumnFamilyOptionsBuilder { opts: self.clone() }
    }
}

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        Self {
            user_comparator: Rc::new(BitwiseComparator {}),
            block_size: 4096,
            write_buf_size: 40 * 1024 * 1024,
            block_restart_interval: 16,
            dir: String::new(),
            temporary: false,
        }
    }
}

pub struct OptionsBuilder {
    opts: Options,
}

impl OptionsBuilder {
    pub fn new() -> Self {
        Self { opts: Options::default() }
    }

    pub fn dir(&mut self, opt: String) -> &mut OptionsBuilder {
        self.opts.core.dir = opt;
        self
    }

    pub fn create_if_missing(&mut self, opt: bool) -> &mut OptionsBuilder {
        self.opts.create_if_missing = opt;
        self
    }

    pub fn error_if_exists(&mut self, opt: bool) -> &mut Self {
        self.opts.error_if_exists = opt;
        self
    }

    pub fn build(&self) -> Options { self.opts.clone() }
}


#[derive(Clone)]
pub struct Options {
    pub core: ColumnFamilyOptions,
    pub env: Arc<dyn Env>,
    pub logger: Arc<slog::Logger>,
    pub create_if_missing: bool,
    pub create_missing_column_families: bool,
    pub error_if_exists: bool,
    pub max_open_files: usize,
    pub max_total_wal_size: usize,
    pub block_cache_capacity: usize,
    pub zstd_compression_level: i32,
}

impl Options {
    pub fn with_dir(dir: String) -> Options {
        let mut opts = Options::default();
        opts.core.dir = dir;
        opts
    }

    pub fn with() -> OptionsBuilder {
        OptionsBuilder::new()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            core: ColumnFamilyOptions::default(),
            env: EnvImpl::new(),
            logger: Arc::new(new_default_logger()),
            create_if_missing: false,
            create_missing_column_families: false,
            error_if_exists: true,
            max_open_files: 1000,
            max_total_wal_size: 80 * 1024 * 1024,
            block_cache_capacity: 10 * 1024 * 1024,
            zstd_compression_level: 3,
        }
    }
}

pub struct ReadOptionsBuilder {
    opts: ReadOptions
}

impl ReadOptionsBuilder {
    pub fn snapshot(&mut self, snapshot: Arc<dyn Snapshot>) -> &mut Self {
        self.opts.snapshot = Some(snapshot);
        self
    }

    pub fn option_snapshot(&mut self, snapshot: Option<Arc<dyn Snapshot>>) -> &mut Self {
        self.opts.snapshot = snapshot;
        self
    }

    pub fn verify_checksum(&mut self, verify_checksum: bool) -> &mut Self {
        self.opts.verify_checksum = verify_checksum;
        self
    }

    pub fn build(&self) -> ReadOptions {
        self.opts.clone()
    }
}

#[derive(Clone)]
pub struct ReadOptions {
    pub snapshot: Option<Arc<dyn Snapshot>>,
    pub verify_checksum: bool,
}

impl ReadOptions {
    pub fn with() -> ReadOptionsBuilder {
        ReadOptionsBuilder {
            opts: Self::default()
        }
    }
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            snapshot: None,
            verify_checksum: true
        }
    }
}

#[derive(Clone, Default)]
pub struct WriteOptions {
    pub sync: bool,
}

/// ColumnFamily is a single LSM-tree.
/// Use DB::new_column_family to create a new column-family.
pub trait ColumnFamily: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Name of column-family
    fn name(&self) -> String;

    /// Id of column-family, 0 is `default_column_family`, normal column-family's id is
    /// greater than 0
    fn id(&self) -> u32;
    fn temporary(&self) -> bool;
    fn comparator(&self) -> Rc<dyn Comparator>;
    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor>;
}

pub trait Snapshot: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub const REDO_HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>();

#[derive(Default, Clone, Debug)]
pub struct WriteBatch {
    redo: Vec<u8>,
    need_wal: u32,
    pub cfs: HashSet<u32>,
    pub number_of_ops: usize,
}

pub trait WriteBatchStub {
    fn did_insert(&self, cf_id: u32, key: &[u8], value: &[u8]);
    fn did_delete(&self, cf_id: u32, key: &[u8]);
}

impl WriteBatch {
    pub fn new() -> Self {
        let mut buf = Vec::with_capacity(REDO_HEADER_SIZE + 64);
        buf.extend(iter::repeat(0).take(REDO_HEADER_SIZE));
        Self {
            redo: buf,
            cfs: HashSet::default(),
            need_wal: 0,
            number_of_ops: 0,
        }
    }

    pub fn should_write_wal(&self) -> bool {
        self.need_wal > 0
    }

    pub fn should_ignore_wal(&self) -> bool {
        !self.should_write_wal()
    }

    pub fn take_redo(mut self, sequence_number: u64) -> Vec<u8> {
        let n = self.number_of_ops as u32;
        let target = self.redo.as_mut_slice();
        let mut offset = 0;
        (&mut target[offset..offset + size_of::<u64>()])
            .write(&sequence_number.to_le_bytes())
            .unwrap();
        offset += size_of::<u64>();
        (&mut target[offset..offset + size_of::<u32>()])
            .write(&n.to_le_bytes())
            .unwrap();
        self.redo
    }

    pub fn insert(&mut self, cf: &Arc<dyn ColumnFamily>, key: &[u8], value: &[u8]) {
        cf.id().write_to(&mut self.redo);
        self.redo.push(Tag::Key.to_byte());
        (key.len() as u32).write_to(&mut self.redo);
        self.redo.write(key).unwrap();
        (value.len() as u32).write_to(&mut self.redo);
        self.redo.write(value).unwrap();
        self.cfs.insert(cf.id());
        self.need_wal += if cf.temporary() { 0 } else { 1 };
    }

    pub fn delete(&mut self, cf: &Arc<dyn ColumnFamily>, key: &[u8]) {
        cf.id().write_to(&mut self.redo);
        self.redo.push(Tag::Deletion.to_byte());
        (key.len() as u32).write_to(&mut self.redo);
        self.redo.write(key).unwrap();
        self.cfs.insert(cf.id());
        self.need_wal += if cf.temporary() { 0 } else { 1 };
    }

    pub fn iterate<S>(&self, stub: &S) where S: WriteBatchStub {
        Self::iterate_since(Self::skip_header(&self.redo.as_slice()), stub)
    }

    pub fn skip_header(buf: &[u8]) -> &[u8] {
        &buf[REDO_HEADER_SIZE..]
    }

    pub fn iterate_since<S>(buf: &[u8], stub: &S) where S: WriteBatchStub {
        let mut decoder = Decoder::new();
        while decoder.offset() < buf.len() {
            let cf_id: u32 = decoder.read_from(buf).unwrap();
            let tag: u8 = decoder.read_from(buf).unwrap();
            let key: Vec<u8> = decoder.read_from(buf).unwrap();
            match tag {
                0 => {
                    let value: Vec<u8> = decoder.read_from(buf).unwrap();
                    stub.did_insert(cf_id, key.as_slice(), value.as_slice())
                }
                1 => stub.did_delete(cf_id, key.as_slice()),
                _ => unreachable!()
            }
        }
    }

    pub fn iterate_since_raw<F>(buf: &[u8], mut callback: F) -> Result<usize>
        where F: FnMut(u32, &[u8]) -> Result<usize> {
        let mut decoder = Decoder::new();
        while decoder.offset() < buf.len() {
            let start = decoder.offset();
            let cf_id: u32 = decoder.read_from(buf).unwrap();
            let _tag: u8 = decoder.read_from(buf).unwrap();
            let key_len: u32 = decoder.read_from(buf).unwrap();
            decoder.advance(key_len as usize);
            let stop = decoder.offset();

            callback(cf_id, &buf[start..stop])?;
        }
        Ok(decoder.offset())
    }
}


pub trait DB: Send + Sync {
    fn get_absolute_path(&self) -> &Path;

    fn new_column_family(&self, name: &str, options: ColumnFamilyOptions)
                         -> Result<Arc<dyn ColumnFamily>>;

    fn drop_column_family(&self, column_family: Arc<dyn ColumnFamily>) -> Result<()>;

    fn get_all_column_families(&self) -> Result<Vec<Arc<dyn ColumnFamily>>>;

    fn default_column_family(&self) -> Arc<dyn ColumnFamily>;

    fn insert(&self, options: &WriteOptions, cf: &Arc<dyn ColumnFamily>, key: &[u8], value: &[u8]) -> Result<()> {
        let mut update = WriteBatch::new();
        update.insert(cf, key, value);
        self.write(options, update)
    }

    fn delete(&self, options: &WriteOptions, cf: &Arc<dyn ColumnFamily>, key: &[u8]) -> Result<()> {
        let mut update = WriteBatch::new();
        update.delete(cf, key);
        self.write(options, update)
    }

    fn write(&self, options: &WriteOptions, updates: WriteBatch) -> Result<()>;

    fn get(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>,
           key: &[u8]) -> Result<Vec<u8>> {
        let value = self.get_pinnable(options, column_family, key)?;
        Ok(value.to_vec())
    }

    fn get_pinnable(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>,
                    key: &[u8]) -> Result<PinnableValue>;

    // fn nearly_prefix_pinnable(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>,
    //                           prefix: &[u8]) -> Result<PinnableValue>;

    fn new_iterator(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>)
                    -> Result<IteratorArc>;

    fn get_snapshot(&self) -> Arc<dyn Snapshot>;
}

pub struct PinnableValue {
    owns: PinnableOwnership,
    addr: *const [u8],
}

enum PinnableOwnership {
    Block(Block),
    Table(Arc<MemoryTable>),
}

impl PinnableValue {
    pub fn from_block(block: &Block, value: &[u8]) -> Self {
        Self {
            owns: PinnableOwnership::Block(block.clone()),
            addr: value as *const [u8],
        }
    }

    pub fn from_memory_table(table: &Arc<MemoryTable>, value: &[u8]) -> Self {
        Self {
            owns: PinnableOwnership::Table(table.clone()),
            addr: value as *const [u8],
        }
    }

    pub fn value(&self) -> &[u8] {
        unsafe { &*self.addr }
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.value().into()
    }

    pub fn to_utf8_string(&self) -> String {
        String::from_utf8_lossy(self.value()).to_string()
    }
}

impl Debug for PinnableValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.value())
    }
}

impl Deref for PinnableValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl Into<Vec<u8>> for PinnableValue {
    fn into(self) -> Vec<u8> {
        self.value().into()
    }
}

#[inline]
pub fn from_io_result<T>(rs: io::Result<T>) -> Result<T> {
    match rs {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}