use std::{io, iter};
use std::any::Any;
use std::cell::RefCell;
use std::io::Write;
use std::mem::size_of;
use std::rc::Rc;
use std::sync::Arc;

use crate::comparator::{BitwiseComparator, Comparator};
use crate::config;
use crate::env::{Env, EnvImpl};
use crate::key::Tag;
use crate::marshal::{VarintDecode, Decoder, Encode};
use crate::status::{Corrupting, Status};

pub type Result<T> = std::result::Result<T, Status>;

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
    pub create_if_missing: bool,
    pub create_missing_column_families: bool,
    pub error_if_exists: bool,
    pub max_open_files: u32,
    pub max_total_wal_size: usize,
    pub block_cache_capacity: usize,
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
            create_if_missing: false,
            create_missing_column_families: false,
            error_if_exists: true,
            max_open_files: 1000,
            max_total_wal_size: 80 * 1024 * 1024,
            block_cache_capacity: 10 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Default)]
pub struct ReadOptions {
    pub snapshot: Option<Arc<dyn Snapshot>>,
    pub verify_checksum: bool,
}

#[derive(Clone, Default)]
pub struct WriteOptions {
    pub sync: bool,
}


pub trait ColumnFamily {
    fn as_any(&self) -> &dyn Any;
    //fn as_any_mut(&mut self) -> &mut dyn Any;
    fn name(&self) -> String;
    fn id(&self) -> u32;
    fn comparator(&self) -> Rc<dyn Comparator>;
    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor>;
}

pub trait Snapshot {
    fn as_any(&self) -> &dyn Any;
}

pub const REDO_HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>();

#[derive(Default, Clone, Debug)]
pub struct WriteBatch {
    redo: Vec<u8>,
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
            number_of_ops: 0,
        }
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
        self.redo.push(Tag::KEY.to_byte());
        (key.len() as u32).write_to(&mut self.redo);
        self.redo.write(key).unwrap();
        (value.len() as u32).write_to(&mut self.redo);
        self.redo.write(value).unwrap();
    }

    pub fn delete(&mut self, cf: &Arc<dyn ColumnFamily>, key: &[u8]) {
        cf.id().write_to(&mut self.redo);
        self.redo.push(Tag::DELETION.to_byte());
        (key.len() as u32).write_to(&mut self.redo);
        self.redo.write(key).unwrap();
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
}


pub trait DB: Send + Sync {
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
           key: &[u8]) -> Result<Vec<u8>>;

    fn get_snapshot(&self) -> Arc<dyn Snapshot>;

    fn release_snapshot(&self, snapshot: Arc<dyn Snapshot>);
}

#[inline]
pub fn from_io_result<T>(rs: io::Result<T>) -> Result<T> {
    match rs {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}