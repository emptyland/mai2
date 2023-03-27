use std::any::Any;
use std::cell::RefCell;
use std::{io, iter};
use std::mem::size_of;
use std::rc::Rc;
use std::sync::Arc;

use crate::comparator::{BitwiseComparator, Comparator};
use crate::config;
use crate::env::{Env, EnvImpl};
use crate::status::{Corrupting, Status};

pub type Result<T> = std::result::Result<T, Status>;

pub const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

pub struct ColumnFamilyDescriptor {
    pub(crate) name: String,
    pub(crate) options: ColumnFamilyOptions,
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
    pub block_restart_interval: i32,
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
    snapshot: Option<Arc<dyn Snapshot>>,
    verify_checksum: bool,
}


pub trait ColumnFamily {
    fn as_any(&self) -> &dyn Any;
    //fn as_any_mut(&mut self) -> &mut dyn Any;
    fn name(&self) -> String;
    fn id(&self) -> u32;
    fn comparator(&self) -> Rc<dyn Comparator>;
    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor>;
}

pub trait Snapshot {}

const REDO_HEADER_SIZE: usize = size_of::<u64>() + size_of::<u32>();

#[derive(Default, Clone, Debug)]
pub struct WriteBatch {
    redo: Vec<u8>,
    number_of_ops: usize
}

impl WriteBatch {
    pub fn new() -> Self {
        let mut buf = Vec::with_capacity(REDO_HEADER_SIZE + 64);
        buf.extend(iter::repeat(0).take(REDO_HEADER_SIZE));
        Self {
            redo: buf,
            number_of_ops: 0
        }
    }

    pub fn insert(cf: &Arc<dyn ColumnFamily>, key: &[u8], value: &[u8]) {
        todo!()
    }
}




pub trait DB {
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
                         -> Result<Arc<dyn ColumnFamily>>;

    fn drop_column_family(&mut self, column_family: Arc<dyn ColumnFamily>) -> Result<()>;

    fn get_all_column_families(&self) -> Result<Vec<Arc<dyn ColumnFamily>>>;

    fn default_column_family(&self) -> Arc<dyn ColumnFamily>;

    fn get(&self, options: ReadOptions, column_family: &Arc<dyn ColumnFamily>,
           key: &[u8]) -> Result<Vec<u8>>;
}

#[inline]
pub fn from_io_result<T>(rs: io::Result<T>) -> Result<T> {
    match rs {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}