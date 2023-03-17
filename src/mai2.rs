use std::any::Any;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::sync::Arc;

//use traitcast::{TraitcastFrom, Traitcast};
use crate::comparator::{BitwiseComparator, Comparator};
use crate::env::{Env, EnvImpl};
use crate::status::{Corrupting, Status};

pub type Result<T> = std::result::Result<T, Status>;

pub const DEFAULT_COLUMN_FAMILY_NAME: &str = "default";

pub struct ColumnFamilyDescriptor {
    pub(crate) name: String,
    pub(crate) options: ColumnFamilyOptions
}

impl Default for ColumnFamilyDescriptor {
    fn default() -> Self {
        ColumnFamilyDescriptor {
            name: String::from("cf"),
            options: ColumnFamilyOptions::default()
        }
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

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        Self {
            user_comparator: Rc::new(BitwiseComparator{}),
            block_size: 4096,
            write_buf_size: 40 * 1024 * 1024,
            block_restart_interval: 16,
            dir: String::new(),
        }
    }
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
            block_cache_capacity: 10 * 1024 * 1024
        }
    }
}

pub trait ColumnFamily  {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn name(&self) -> String;
    fn id(&self) -> u32;
    fn comparator(&self) -> Rc<dyn Comparator>;
    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor>;
}

pub trait Snapshot {

}

pub trait DB {
    //fn open() -> Result<Box<dyn DB>, Status>;
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
        -> Result<Arc<RefCell<dyn ColumnFamily>>>;

    fn drop_column_family(&mut self, column_family: Arc<RefCell<dyn ColumnFamily>>) -> Result<()>;

    fn release_column_family(&mut self, column_family: Arc<RefCell<dyn ColumnFamily>>) -> Result<()>;
}

#[inline]
pub fn from_io_result<T>(rs: io::Result<T>) -> Result<T> {
    match rs {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}