mod key;
mod skip_list;
mod db;
mod column_family;
mod version;
mod files;
mod wal;
mod memory_table;
mod snapshot;
mod cache;
mod sst_builder;
mod sst_reader;
mod inline_skip_list;
mod compaction;

pub mod env;
pub mod mai2;
pub mod comparator;
pub mod iterator;
pub mod config;

use std::sync::Arc;
pub use env::*;
pub use self::mai2::*;
pub use crate::status::*;
pub use comparator::*;
pub use iterator::*;
use crate::storage::db::DBImpl;

pub type Result<T> = std::result::Result<T, Status>;

pub fn open_kv_storage(options: Options, name: String, column_family_descriptors: &[ColumnFamilyDescriptor])
    -> Result<(Arc<dyn DB>, Vec<Arc<dyn ColumnFamily>>)> {
    let (db, cfs) = DBImpl::open(options, name, column_family_descriptors)?;
    Ok((db, cfs))
}