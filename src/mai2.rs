use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

//use traitcast::{TraitcastFrom, Traitcast};
use crate::comparator::{BitwiseComparator, Comparator};
use crate::status::Status;

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
}

impl Default for ColumnFamilyOptions {
    fn default() -> Self {
        Self {
            user_comparator: Rc::new(BitwiseComparator{}),
            block_size: 4096,
        }
    }
}


#[derive(Clone)]
pub struct Options {
    pub user_comparator: Rc<dyn Comparator>,
    pub block_size: u64,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            user_comparator: Rc::new(BitwiseComparator{}),
            block_size: 4096
        }
    }
}

pub trait ColumnFamily  {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn name(&self) -> String;
    fn id(&self) -> u32;
    fn comparator(&self) -> Rc<dyn Comparator>;
    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor, Status>;
}

pub trait Snapshot {

}

pub trait DB {
    //fn open() -> Result<Box<dyn DB>, Status>;
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
        -> Result<Arc<RefCell<dyn ColumnFamily>>, Status>;

    fn drop_column_family(&mut self, column_family: Arc<dyn ColumnFamily>);

    fn release_column_family(&mut self, column_family: Arc<dyn ColumnFamily>);
}

