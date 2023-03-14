use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

//use traitcast::{TraitcastFrom, Traitcast};
use crate::comparator::Comparator;
use crate::status::Status;

pub struct ColumnFamilyDescriptor {
    name: String,
    options: ColumnFamilyOptions
}

impl Default for ColumnFamilyDescriptor {
    fn default() -> Self {
        ColumnFamilyDescriptor {
            name: String::from("cf"),
            options: ColumnFamilyOptions::default()
        }
    }
}

#[derive(Default)]
#[derive(Debug)]
pub struct ColumnFamilyOptions {

}

#[derive(Default)]
#[derive(Debug)]
pub struct Options {

}

pub trait ColumnFamily  {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn name(&self) -> String;
    fn id(&self) -> u32;
    fn comparator(&self) -> Rc<dyn Comparator>;
    fn get_descriptor<'a>(&self) -> Result<&'a ColumnFamilyDescriptor, Status>;
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

