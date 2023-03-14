use std::cell::{Ref, RefCell};
use std::sync::Arc;
use crate::column_family::ColumnFamilyHandle;

use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DB, Options};
use crate::status::Status;

pub struct DBImpl {

}

impl DB for DBImpl {

    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
        -> Result<Arc<RefCell<dyn ColumnFamily>>, Status> {
        todo!()
    }

    fn drop_column_family(&mut self, column_family: Arc<dyn ColumnFamily>) {
        todo!()
    }

    fn release_column_family(&mut self, column_family: Arc<dyn ColumnFamily>) {
        todo!()
    }
}

impl DBImpl {
    pub fn open(options: Options, name: String, column_family_descriptors: &[ColumnFamilyDescriptor])
            -> Result<(Arc<RefCell<dyn DB>>, Vec<Arc<dyn ColumnFamily>>), Status> {
        let db : Arc<RefCell<dyn DB>> = Arc::new(RefCell::new(DBImpl{

        }));
        // TODO:
        let cf = db.borrow_mut().new_column_family("ok", ColumnFamilyOptions{}).unwrap();
        let handle = cf.borrow_mut().as_any_mut().downcast_mut::<ColumnFamilyHandle>().unwrap();

        //cf.borrow_mut().as_any().downcast_mut::<ColumnFamilyHandle>();
        Ok((db, Vec::new()))
    }
}