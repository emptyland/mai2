use std::cell::{Ref, RefCell};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::column_family::ColumnFamilyHandle;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DB, Options};
use crate::status::Status;
use crate::version::VersionSet;

pub struct DBImpl {
    versions: Arc<Mutex<VersionSet>>
}

impl DB for DBImpl {
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
                         -> Result<Arc<RefCell<dyn ColumnFamily>>, Status> {
        todo!()
    }

    fn drop_column_family(&mut self, column_family: Arc<dyn ColumnFamily>) {
        let versions = self.versions.lock().unwrap();
        todo!()
    }

    fn release_column_family(&mut self, column_family: Arc<dyn ColumnFamily>) {
        todo!()
    }
}

impl DBImpl {
    pub fn open(options: Options, name: String, column_family_descriptors: &[ColumnFamilyDescriptor])
                -> Result<(Arc<RefCell<dyn DB>>, Vec<Arc<dyn ColumnFamily>>), Status> {

        let db: Arc<RefCell<dyn DB>> = Arc::new(RefCell::new(DBImpl {
            versions: Arc::new(Mutex::new(VersionSet::new_dummy(name, options)))
        }));
        // TODO:
        // let cf = db.borrow_mut().new_column_family("ok", ColumnFamilyOptions{}).unwrap();
        // let handle = cf.borrow_mut().as_any_mut().downcast_mut::<ColumnFamilyHandle>().unwrap();

        //cf.borrow_mut().as_any().downcast_mut::<ColumnFamilyHandle>();
        Ok((db, Vec::new()))
    }
}

impl Drop for DBImpl {
    fn drop(&mut self) {
        // TODO:
        println!("drop it! DBImpl");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let descs = [
            ColumnFamilyDescriptor {
                name: String::from("cf"),
                options: ColumnFamilyOptions::default(),
            }];
        let rs = DBImpl::open(Options::default(), String::from("demo"), &descs);
        assert!(rs.is_ok());
        let (_db, _cfs) = rs.unwrap();
    }
}