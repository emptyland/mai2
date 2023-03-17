use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, AtomicUsize};

use crate::column_family::{ColumnFamilyHandle, ColumnFamilyImpl, ColumnFamilySet};
use crate::env::{Env, WritableFile};
use crate::{files, mai2};
use crate::files::paths;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DB, DEFAULT_COLUMN_FAMILY_NAME, from_io_result, Options};
use crate::status::{Corrupting, Status};
use crate::version::VersionSet;

pub struct DBImpl {
    pub db_name: String,
    pub abs_db_path: PathBuf,
    options: Options,
    env: Arc<dyn Env>,
    versions: Arc<RefCell<VersionSet>>,
    default_column_family: Option<Arc<RefCell<dyn ColumnFamily>>>,
    log_file: Option<Arc<RefCell<dyn WritableFile>>>,
    log_file_number: u64,
    bkg_active: AtomicI32,
    shutting_down: AtomicBool,
    total_wal_size: AtomicU64,
    flush_request: AtomicI32,
    mutex: Arc<Mutex<Locking>>,
}

struct Locking {
    count: u64
}

impl Locking {
    pub fn new() -> Arc<Mutex<Locking>> {
        Arc::new(Mutex::new(Locking{count: 0}))
    }
}

impl DBImpl {
    pub fn open(options: Options, name: String, column_family_descriptors: &[ColumnFamilyDescriptor])
                -> mai2::Result<(Arc<RefCell<dyn DB>>, Vec<Arc<RefCell<dyn ColumnFamily>>>)> {
        let env = options.env.clone();
        let versions = VersionSet::new_dummy(name.clone().into(), &options);
        let mut db = DBImpl {
            db_name: name.clone(),
            abs_db_path: env.get_absolute_path(Path::new(&name)).unwrap(),
            options,
            env: env.clone(),
            versions: versions.clone(),
            bkg_active: AtomicI32::new(0),
            shutting_down: AtomicBool::new(false),
            total_wal_size: AtomicU64::new(0),
            flush_request: AtomicI32::new(0),
            default_column_family: None,
            log_file: None,
            log_file_number: 0,
            mutex: Locking::new()
        };
        if env.file_not_exists(paths::current_file(db.abs_db_path.as_path()).as_path()) {
            if !db.options.create_if_missing {
                return Err(Status::corrupted("db miss and create_if_missing is false."))
            }
            db.new_db(column_family_descriptors)?;
        } else {
            if db.options.error_if_exists {
                return Err(Status::corrupted("db exists and error_if_exists is true."))
            }
            db.recovery(column_family_descriptors)?;
        }
        let borrowed_versions = versions.borrow();
        let column_families = borrowed_versions.column_families();

        let mut cfs = Vec::<Arc<RefCell<dyn ColumnFamily>>>::new();
        for desc in column_family_descriptors {
            let borrow = column_families.borrow();
            let cfi = borrow.get_column_family_by_name(&desc.name).unwrap();
            let cfh = ColumnFamilyHandle::new(cfi.clone());
            cfs.push(cfh);
        }

        let cfi = column_families.borrow().default_column_family();
        db.default_column_family = Some(ColumnFamilyHandle::new(cfi));

        // TODO: start flush worker
        Ok((Arc::new(RefCell::new(db)), cfs))
    }

    fn new_db(&mut self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
        if self.env.file_not_exists(self.abs_db_path.as_path()) {
            from_io_result(self.env.make_dir(self.abs_db_path.as_path()))?;
        }

        {
            let borrowed_mutex = self.mutex.clone();
            let _lock = borrowed_mutex.lock().unwrap();
            self.renew_logger()?;
        }

        let mut cfs = HashMap::<String, ColumnFamilyOptions>::new();
        for item in desc {
            cfs.insert(item.name.clone(), item.options.clone());
        }

        let column_families = self.versions.borrow().column_families().clone();
        let default_cf_name = String::from(DEFAULT_COLUMN_FAMILY_NAME);
        if let Some(opts) = cfs.get(&default_cf_name) {
            ColumnFamilySet::new_column_family(&column_families, 0, default_cf_name, opts.clone());
        } else {
            ColumnFamilySet::new_column_family(&column_families, 0, default_cf_name, self.options.core.clone());
        }

        for (name, opts) in cfs {
            if name == DEFAULT_COLUMN_FAMILY_NAME {
                continue; //
            }
            let mut borrowed_cfs = column_families.borrow_mut();
            let id = borrowed_cfs.next_column_family_id();
            ColumnFamilySet::new_column_family(&column_families, id, name, opts.clone());
        }

        for cfi in column_families.borrow().column_family_impls() {
            from_io_result(cfi.borrow_mut().install())?;
            cfi.borrow_mut().set_redo_log_number(self.log_file_number);
        }
        todo!()
    }

    fn recovery(&mut self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
        todo!()
    }

    fn renew_logger(&mut self) -> mai2::Result<()> {
        if let Some(file) = &self.log_file {
            let mut borrow = file.borrow_mut();
            from_io_result(borrow.flush())?;
            from_io_result(borrow.sync())?;
        }

        let mut versions = self.versions.borrow_mut();
        let new_log_number = versions.generate_file_number();
        let log_file_path = paths::log_file(self.abs_db_path.as_path(), new_log_number);
        let rs = from_io_result(self.env.new_writable_file(log_file_path.as_path(), true));
        if rs.is_err() {
            versions.reuse_file_number(new_log_number);
            rs?;
        }
        self.log_file_number = new_log_number;
        // TODO: logger
        Ok(())
    }
}

impl DB for DBImpl {
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
                         -> Result<Arc<RefCell<dyn ColumnFamily>>, Status> {
        todo!()
    }

    fn drop_column_family(&mut self, column_family: Arc<RefCell<dyn ColumnFamily>>) -> mai2::Result<()> {
        let _lock = self.mutex.lock().unwrap();
        todo!()
    }

    fn release_column_family(&mut self, column_family: Arc<RefCell<dyn ColumnFamily>>) -> mai2::Result<()> {
        let _lock = self.mutex.lock().unwrap();
        let cfi = ColumnFamilyImpl::from(&column_family);
        dbg!(cfi);


        todo!()
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