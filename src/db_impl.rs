use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};

use crate::mai2;
use crate::column_family::{ColumnFamilyHandle, ColumnFamilyImpl, ColumnFamilySet};
use crate::env::{Env, WritableFile};
use crate::files::paths;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DB, DEFAULT_COLUMN_FAMILY_NAME, from_io_result, Options};
use crate::status::{Corrupting, Status};
use crate::version::{VersionPatch, VersionSet};

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

pub struct Locking {
    count: u64,
}

impl Locking {
    pub fn new() -> Arc<Mutex<Locking>> {
        Arc::new(Mutex::new(Locking { count: 0 }))
    }
}

impl DBImpl {
    pub fn open(options: Options, name: String, column_family_descriptors: &[ColumnFamilyDescriptor])
                -> mai2::Result<(Arc<RefCell<dyn DB>>, Vec<Arc<RefCell<dyn ColumnFamily>>>)> {
        let env = options.env.clone();
        let abs_db_path = DBImpl::make_abs_db_path(&options.core.dir, &name, &env)?;
        let versions = VersionSet::new_dummy(abs_db_path.clone(), &options);

        let mut db = DBImpl {
            db_name: name.clone(),
            abs_db_path,
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
            mutex: Locking::new(),
        };
        if env.file_not_exists(paths::current_file(db.abs_db_path.as_path()).as_path()) {
            if !db.options.create_if_missing {
                return Err(Status::corrupted("db miss and create_if_missing is false."));
            }
            db.new_db(column_family_descriptors)?;
        } else {
            if db.options.error_if_exists {
                return Err(Status::corrupted("db exists and error_if_exists is true."));
            }
            db.recovery(column_family_descriptors)?;
        }
        let borrowed_versions = versions.borrow();
        let column_families = borrowed_versions.column_families();

        let mut cfs = Vec::<Arc<RefCell<dyn ColumnFamily>>>::new();
        for desc in column_family_descriptors {
            let borrowed_cfs = column_families.borrow();
            let cfi = borrowed_cfs.get_column_family_by_name(&desc.name).unwrap();
            let cfh = ColumnFamilyHandle::new(cfi.clone());
            cfs.push(cfh);
        }

        let cfi = column_families.borrow().default_column_family();
        db.default_column_family = Some(ColumnFamilyHandle::new(cfi));

        #[cfg(test)]
        {
            println!("----------");
            let borrowed_cfs = column_families.borrow();
            for cfi in borrowed_cfs.column_family_impls() {
                println!("cf[{}][{}]", cfi.borrow().id(), cfi.borrow().name());
            }
            println!("----------");
        }

        // TODO: start flush worker
        Ok((Arc::new(RefCell::new(db)), cfs))
    }

    fn make_abs_db_path(dir: &String, name: &String, env: &Arc<dyn Env>) -> mai2::Result<PathBuf> {
        let work_path = from_io_result(env.get_work_dir())?;
        let rs = if dir.is_empty() {
            work_path.join(Path::new(&name))
        } else {
            let parent_path = PathBuf::from(dir);
            if parent_path.is_absolute() {
                parent_path.join(Path::new(name))
            } else {
                work_path.join(parent_path).join(Path::new(name))
            }
        };
        Ok(rs)
    }

    fn new_db(&mut self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
        if self.env.file_not_exists(self.abs_db_path.as_path()) {
            from_io_result(self.env.make_dir(self.abs_db_path.as_path()))?;
        }

        {
            let borrowed_mutex = self.mutex.clone();
            let lock = borrowed_mutex.lock().unwrap();
            self.renew_logger(&lock)?;
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
            ColumnFamilySet::new_column_family(&column_families, 0, default_cf_name,
                                               self.options.core
                                                   .with_modify()
                                                   .dir(String::new())
                                                   .build());
        }

        for (name, opts) in cfs {
            if name == DEFAULT_COLUMN_FAMILY_NAME {
                continue; //
            }
            let id = {
                let mut borrowed_cfs = column_families.borrow_mut();
                borrowed_cfs.next_column_family_id()
            };
            ColumnFamilySet::new_column_family(&column_families, id, name, opts.clone());
        }

        for cfi in column_families.borrow().column_family_impls() {
            from_io_result(cfi.borrow_mut().install())?;
            cfi.borrow_mut().set_redo_log_number(self.log_file_number);
        }

        let mut patch = VersionPatch::default();
        patch.set_prev_log_number(0);
        patch.set_redo_log_number(self.log_file_number);
        {
            let borrowed_mutex = self.mutex.clone();
            let lock = borrowed_mutex.lock().unwrap();
            let mut versions = self.versions.borrow_mut();
            versions.add_sequence_number(1);
            from_io_result(versions.log_and_apply(self.options.core.clone(), patch, &lock))
        }
    }

    fn recovery(&mut self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
        todo!()
    }

    fn renew_logger(&mut self, _locking: &MutexGuard<Locking>) -> mai2::Result<()> {
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
    use rand::distributions::uniform::SampleBorrow;
    use crate::env::JunkFilesCleaner;

    use super::*;

    #[test]
    fn sanity() -> mai2::Result<()> {
        let junk = JunkFilesCleaner::new("tests/demo");

        let descs = [
            ColumnFamilyDescriptor {
                name: String::from("cf"),
                options: ColumnFamilyOptions::default(),
            }];
        let options = Options::with()
            .create_if_missing(true)
            .dir(String::from("tests"))
            .build();
        let (_db, cfs) = DBImpl::open(options, String::from("demo"), &descs)?;
        assert_eq!(1, cfs.len());

        let cf = cfs.get(0).unwrap().clone();
        assert_eq!("cf", cf.borrow().name());
        assert_ne!(0, cf.borrow().id());

        assert!(junk.path(0).exists());
        assert!(junk.path(0).join("cf").exists());
        assert!(junk.path(0).join("default").exists());
        Ok(())
    }
}