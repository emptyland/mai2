use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};

use crate::column_family::{ColumnFamilyHandle, ColumnFamilyImpl, ColumnFamilySet};
use crate::env::{Env, WritableFile};
use crate::files::paths;
use crate::mai2;
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
        let versions = VersionSet::new(abs_db_path.clone(), &options);

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

    fn internal_new_column_family(&mut self, name: String, options: ColumnFamilyOptions,
                                  locking: &MutexGuard<Locking>)
                                  -> mai2::Result<u32> {
        let (id, patch) = {
            let versions = self.versions.borrow_mut();
            let mut cfs = versions.column_families().borrow_mut();
            if cfs.get_column_family_by_name(&name).is_some() {
                return Err(Status::corrupted(format!("duplicated column family name: {}", &name)));
            }

            let new_id = cfs.next_column_family_id();
            let mut patch = VersionPatch::default();
            patch.create_column_family(name.clone(), new_id,
                                       options.user_comparator.name().clone());
            (new_id, patch)
        };

        from_io_result(self.versions.borrow_mut().log_and_apply(options, patch, locking))?;
        Ok(id)
    }
}

impl DB for DBImpl {
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
                         -> mai2::Result<Arc<RefCell<dyn ColumnFamily>>> {
        let mutex = self.mutex.clone();
        let lock = mutex.lock().unwrap();
        //--------------------------lock version-set------------------------------------------------
        let id = self.internal_new_column_family(String::from(name), options, &lock)?;

        let versions = self.versions.borrow();
        let cfs = versions.column_families().borrow();
        let cfi = cfs.get_column_family_by_id(id).unwrap();
        from_io_result(cfi.borrow_mut().install())?;
        cfi.borrow_mut().set_redo_log_number(self.log_file_number);
        assert!(cfi.borrow().initialized());
        Ok(ColumnFamilyHandle::new(cfi.clone()))
        //--------------------------unlock version-set----------------------------------------------
    }

    fn drop_column_family(&mut self, column_family: Arc<RefCell<dyn ColumnFamily>>) -> mai2::Result<()> {
        {
            let cf = column_family.borrow();
            if cf.id() == 0 {
                return Err(Status::corrupted("can not drop default column family!"));
            }
        }
        let cfi = ColumnFamilyImpl::from(&column_family);
        if cfi.borrow().dropped() {
            return Err(Status::corrupted(format!("column family: {} has dropped",
                                                 cfi.borrow().name())));
        }

        let mutex = self.mutex.clone();
        let lock = mutex.lock().unwrap();
        //--------------------------lock version-set------------------------------------------------
        while cfi.borrow().background_progress() {
            // Waiting for all background progress done
            todo!()
        }
        let mut patch = VersionPatch::default();
        patch.drop_column_family(cfi.borrow().id());

        let cf_opts = cfi.borrow().options().clone();
        from_io_result(self.versions.borrow_mut()
            .log_and_apply(cf_opts, patch, &lock))?;
        from_io_result(cfi.borrow_mut().uninstall())?;
        Ok(())
        //--------------------------unlock version-set----------------------------------------------
    }

    fn get_all_column_families(&self) -> mai2::Result<Vec<Arc<RefCell<dyn ColumnFamily>>>> {
        let mutex = self.mutex.clone();
        let _lock = mutex.lock().unwrap();
        //--------------------------lock version-set------------------------------------------------

        let mut cfs: Vec<Arc<RefCell<dyn ColumnFamily>>> = self.versions
            .borrow().column_families()
            .borrow().column_family_impls()
            .iter()
            .map(|x| (**x).clone())
            .filter(|x| !x.borrow().dropped() && x.borrow().initialized())
            .map(|x| ColumnFamilyHandle::new(x.clone()))
            .collect();
        cfs.sort_by(|a, b| a.borrow().id().cmp(&b.borrow().id()));
        Ok(cfs)
        //--------------------------unlock version-set----------------------------------------------
    }

    fn default_column_family(&self) -> Arc<RefCell<dyn ColumnFamily>> {
        self.default_column_family.as_ref().unwrap().clone()
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

    #[test]
    fn default_cf() -> mai2::Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db0");
        let db = open_simple_db("db0")?;
        let cf = db.borrow().default_column_family();
        assert_eq!(0, cf.borrow().id());
        assert_eq!(DEFAULT_COLUMN_FAMILY_NAME, cf.borrow().name());

        let cfs = db.borrow().get_all_column_families()?;
        assert_eq!(1, cfs.len());
        assert_eq!(cf.borrow().id(), cfs[0].borrow().id());
        assert_eq!(cf.borrow().name(), cfs[0].borrow().name());
        Ok(())
    }

    #[test]
    fn new_cf() -> mai2::Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db1");

        let descs = [
            ColumnFamilyDescriptor {
                name: String::from("cf"),
                options: ColumnFamilyOptions::default(),
            }];
        let options = Options::with()
            .create_if_missing(true)
            .dir(String::from("tests"))
            .build();
        let (db, cfs) = DBImpl::open(options, String::from("db1"), &descs)?;
        assert_eq!(1, cfs.len());

        let cf1 = db.borrow_mut()
            .new_column_family("cf1", ColumnFamilyOptions::default())?;
        assert_eq!(2, cf1.borrow().id());
        assert_eq!("cf1", cf1.borrow().name());

        db.borrow_mut().drop_column_family(cfs[0].clone())?;

        {
            let cfs = db.borrow().get_all_column_families()?;
            assert_eq!(2, cfs.len());

            assert_eq!(0, cfs[0].borrow().id());
            assert_eq!(DEFAULT_COLUMN_FAMILY_NAME, cfs[0].borrow().name());

            assert_eq!(2, cfs[1].borrow().id());
            assert_eq!("cf1", cfs[1].borrow().name());
        }
        Ok(())
    }

    fn open_simple_db(name: &str) -> mai2::Result<Arc<RefCell<dyn DB>>> {
        let options = Options::with()
            .create_if_missing(true)
            .dir(String::from("tests"))
            .build();
        let (db, _cfs) = DBImpl::open(options, String::from(name), &Vec::new())?;
        Ok(db)
    }
}