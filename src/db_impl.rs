use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::{io, thread};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;

use crate::column_family::{ColumnFamilyHandle, ColumnFamilyImpl, ColumnFamilySet};
use crate::env::{Env, WritableFile};
use crate::files::{Kind, paths};
use crate::{files, mai2, wal};
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DB, DEFAULT_COLUMN_FAMILY_NAME, from_io_result, Options, ReadOptions};
use crate::status::{Corrupting, Status};
use crate::version::{VersionPatch, VersionSet};
use crate::wal::LogWriter;

pub struct DBImpl {
    pub db_name: String,
    pub abs_db_path: PathBuf,
    options: Options,
    env: Arc<dyn Env>,
    versions: Arc<Mutex<VersionSet>>,
    default_column_family: Option<Arc<dyn ColumnFamily>>,
    redo_log: Option<WALDescriptor>,
    bkg_active: AtomicI32,
    shutting_down: AtomicBool,
    total_wal_size: AtomicU64,
    //flush_request: AtomicI32,
    flush_worker: Option<WorkerDescriptor>,
    //mutex: Arc<Mutex<Locking>>,
}

enum WorkerCommand {
    Work(Arc<RefCell<dyn WritableFile>>, Arc<Mutex<VersionSet>>),
    Exit,
}

unsafe impl Send for WorkerCommand {}

struct WorkerDescriptor {
    tx: Sender<WorkerCommand>,
    //rx: Receiver<WorkerCommand>,
    join_handle: JoinHandle<()>,
}

// Write-ahead-log
struct WALDescriptor {
    log: LogWriter,
    file: Arc<RefCell<dyn WritableFile>>,
    number: u64,
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
                -> mai2::Result<(Arc<RefCell<dyn DB>>, Vec<Arc<dyn ColumnFamily>>)> {
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
            default_column_family: None,
            redo_log: None,
            flush_worker: None,
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
        let locked_versions = versions.lock().unwrap();
        let column_families = locked_versions.column_families();

        let mut cfs = Vec::<Arc<dyn ColumnFamily>>::new();
        for desc in column_family_descriptors {
            let borrowed_cfs = column_families.borrow();
            let cfi = borrowed_cfs.get_column_family_by_name(&desc.name).unwrap();
            let cfh = ColumnFamilyHandle::new(cfi, &versions);
            cfs.push(cfh);
        }

        let cfi = column_families.borrow().default_column_family();
        db.default_column_family = Some(ColumnFamilyHandle::new(&cfi, &versions));

        #[cfg(test)]
        {
            println!("----------");
            let borrowed_cfs = column_families.borrow();
            for cfi in borrowed_cfs.column_family_impls() {
                println!("cf[{}][{}]", cfi.borrow().id(), cfi.borrow().name());
            }
            println!("----------");
        }

        Ok((Arc::new(RefCell::new(db)), cfs))
    }

    fn flush_redo_log(&mut self, sync: bool, locking: &MutexGuard<VersionSet>) {
        if let Some(desc) = self.redo_log.as_ref() {
            let worker = self.flush_worker.as_ref().unwrap();
            if sync {
                todo!()
            } else {
                worker.tx.send(WorkerCommand::Work(desc.file.clone(),
                                                   self.versions.clone())).unwrap();
            }
        }
    }

    fn start_flush_worker(&mut self) {
        let (tx, rx) = channel();
        let join_handle = thread::Builder::new()
            .name("flush-worker".to_string())
            .spawn(move || {
                loop {
                    let command = rx.recv().unwrap();
                    match command {
                        WorkerCommand::Work(file, mutex) => {
                            let _locking = mutex.lock().unwrap();
                            file.borrow_mut().flush().unwrap();
                            file.borrow_mut().sync().unwrap();
                        }
                        WorkerCommand::Exit => break
                    }
                }
                drop(rx);
            }).unwrap();
        self.flush_worker = Some(WorkerDescriptor {
            tx,
            join_handle,
        })
    }

    fn get_total_redo_log_size(&mut self) -> io::Result<u64> {
        let env = self.env.clone();
        let children = env.get_children(self.abs_db_path.as_path())?;
        let mut total_size = 0u64;
        for child in children {
            let (kind, _) = files::parse_name(&child);
            if kind == Kind::Log {
                total_size += env.get_file_size(self.abs_db_path.join(child).as_path())?;
            }
        }

        Ok(total_size)
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

        //------------------------------lock version-set--------------------------------------------
        // self.renew_log_file(&mut self.versions.lock().unwrap())?;
        //------------------------------unlock version-set------------------------------------------

        let mut cfs = HashMap::<String, ColumnFamilyOptions>::new();
        for item in desc {
            cfs.insert(item.name.clone(), item.options.clone());
        }

        //------------------------------lock version-set--------------------------------------------
        let mutex = self.versions.clone();
        let mut versions = mutex.lock().unwrap();
        self.renew_log_file(&mut versions)?;

        let column_families = versions.column_families().clone();
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
            from_io_result(cfi.borrow_mut().install(&self.env))?;
            cfi.borrow_mut().set_redo_log_number(self.redo_log_number());
        }

        let mut patch = VersionPatch::default();
        patch.set_prev_log_number(0);
        patch.set_redo_log_number(self.redo_log_number());
        versions.add_sequence_number(1);

        from_io_result(versions.log_and_apply(self.options.core.clone(), patch))
        //------------------------------unlock version-set--------------------------------------
    }

    fn recovery(&mut self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
        todo!()
    }

    fn redo_log_number(&self) -> u64 { self.redo_log.as_ref().unwrap().number }

    fn renew_log_file(&mut self, locking: &mut MutexGuard<VersionSet>) -> mai2::Result<()> {
        if let Some(redo_log) = &self.redo_log {
            let mut borrow = redo_log.file.borrow_mut();
            from_io_result(borrow.flush())?;
            from_io_result(borrow.sync())?;
        }

        let versions = locking.deref_mut();
        let new_log_number = versions.generate_file_number();
        let log_file_path = paths::log_file(self.abs_db_path.as_path(), new_log_number);
        let rs = from_io_result(self.env.new_writable_file(log_file_path.as_path(), true));
        match rs {
            Err(_) => {
                versions.reuse_file_number(new_log_number);
                rs?;
            }
            Ok(file) => {
                self.redo_log = Some(WALDescriptor {
                    file: file.clone(),
                    number: new_log_number,
                    log: LogWriter::new(file, wal::DEFAULT_BLOCK_SIZE),
                });
            }
        }

        if let None = self.flush_worker {
            self.start_flush_worker();
        }
        Ok(())
    }

    fn internal_new_column_family(&mut self, name: String, options: ColumnFamilyOptions,
                                  locking: &mut MutexGuard<VersionSet>)
                                  -> mai2::Result<u32> {
        let (id, patch) = {
            let versions = locking.deref_mut();
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

        from_io_result(locking.log_and_apply(options, patch))?;
        Ok(id)
    }
}

impl DB for DBImpl {
    fn new_column_family(&mut self, name: &str, options: ColumnFamilyOptions)
                         -> mai2::Result<Arc<dyn ColumnFamily>> {
        //--------------------------lock version-set------------------------------------------------
        let mutex = self.versions.clone();
        let mut versions = mutex.lock().unwrap();
        let id = self.internal_new_column_family(String::from(name), options, &mut versions)?;

        let cfs = versions.column_families().borrow();
        let cfi = cfs.get_column_family_by_id(id).unwrap();
        from_io_result(cfi.borrow_mut().install(&self.env))?;
        cfi.borrow_mut().set_redo_log_number(self.redo_log_number());
        assert!(cfi.borrow().initialized());
        Ok(ColumnFamilyHandle::new(&cfi, &self.versions))
        //--------------------------unlock version-set----------------------------------------------
    }

    fn drop_column_family(&mut self, column_family: Arc<dyn ColumnFamily>) -> mai2::Result<()> {
        if column_family.id() == 0 {
            return Err(Status::corrupted("can not drop default column family!"));
        }
        let cfi = ColumnFamilyImpl::from(&column_family);
        if cfi.borrow().dropped() {
            return Err(Status::corrupted(format!("column family: {} has dropped",
                                                 cfi.borrow().name())));
        }

        //--------------------------lock version-set------------------------------------------------
        let mut versions = self.versions.lock().unwrap();
        while cfi.borrow().background_progress() {
            // Waiting for all background progress done
            todo!()
        }
        let mut patch = VersionPatch::default();
        patch.drop_column_family(cfi.borrow().id());

        let cf_opts = cfi.borrow().options().clone();
        from_io_result(versions.log_and_apply(cf_opts, patch))?;
        from_io_result(cfi.borrow_mut().uninstall(&self.env))?;
        Ok(())
        //--------------------------unlock version-set----------------------------------------------
    }

    fn get_all_column_families(&self) -> mai2::Result<Vec<Arc<dyn ColumnFamily>>> {
        //--------------------------lock version-set------------------------------------------------
        let versions = self.versions.lock().unwrap();
        let mut cfs: Vec<Arc<dyn ColumnFamily>> = versions
            .column_families()
            .borrow().column_family_impls()
            .iter()
            .map(|x| (**x).clone())
            .filter(|x| !x.borrow().dropped() && x.borrow().initialized())
            .map(|x| ColumnFamilyHandle::new(&x, &self.versions))
            .collect();
        cfs.sort_by(|a, b| a.id().cmp(&b.id()));
        Ok(cfs)
        //--------------------------unlock version-set----------------------------------------------
    }

    fn default_column_family(&self) -> Arc<dyn ColumnFamily> {
        self.default_column_family.as_ref().unwrap().clone()
    }

    fn get(&self, options: ReadOptions, column_family: &Arc<dyn ColumnFamily>, key: &[u8])
           -> mai2::Result<Vec<u8>> {
        todo!()
    }
}

impl Drop for DBImpl {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Release);

        if let Some(worker) = self.flush_worker.as_ref() {
            worker.tx.send(WorkerCommand::Exit).unwrap();
            //worker.join_handle.join().unwrap();
        };
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
        assert_eq!("cf", cf.name());
        assert_ne!(0, cf.id());

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
        assert_eq!(0, cf.id());
        assert_eq!(DEFAULT_COLUMN_FAMILY_NAME, cf.name());

        let cfs = db.borrow().get_all_column_families()?;
        assert_eq!(1, cfs.len());
        assert_eq!(cf.id(), cfs[0].id());
        assert_eq!(cf.name(), cfs[0].name());
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
        assert_eq!(2, cf1.id());
        assert_eq!("cf1", cf1.name());

        db.borrow_mut().drop_column_family(cfs[0].clone())?;

        {
            let cfs = db.borrow().get_all_column_families()?;
            assert_eq!(2, cfs.len());

            assert_eq!(0, cfs[0].id());
            assert_eq!(DEFAULT_COLUMN_FAMILY_NAME, cfs[0].name());

            assert_eq!(2, cfs[1].id());
            assert_eq!("cf1", cfs[1].name());
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