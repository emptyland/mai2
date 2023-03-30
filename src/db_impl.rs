use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::{io, thread};
use std::cmp::max;
use std::collections::btree_set::BTreeSet;
use std::io::{ErrorKind, Write};
use std::mem::size_of;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::thread::JoinHandle;

use crate::column_family::{ColumnFamilyHandle, ColumnFamilyImpl, ColumnFamilySet};
use crate::env::{Env, WritableFile};
use crate::files::{Kind, paths};
use crate::{files, mai2, wal};
use crate::key::Tag;
use crate::mai2::*;
use crate::memory_table::MemoryTable;
use crate::snapshot::{SnapshotImpl, SnapshotSet};
use crate::status::{Corrupting, Status};
use crate::version::{VersionPatch, VersionSet};
use crate::wal::{LogReader, LogWriter};

pub struct DBImpl {
    pub db_name: String,
    pub abs_db_path: PathBuf,
    options: Options,
    env: Arc<dyn Env>,
    versions: Arc<Mutex<VersionSet>>,
    default_column_family: Option<Arc<dyn ColumnFamily>>,
    redo_log: Cell<Option<WALDescriptor>>,
    redo_log_number: Cell<u64>,
    shutting_down: AtomicBool,
    total_wal_size: AtomicU64,
    flush_worker: Cell<Option<WorkerDescriptor>>,
    snapshots: Arc<SnapshotSet>,
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

impl DBImpl {
    pub fn open(options: Options, name: String, column_family_descriptors: &[ColumnFamilyDescriptor])
                -> mai2::Result<(Arc<DBImpl>, Vec<Arc<dyn ColumnFamily>>)> {
        let env = options.env.clone();
        let abs_db_path = DBImpl::make_abs_db_path(&options.core.dir, &name, &env)?;
        let versions = VersionSet::new(abs_db_path.clone(), &options);

        let mut db = DBImpl {
            db_name: name.clone(),
            abs_db_path,
            options,
            env: env.clone(),
            versions: versions.clone(),
            //bkg_active: AtomicI32::new(0),
            shutting_down: AtomicBool::new(false),
            total_wal_size: AtomicU64::new(0),
            default_column_family: None,
            redo_log: Cell::new(None),
            redo_log_number: Cell::new(0),
            flush_worker: Cell::new(None),
            snapshots: SnapshotSet::new(),
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

        Ok((Arc::new(db), cfs))
    }

    fn flush_redo_log(&self, sync: bool, _locking: &MutexGuard<VersionSet>) {
        if let Some(desc) = self.redo_log.take() {
            let worker = self.flush_worker.take().unwrap();
            if sync {
                todo!()
            } else {
                worker.tx.send(WorkerCommand::Work(desc.file.clone(),
                                                   self.versions.clone())).unwrap();
            }
            self.flush_worker.set(Some(worker));
            self.redo_log.set(Some(desc));
        }
    }

    fn start_flush_worker() -> WorkerDescriptor {
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
        WorkerDescriptor {
            tx,
            join_handle,
        }
    }

    fn get_total_redo_log_size(&self) -> io::Result<u64> {
        let children = self.env.get_children(self.abs_db_path.as_path())?;
        let mut total_size = 0u64;
        for child in children {
            let (kind, _) = files::parse_name(&child);
            if kind == Kind::Log {
                total_size += self.env.get_file_size(self.abs_db_path.join(child).as_path())?;
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
            cfi.borrow_mut().set_redo_log_number(self.redo_log_number.get());
        }

        let mut patch = VersionPatch::default();
        patch.set_prev_log_number(0);
        patch.set_redo_log_number(self.redo_log_number.get());
        versions.add_sequence_number(1);

        from_io_result(versions.log_and_apply(self.options.core.clone(), patch))
        //------------------------------unlock version-set--------------------------------------
    }

    fn recovery(&self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
        let current_file_path = paths::current_file(self.abs_db_path.as_path());
        let rv = from_io_result(self.env.read_to_string(current_file_path.as_path()))?;
        let rs = u64::from_str_radix(rv.as_str(), 10);
        if let Err(_) = rs {
            return Err(Status::corrupted("bad CURRENT file"));
        }
        let manifest_file_number = rs.unwrap();
        drop(rv);

        let mut cfs = HashMap::<String, ColumnFamilyOptions>::new();
        for item in desc {
            cfs.insert(item.name.clone(), item.options.clone());
        }

        //------------------------------lock version-set--------------------------------------------
        let mutex = self.versions.clone();
        let mut versions = mutex.lock().unwrap();
        let mut history: BTreeSet<u64> = from_io_result(versions.recover(&cfs, manifest_file_number))?;
        assert!(history.len() >= 1);

        if self.options.create_missing_column_families {
            for cfi in versions.column_families().borrow().column_family_impls() {
                cfs.remove(cfi.borrow().name());
            }
            for (name, opts) in cfs {
                self.internal_new_column_family(name, opts, &mut versions)?;
            }
        }

        // Should replay all column families's redo log file.
        let mut numbers = BTreeSet::new();
        for cfi in versions.column_families().borrow().column_family_impls() {
            let mut borrowed_cfi = cfi.borrow_mut();
            if !borrowed_cfi.initialized() {
                from_io_result(borrowed_cfi.install(&self.env))?;
            }
            numbers.insert(borrowed_cfi.redo_log_number());
        }
        let mut max_number = *numbers.last().unwrap_or(&0);

        // Add undumped log files.
        numbers.append(&mut history.split_off(&(max_number + 1)));
        max_number = *numbers.last().unwrap(); // last one is max one
        assert!(history.len() >= numbers.len());

        let mut update_sequence_number = 0;
        for number in numbers {
            // The newest redo log file filter is not need.
            update_sequence_number = self.redo(number, max_number != number, &versions)?;
        }

        // The max one is newest file.
        assert_eq!(max_number, versions.redo_log_number);
        self.redo_log_number.set(versions.redo_log_number);

        versions.update_sequence_number(update_sequence_number);

        let log_file_path = paths::log_file(self.abs_db_path.as_path(), self.redo_log_number.get());
        let file = from_io_result(self.env.new_writable_file(log_file_path.as_path(), true))?;
        self.redo_log.set(Some(WALDescriptor {
            log: LogWriter::new(file.clone(), wal::DEFAULT_BLOCK_SIZE),
            file,
            number: self.redo_log_number.get(),
        }));

        let total_wal_size = from_io_result(self.get_total_redo_log_size())?;
        self.total_wal_size.store(total_wal_size, Ordering::Release);
        Ok(())
        //------------------------------unlock version-set------------------------------------------
    }

    fn redo(&self, log_file_number: u64, filter: bool, versions: &MutexGuard<VersionSet>) -> mai2::Result<u64> {
        let log_file_path = paths::log_file(self.abs_db_path.as_path(), log_file_number);
        let file = from_io_result(self.env.new_sequential_file(log_file_path.as_path()))?;

        let mut handler = WritingHandler::new(log_file_number, filter, 0,
                                              versions.column_families().clone());
        let mut update_sequence_number = 0;
        let mut reader = LogReader::new(file, true, wal::DEFAULT_BLOCK_SIZE);
        loop {
            let mut record = from_io_result(reader.read())?;
            if record.is_empty() {
                break;
            }
            let mut offset = 0;
            let sequence_number = {
                let mut buf: [u8; 8] = [0; 8];
                (&mut buf[..]).write(&record.as_slice()[offset..offset + 8]).unwrap();
                u64::from_le_bytes(buf)
            };
            offset += 8;
            let _n_entries = {
                let mut buf: [u8; 4] = [0; 4];
                (&mut buf[..]).write(&record.as_slice()[offset..offset + 4]).unwrap();
                u32::from_le_bytes(buf)
            };

            handler.reset_last_sequence_number(sequence_number);
            WriteBatch::iterate_since(WriteBatch::skip_header(record.as_slice()), &handler);
            //assert_eq!(n_entries as u64, handler.sequence_number_count.get());

            update_sequence_number = sequence_number + handler.sequence_number_count.get();
        }

        Ok(update_sequence_number)
    }

    //fn redo_log_number(&self) -> u64 { self.redo_log.into_inner().as_ref().unwrap().number }

    fn renew_log_file(&self, locking: &mut MutexGuard<VersionSet>) -> mai2::Result<()> {
        if let Some(redo_log) = self.redo_log.take() {
            {
                let mut borrow = redo_log.file.borrow_mut();
                from_io_result(borrow.flush())?;
                from_io_result(borrow.sync())?;
            }
            self.redo_log.set(Some(redo_log));
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
                self.redo_log.replace(Some(WALDescriptor {
                    file: file.clone(),
                    number: new_log_number,
                    log: LogWriter::new(file, wal::DEFAULT_BLOCK_SIZE),
                }));
            }
        }

        if let Some(worker) = self.flush_worker.take() {
            self.flush_worker.set(Some(worker));
        } else {
            self.flush_worker.set(Some(Self::start_flush_worker()));
        }
        Ok(())
    }

    fn internal_new_column_family(&self, name: String, options: ColumnFamilyOptions,
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
            patch.create_column_family(new_id, name.clone(),
                                       options.user_comparator.name().clone());
            (new_id, patch)
        };

        from_io_result(locking.log_and_apply(options, patch))?;
        Ok(id)
    }

    fn write_impl(&self, options: &WriteOptions, updates: WriteBatch) -> mai2::Result<()> {
        let mutex = self.versions.clone();
        let mut versions = mutex.lock().unwrap();
        //------------------------------lock version-set--------------------------------------------
        let last_version = versions.last_sequence_number();
        {
            let cfs = versions.column_families().borrow();
            for cfi in cfs.column_family_impls() {
                self.make_room_for_write(cfi, &versions)?;
            }
        }
        let redo = updates.take_redo(last_version + 1);
        let mut redo_logger = self.redo_log.take().unwrap();
        from_io_result(redo_logger.log.append(redo.as_slice()))?;
        self.redo_log.set(Some(redo_logger));

        self.flush_redo_log(options.sync, &mut versions);

        let handler = WritingHandler::new(0, false,
                                          last_version + 1,
                                          versions.column_families().clone());
        WriteBatch::iterate_since(WriteBatch::skip_header(redo.as_slice()), &handler);

        versions.add_sequence_number(handler.sequence_number_count.get());
        //------------------------------unlock version-set------------------------------------------
        Ok(())
    }

    fn make_room_for_write(&self, _cfi: &Arc<RefCell<ColumnFamilyImpl>>, _locking: &MutexGuard<VersionSet>) -> mai2::Result<()> {
        // TODO:
        Ok(())
    }

    fn prepare_for_get(&self, options: &ReadOptions, cf: &Arc<dyn ColumnFamily>) -> mai2::Result<Get> {
        let cfi = ColumnFamilyImpl::from(cf);

        //------------------------------lock version-set--------------------------------------------
        let mutex = self.versions.clone();
        let versions = mutex.lock().unwrap();
        let borrowed_cfi = cfi.borrow();

        if borrowed_cfi.dropped() {
            return Err(Status::corrupted("column family has dropped"));
        }
        let last_sequence_number =
            if let Some(snapshot) = options.snapshot.as_ref() {
                SnapshotImpl::from(snapshot).sequence_number
            } else {
                versions.last_sequence_number()
            };
        if options.snapshot.is_some() {
            if !self.snapshots.is_snapshot_valid(last_sequence_number) {
                return Err(Status::corrupted("invalid snapshot, it has been released?"));
            }
        }

        let mut memory_tables = Vec::new();
        memory_tables.push(borrowed_cfi.mutable.clone());
        memory_tables.append(&mut borrowed_cfi.immutable_pipeline.peek_all());
        // TODO: get current version

        //------------------------------unlock version-set------------------------------------------
        Ok(Get {
            last_sequence_number,
            memory_tables,
            cfi: cfi.clone(),
        })
    }
}

impl DB for DBImpl {
    fn new_column_family(&self, name: &str, options: ColumnFamilyOptions)
                         -> mai2::Result<Arc<dyn ColumnFamily>> {
        //--------------------------lock version-set------------------------------------------------
        let mutex = self.versions.clone();
        let mut versions = mutex.lock().unwrap();
        let id = self.internal_new_column_family(String::from(name), options, &mut versions)?;

        let cfs = versions.column_families().borrow();
        let cfi = cfs.get_column_family_by_id(id).unwrap();
        from_io_result(cfi.borrow_mut().install(&self.env))?;
        cfi.borrow_mut().set_redo_log_number(self.redo_log_number.get());
        assert!(cfi.borrow().initialized());
        Ok(ColumnFamilyHandle::new(&cfi, &self.versions))
        //--------------------------unlock version-set----------------------------------------------
    }

    fn drop_column_family(&self, column_family: Arc<dyn ColumnFamily>) -> mai2::Result<()> {
        if column_family.id() == 0 {
            return Err(Status::corrupted("can not drop default column family!"));
        }

        //--------------------------lock version-set------------------------------------------------
        let cfi = ColumnFamilyImpl::from(&column_family);
        if cfi.borrow().dropped() {
            return Err(Status::corrupted(format!("column family: {} has dropped",
                                                 cfi.borrow().name())));
        }

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

    fn write(&self, options: &WriteOptions, updates: WriteBatch) -> mai2::Result<()> {
        self.write_impl(options, updates)
    }

    fn get(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>, key: &[u8])
           -> mai2::Result<Vec<u8>> {
        let get = self.prepare_for_get(options, column_family)?;
        //-------------------------------------Lock-free--------------------------------------------
        for table in &get.memory_tables {
            match table.get(key, get.last_sequence_number) {
                Ok((value, tag)) =>
                    return if tag == Tag::DELETION {
                        Err(Status::NotFound)
                    } else {
                        Ok(value.to_vec())
                    },
                Err(_) => ()
            }
        }

        Err(Status::NotFound)
    }

    fn get_snapshot(&self) -> Arc<dyn Snapshot> {
        let mutex = self.versions.clone();
        let versions = mutex.lock().unwrap();
        //--------------------------lock version-set------------------------------------------------
        SnapshotSet::new_snapshot(&self.snapshots, versions.last_sequence_number(),
                                  self.env.current_time_mills())
        //--------------------------unlock version-set----------------------------------------------
    }

    fn release_snapshot(&self, snapshot: Arc<dyn Snapshot>) {
        let mutex = self.versions.clone();
        let _locking = mutex.lock().unwrap();
        //--------------------------lock version-set------------------------------------------------
        self.snapshots.remove_snapshot(snapshot);
        //--------------------------unlock version-set----------------------------------------------
    }
}

unsafe impl Send for DBImpl {}

unsafe impl Sync for DBImpl {}

impl Drop for DBImpl {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Release);

        if let Some(worker) = self.flush_worker.take() {
            worker.tx.send(WorkerCommand::Exit).unwrap();
            worker.join_handle.join().unwrap();
            drop(worker.tx);
        };
        println!("drop it! DBImpl");
    }
}

struct WritingHandler {
    redo_log_number: u64,
    filter: bool,
    column_families: Arc<RefCell<ColumnFamilySet>>,
    last_sequence_number: u64,
    written_in_bytes: Cell<usize>,
    pub sequence_number_count: Cell<u64>,
}

impl WritingHandler {
    pub fn new(redo_log_number: u64, filter: bool, last_sequence_number: u64,
               column_families: Arc<RefCell<ColumnFamilySet>>) -> Self {
        Self {
            redo_log_number,
            filter,
            column_families,
            last_sequence_number,
            written_in_bytes: Cell::new(0),
            sequence_number_count: Cell::new(0),
        }
    }

    pub fn reset_last_sequence_number(&mut self, sequence_number: u64) {
        self.sequence_number_count.set(0);
        self.last_sequence_number = sequence_number;
    }

    fn get_memory_table(&self, cf_id: u32) -> Arc<MemoryTable> {
        let cfs = self.column_families.borrow();
        let cfi = cfs.get_column_family_by_id(cf_id).unwrap().clone();
        let borrowed_cfi = cfi.borrow();
        borrowed_cfi.mutable.clone()
    }

    fn sequence_number(&self) -> u64 { self.last_sequence_number + self.sequence_number_count.get() }

    fn advance(&self, writen_in_bytes: usize) {
        self.written_in_bytes.set(self.written_in_bytes.get() + writen_in_bytes);
        self.sequence_number_count.set(self.sequence_number_count.get() + 1);
    }
}

impl WriteBatchStub for WritingHandler {
    fn did_insert(&self, cf_id: u32, key: &[u8], value: &[u8]) {
        let table = self.get_memory_table(cf_id);
        table.insert(key, value, self.sequence_number(), Tag::KEY);
        self.advance(key.len() + size_of::<u32>() + size_of::<u64>() + value.len());
    }

    fn did_delete(&self, cf_id: u32, key: &[u8]) {
        let table = self.get_memory_table(cf_id);
        table.insert(key, "".as_bytes(), self.sequence_number(), Tag::DELETION);
        self.advance(key.len() + size_of::<u32>() + size_of::<u64>());
    }
}

struct Get {
    memory_tables: Vec<Arc<MemoryTable>>,
    last_sequence_number: u64,
    cfi: Arc<RefCell<ColumnFamilyImpl>>,
    //version: Arc<Version>,
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
        let db = open_test_db("db0")?;
        let cf = db.default_column_family();
        assert_eq!(0, cf.id());
        assert_eq!(DEFAULT_COLUMN_FAMILY_NAME, cf.name());

        let cfs = db.get_all_column_families()?;
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

        let cf1 = db.new_column_family("cf1", ColumnFamilyOptions::default())?;
        assert_eq!(2, cf1.id());
        assert_eq!("cf1", cf1.name());

        db.drop_column_family(cfs[0].clone())?;

        {
            let cfs = db.get_all_column_families()?;
            assert_eq!(2, cfs.len());

            assert_eq!(0, cfs[0].id());
            assert_eq!(DEFAULT_COLUMN_FAMILY_NAME, cfs[0].name());

            assert_eq!(2, cfs[1].id());
            assert_eq!("cf1", cfs[1].name());
        }
        Ok(())
    }

    #[test]
    fn thread_safe_column_family() -> mai2::Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db2");
        let db = open_test_db("db2")?;
        let db1 = db.clone();
        let t1 = thread::spawn(move || for _ in 0..100 {
            let cf = db1.new_column_family("cf1",
                                           ColumnFamilyOptions::default()).unwrap();
            db1.drop_column_family(cf).unwrap();
        });
        let db2 = db.clone();
        let t2 = thread::spawn(move || for _ in 0..100 {
            let cf = db2.new_column_family("cf2",
                                           ColumnFamilyOptions::default()).unwrap();
            db2.drop_column_family(cf).unwrap();
        });
        t1.join().unwrap();
        t2.join().unwrap();

        Ok(())
    }

    #[test]
    fn write_batch() -> mai2::Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db3");
        let db = open_test_db("db3")?;
        let cf = db.default_column_family();

        let mut updates = WriteBatch::new();
        updates.insert(&cf, "1111".as_bytes(), "ok".as_bytes());
        updates.delete(&cf, "1111".as_bytes());

        let wr_opts = WriteOptions::default();
        db.write(&wr_opts, updates)?;
        db.insert(&wr_opts, &cf, "2222".as_bytes(), "bbb".as_bytes())?;

        let rd_opts = ReadOptions::default();
        let rs = db.get(&rd_opts, &cf, "1111".as_bytes());
        assert!(rs.is_err());
        assert!(matches!(rs, Err(Status::NotFound)));

        let rs = db.get(&rd_opts, &cf, "2222".as_bytes());
        assert!(rs.is_ok());
        assert_eq!("bbb".as_bytes(), rs.unwrap().as_slice());
        Ok(())
    }

    #[test]
    fn get_snapshot() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db4");
        let db = open_test_db("db4")?;
        let snapshot = db.get_snapshot();
        let snapshot_impl = SnapshotImpl::from(&snapshot);
        assert_eq!(1, snapshot_impl.sequence_number);

        db.release_snapshot(snapshot);
        Ok(())
    }

    #[test]
    fn recover_db() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db5");
        {
            let db = open_test_db("db5")?;
            let cf0 = db.default_column_family();
            let cf1 = db.new_column_family("cf1", ColumnFamilyOptions::default())?;
            let wr_opts = WriteOptions::default();

            db.insert(&wr_opts, &cf0, "1111".as_bytes(), "a".as_bytes())?;
            db.insert(&wr_opts, &cf0, "3333".as_bytes(), "b".as_bytes())?;
            db.insert(&wr_opts, &cf0, "4444".as_bytes(), "c".as_bytes())?;

            db.insert(&wr_opts, &cf1, "2222".as_bytes(), "cc".as_bytes())?;
        }

        {
            let desc = [ColumnFamilyDescriptor {
                name: DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                options: ColumnFamilyOptions::default(),
            }, ColumnFamilyDescriptor {
                name: "cf1".to_string(),
                options: ColumnFamilyOptions::default(),
            }];

            let db = load_test_db("db5", &desc)?;
            let cf0 = db.default_column_family();
            let cfs = db.get_all_column_families()?;
            let maybe_cf0 = cfs.iter().find(|x| { x.name() == "cf1" });
            assert!(maybe_cf0.is_some());
            let cf1 = maybe_cf0.unwrap();

            check_key_value_pairs(&db, &cf0,
                                  &vec!["1111", "3333", "4444"],
                                  &vec!["a", "b", "c"])?;
            check_key_value_pairs(&db, &cf1, &vec!["2222"], &vec!["cc"])?;
        }
        Ok(())
    }

    fn check_key_value_pairs(db: &Arc<DBImpl>, cf: &Arc<dyn ColumnFamily>, keys: &[&str], values: &[&str]) -> Result<()> {
        assert_eq!(keys.len(), values.len());
        let rd_opts = ReadOptions::default();
        for i in 0..keys.len() {
            let v = db.get(&rd_opts, &cf, keys[i].as_bytes())?;
            assert_eq!(values[i].as_bytes(), v.as_slice());
        }
        Ok(())
    }

    fn open_test_db(name: &str) -> mai2::Result<Arc<DBImpl>> {
        let options = Options::with()
            .create_if_missing(true)
            .dir(String::from("tests"))
            .build();
        let (db, _cfs) = DBImpl::open(options, String::from(name), &Vec::new())?;
        Ok(db)
    }

    fn load_test_db(name: &str, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<Arc<DBImpl>> {
        let options = Options::with()
            .dir("tests".to_string())
            .error_if_exists(false)
            .build();
        let (db, _cfs) = DBImpl::open(options, String::from(name), desc)?;
        Ok(db)
    }
}