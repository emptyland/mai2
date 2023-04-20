use std::{io, thread};
use std::cell::{Cell, RefCell};
use std::collections::btree_set::BTreeSet;
use std::collections::HashMap;
use std::io::Write;
use std::iter::Iterator;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::panic::catch_unwind;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread::JoinHandle;

use crate::{config, files, log_debug, log_error, log_info, mai2, wal};
use crate::cache::TableCache;
use crate::column_family::{ColumnFamilyHandle, ColumnFamilyImpl, ColumnFamilySet};
use crate::compaction::{Compact, Compaction};
use crate::comparator::Comparator;
use crate::env::{Env, WritableFile};
use crate::files::{Kind, paths};
use crate::iterator::{DBIterator, Iterator as MaiIterator, IteratorArc, IteratorRc, MergingIterator};
use crate::key::Tag;
use crate::log::Logger;
use crate::mai2::*;
use crate::memory_table::MemoryTable;
use crate::snapshot::{SnapshotImpl, SnapshotSet};
use crate::sst_builder::SSTBuilder;
use crate::sst_reader::SSTReader;
use crate::status::{Corrupting, Status};
use crate::version::{FileMetadata, Version, VersionPatch, VersionSet};
use crate::wal::{LogReader, LogWriter};

pub struct DBImpl {
    pub db_name: String,
    pub abs_db_path: PathBuf,
    this: Option<Weak<DBImpl>>,
    options: Options,
    env: Arc<dyn Env>,
    logger: Arc<dyn Logger>,
    versions: Arc<Mutex<VersionSet>>,
    table_cache: TableCache,
    default_column_family: Option<Arc<dyn ColumnFamily>>,
    redo_log: Cell<Option<WALDescriptor>>,
    redo_log_number: Cell<u64>,
    background_active: AtomicBool,
    shutting_down: AtomicBool,
    total_wal_size: AtomicU64,
    flush_worker: Cell<Option<WorkerDescriptor>>,
    compaction_worker: Cell<Option<WorkerDescriptor>>,
    snapshots: Arc<SnapshotSet>,
}

enum WorkerCommand {
    Work(Arc<RefCell<dyn WritableFile>>, Arc<Mutex<VersionSet>>),
    Compact(Weak<DBImpl>, Arc<ColumnFamilyImpl>),
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
        let table_cache = TableCache::new(&abs_db_path, &env, options.max_open_files,
                                          options.block_cache_capacity);
        let logger = options.logger.clone();
        let mut db = DBImpl {
            db_name: name.clone(),
            abs_db_path,
            this: None,
            options,
            env: env.clone(),
            logger: logger.clone(),
            versions: versions.clone(),
            table_cache,
            background_active: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
            total_wal_size: AtomicU64::new(0),
            default_column_family: None,
            redo_log: Cell::new(None),
            redo_log_number: Cell::new(0),
            flush_worker: Cell::new(None),
            compaction_worker: Cell::new(None),
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
            db.recover(column_family_descriptors)?;
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
            log_debug!(logger, "----------");
            let borrowed_cfs = column_families.borrow();
            for cfi in borrowed_cfs.column_family_impls() {
                log_debug!(logger, "cf[{}][{}]", cfi.id(), cfi.name());
            }
            log_debug!(logger, "----------");
        }

        Ok((Arc::new_cyclic(|this| {
            db.this = Some(this.clone());
            db
        }), cfs))
    }

    fn flush_redo_log(&self, sync: bool, _locking: &MutexGuard<VersionSet>) {
        if let Some(desc) = self.redo_log.take() {
            let worker = self.flush_worker.take().unwrap();
            if sync {
                desc.file.borrow_mut().flush().unwrap();
                desc.file.borrow_mut().sync().unwrap();
            } else {
                // worker.tx.send(WorkerCommand::Work(desc.file.clone(),
                //                                    self.versions.clone())).unwrap();
            }
            self.flush_worker.set(Some(worker));
            self.redo_log.set(Some(desc));
        }
    }

    fn start_flush_worker(logger: Arc<dyn Logger>) -> WorkerDescriptor {
        let (tx, rx) = channel();
        let join_handle = thread::Builder::new()
            .name("flush-worker".to_string())
            .spawn(move || {
                log_debug!(logger, "flush worker start...");
                let rs = catch_unwind(move || {
                    loop {
                        let command = rx.recv().unwrap();
                        match command {
                            WorkerCommand::Work(file, mutex) => {
                                let _locking = mutex.lock().unwrap();
                                file.borrow_mut().flush().unwrap();
                                file.borrow_mut().sync().unwrap();
                            }
                            WorkerCommand::Exit => break,
                            _ => unreachable!(),
                        }
                    }
                    drop(rx);
                });
                if let Err(e) = rs {
                    log_error!(logger, "flush worker panic! {:#?}", e);
                } else {
                    log_debug!(logger, "flush worker stop.");
                }
            }).unwrap();
        WorkerDescriptor {
            tx,
            join_handle,
        }
    }

    fn get_total_redo_log_size(&self) -> io::Result<u64> {
        let children = self.env.get_children(&self.abs_db_path)?;
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
            from_io_result(cfi.install(&self.env))?;
            cfi.set_redo_log_number(self.redo_log_number.get());
        }

        let mut patch = VersionPatch::default();
        patch.set_prev_log_number(0);
        patch.set_redo_log_number(self.redo_log_number.get());
        versions.add_sequence_number(1);

        from_io_result(versions.log_and_apply(self.options.core.clone(), patch))
        //------------------------------unlock version-set--------------------------------------
    }

    fn recover(&self, desc: &[ColumnFamilyDescriptor]) -> mai2::Result<()> {
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

        #[cfg(test)]
        {
            log_debug!(self.logger, "recover versions history:");
            for number in history.iter() {
                log_debug!(self.logger, "history={}", number);
            }
        }

        if self.options.create_missing_column_families {
            for cfi in versions.column_families().borrow().column_family_impls() {
                cfs.remove(cfi.name());
            }
            for (name, opts) in cfs {
                self.internal_new_column_family(name, opts, &mut versions)?;
            }
        }

        // Should replay all column families's redo log file.
        let mut numbers = BTreeSet::new();
        for cfi in versions.column_families().borrow().column_family_impls() {
            if !cfi.initialized() {
                from_io_result(cfi.install(&self.env))?;
            }
            numbers.insert(cfi.redo_log_number());
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

        log_debug!(self.logger, "recover ok.\ntotal_wal_size={total_wal_size}\nredo_log_number={}",
            versions.redo_log_number);
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
                self.redo_log_number.set(new_log_number);
            }
        }

        if let Some(worker) = self.flush_worker.take() {
            self.flush_worker.set(Some(worker));
        } else {
            self.flush_worker.set(Some(Self::start_flush_worker(self.logger.clone())));
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
            let cfs = versions.column_families().clone();
            for cfi in cfs.borrow().column_family_impls() {
                versions = self.make_room_for_write(cfi, versions)?;
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

    fn make_room_for_write<'a>(&'a self, cfi: &Arc<ColumnFamilyImpl>, mut locking: MutexGuard<'a, VersionSet>) -> mai2::Result<MutexGuard<'a, VersionSet>> {
        loop {
            if let Err(e) = cfi.background_result.replace(Ok(())) {
                return Err(e.clone());
            } else if cfi.mutable().approximate_memory_usage() < cfi.options().write_buf_size {
                // Memory table usage samll than write buffer. Ignore it.
                // Memory table conflict-factor too small. Ignore it.
                break;
            } else if cfi.immutable_pipeline.is_not_empty() {
                // Immutable table pipeline in progress.
                break;
            } else if cfi.background_progress() &&
                cfi.current().level_files(0).len() > config::MAX_NUMBER_OF_LEVEL_0_FILES as usize {
                let cv = &cfi.background_cv;
                return Ok(cv.wait(locking).unwrap());
            } else {
                assert_eq!(0, locking.prev_log_number);
                self.renew_log_file(&mut locking)?;

                let mut patch = VersionPatch::default();
                patch.set_redo_log_number(self.redo_log_number.get());
                from_io_result(locking.log_and_apply(self.options.core.clone(), patch))?;

                cfi.make_immutable_pipeline(self.redo_log_number.get());

                self.maybe_schedule_compacting(cfi, &locking);
            }
        }
        Ok(locking)
    }

    fn maybe_schedule_compacting(&self, cfi: &Arc<ColumnFamilyImpl>,
                                 _locking: &MutexGuard<VersionSet>) {
        if self.background_active.load(Ordering::Acquire) {
            // Compaction is running.
            return;
        }
        if self.shutting_down.load(Ordering::Acquire) {
            // Is shutting down, ignore schedule
            return;
        }
        if cfi.immutable_pipeline.is_empty() && !cfi.need_compaction() {
            return; // Compaction is no need
        }

        let this = self.this.as_ref().unwrap().clone();
        if let Some(worker) = self.compaction_worker.take() {
            worker.tx.send(WorkerCommand::Compact(this, cfi.clone())).unwrap();
            self.compaction_worker.set(Some(worker));
        } else {
            let worker = self.start_compacting_worker(self.logger.clone());
            worker.tx.send(WorkerCommand::Compact(this, cfi.clone())).unwrap();
            self.compaction_worker.set(Some(worker));
        }
    }

    fn start_compacting_worker(&self, logger: Arc<dyn Logger>) -> WorkerDescriptor {
        let (tx, rx) = channel();
        let join_handle = thread::Builder::new()
            .name("compact-worker".to_string())
            .spawn(move || {
                let rs = catch_unwind(move || {
                    Self::do_compacting_work(rx);
                });
                if let Err(e) = rs {
                    log_error!(logger, "compacting worker panic! {:#?}", e);
                }
            }).unwrap();
        WorkerDescriptor {
            tx,
            join_handle,
        }
    }

    fn do_compacting_work(rx: Receiver<WorkerCommand>) {
        loop {
            let command = rx.recv().unwrap();
            match command {
                WorkerCommand::Compact(this, cfi) => {
                    let db = if let Some(db) = this.upgrade() {
                        db
                    } else {
                        break;
                    };

                    db.background_active.store(true, Ordering::Release);
                    cfi.set_background_progress(true);

                    if !db.shutting_down.load(Ordering::Acquire) {
                        let mutex = db.versions.clone();
                        db.compact(&cfi, mutex);
                    }

                    db.background_active.store(false, Ordering::Release);
                    cfi.set_background_progress(false);

                    let mutex = db.versions.clone();
                    db.maybe_schedule_compacting(&cfi, &mutex.lock().unwrap());

                    cfi.background_cv.notify_all();
                }
                WorkerCommand::Exit => break,
                _ => unreachable!(),
            }
        }
    }

    fn compact(&self, cfi: &Arc<ColumnFamilyImpl>, mutex: Arc<Mutex<VersionSet>>) {
        let mut versions = mutex.lock().unwrap();
        if cfi.immutable_pipeline.is_not_empty() {
            let rs = self.compact_memory_table(cfi, mutex.deref(), versions);
            if let Err(e) = rs {
                cfi.set_background_result(Err(dbg!(e)));
                return;
            }
            versions = rs.unwrap();
        }

        if let Some(mut compact) = cfi.pick_compaction() {
            let rs = self.compact_file_table(cfi, &mut compact, mutex.deref(), versions);
            if let Err(e) = rs {
                cfi.set_background_result(Err(dbg!(e)));
                return;
            }
            versions = rs.unwrap();

            if let Err(e) = versions.log_and_apply(ColumnFamilyOptions::default(), compact.patch) {
                cfi.set_background_result(Err(Status::corrupted(e.to_string())));
                return;
            }
        }

        if let Err(e) = self.delete_obsolete_files(cfi, &mut versions) {
            cfi.set_background_result(Err(Status::corrupted(e.to_string())));
        }
    }

    fn compact_file_table<'a>(&self, cfi: &Arc<ColumnFamilyImpl>, compact: &mut Compact,
                              mutex: &'a Mutex<VersionSet>, mut versions: MutexGuard<'a, VersionSet>)
                              -> Result<MutexGuard<'a, VersionSet>> {
        assert!(compact.level < config::MAX_LEVEL);
        let jiffies = self.env.current_time_mills();
        let mut n_entries = 0;
        for i in 0..compact.inputs.len() {
            for file in &compact.inputs[i] {
                let rd = from_io_result(self.table_cache.get_reader(cfi,
                                                                    file.number))?;
                n_entries += rd.table_properties.n_entries;
            }
        }
        let new_n_slots = config::compute_number_of_slots(compact.level + 1,
                                                          n_entries as usize,
                                                          config::LIMIT_MIN_NUMBER_OF_SLOTS);
        let mut compaction = Compaction::new(&self.abs_db_path,
                                             &cfi.internal_key_cmp,
                                             &self.logger,
                                             &cfi,
                                             &compact.input_version,
                                             compact.level,
                                             versions.generate_file_number(),
                                             &cfi.compaction_point(compact.level));
        compaction.smallest_snapshot = if self.snapshots.is_empty() {
            versions.last_sequence_number()
        } else {
            self.snapshots.oldest().sequence_number
        };

        for i in 0..compact.inputs.len() {
            for file in &compact.inputs[i] {
                let rd = from_io_result(self.table_cache.get_reader(cfi,
                                                                    file.number))?;
                let iter = from_io_result(SSTReader::new_iterator(&rd,
                                                                  &ReadOptions::default(),
                                                                  &cfi.internal_key_cmp))?;
                compaction.original_inputs.push(Rc::new(RefCell::new(iter)));
                compact.patch.delete_file(cfi.id(), compact.level as i32, file.number);
            }
        }

        let table_file_path = cfi.get_table_file_path(&self.env, compaction.target_file_number);
        let rs = self.env.new_writable_file(&table_file_path, false);
        if let Err(e) = &rs {
            versions.reuse_file_number(compaction.target_file_number);
            Err(Status::corrupted(e.to_string()))?;
        }

        let mut builder = SSTBuilder::new(&cfi.internal_key_cmp,
                                          rs.as_ref().unwrap().clone(),
                                          cfi.options().block_size,
                                          cfi.options().block_restart_interval,
                                          new_n_slots);
        drop(versions);
        //------------------------------unlock------------------------------------------------------
        let result = from_io_result(compaction.run(&self.table_cache, &mut builder))?;
        //------------------------------lock again--------------------------------------------------
        versions = mutex.lock().unwrap();

        let mut file = FileMetadata::default();
        file.number = compaction.target_file_number;
        file.ctime = self.env.current_time_mills();
        file.size = from_io_result(builder.file_size())?;
        file.largest_key = result.largest_key.clone();
        file.smallest_key = result.smallest_key.clone();

        compact.patch.create_file_by_file_metadata(cfi.id(), compaction.target_level as i32,
                                                   Arc::new(file));
        log_info!(self.logger, "compact level-{} file: {}.sst ok, cost: {} mills",
            compaction.target_level,
            compaction.target_file_number,
            self.env.current_time_mills() - jiffies);
        Ok(versions)
    }

    fn compact_memory_table<'a>(&self, cfi: &Arc<ColumnFamilyImpl>, mutex: &'a Mutex<VersionSet>,
                                mut locking: MutexGuard<'a, VersionSet>)
                                -> Result<MutexGuard<'a, VersionSet>> {
        assert!(cfi.immutable_pipeline.is_not_empty());

        while let Some(memory_table) = cfi.immutable_pipeline.take() {
            let new_file_number = locking.generate_file_number();
            drop(locking);
            //------------------------------unlock--------------------------------------------------
            let mut patch = VersionPatch::default();
            let file_number = memory_table.associated_file_number();
            let rs = self.write_level0_table(cfi.current(), new_file_number, memory_table, &mut patch);
            if let Err(e) = rs {
                // reuse the fucking file number again.
                mutex.lock().unwrap().reuse_file_number(new_file_number);
                from_io_result(Err(e))?;
            }

            if self.shutting_down.load(Ordering::Acquire) {
                return Err(Status::corrupted("Deleting DB during memtable compaction"));
            }
            patch.set_prev_log_number(0);
            patch.set_redo_log(cfi.id(), file_number);
            //log_debug!(self.logger, "column family: {}, new redo log is {}", cfi.name(), file_number);

            //------------------------------lock again----------------------------------------------
            locking = mutex.lock().unwrap();
            from_io_result(locking.log_and_apply(ColumnFamilyOptions::default(), patch))?;
        }
        Ok(locking)
    }

    fn write_level0_table(&self, version: Arc<Version>, file_number: u64,
                          memory_table: Arc<MemoryTable>, patch: &mut VersionPatch)
                          -> io::Result<u64> {
        let jiffies = self.env.current_time_mills();
        let cfi = version.owns.upgrade().unwrap();
        let file_path = paths::table_file_by_cf(cfi.get_work_path(&self.env).as_path(), file_number);
        let file = self.env.new_writable_file(file_path.as_path(), false)?;

        let mut builder = SSTBuilder::new(&cfi.internal_key_cmp, file, cfi.options().block_size,
                                          cfi.options().block_restart_interval,
                                          memory_table.number_of_entries());

        let mut smallest_key = Vec::new();
        let mut largest_key = Vec::new();
        let mut iter = MemoryTable::iter(memory_table);
        iter.seek_to_first();
        while iter.valid() {
            if let Err(e) = builder.add(iter.key(), iter.value()) {
                builder.abandon()?;
                return Err(e);
            }

            if smallest_key.is_empty() || cfi.internal_key_cmp.lt(iter.key(), &smallest_key) {
                smallest_key = iter.key().to_vec();
            }
            if largest_key.is_empty() || cfi.internal_key_cmp.gt(iter.key(), &largest_key) {
                largest_key = iter.key().to_vec();
            }
            iter.move_next();
        }

        builder.finish()?;

        let metadata = FileMetadata {
            number: file_number,
            smallest_key,
            largest_key,
            size: builder.file_size()?,
            ctime: self.env.current_time_mills(),
        };
        patch.create_file_by_file_metadata(cfi.id(), 0, Arc::new(metadata));
        log_info!(self.logger, "write level-0 file ok, cost: {} mills",
            self.env.current_time_mills() - jiffies);
        Ok(file_number)
    }

    fn delete_obsolete_files(&self, cfi: &Arc<ColumnFamilyImpl>, versions: &mut MutexGuard<VersionSet>) -> io::Result<()> {
        let mut cleanup = HashMap::new();
        for name in self.env.get_children(&self.abs_db_path)? {
            let (kind, number) = files::parse_name(&name);
            match kind {
                Kind::Log | Kind::Manifest => {
                    cleanup.insert(number, self.abs_db_path.join(name));
                }
                _ => ()
            }
        }
        cleanup.remove(&self.redo_log_number.get());
        cleanup.remove(&versions.manifest_file_number);

        for cfi in versions.column_families().borrow().column_family_impls() {
            cleanup.remove(&cfi.redo_log_number());
        }

        let cfi_work_path = cfi.get_work_path(&self.env);
        for name in self.env.get_children(&cfi_work_path)? {
            let (kind, number) = files::parse_name(&name);
            match kind {
                Kind::SstTable => {
                    cleanup.insert(number, cfi_work_path.join(name));
                }
                _ => ()
            }
        }

        for i in 0..config::MAX_LEVEL {
            for file in cfi.current().level_files(i) {
                cleanup.remove(&file.number);
            }
        }

        for (_, path) in cleanup {
            self.env.delete_file(&path, true)?;
            log_info!(self.logger, "delete obsolete file: {}", path.to_str().unwrap());
        }
        Ok(())
    }

    fn new_internal_iterator(&self, options: &ReadOptions, cfi: &Arc<ColumnFamilyImpl>, _locking: &MutexGuard<VersionSet>)
                             -> Result<IteratorRc> {
        let mut iters = Vec::<IteratorRc>::new();
        iters.push(Rc::new(RefCell::new(MemoryTable::iter(cfi.mutable().clone()))));

        for table in cfi.immutable_pipeline.peek_all() {
            iters.push(Rc::new(RefCell::new(MemoryTable::iter(table))));
        }

        from_io_result(cfi.get_levels_iters(options, &self.table_cache, &mut iters))?;

        // No need register cleanup:
        // Memory table's iterator can cleanup itself reference count.
        let iter = MergingIterator::new(cfi.internal_key_cmp.clone(), iters);
        Ok(Rc::new(RefCell::new(iter)))
    }

    fn prepare_for_get(&self, options: &ReadOptions, cf: &Arc<dyn ColumnFamily>) -> mai2::Result<Get> {
        let cfi = ColumnFamilyImpl::from(cf);

        //------------------------------lock version-set--------------------------------------------
        let mutex = self.versions.clone();
        let versions = mutex.lock().unwrap();

        if cfi.dropped() {
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
        memory_tables.push(cfi.mutable().clone());
        memory_tables.append(&mut cfi.immutable_pipeline.peek_all());

        //------------------------------unlock version-set------------------------------------------
        Ok(Get {
            last_sequence_number,
            memory_tables,
            cfi: cfi.clone(),
            version: cfi.current(),
        })
    }

    fn _test_force_dump_immutable_table(&self, cf: &Arc<dyn ColumnFamily>, sync: bool) -> Result<()> {
        let cfi = ColumnFamilyImpl::from(cf);
        let mutex = self.versions.clone();
        {
            let mut versions = mutex.lock().unwrap();
            assert_eq!(0, versions.prev_log_number);
            self.renew_log_file(&mut versions)?;

            let mut patch = VersionPatch::default();
            patch.set_redo_log_number(self.redo_log_number.get());
            from_io_result(versions.log_and_apply(self.options.core.clone(), patch))?;

            cfi.make_immutable_pipeline(self.redo_log_number.get());
            self.maybe_schedule_compacting(&cfi, &versions);
        }

        if sync {
            let lock = mutex.lock().unwrap();
            let _unused = cfi.background_cv.wait(lock).unwrap();
        }
        Ok(())
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
        from_io_result(cfi.install(&self.env))?;
        cfi.set_redo_log_number(self.redo_log_number.get());
        assert!(cfi.initialized());
        Ok(ColumnFamilyHandle::new(&cfi, &self.versions))
        //--------------------------unlock version-set----------------------------------------------
    }

    fn drop_column_family(&self, column_family: Arc<dyn ColumnFamily>) -> mai2::Result<()> {
        if column_family.id() == 0 {
            return Err(Status::corrupted("can not drop default column family!"));
        }

        //--------------------------lock version-set------------------------------------------------
        let cfi = ColumnFamilyImpl::from(&column_family);
        if cfi.dropped() {
            return Err(Status::corrupted(format!("column family: {} has dropped",
                                                 cfi.name())));
        }

        let mut versions = self.versions.lock().unwrap();
        while cfi.background_progress() {
            // Waiting for all background progress done
            versions = cfi.background_cv.wait(versions).unwrap();
        }
        let mut patch = VersionPatch::default();
        patch.drop_column_family(cfi.id());

        let cf_opts = cfi.options().clone();
        from_io_result(versions.log_and_apply(cf_opts, patch))?;
        from_io_result(cfi.uninstall(&self.env))?;
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
            .filter(|x| !x.dropped() && x.initialized())
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

    fn get_pinnable(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>, key: &[u8])
                    -> mai2::Result<PinnableValue> {
        let get = self.prepare_for_get(options, column_family)?;
        //-------------------------------------Lock-free--------------------------------------------
        for table in &get.memory_tables {
            match table.get(key, get.last_sequence_number) {
                Ok((value, tag)) =>
                    return if tag == Tag::Deletion {
                        Err(Status::NotFound)
                    } else {
                        Ok(PinnableValue::from_memory_table(table, value))
                    },
                Err(_) => ()
            }
        }

        match get.version.get(options, key, get.last_sequence_number, &self.table_cache) {
            Err(e) => Err(e),
            Ok((value, tag)) => if tag == Tag::Deletion {
                Err(Status::NotFound)
            } else {
                Ok(value)
            }
        }
    }

    fn new_iterator(&self, options: &ReadOptions, column_family: &Arc<dyn ColumnFamily>)
                    -> Result<IteratorArc> {
        let get = self.prepare_for_get(options, column_family)?;
        let cfi = ColumnFamilyImpl::from(column_family);

        let mutex = self.versions.clone();
        let locking = mutex.lock().unwrap();
        //--------------------------lock version-set------------------------------------------------
        let internal_iter = self.new_internal_iterator(options, &cfi, &locking)?;
        let iter = DBIterator::new(&cfi.internal_key_cmp.user_cmp,
                                   internal_iter,
                                   get.last_sequence_number);
        Ok(Arc::new(RefCell::new(iter)))
        //--------------------------unlock version-set----------------------------------------------
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

        if let Some(worker) = self.compaction_worker.take() {
            worker.tx.send(WorkerCommand::Exit).unwrap();
            worker.join_handle.join().unwrap();
            drop(worker.tx);
        }
        log_debug!(self.logger, "drop it! DBImpl");
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
        let cfs = unsafe { &*self.column_families.as_ptr() };
        let cfi = cfs.get_column_family_by_id(cf_id).unwrap().clone();
        cfi.mutable().clone()
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
        table.insert(key, value, self.sequence_number(), Tag::Key);
        self.advance(key.len() + size_of::<u32>() + size_of::<u64>() + value.len());
    }

    fn did_delete(&self, cf_id: u32, key: &[u8]) {
        let table = self.get_memory_table(cf_id);
        table.insert(key, "".as_bytes(), self.sequence_number(), Tag::Deletion);
        self.advance(key.len() + size_of::<u32>() + size_of::<u64>());
    }
}

struct Get {
    memory_tables: Vec<Arc<MemoryTable>>,
    last_sequence_number: u64,
    cfi: Arc<ColumnFamilyImpl>,
    version: Arc<Version>,
}

#[cfg(test)]
mod tests {
    use std::iter;

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

            check_key_value_pairs(&db, &cf0, &[("1111", "a"), ("3333", "b"), ("4444", "c")])?;
            check_key_value_pairs(&db, &cf1, &[("2222", "cc")])?;
            //check_key_value_pairs(&db, &cf1, &[("2222, "")])?;
        }
        Ok(())
    }

    #[test]
    fn dump_level0_sst() -> Result<()> {
        let pairs = [
            ("1111", "a"),
            ("3333", "b"),
            ("4444", "c"),
            ("2222", "cc"),
        ];
        let _junk = JunkFilesCleaner::new("tests/db6");
        let db = open_test_db("db6")?;
        let cf0 = db.default_column_family();

        insert_key_value_pairs(&db, &cf0, &pairs)?;

        db._test_force_dump_immutable_table(&cf0, true)?;

        let rd_opts = ReadOptions::default();
        let value = db.get(&rd_opts, &cf0, "1111".as_bytes())?;
        assert_eq!("a".as_bytes(), value);

        check_key_value_pairs(&db, &cf0, &pairs)?;

        Ok(())
    }

    #[test]
    fn large_insert() -> Result<()> {
        let n = 100000;

        let _junk = JunkFilesCleaner::new("tests/db7");

        {
            let db = open_test_db("db7")?;
            let cf0 = db.default_column_family();

            let blank_val = String::from_iter(iter::repeat('a').take(1000));
            let wr_opts = WriteOptions::default();
            for i in 0..n {
                let key = format!("key.{}", i);
                //let value = format!("val.{}", i);
                db.insert(&wr_opts, &cf0, key.as_bytes(), blank_val.as_bytes())?;
            }
        }

        {
            let desc = [ColumnFamilyDescriptor {
                name: DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                options: ColumnFamilyOptions::default(),
            }];
            let db = load_test_db("db7", &desc)?;
            let cf0 = db.default_column_family();
            let rd_opts = ReadOptions::default();
            for i in 0..n {
                let key = format!("key.{}", i);
                let value = db.get_pinnable(&rd_opts, &cf0, key.as_bytes())?;
                assert_eq!(1000, value.value().len());
            }
        }

        Ok(())
    }

    fn insert_key_value_pairs(db: &Arc<DBImpl>, cf: &Arc<dyn ColumnFamily>, pairs: &[(&str, &str)]) -> Result<()> {
        let wr_opts = WriteOptions::default();
        for (key, value) in pairs {
            db.insert(&wr_opts, &cf, key.as_bytes(), value.as_bytes())?;
        }
        Ok(())
    }

    fn check_key_value_pairs(db: &Arc<DBImpl>, cf: &Arc<dyn ColumnFamily>, pairs: &[(&str, &str)]) -> Result<()> {
        let rd_opts = ReadOptions::default();
        for (key, value) in pairs {
            let v = db.get(&rd_opts, &cf, key.as_bytes())?;
            assert_eq!(value.as_bytes(), v.as_slice());
        }
        Ok(())
    }

    fn open_test_db(name: &str) -> Result<Arc<DBImpl>> {
        let options = Options::with()
            .create_if_missing(true)
            .dir(String::from("tests"))
            .build();
        let (db, _cfs) = DBImpl::open(options, String::from(name), &Vec::new())?;
        Ok(db)
    }

    // fn open_dummy_db(name: &str) -> mai2::Result<Arc<DBImpl>> {
    //     let options = Options::with()
    //         .create_if_missing(true)
    //         .dir(String::from("dummies"))
    //         .build();
    //     let (db, _cfs) = DBImpl::open(options, String::from(name), &Vec::new())?;
    //     Ok(db)
    // }

    fn load_test_db(name: &str, desc: &[ColumnFamilyDescriptor]) -> Result<Arc<DBImpl>> {
        let options = Options::with()
            .dir("tests".to_string())
            .error_if_exists(false)
            .build();
        let (db, _cfs) = DBImpl::open(options, String::from(name), desc)?;
        Ok(db)
    }

    fn load_dummy_db(name: &str, desc: &[ColumnFamilyDescriptor]) -> Result<Arc<DBImpl>> {
        let options = Options::with()
            .dir("dummies".to_string())
            .error_if_exists(false)
            .build();
        let (db, _cfs) = DBImpl::open(options, String::from(name), desc)?;
        Ok(db)
    }
}