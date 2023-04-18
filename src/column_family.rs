use std::{array, io, iter};
use std::any::Any;
use std::cell::{Cell, RefCell};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::{config, files, log_debug, mai2};
use crate::compaction::Compact;
use crate::comparator::Comparator;
use crate::env::Env;
use crate::key::InternalKeyComparator;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions};
use crate::memory_table::MemoryTable;
use crate::queue::NonBlockingQueue;
use crate::status::Status;
use crate::version::{FileMetadata, Version, VersionSet};

pub struct ColumnFamilyImpl {
    name: String,
    id: u32,
    options: ColumnFamilyOptions,
    owns: Weak<RefCell<ColumnFamilySet>>,
    //logger: Arc<dyn Logger>,
    dropped: AtomicBool,
    pub internal_key_cmp: InternalKeyComparator,
    initialized: Cell<bool>,
    background_progress: AtomicBool,
    pub background_result: Cell<mai2::Result<()>>,
    pub background_cv: Condvar,
    redo_log_number: Cell<u64>,

    history: Cell<Vec<Arc<Version>>>,
    non_version: Arc<Version>, // flag by current version not exists

    compaction_points: RefCell<[Vec<u8>;config::MAX_LEVEL]>,

    mutable: Cell<Arc<MemoryTable>>,
    pub immutable_pipeline: Arc<NonBlockingQueue<Arc<MemoryTable>>>,
    // TODO:
}

impl Debug for ColumnFamilyImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamilyImpl")
            .field("name", &self.name)
            .field("id", &self.id)
            .field("user_key", &self.internal_key_cmp.user_cmp().name())
            .finish()
    }
}

impl ColumnFamilyImpl {
    fn new(name: String, id: u32, options: ColumnFamilyOptions, owns: Weak<RefCell<ColumnFamilySet>>)
           -> Arc<ColumnFamilyImpl> {
        let ikc = options.user_comparator.clone();
        let internal_key_cmp = InternalKeyComparator::new(ikc);

        Arc::new_cyclic(|weak| {
            ColumnFamilyImpl {
                name,
                id,
                options,
                owns,
                //logger,
                dropped: AtomicBool::from(false),
                internal_key_cmp: internal_key_cmp.clone(),
                initialized: Cell::new(false),
                background_progress: AtomicBool::new(false),
                background_result: Cell::new(Ok(())),
                background_cv: Condvar::new(),
                redo_log_number: Cell::new(0),
                history: Cell::new(Vec::new()),
                non_version: Arc::new(Version::new(weak.clone())),
                mutable: Cell::new(MemoryTable::new_rc(internal_key_cmp.clone())),
                immutable_pipeline: Arc::new(NonBlockingQueue::new()),
                compaction_points: RefCell::new(array::from_fn(|_|{Vec::new()})),
            }
        })
    }

    #[inline]
    pub fn from(cf: &Arc<dyn ColumnFamily>) -> Arc<Self> {
        let handle = cf.as_any().downcast_ref::<ColumnFamilyHandle>().unwrap();
        handle.core().clone()
    }

    pub fn install(&self, env: &Arc<dyn Env>) -> io::Result<()> {
        assert!(!self.initialized());

        let cf_path = self.get_work_path(env);
        env.make_dir(cf_path.as_path())?;

        self.initialized.set(true);
        Ok(())
    }

    pub fn uninstall(&self, env: &Arc<dyn Env>) -> io::Result<()> {
        assert!(self.initialized());
        assert!(!self.background_progress());
        assert!(self.dropped());

        let work_path = self.get_work_path(env);
        let owns = self.owns.upgrade().unwrap();
        env.delete_file(work_path.as_path(), true)?;
        self.initialized.set(false);
        Ok(())
    }

    pub fn make_immutable_pipeline(&self, redo_log_number: u64) {
        let immutable = self.mutable.replace(MemoryTable::new_rc(self.internal_key_cmp.clone()));
        immutable.associate_file_number_to(redo_log_number);
        self.immutable_pipeline.add(immutable);
    }

    pub fn append(&self, version: Version) {
        let mut history = self.history.take();
        let record = Arc::new(version);
        history.push(record);
        self.history.set(history);
    }

    pub fn current(&self) -> Arc<Version> {
        let history = self.history.take();
        let current = history.last()
            .or_else(|| { Some(&self.non_version) })
            .unwrap()
            .clone();
        self.history.set(history);
        current
    }

    pub fn set_compaction_point(&self, level: usize, key: Vec<u8>) {
        self.compaction_points.borrow_mut()[level] = key;
    }

    pub fn compaction_point(&self, level: usize) -> Vec<u8> {
        self.compaction_points.borrow()[level].clone()
    }

    pub fn pick_compaction(&self) -> Option<Compact> {
        if !self.need_compaction() {
            return None;
        }

        let should_compact = self.current().compaction_score >= 1.0;
        if !should_compact {
            return None;
        }

        assert!(self.current().compaction_level >= 0);
        let level = self.current().compaction_level as usize;
        assert!(level + 1 < config::MAX_LEVEL);

        //let compaction_points = self.compaction_points.take();
        let mut inputs = [Vec::new(), Vec::new()];
        for file in self.current().level_files(level) {
            if self.compaction_point(level).is_empty() ||
                self.internal_key_cmp.lt(&self.compaction_point(level), &file.largest_key) {
                inputs[0].push(file.clone());
                break;
            }
        }

        if inputs[0].is_empty() {
            inputs[0].push(self.current().level_files(level).first().unwrap().clone());
        }
        //self.compaction_points.set(compaction_points);

        // Files in level 0 may overlap each other, so pick up all overlapping ones
        if level == 0 {
            let (smallest, largest) = self.get_range(&mut inputs[0]);

            // Note that the next call will discard the file we placed in
            // c->inputs_[0] earlier and replace it with an overlapping set
            // which will include the picked file.
            self.current().get_overlapping_inputs(0, &smallest, &largest, &mut inputs[0]);
            assert!(!inputs[0].is_empty());
        }

        Some(self.setup_other_inputs(Compact {
            level,
            input_version: self.current(),
            patch: Default::default(),
            inputs,
        }))
    }

    fn setup_other_inputs(&self, mut compact: Compact) -> Compact {
        let level = compact.level;
        let (smallest, largest) = self.get_range(&mut compact.inputs[0]);

        self.current().get_overlapping_inputs(level + 1, &smallest, &largest,
                                              &mut compact.inputs[0]);

        // Get entire range covered by compaction
        let (_all_start, _all_limit) = self.get_range2(&mut compact.inputs);

        // TODO:

        // Update the place where we will do the next compaction for this level.
        // We update this immediately instead of waiting for the VersionEdit
        // to be applied so that if the compaction fails, we will try a different
        // key range next time.
        self.set_compaction_point(level, largest.clone());

        compact.patch.set_compaction_point(self.id, level as i32, &largest);
        compact
    }

    // Stores the minimal range that covers all entries in inputs in
    // *smallest, *largest.
    // REQUIRES: inputs is not empty
    fn get_range(&self, inputs: &mut Vec<Arc<FileMetadata>>) -> (Vec<u8>, Vec<u8>) {
        assert!(!inputs.is_empty());
        let mut smallest = Vec::new();
        let mut largest = Vec::new();
        for i in 0..inputs.len() {
            let file = &inputs[i];
            if i == 0 {
                smallest = file.smallest_key.clone();
                largest = file.largest_key.clone();
            } else {
                if self.internal_key_cmp.lt(&file.smallest_key, &smallest) {
                    smallest = file.smallest_key.clone();
                }
                if self.internal_key_cmp.gt(&file.largest_key, &largest) {
                    largest = file.largest_key.clone();
                }
            }
        }
        (smallest, largest)
    }

    // Stores the minimal range that covers all entries in inputs1 and inputs2
    // in *smallest, *largest.
    // REQUIRES: inputs is not empty
    fn get_range2(&self, inputs: &mut [Vec<Arc<FileMetadata>>]) ->(Vec<u8>, Vec<u8>) {
        let mut all = inputs[0].clone();
        inputs[1].iter().for_each(|x| {
            all.push(x.clone())
        });
        self.get_range(&mut all)
    }


    pub fn get_table_file_path(&self, env: &Arc<dyn Env>, file_number: u64) -> PathBuf {
        files::paths::table_file_by_cf(&self.get_work_path(env), file_number)
    }

    pub fn get_work_path(&self, env: &Arc<dyn Env>) -> PathBuf {
        let owns = self.owns.upgrade().unwrap();
        let path = PathBuf::new();
        if self.options.dir.is_empty() {
            path.join(owns.borrow().abs_db_path.as_path()).join(self.name())
        } else {
            path.join(env.get_absolute_path(Path::new(&self.options.dir)).unwrap())
                .join(self.name())
        }
    }

    pub const fn id(&self) -> u32 { self.id }

    pub const fn name(&self) -> &String { &self.name }

    pub const fn options(&self) -> &ColumnFamilyOptions { &self.options }

    pub fn dropped(&self) -> bool { self.dropped.load(Ordering::Acquire) }

    pub fn initialized(&self) -> bool { self.initialized.get() }

    pub fn background_progress(&self) -> bool { self.background_progress.load(Ordering::Relaxed) }

    pub fn set_background_progress(&self, in_progress: bool) {
        self.background_progress.store(in_progress, Ordering::Relaxed)
    }

    pub fn set_background_result(&self, rs: mai2::Result<()>) {
        self.background_result.set(rs);
    }

    pub fn redo_log_number(&self) -> u64 { self.redo_log_number.get() }

    pub fn set_redo_log_number(&self, number: u64) { self.redo_log_number.set(number); }

    pub fn internal_key_cmp(&self) -> &dyn Comparator { &self.internal_key_cmp }

    pub fn need_compaction(&self) -> bool { self.current().compaction_score >= 1.0 }

    pub fn mutable(&self) -> &Arc<MemoryTable> {
        unsafe { &*self.mutable.as_ptr() }
    }

    pub fn drop_it(&self) {
        assert_ne!(0, self.id(), "don't drop default column family");
        assert!(!self.dropped(), "don't drop again");
        self.dropped.store(true, Ordering::Release);
        if let Some(owns) = self.owns.upgrade() {
            owns.borrow_mut().remove_column_family(self);
        }
    }
}

impl Drop for ColumnFamilyImpl {
    fn drop(&mut self) {
        if !self.dropped() {
            if let Some(owns) = self.owns.upgrade() {
                owns.borrow_mut().remove_column_family(self);
            }
        }

        // if let Some(owns) = self.owns.upgrade() {
        //     if let Some(mutex) = owns.borrow().owns.upgrade() {
        //         let versions = mutex.lock().unwrap();
        //         log_debug!(versions.logger, "drop column-family-impl: {}", self.name);
        //     }
        // }
    }
}

pub struct ColumnFamilyHandle {
    db: *const u8,
    id: u32,
    name: String,
    mutex: Arc<Mutex<VersionSet>>,
    core: Arc<ColumnFamilyImpl>,
}

impl ColumnFamilyHandle {
    pub fn new(core: &Arc<ColumnFamilyImpl>, mutex: &Arc<Mutex<VersionSet>>) -> Arc<dyn ColumnFamily> {
        let id = core.id();
        let name = core.name.clone();
        Arc::new(ColumnFamilyHandle {
            db: 0 as *const u8,
            id,
            name,
            core: core.clone(),
            mutex: mutex.clone(),
        })
    }

    pub fn core(&self) -> &Arc<ColumnFamilyImpl> {
        &self.core
    }
}

impl ColumnFamily for ColumnFamilyHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> u32 {
        self.id
    }

    fn comparator(&self) -> Rc<dyn Comparator> {
        todo!()
    }

    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor, Status> {
        let _locking = self.mutex.lock().unwrap();
        if self.core().dropped() {
            Err(Status::Corruption(String::from("Column family is dropped!")))
        } else {
            Ok(ColumnFamilyDescriptor {
                name: self.name(),
                options: self.core().options().clone(),
            })
        }
    }
}


pub struct ColumnFamilySet {
    owns: Weak<Mutex<VersionSet>>,
    pub abs_db_path: PathBuf,
    max_column_family_id: u32,
    default_column_family: Option<Arc<ColumnFamilyImpl>>,
    pub column_families: HashMap<u32, Arc<ColumnFamilyImpl>>,
    column_family_names: HashMap<String, Arc<ColumnFamilyImpl>>,

}

impl ColumnFamilySet {
    pub fn new_dummy(owns: Weak<Mutex<VersionSet>>, abs_db_path: PathBuf) -> Arc<RefCell<Self>> {
        let cfs = Self {
            owns,
            abs_db_path,
            max_column_family_id: 0,
            default_column_family: None,
            column_families: HashMap::new(),
            column_family_names: HashMap::new(),
        };
        let owns = Arc::new(RefCell::new(cfs));
        owns
    }

    pub fn new_column_family(this: &Arc<RefCell<Self>>, id: u32, name: String, options: ColumnFamilyOptions)
                             -> Arc<ColumnFamilyImpl> {
        // TODO:
        // let logger = this.borrow()
        //     .owns.upgrade().unwrap()
        //     .lock().unwrap()
        //     .logger.clone();
        let cf = ColumnFamilyImpl::new(name, id, options, Arc::downgrade(this));
        let mut borrowed_this = this.borrow_mut();

        assert!(borrowed_this.get_column_family_by_id(cf.id()).is_none());
        borrowed_this.column_families.insert(cf.id(), cf.clone());

        assert!(borrowed_this.get_column_family_by_name(cf.name()).is_none());
        borrowed_this.column_family_names.insert(cf.name().clone(), cf.clone());

        borrowed_this.update_max_column_family_id(id);
        if id == 0 {
            borrowed_this.default_column_family = Some(cf.clone());
        }
        cf
    }

    pub fn default_column_family(&self) -> Arc<ColumnFamilyImpl> {
        self.default_column_family.clone().unwrap().clone()
    }

    pub fn next_column_family_id(&mut self) -> u32 {
        self.max_column_family_id += 1;
        self.max_column_family_id
    }

    pub const fn max_column_family_id(&self) -> u32 {
        self.max_column_family_id
    }

    pub fn remove_column_family(&mut self, cfi: &ColumnFamilyImpl) {
        self.column_families.remove(&cfi.id());
        self.column_family_names.remove(cfi.name());
    }

    pub fn get_column_family_by_id(&self, id: u32) -> Option<&Arc<ColumnFamilyImpl>> {
        self.column_families.get(&id)
    }

    pub fn get_column_family_by_name(&self, name: &String) -> Option<&Arc<ColumnFamilyImpl>> {
        self.column_family_names.get(name)
    }

    pub fn update_max_column_family_id(&mut self, new_id: u32) {
        self.max_column_family_id = max(self.max_column_family_id, new_id);
    }

    pub fn column_family_impls(&self) -> Vec<&Arc<ColumnFamilyImpl>> {
        self.column_families.values()
            .enumerate()
            .map(|x| x.1)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::mai2::Options;

    use super::*;

    #[test]
    fn sanity() {
        // let vss = VersionSet::new(PathBuf::from("db"), &Options::default());
        // let vs = vss.borrow();
        // let cfs = vs.column_families();
        // assert_eq!(0, cfs.borrow().max_column_family_id());
        // let dcf = cfs.borrow().default_column_family();
        // assert_eq!(0, dcf.borrow().id());
        // assert_eq!("default", dcf.borrow().name());
    }
}