use std::{array, io};
use std::any::Any;
use std::cell::RefCell;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::comparator::Comparator;
use crate::config;
use crate::env::Env;
use crate::key::InternalKeyComparator;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DEFAULT_COLUMN_FAMILY_NAME};
use crate::memory_table::MemoryTable;
use crate::status::Status;
use crate::version::{Version, VersionSet};

pub struct ColumnFamilyImpl {
    name: String,
    id: u32,
    options: ColumnFamilyOptions,
    owns: Weak<RefCell<ColumnFamilySet>>,
    dropped: AtomicBool,
    pub internal_key_cmp: InternalKeyComparator,
    initialized: bool,
    background_progress: AtomicBool,
    redo_log_number: u64,

    history: Vec<Version>,
    current_version_index: usize,

    compaction_points: [Vec<u8>; config::MAX_LEVEL],

    mutable: Arc<RefCell<MemoryTable>>,
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
    fn new_dummy(name: String, id: u32, options: ColumnFamilyOptions, owns: Weak<RefCell<ColumnFamilySet>>)
                 -> Arc<RefCell<ColumnFamilyImpl>> {
        let ikc = options.user_comparator.clone();
        let internal_key_cmp = InternalKeyComparator::new(ikc);
        let cfi = ColumnFamilyImpl {
            name,
            id,
            options,
            owns,
            dropped: AtomicBool::from(false),
            internal_key_cmp: internal_key_cmp.clone(),
            initialized: false,
            background_progress: AtomicBool::new(false),
            redo_log_number: 0,
            history: Vec::new(),
            current_version_index: 0,
            mutable: MemoryTable::new_rc(internal_key_cmp.clone()),
            compaction_points: array::from_fn(|_| Vec::new()),

        };
        Arc::new(RefCell::new(cfi))
    }

    #[inline]
    pub fn from(cf: &Arc<dyn ColumnFamily>) -> Arc<RefCell<Self>> {
        let handle = cf.as_any().downcast_ref::<ColumnFamilyHandle>().unwrap();
        handle.core().clone()
    }

    pub fn install(&mut self) -> io::Result<()> {
        assert!(!self.initialized());

        let cf_path = self.get_work_path();
        let owns = self.owns.upgrade().unwrap();
        owns.borrow().env().make_dir(cf_path.as_path())?;

        self.initialized = true;
        Ok(())
    }

    pub fn uninstall(&mut self) -> io::Result<()> {
        assert!(self.initialized());
        assert!(!self.background_progress());
        assert!(self.dropped());

        let work_path = self.get_work_path();
        let owns = self.owns.upgrade().unwrap();
        owns.borrow().env().delete_file(work_path.as_path(), true)?;
        self.initialized = false;
        Ok(())
    }

    pub fn append(&mut self, version: Version) { self.history.push(version); }

    pub fn set_compaction_point(&mut self, level: usize, key: Vec<u8>) {
        self.compaction_points[level] = key;
    }

    pub fn get_work_path(&self) -> PathBuf {
        let owns = self.owns.upgrade().unwrap();
        let path = PathBuf::new();
        if self.options.dir.is_empty() {
            path.join(owns.borrow().abs_db_path()).join(self.name())
        } else {
            path.join(owns.borrow().env().get_absolute_path(Path::new(&self.options.dir)).unwrap())
                .join(self.name())
        }
    }

    pub fn current(&self) -> &Version {
        self.history.get(self.current_version_index).unwrap()
    }

    pub const fn id(&self) -> u32 { self.id }

    pub const fn name(&self) -> &String { &self.name }

    pub const fn options(&self) -> &ColumnFamilyOptions { &self.options }

    pub fn dropped(&self) -> bool { self.dropped.load(Ordering::Acquire) }

    pub fn initialized(&self) -> bool { self.initialized }

    pub fn background_progress(&self) -> bool { self.background_progress.load(Ordering::Relaxed) }

    pub fn redo_log_number(&self) -> u64 { self.redo_log_number }

    pub fn set_redo_log_number(&mut self, number: u64) { self.redo_log_number = number; }

    pub fn internal_key_cmp(&self) -> &dyn Comparator { &self.internal_key_cmp }

    pub fn drop_it(&mut self) {
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

        // TODO:
    }
}

pub struct ColumnFamilyHandle {
    db: *const u8,
    core: Arc<RefCell<ColumnFamilyImpl>>,
}

impl ColumnFamilyHandle {
    pub fn new(core: Arc<RefCell<ColumnFamilyImpl>>) -> Arc<dyn ColumnFamily> {
        Arc::new(ColumnFamilyHandle {
            db: 0 as *const u8,
            core,
        })
    }

    pub fn core(&self) -> &Arc<RefCell<ColumnFamilyImpl>> {
        &self.core
    }
}

impl ColumnFamily for ColumnFamilyHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        String::from(self.core().borrow().name())
    }

    fn id(&self) -> u32 {
        self.core().borrow().id
    }

    fn comparator(&self) -> Rc<dyn Comparator> {
        todo!()
    }

    fn get_descriptor(&self) -> Result<ColumnFamilyDescriptor, Status> {
        if self.core().borrow().dropped() {
            Err(Status::Corruption(String::from("Column family is dropped!")))
        } else {
            Ok(ColumnFamilyDescriptor {
                name: self.name(),
                options: self.core().borrow().options().clone(),
            })
        }
    }
}


pub struct ColumnFamilySet {
    owns: Weak<RefCell<VersionSet>>,
    max_column_family_id: u32,
    default_column_family: Option<Arc<RefCell<ColumnFamilyImpl>>>,
    column_families: HashMap<u32, Arc<RefCell<ColumnFamilyImpl>>>,
    column_family_names: HashMap<String, Arc<RefCell<ColumnFamilyImpl>>>,

}

impl ColumnFamilySet {
    pub fn new_dummy(owns: Weak<RefCell<VersionSet>>) -> Arc<RefCell<Self>> {
        let mut cfs = Self {
            owns,
            max_column_family_id: 0,
            default_column_family: None,
            column_families: HashMap::new(),
            column_family_names: HashMap::new(),
        };
        let owns = Arc::new(RefCell::new(cfs));
        // let cf = Self::new_column_family_dummy(&owns, String::from(DEFAULT_COLUMN_FAMILY_NAME), ColumnFamilyOptions::default());
        // owns.borrow_mut().default_column_family = Some(cf);
        owns
    }

    // fn new_column_family_dummy(this: &Arc<RefCell<Self>>, name: String, options: ColumnFamilyOptions)
    //     -> Arc<RefCell<ColumnFamilyImpl>> {
    //     let cf = ColumnFamilyImpl::new_dummy(name,
    //                                          this.borrow_mut().next_column_family_id(),
    //                                          options, Arc::downgrade(this));
    //     this.borrow_mut().column_families.insert(cf.borrow().id(), cf.clone());
    //     cf
    // }

    pub fn new_column_family(this: &Arc<RefCell<Self>>, id: u32, name: String, options: ColumnFamilyOptions)
                             -> Arc<RefCell<ColumnFamilyImpl>> {
        // TODO:
        let cf = ColumnFamilyImpl::new_dummy(name, id, options, Arc::downgrade(this));
        let mut borrowed_this = this.borrow_mut();

        assert!(borrowed_this.get_column_family_by_id(cf.borrow().id()).is_none());
        borrowed_this.column_families.insert(cf.borrow().id(), cf.clone());

        assert!(borrowed_this.get_column_family_by_name(cf.borrow().name()).is_none());
        borrowed_this.column_family_names.insert(cf.borrow().name().clone(), cf.clone());

        borrowed_this.update_max_column_family_id(id);
        if id == 0 {
            borrowed_this.default_column_family = Some(cf.clone());
        }
        cf
    }

    pub fn default_column_family(&self) -> Arc<RefCell<ColumnFamilyImpl>> {
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

    pub fn get_column_family_by_id(&self, id: u32) -> Option<&Arc<RefCell<ColumnFamilyImpl>>> {
        self.column_families.get(&id)
    }

    pub fn get_column_family_by_name(&self, name: &String) -> Option<&Arc<RefCell<ColumnFamilyImpl>>> {
        self.column_family_names.get(name)
    }

    pub fn update_max_column_family_id(&mut self, new_id: u32) {
        self.max_column_family_id = max(self.max_column_family_id, new_id);
    }

    pub fn column_family_impls(&self) -> Vec<&Arc<RefCell<ColumnFamilyImpl>>> {
        self.column_families.values()
            .enumerate()
            .map(|x| x.1)
            .collect()
    }

    pub fn abs_db_path(&self) -> PathBuf {
        let owns = self.owns.upgrade().unwrap();
        let borrowed = owns.borrow();
        borrowed.abs_db_path().to_path_buf()
    }

    pub fn env(&self) -> Arc<dyn Env> {
        self.owns.upgrade().unwrap().borrow().env().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::mai2::Options;

    use super::*;

    #[test]
    #[ignore]
    fn sanity() {
        let vss = VersionSet::new(PathBuf::from("db"), &Options::default());
        let vs = vss.borrow();
        let cfs = vs.column_families();
        assert_eq!(0, cfs.borrow().max_column_family_id());
        let dcf = cfs.borrow().default_column_family();
        assert_eq!(0, dcf.borrow().id());
        assert_eq!("default", dcf.borrow().name());
    }
}