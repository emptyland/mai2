use std::any::Any;
use std::cell::{Ref, RefCell};
use std::cmp::max;
use std::collections::{HashMap, LinkedList};
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::comparator::Comparator;
use crate::key::InternalKeyComparator;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions};
use crate::status::Status;

pub struct ColumnFamilyImpl {
    name: String,
    id: u32,
    options: ColumnFamilyOptions,
    owns: Weak<RefCell<ColumnFamilySet>>,
    dropped: AtomicBool,
    internal_key_cmp: InternalKeyComparator,
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
    fn new_dummy(name: String, id: u32, options: ColumnFamilyOptions, owns: &Arc<RefCell<ColumnFamilySet>>)
                 -> Arc<RefCell<ColumnFamilyImpl>> {
        let ikc = options.user_comparator.clone();
        let cfi = ColumnFamilyImpl {
            name,
            id,
            options,
            owns: Arc::downgrade(owns),
            dropped: AtomicBool::from(false),
            internal_key_cmp: InternalKeyComparator::new(ikc)
        };
        Arc::new(RefCell::new(cfi))
    }

    #[inline]
    pub fn from(cf: &Arc<RefCell<dyn ColumnFamily>>) -> Arc<RefCell<Self>> {
        let mut borrowed = cf.borrow_mut();
        let handle = borrowed.as_any_mut().downcast_mut::<ColumnFamilyHandle>().unwrap();
        handle.core().clone()
    }

    pub const fn id(&self) -> u32 { self.id }

    pub const fn name(&self) -> &String { &self.name }

    pub const fn options(&self) -> &ColumnFamilyOptions { &self.options }

    pub fn dropped(&self) -> bool { self.dropped.load(Ordering::Acquire) }

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
    pub fn new(core: Arc<RefCell<ColumnFamilyImpl>>) -> Arc<RefCell<dyn ColumnFamily>> {
        Arc::new( RefCell::new(ColumnFamilyHandle {
            db: 0 as *const u8,
            core,
        }))
    }

    pub fn core(&self) -> &Arc<RefCell<ColumnFamilyImpl>> {
        &self.core
    }
}

impl ColumnFamily for ColumnFamilyHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
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
            Ok(ColumnFamilyDescriptor{
                name: self.name(),
                options: self.core().borrow().options().clone(),
            })
        }
    }
}


pub struct ColumnFamilySet {
    max_column_family_id: u32,
    default_column_family: Option<Arc<RefCell<ColumnFamilyImpl>>>,
    column_families: HashMap<u32, Arc<RefCell<ColumnFamilyImpl>>>,
    column_family_names: HashMap<String, Arc<RefCell<ColumnFamilyImpl>>>,

}

impl ColumnFamilySet {
    pub fn new_dummy() -> Arc<RefCell<Self>> {
        let mut cfs = Self {
            max_column_family_id: 0,
            default_column_family: None,
            column_families: HashMap::new(),
            column_family_names: HashMap::new(),
        };
        let owns = Arc::new(RefCell::new(cfs));
        let cf = Self::new_column_family_dummy(&owns, String::from("default"), ColumnFamilyOptions::default());
        owns.borrow_mut().default_column_family = Some(cf);
        owns
    }

    fn new_column_family_dummy(this: &Arc<RefCell<Self>>, name: String, options: ColumnFamilyOptions)
        -> Arc<RefCell<ColumnFamilyImpl>> {
        let cf = ColumnFamilyImpl::new_dummy(name, this.borrow_mut().next_column_family_id(), options, this);
        this.borrow_mut().column_families.insert(cf.borrow().id(), cf.clone());
        cf
    }

    pub fn new_column_family(this: &Arc<RefCell<Self>>, id: u32, name: String, options: ColumnFamilyOptions)
        -> Arc<RefCell<ColumnFamilyImpl>> {
        // TODO:
        let cf = ColumnFamilyImpl::new_dummy(name, id, options, this);
        this.borrow_mut().column_families.insert(cf.borrow().id(), cf.clone());
        this.borrow_mut().column_family_names.insert(cf.borrow().name().clone(), cf.clone());
        this.borrow_mut().update_max_column_family_id(id);
        if id == 0 {
            this.borrow_mut().default_column_family = Some(cf.clone());
        }
        cf
    }

    pub fn default_column_family(&self) -> Arc<RefCell<ColumnFamilyImpl>> {
        self.default_column_family.clone().unwrap().clone()
    }

    fn next_column_family_id(&mut self) -> u32 {
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
}

#[cfg(test)]
mod tests {
    use crate::column_family::ColumnFamilySet;

    #[test]
    fn sanity() {
        let cfs = ColumnFamilySet::new_dummy();
        assert_eq!(1, cfs.borrow().max_column_family_id());
        let dcf = cfs.borrow().default_column_family();
        assert_eq!(1, dcf.borrow().id());
        assert_eq!("default", dcf.borrow().name());
    }
}