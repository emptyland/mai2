use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Weak};

use crate::comparator::Comparator;
use crate::db_impl::DBImpl;
use crate::mai2::{ColumnFamily, ColumnFamilyDescriptor};
use crate::status::Status;

pub struct ColumnFamilyImpl {

}

pub struct ColumnFamilyHandle {
    db: Weak<DBImpl>,
    core: Arc<ColumnFamilyImpl>
}

impl ColumnFamilyHandle {
    pub fn new(db: Weak<DBImpl>, core: Arc<ColumnFamilyImpl>) -> Arc<dyn ColumnFamily> {

        Arc::new(ColumnFamilyHandle{
            db,
            core
        })
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
        todo!()
    }

    fn id(&self) -> u32 {
        todo!()
    }

    fn comparator(&self) -> Rc<dyn Comparator> {
        todo!()
    }

    fn get_descriptor<'a>(&self) -> Result<&'a ColumnFamilyDescriptor, Status> {
        todo!()
    }
}