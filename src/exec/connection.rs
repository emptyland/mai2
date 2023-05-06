use std::cell::RefCell;
use std::io::Read;
use std::rc::Rc;
use std::sync::Weak;
use crate::base::{Arena, ArenaBox, ArenaVec};
use crate::exec::db::DB;
use crate::exec::executor::{Executor, PreparedStatement};
use crate::Result;
use crate::storage::MemorySequentialFile;

pub struct Connection {
    pub id: u64,
    executor: RefCell<Executor>,
}

impl Connection {
    pub fn new(id: u64, db: &Weak<DB>) -> Self {
        Connection {
            id,
            executor: RefCell::new(Executor::new(db))
        }
    }

    pub fn execute_str(&self, sql: &str, arena: &Rc<RefCell<Arena>>) -> Result<()> {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        self.execute(&mut rd, arena)
    }

    pub fn execute(&self, reader: &mut dyn Read, arena: &Rc<RefCell<Arena>>) -> Result<()> {
        self.executor.borrow_mut().execute(reader, arena)
    }

    pub fn execute_prepared_statement(&self, prepared: &mut ArenaBox<PreparedStatement>) -> Result<()> {
        let arena = Arena::new_rc();
        self.executor.borrow_mut().execute_prepared_statement(prepared, &arena)
    }

    pub fn prepare_str(&self, sql: &str, arena: &Rc<RefCell<Arena>>) -> Result<ArenaVec<ArenaBox<PreparedStatement>>> {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        self.prepare(&mut rd, arena)
    }

    pub fn prepare(&self, reader: &mut dyn Read, arena: &Rc<RefCell<Arena>>) -> Result<ArenaVec<ArenaBox<PreparedStatement>>> {
        self.executor.borrow_mut().prepare(reader, arena)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        match self.executor.borrow().db.upgrade() {
            Some(db) => db.remove_connection(self),
            None => ()
        }
    }
}