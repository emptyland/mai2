use std::cell::RefCell;
use std::io::Read;
use std::rc::Rc;
use std::sync::Weak;
use crate::base::Arena;
use crate::exec::db::DB;
use crate::exec::executor::Executor;
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
}

impl Drop for Connection {
    fn drop(&mut self) {
        match self.executor.borrow().db.upgrade() {
            Some(db) => db.remove_connection(self),
            None => ()
        }
    }
}