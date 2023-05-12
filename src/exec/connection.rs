use std::cell::RefCell;
use std::io::Read;
use std::sync::Weak;
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaRef, ArenaVec};
use crate::exec::db::DB;
use crate::exec::executor::{Executor, PreparedStatement, Tuple};
use crate::exec::physical_plan::{Feedback, PhysicalPlanOps};
use crate::{Result, Status};
use crate::storage::{config, MemorySequentialFile};

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

    pub fn execute_str(&self, sql: &str, arena: &ArenaMut<Arena>) -> Result<u64> {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        self.execute(&mut rd, arena)
    }

    pub fn execute(&self, reader: &mut dyn Read, arena: &ArenaMut<Arena>) -> Result<u64> {
        self.executor.borrow_mut().execute(reader, arena)
    }

    pub fn execute_prepared_statement(&self, prepared: &mut ArenaBox<PreparedStatement>) -> Result<u64> {
        let arena = Arena::new_val();
        self.executor.borrow_mut().execute_prepared_statement(prepared, &arena.get_mut())
    }

    pub fn prepare_str(&self, sql: &str, arena: &ArenaMut<Arena>) -> Result<ArenaVec<ArenaBox<PreparedStatement>>> {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        self.prepare(&mut rd, arena)
    }

    pub fn prepare(&self, reader: &mut dyn Read, arena: &ArenaMut<Arena>) -> Result<ArenaVec<ArenaBox<PreparedStatement>>> {
        self.executor.borrow_mut().prepare(reader, arena)
    }

    pub fn execute_query_str(&self, sql: &str, arena: &ArenaMut<Arena>) -> ResultSet {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        self.execute_query(&mut rd, arena)
    }

    pub fn execute_query(&self, reader: &mut dyn Read, arena: &ArenaMut<Arena>) -> ResultSet {
        todo!()
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

pub struct ResultSet {
    arena: ArenaRef<Arena>, // for result tuple
    current: Option<Tuple>,
    pub status: Status,
    fetched_rows: u64,
    affected_rows: u64,
    returning_id: Option<i64>,
    plan_root: ArenaBox<dyn PhysicalPlanOps>,
}

impl ResultSet {
    fn from_dcl_stmt(mut plan_root: ArenaBox<dyn PhysicalPlanOps>) -> Result<Self> {
        plan_root.prepare()?;
        Ok(Self {
            arena: Arena::new_ref(),
            current: None,
            status: Status::Ok,
            fetched_rows: 0,
            affected_rows: 0,
            returning_id: None,
            plan_root,
        })
    }

    pub fn next(&mut self) -> bool {
        self.current = None;

        if self.arena.rss_in_bytes >= 10 * config::MB {
            self.arena = Arena::new_ref();
        }

        let mut feedback = FeedbackImpl { status: Status::Ok };
        self.current = self.plan_root.next(&mut feedback, &self.arena);
        self.status = feedback.status;

        match &self.current {
            Some(_) => {
                self.fetched_rows += 1;
                true
            }
            None => false
        }
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        self.plan_root.finalize()
    }
}

struct FeedbackImpl {
    status: Status
}

impl Feedback for FeedbackImpl {
    fn catch_error(&mut self, status: Status) {
        self.status = status;
    }
}