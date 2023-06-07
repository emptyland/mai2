use std::cell::RefCell;
use std::io::Read;
use std::ops::Deref;
use std::sync::Weak;
use serde_yaml::Value;
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaRef, ArenaVec};
use crate::exec::db::{ColumnType, DB};
use crate::exec::executor::{ColumnSet, Executor, PreparedStatement, Tuple};
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

    pub fn execute_query_str(&self, sql: &str, arena: &ArenaMut<Arena>) -> Result<ResultSet> {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        self.execute_query(&mut rd, arena)
    }

    pub fn execute_query(&self, reader: &mut dyn Read, arena: &ArenaMut<Arena>) -> Result<ResultSet> {
        self.executor.borrow_mut().execute_query(reader, arena)
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
    columns: ArenaBox<ColumnSet>,
    current: Option<Tuple>,
    pub status: Status,
    fetched_rows: u64,
    affected_rows: u64,
    returning_id: Option<i64>,
    plan_root: ArenaBox<dyn PhysicalPlanOps>,
}

impl ResultSet {
    pub fn from_dcl_stmt(mut plan_root: ArenaBox<dyn PhysicalPlanOps>) -> Result<Self> {
        let columns = plan_root.prepare()?;
        Ok(Self {
            arena: Arena::new_ref(),
            columns,
            current: None,
            status: Status::Ok,
            fetched_rows: 0,
            affected_rows: 0,
            returning_id: None,
            plan_root,
        })
    }

    pub fn columns(&self) -> &ColumnSet { self.columns.deref() }

    pub fn column_ty(&self, i: usize) -> &ColumnType { &self.columns().columns[i].ty }

    pub fn column_name(&self, i: usize) -> &str { self.columns().columns[i].name.as_str() }

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

    pub fn current(&self) -> Result<ResultRow> {
        if self.status != Status::Ok {
            return Err(self.status.clone());
        }
        match &self.current {
            Some(tuple) => Ok(ResultRow {
                tuple,
                owns: self
            }),
            None => Err(Status::NotFound)
        }
    }
}

pub struct ResultRow<'a> {
    tuple: &'a Tuple,
    owns: &'a ResultSet
}

impl <'a> ResultRow<'a> {
    pub fn columns(&self) -> &ColumnSet { self.tuple.columns() }

    pub fn column_ty(&self, i: usize) -> &ColumnType { &self.columns().columns[i].ty }

    pub fn get_null(&self, i: usize) -> bool { self.tuple.get_null(i) }
    pub fn get_i64(&self, i: usize) -> Option<i64> { self.tuple.get_i64(i) }
    pub fn get_str(&self, i: usize) -> Option<&str> { self.tuple.get_str(i) }
    pub fn get(&self, i: usize) -> &crate::exec::evaluator::Value { self.tuple.get(i) }

    pub fn to_string(&self) -> String { format!("{}", self.tuple) }
}


impl Drop for ResultSet {
    fn drop(&mut self) {
        self.plan_root.finalize()
    }
}

pub struct FeedbackImpl {
    pub status: Status
}

impl Feedback for FeedbackImpl {
    fn catch_error(&mut self, status: Status) {
        self.status = status;
    }
}