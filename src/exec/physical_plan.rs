use std::cell::Cell;
use std::collections::HashMap;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaRef, ArenaStr, ArenaVec};
use crate::exec::executor::{ColumnSet, PreparedStatement, Tuple, UpstreamContext};
use crate::{Result, Status, storage};
use crate::exec::db::{ColumnType, DB};
use crate::exec::evaluator::Evaluator;
use crate::sql::ast::Expression;
use crate::storage::{ColumnFamily, ReadOptions};

pub trait PhysicalPlanOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>>;

    fn finalize(&mut self) {
        for child in self.children().iter_mut() {
            child.finalize();
        }
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple>;
    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>>;
}

pub trait Feedback {
    fn catch_error(&mut self, status: Status);
}

pub struct RangeScanOps {
    projected_columns: ArenaBox<ColumnSet>,
    // (col_id, ty)
    pulled_rows: u64,
    limit: Option<u64>,
    offset: Option<u64>,
    range_begin: ArenaVec<u8>,
    left_close: bool,
    range_end: ArenaVec<u8>,
    right_close: bool,
    key_id: u64,
    eof: Cell<bool>,

    arena: ArenaMut<Arena>,
    iter: Option<storage::IteratorArc>,
    storage: Option<Arc<dyn storage::DB>>,
    cf: Option<Arc<dyn ColumnFamily>>,
    col_id_to_order: HashMap<u32, usize>,
}

impl RangeScanOps {
    pub fn new(projected_columns: ArenaBox<ColumnSet>,
               range_begin: ArenaVec<u8>,
               left_close: bool,
               range_end: ArenaVec<u8>,
               right_close: bool,
               limit: Option<u64>,
               offset: Option<u64>,
               arena: ArenaMut<Arena>,
               storage: Arc<dyn storage::DB>,
               cf: Arc<dyn ColumnFamily>) -> Self {
        let mut col_id_to_order = HashMap::new();
        for i in 0..projected_columns.columns.len() {
            let col = &projected_columns.columns[i];
            col_id_to_order.insert(col.id, i);
        }
        let key_id = DB::parse_key_id(&range_begin);
        Self {
            projected_columns,
            pulled_rows: 0,
            limit,
            offset,
            range_begin,
            left_close,
            range_end,
            right_close,
            key_id,
            eof: Cell::new(false),
            arena,
            iter: None,
            storage: Some(storage),
            cf: Some(cf),
            col_id_to_order,
        }
    }

    fn key_id_bytes(&self) -> &[u8] {
        &self.range_begin.as_slice()[..DB::KEY_ID_LEN]
    }

    fn is_primary_key_scanning(&self) -> bool { self.key_id == 0 }

    fn next_row<F>(&self, iter: &mut dyn storage::Iterator, mut each_col: F, arena: &ArenaMut<Arena>) -> Result<()>
        where F: FnMut(&[u8], &[u8]) {
        debug_assert!(iter.key().len() >= DB::KEY_ID_LEN + DB::COL_ID_LEN);
        debug_assert!(!self.eof.get());

        let key_ref = if self.is_primary_key_scanning() {
            &iter.key()[..iter.key().len() - DB::COL_ID_LEN]
        } else {
            iter.key()
        };
        if !key_ref.starts_with(self.key_id_bytes()) {
            return Err(Status::NotFound);
        }
        if key_ref.starts_with(&self.range_end) {
            self.eof.set(true);
            if !self.right_close {
                return Err(Status::NotFound);
            }
        }

        let mut row_key = ArenaVec::<u8>::new(arena);
        if self.key_id != 0 { // 2rd index
            let rd_opts = ReadOptions::default();
            let db = self.storage.as_ref().unwrap();

            row_key.write(iter.value()).unwrap();
            let prefix_len = row_key.len();

            for (col_id, _) in self.col_id_to_order.iter() {
                row_key.write(&col_id.to_be_bytes()).unwrap();
                let rs = db.get_pinnable(&rd_opts, self.cf.as_ref().unwrap(), &row_key);
                match rs {
                    Err(e) => if e.is_not_found() {
                        // ignore, it's null value
                    } else {
                        return Err(e);
                    }
                    Ok(pinning) => each_col(&row_key, pinning.value())
                }
                row_key.truncate(prefix_len);
            }
            iter.move_next();
        } else {
            row_key.write(key_ref).unwrap();

            debug_assert!(iter.valid());
            while iter.valid() && iter.key().starts_with(&row_key) {
                each_col(iter.key(), iter.value());
                iter.move_next();
            }
        }


        if iter.status().is_corruption() {
            Err(iter.status())
        } else {
            //self.pulled_rows += 1;
            Ok(())
        }
    }
}

impl PhysicalPlanOps for RangeScanOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let rd_opts = ReadOptions::default();
        self.iter = Some(self.storage.as_ref().unwrap().new_iterator(&rd_opts, self.cf.as_ref().unwrap())?);

        let iter_box = self.iter.as_ref().cloned().unwrap();
        let mut iter = iter_box.borrow_mut();
        iter.seek(&self.range_begin);
        if iter.status().is_corruption() {
            Err(iter.status())
        } else {
            // skip offset
            if let Some(offset) = self.offset {
                while self.pulled_rows < offset {
                    self.next_row(iter.deref_mut(), |_, _| {}, &mut self.arena.clone())?;
                    self.pulled_rows += 1;
                }
            }
            Ok(self.projected_columns.clone())
        }
    }

    fn finalize(&mut self) {
        self.iter = None;
        self.storage = None;
        self.col_id_to_order = HashMap::default();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.eof.get() {
            return None;
        }

        if let Some(limit) = self.limit {
            if self.pulled_rows >= limit {
                return None;
            }
        }

        let iter_box = self.iter.as_ref().cloned().unwrap();
        let mut iter = iter_box.borrow_mut();
        if !iter.valid() {
            if iter.status().is_corruption() {
                feedback.catch_error(iter.status());
            }
            return None;
        }

        let arena = zone.get_mut();
        let mut tuple = Tuple::with(&self.projected_columns, &arena);
        let rs = self.next_row(iter.deref_mut(), |key, v| {
            let (_, col_id) = DB::parse_key(key);
            if let Some(index) = self.col_id_to_order.get(&col_id) {
                let ty = &tuple.columns().columns[*index].ty;
                let value = DB::decode_column_value(ty, v, arena.get_mut());
                tuple.set(*index, value);
            }
        }, &arena);
        if let Err(e) = rs {
            if !e.is_not_found() {
                feedback.catch_error(e);
            }
            None
        } else {
            self.pulled_rows += 1;
            Some(tuple)
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::new(&self.arena)
    }
}

pub struct MergingOps {
    current: usize,
    children: ArenaVec<ArenaBox<dyn PhysicalPlanOps>>,
    pulled_rows: u64,
    delta_rows: u64,
    limit: Option<u64>,
    offset: Option<u64>,
}

impl MergingOps {
    pub fn new(children: ArenaVec<ArenaBox<dyn PhysicalPlanOps>>, limit: Option<u64>, offset: Option<u64>) -> Self {
        Self {
            current: 0,
            children,
            pulled_rows: 0,
            delta_rows: offset.map_or(0, |x| { x }),
            limit,
            offset,
        }
    }
}

impl PhysicalPlanOps for MergingOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let mut columns = None;
        for child in self.children.iter_mut() {
            columns = Some(child.prepare()?)
        }
        Ok(columns.unwrap())
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        while self.current < self.children.len() {
            match self.children[self.current].next(feedback, zone) {
                Some(row) => {
                    self.pulled_rows += 1;
                    if let Some(offset) = &self.offset {
                        if self.pulled_rows < *offset {
                            continue;
                        }
                    }
                    return Some(row);
                }
                None => {
                    self.current += 1;
                }
            }

            if let Some(limit) = &self.limit {
                if self.pulled_rows - self.delta_rows >= *limit {
                    break;
                }
            }
        }
        None
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        self.children.clone()
    }
}

pub struct FilteringOps {
    expr: ArenaBox<dyn Expression>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,

    context: Option<Arc<UpstreamContext>>,
}

impl FilteringOps {
    pub fn new(expr: &ArenaBox<dyn Expression>,
               child: &ArenaBox<dyn PhysicalPlanOps>,
               projected_columns: &ArenaBox<ColumnSet>,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>,
               arena: &ArenaMut<Arena>) -> Self {
        Self {
            expr: expr.clone(),
            child: child.clone(),
            arena: arena.clone(),
            projected_columns: projected_columns.clone(),
            prepared_stmt,
            context: None,
        }
    }
}

impl PhysicalPlanOps for FilteringOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let mut context = UpstreamContext::new(self.prepared_stmt.clone(), &self.arena);
        context.add(self.projected_columns.deref());
        self.context = Some(Arc::new(context));
        self.child.prepare()?;
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.context = None;
        self.child.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let mut arena = zone.get_mut();

        loop {
            let prev = self.child.next(feedback, zone);
            if prev.is_none() {
                break None;
            }
            let tuple = prev.unwrap();
            let mut evaluator = Evaluator::new(&mut arena);

            let context = self.context.as_ref().unwrap().clone();
            context.attach(&tuple);

            let rs = evaluator.evaluate(self.expr.deref_mut(), context);
            match rs {
                Err(e) => {
                    feedback.catch_error(e);
                    break None;
                }
                Ok(value) => if Evaluator::normalize_to_bool(&value) {
                    break Some(tuple);
                }
            }
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::of(self.child.clone(), &self.arena)
    }
}

pub struct ProjectingOps {
    columns: ArenaVec<ArenaBox<dyn Expression>>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,

    context: Option<Arc<UpstreamContext>>,
}

impl ProjectingOps {
    pub fn new(columns: ArenaVec<ArenaBox<dyn Expression>>,
               child: ArenaBox<dyn PhysicalPlanOps>,
               arena: ArenaMut<Arena>,
               projected_columns: ArenaBox<ColumnSet>,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>) -> Self {
        Self {
            columns,
            child,
            arena,
            projected_columns,
            prepared_stmt,
            context: None,
        }
    }
}

impl PhysicalPlanOps for ProjectingOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let mut context = UpstreamContext::new(self.prepared_stmt.clone(),
                                               &self.arena);
        context.add(self.projected_columns.deref());
        self.context = Some(Arc::new(context));
        self.child.prepare()?;
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.context = None;
        self.child.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let mut arena = zone.get_mut();

        match self.child.next(feedback, zone) {
            Some(tuple) => {
                let mut evaluator = Evaluator::new(&mut arena);

                let context = self.context.as_ref().unwrap();
                context.attach(&tuple);

                let mut rv = Tuple::with(&self.projected_columns, &arena);
                for i in 0..self.projected_columns.columns.len() {
                    let expr = &mut self.columns[i];
                    let rs = evaluator.evaluate(expr.deref_mut(),
                                                context.clone());
                    match rs {
                        Ok(value) => rv.set(i, value),
                        Err(e) => {
                            feedback.catch_error(e);
                            return None;
                        }
                    }
                }
                Some(rv)
            }
            None => None
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::of(self.child.clone(), &self.arena)
    }
}

pub struct ReturningOneDummyOps {
    projected_columns: ArenaBox<ColumnSet>,
    returning_count: usize,
    arena: ArenaMut<Arena>,
}

impl ReturningOneDummyOps {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        let mut columns = ColumnSet::new("", arena);
        columns.append_with_name("$$", ColumnType::TinyInt(1));

        Self {
            projected_columns: ArenaBox::new(columns, arena.get_mut()),
            returning_count: 0,
            arena: arena.clone(),
        }
    }
}

impl PhysicalPlanOps for ReturningOneDummyOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        Ok(self.projected_columns.clone())
    }

    fn next(&mut self, _feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.returning_count > 0 {
            None
        } else {
            let arena = zone.get_mut();
            self.returning_count += 1;
            Some(Tuple::with(&self.projected_columns, &arena))
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::new(&self.arena)
    }
}


pub struct RenamingOps {
    schema_as: ArenaStr,
    columns_as: ArenaVec<ArenaStr>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: Option<ArenaBox<ColumnSet>>,
}

impl PhysicalPlanOps for RenamingOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let origin = self.child.prepare()?;
        let mut cols = ColumnSet::new(self.schema_as.as_str(), &self.arena);
        debug_assert_eq!(origin.columns.len(), self.columns_as.len());
        for i in 0..origin.columns.len() {
            let col = &origin.columns[i];
            let name = if self.columns_as[i].is_empty() {
                col.name.as_str()
            } else {
                self.columns_as[i].as_str()
            };
            cols.append(name, col.desc.as_str(), col.id, col.ty.clone());
        }
        self.projected_columns = Some(ArenaBox::new(cols, self.arena.get_mut()));
        Ok(self.projected_columns.clone().unwrap())
    }

    fn next(&mut self, feedback: &mut dyn Feedback, arena: &ArenaRef<Arena>) -> Option<Tuple> {
        match self.child.next(feedback, arena) {
            Some(mut tuple) => {
                tuple.rename(self.projected_columns.as_ref().unwrap());
                Some(tuple)
            }
            _ => None
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::of(self.child.clone(), &self.arena)
    }
}

impl<T: PhysicalPlanOps + 'static> From<ArenaBox<T>> for ArenaBox<dyn PhysicalPlanOps> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::Arena;
    use crate::exec::connection::FeedbackImpl;
    use crate::exec::db::ColumnType;
    use crate::exec::evaluator::Value;
    use crate::storage;
    use crate::storage::{JunkFilesCleaner, open_kv_storage, Options, ReadOptions, WriteOptions};

    #[test]
    fn range_scan_plan() -> Result<()> {
        const N: i64 = 100;

        let _junk = JunkFilesCleaner::new("tests/db200");
        let db = open_test_db("db200")?;

        let zone = Arena::new_ref();
        let arena = zone.get_mut();
        mock_int_primary_key(&db, N, &arena)?;

        let ty = ColumnType::Int(11);
        let mut begin_key = ArenaVec::new(&arena);
        DB::encode_idx_id(0, &mut begin_key);
        DB::encode_row_key(&Value::Int(0), &ty, &mut begin_key)?;

        let mut end_key = ArenaVec::new(&arena);
        DB::encode_idx_id(0, &mut end_key);
        DB::encode_row_key(&Value::Int(N), &ty, &mut end_key)?;

        let mut columns = ArenaBox::new(ColumnSet::new("t1", &arena), arena.get_mut());
        columns.append("id", "", 1, ColumnType::Int(11));
        columns.append("name", "", 2, ColumnType::Varchar(64));

        let mut plan = RangeScanOps::new(columns,
                                         begin_key, true,
                                         end_key, true, None, None,
                                         arena.clone(), db.clone(),
                                         db.default_column_family().clone());
        plan.prepare()?;
        let mut feedback = FeedbackImpl { status: Status::Ok };
        let mut i = 0i64;
        loop {
            let rs = plan.next(&mut feedback, &zone);
            if rs.is_none() {
                break;
            }
            let tuple = rs.unwrap();
            assert_eq!(Some(i), tuple.get_i64(0));
            assert_eq!(Some("ok"), tuple.get_str(1));
            i += 1;
        }
        plan.finalize();
        assert_eq!(N, i);
        Ok(())
    }

    fn mock_int_primary_key(db: &Arc<dyn storage::DB>, max: i64, arena: &ArenaMut<Arena>) -> Result<()> {
        let mut key = ArenaVec::<u8>::new(&arena);
        let wr_opts = WriteOptions::default();
        let cf = db.default_column_family();
        let ty = ColumnType::Int(11);
        for i in 0..max {
            key.clear();
            DB::encode_idx_id(0, &mut key);
            DB::encode_row_key(&Value::Int(i), &ty, &mut key)?;

            let prefix_len = key.len();
            DB::encode_col_id(1, &mut key);
            db.insert(&wr_opts, &cf, &key, &i.to_le_bytes())?;

            key.truncate(prefix_len);
            DB::encode_col_id(2, &mut key);
            db.insert(&wr_opts, &cf, &key, "ok".as_bytes())?;
        }
        Ok(())
    }

    fn open_test_db(name: &str) -> Result<Arc<dyn storage::DB>> {
        let options = Options::with()
            .create_if_missing(true)
            .dir(String::from("tests"))
            .build();
        let (db, _cfs) = open_kv_storage(options, String::from(name), &Vec::new())?;
        Ok(db)
    }
}