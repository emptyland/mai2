use std::cell::{Cell, Ref, RefCell};
use std::collections::HashMap;
use std::io::Write;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::{arena_vec, corrupted_err, Corrupting, Result, Status, storage, zone_limit_guard, zone_limit_guard_on};
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaRef, ArenaStr, ArenaVec};
use crate::exec::connection::FeedbackImpl;
use crate::exec::db::{ColumnType, DB};
use crate::exec::evaluator::{Evaluator, Value};
use crate::exec::executor::{ColumnSet, PreparedStatement, Tuple, UpstreamContext};
use crate::exec::function::{Aggregator, Writable};
use crate::exec::interpreter::{BytecodeBuildingVisitor, BytecodeVector, Interpreter};
use crate::sql::ast::{Expression, JoinOp, SqlOrdering};
use crate::storage::{ColumnFamily, ColumnFamilyOptions, config, IteratorArc, ReadOptions, WriteOptions};

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
    fn need_row_key(&self) -> bool { false }
}

// TODO:
pub struct RowsGettingOps {
    projected_columns: ArenaBox<ColumnSet>,
    keys: ArenaVec<ArenaVec<u8>>,
    arena: ArenaMut<Arena>,
    // key_id: u64,
    current: usize,

    storage: Option<Arc<dyn storage::DB>>,
    snapshot: Option<Arc<dyn storage::Snapshot>>,
    iter: Option<IteratorArc>,
    cf: Option<Arc<dyn ColumnFamily>>,
}

impl RowsGettingOps {
    pub fn new(projected_columns: ArenaBox<ColumnSet>,
               keys: ArenaVec<ArenaVec<u8>>,
               arena: ArenaMut<Arena>,
               storage: Arc<dyn storage::DB>,
               snapshot: Arc<dyn storage::Snapshot>,
               cf: Arc<dyn ColumnFamily>) -> Self {
        Self {
            projected_columns,
            keys,
            arena,
            current: 0,
            storage: Some(storage),
            snapshot: Some(snapshot),
            iter: None,
            cf: Some(cf),
        }
    }

    fn get_row_by_pk(&self, storage: &Arc<dyn storage::DB>, cf: &Arc<dyn ColumnFamily>, rd_ops: &ReadOptions,
                     row_key: &[u8], arena: &ArenaMut<Arena>) -> Result<Tuple> {
        let mut tuple =
            Tuple::with_filling(&self.projected_columns, |_| { Value::Null }, arena.get_mut());

        let mut key = ArenaVec::with_data(arena, row_key);
        for i in 0..self.projected_columns.len() {
            let col = &self.projected_columns[i];
            DB::encode_col_id(col.id, &mut key);

            match storage.get_pinnable(rd_ops, cf, &key) {
                Err(e) => {
                    if e.is_not_found() {
                        continue;
                    }
                    return Err(e);
                }
                Ok(value) =>
                    tuple.set(i, DB::decode_column_value(&col.ty, &value, arena.get_mut()))
            }

            key.truncate(row_key.len());
        }

        Ok(tuple)
    }

    fn next_key(&mut self, incremental: usize) -> Result<()> {
        self.current += incremental;
        if self.current >= self.keys.len() {
            return Ok(());
        }

        let storage = self.storage.as_ref().unwrap();
        let cf = self.cf.as_ref().unwrap();
        let snapshot = self.snapshot.as_ref().unwrap();

        let key = &self.keys[self.current];
        let key_id = DB::decode_idx_id(key);
        if !DB::is_primary_key(key_id) {
            let mut iter = self.iter.as_ref().unwrap().borrow_mut();
            iter.seek(&key);
            if iter.status().is_not_ok() && !iter.status().is_not_found() {
                Err(iter.status().clone())?;
            }
        }
        Ok(())
    }
}

impl PhysicalPlanOps for RowsGettingOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let storage = self.storage.as_ref().unwrap();
        let cf = self.cf.as_ref().unwrap();
        let snapshot = self.snapshot.as_ref().unwrap();

        let rd_opts = ReadOptions::with()
            .snapshot(snapshot.clone())
            .build();
        let iter = storage.new_iterator(&rd_opts, &cf)?;
        iter.borrow_mut().seek_to_first();

        self.current = 0;
        self.iter = Some(iter);

        self.next_key(0)?;
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.storage = None;
        self.snapshot = None;
        self.iter = None;
        self.cf = None;
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.current >= self.keys.len() {
            return None;
        }
        let arena = zone.get_mut();

        let storage = self.storage.clone().unwrap();
        let snapshot = self.snapshot.as_ref().unwrap();
        let cf = self.cf.clone().unwrap();

        let rd_opts = ReadOptions::with()
            .snapshot(snapshot.clone())
            .build();

        let mut row_key = arena_vec!(&arena);
        let rs = loop {
            row_key.clear();

            if self.current >= self.keys.len() {
                break Err(Status::NotFound);
            }
            let key = &self.keys[self.current];
            let key_id = DB::decode_idx_id(key);

            if !DB::is_primary_key(key_id) {
                let iter_box = self.iter.clone().unwrap();
                let mut iter = iter_box.borrow_mut();
                if !iter.valid() {
                    if let Err(s) = self.next_key(1) {
                        feedback.catch_error(s.clone());
                        break Err(s);
                    }
                    continue;
                }

                let pk = DB::decode_row_key_from_secondary_index(iter.key(), iter.value());
                let rs = self.get_row_by_pk(&storage, &cf, &rd_opts, &key, &arena);
                if rs.is_ok() && feedback.need_row_key() {
                    row_key.write(&pk).unwrap();
                }
                iter.move_next();
                break rs;
            } else {
                let rs = self.get_row_by_pk(&storage, &cf, &rd_opts, &key, &arena);
                if rs.is_ok() && feedback.need_row_key() {
                    row_key.write(&key).unwrap();
                }
                if let Err(s) = self.next_key(1) {
                    feedback.catch_error(s.clone());
                    break Err(s);
                }
                break rs;
            }
        };

        match rs {
            Err(e) => {
                if !e.is_not_found() {
                    feedback.catch_error(e);
                }
                None
            }
            Ok(mut tuple) => {
                if feedback.need_row_key() {
                    tuple.attach_row_key(row_key);
                }
                Some(tuple)
            }
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        arena_vec!(&self.arena)
    }
}

pub struct RangeScanningOps {
    projected_columns: ArenaBox<ColumnSet>,
    // (col_id, ty)
    limit: LimitingState,
    range_begin: ArenaVec<u8>,
    left_close: bool,
    range_end: ArenaVec<u8>,
    right_close: bool,
    key_id: u64,
    eof: Cell<bool>,
    arena: ArenaMut<Arena>,

    iter: Option<IteratorArc>,
    storage: Option<Arc<dyn storage::DB>>,
    snapshot: Option<Arc<dyn storage::Snapshot>>,
    cf: Option<Arc<dyn ColumnFamily>>,
    col_id_to_order: HashMap<u32, usize>,
}

impl RangeScanningOps {
    pub fn new(projected_columns: ArenaBox<ColumnSet>,
               range_begin: ArenaVec<u8>,
               left_close: bool,
               range_end: ArenaVec<u8>,
               right_close: bool,
               limit: Option<u64>,
               offset: Option<u64>,
               arena: ArenaMut<Arena>,
               storage: Arc<dyn storage::DB>,
               snapshot: Arc<dyn storage::Snapshot>,
               cf: Arc<dyn ColumnFamily>) -> Self {
        let mut col_id_to_order = HashMap::new();
        for i in 0..projected_columns.columns.len() {
            let col = &projected_columns.columns[i];
            col_id_to_order.insert(col.id, i);
        }
        debug_assert_eq!(projected_columns.len(), col_id_to_order.len());
        let key_id = DB::parse_key_id(&range_begin);
        Self {
            projected_columns,
            limit: LimitingState::new(limit, offset),
            range_begin,
            left_close,
            range_end,
            right_close,
            key_id,
            eof: Cell::new(false),
            arena,
            iter: None,
            storage: Some(storage),
            snapshot: Some(snapshot),
            cf: Some(cf),
            col_id_to_order,
        }
    }

    fn key_id_bytes(&self) -> &[u8] {
        &self.range_begin.as_slice()[..DB::KEY_ID_LEN]
    }

    fn is_primary_key_scanning(&self) -> bool { DB::is_primary_key(self.key_id) }

    fn get_index_key<'a>(&self, key: &'a [u8], pack_info: &[u8]) -> &'a [u8] {
        if self.is_primary_key_scanning() {
            &key[..key.len() - DB::COL_ID_LEN]
        } else {
            DB::decode_index_from_secondary_index(key, pack_info)
        }
    }

    fn next_row<F>(&self, iter: &mut dyn storage::Iterator, mut each_col: F, arena: &ArenaMut<Arena>) -> Result<()>
        where F: FnMut(&[u8], &[u8]) {
        debug_assert!(iter.key().len() >= DB::KEY_ID_LEN + DB::COL_ID_LEN);
        debug_assert!(!self.eof.get());

        if !iter.key().starts_with(self.key_id_bytes()) {
            return Err(Status::NotFound);
        }

        let key_ref = self.get_index_key(iter.key(), iter.value());
        if key_ref == self.range_end.as_slice() {
            self.eof.set(true);
            if !self.right_close {
                return Err(Status::NotFound);
            }
        }

        let mut row_key = ArenaVec::<u8>::new(arena);
        if !self.is_primary_key_scanning() { // secondary index
            let rd_opts = ReadOptions::default();
            let db = self.storage.as_ref().unwrap();

            row_key.write(DB::decode_row_key_from_secondary_index(iter.key(), iter.value())).unwrap();
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

impl PhysicalPlanOps for RangeScanningOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let rd_opts = ReadOptions::with()
            .option_snapshot(self.snapshot.clone())
            .build();

        self.iter = Some(self.storage.as_ref().unwrap().new_iterator(&rd_opts, self.cf.as_ref().unwrap())?);

        let iter_box = self.iter.as_ref().cloned().unwrap();
        let mut iter = iter_box.borrow_mut();
        iter.seek(&self.range_begin);
        if !iter.valid() {
            return if iter.status().is_corruption() {
                Err(iter.status())
            } else {
                Ok(self.projected_columns.clone())
            };
        }

        let key = self.get_index_key(iter.key(), iter.value());
        if key == self.range_begin.as_slice() {
            if !self.left_close {
                iter.move_next();
            }
        }

        if self.limit.offset.is_some() {
            while self.limit.apply_offset_should_skip(1) {
                self.next_row(iter.deref_mut(), |_, _| {}, &mut self.arena.clone())?;
            }
        }
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.iter = None;
        self.storage = None;
        self.cf = None;
        self.snapshot = None;
        self.col_id_to_order = HashMap::default();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.eof.get() {
            return None;
        }

        if self.limit.exceeding() {
            return None;
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
        //let mut tuple = Tuple::with(&self.projected_columns, &arena);
        let mut tuple = Tuple::with_filling(&self.projected_columns, |x| { Value::Null }, arena.get_mut());
        let rs = self.next_row(iter.deref_mut(), |key, v| {
            if feedback.need_row_key() && tuple.row_key().is_empty() {
                tuple.attach_row_key(ArenaVec::with_data(&arena, &key[..key.len() - DB::COL_ID_LEN]));
            }
            let (_, col_id) = DB::parse_row_key(key);
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
            #[cfg(test)]
            for i in 0..tuple.len() {
                debug_assert!(!tuple[i].is_undefined());
            }
            //self.pulled_rows += 1;
            assert!(!self.limit.apply_offset_should_skip(1));
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
    limit: LimitingState,
}

impl MergingOps {
    pub fn new(children: ArenaVec<ArenaBox<dyn PhysicalPlanOps>>, limit: Option<u64>, offset: Option<u64>) -> Self {
        Self {
            current: 0,
            children,
            limit: LimitingState::new(limit, offset),
            // pulled_rows: 0,
            // delta_rows: offset.map_or(0, |x| { x }),
            // limit,
            // offset,
        }
    }
}

impl PhysicalPlanOps for MergingOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let mut columns = None;
        for child in self.children.iter_mut() {
            columns = Some(child.prepare()?)
        }
        self.current = 0;
        self.limit.pulled_rows = 0;
        Ok(columns.unwrap())
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.limit.exceeding() {
            return None;
        }
        while self.current < self.children.len() {
            match self.children[self.current].next(feedback, zone) {
                Some(row) => {
                    // self.pulled_rows += 1;
                    // if let Some(offset) = &self.offset {
                    //     if self.pulled_rows < *offset {
                    //         continue;
                    //     }
                    // }
                    if self.limit.apply_offset_should_skip(1) {
                        continue;
                    }
                    return Some(row);
                }
                None => {
                    self.current += 1;
                }
            }
        }
        None
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        self.children.clone()
    }
}

pub struct DistinctOps {
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,
    rd_opts: ReadOptions,
    wr_opts: WriteOptions,

    storage: Option<Arc<dyn storage::DB>>,
    cf: Option<Arc<dyn ColumnFamily>>,
}

impl DistinctOps {
    pub fn new(projected_columns: ArenaBox<ColumnSet>,
               child: ArenaBox<dyn PhysicalPlanOps>,
               arena: ArenaMut<Arena>,
               storage: Arc<dyn storage::DB>) -> Self {
        Self {
            child,
            arena,
            projected_columns,
            rd_opts: Default::default(),
            wr_opts: Default::default(),
            storage: Some(storage),
            cf: None,
        }
    }

    fn drop_column_family_if_needed(&mut self) {
        match self.cf.as_ref() {
            Some(cf) =>
                self.storage.as_ref().unwrap().drop_column_family(cf.clone()).unwrap(),
            None => ()
        }
    }
}

impl PhysicalPlanOps for DistinctOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        self.child.prepare()?;
        self.drop_column_family_if_needed();
        let opts = ColumnFamilyOptions::with()
            .temporary(true)
            .build();
        let cf = self.storage.as_ref().unwrap()
            .new_column_family("distinct", opts)?;

        self.cf = Some(cf);
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.drop_column_family_if_needed();
        self.storage = None;
        self.cf = None;
        self.child.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let arena = zone.get_mut();
        let storage = self.storage.as_ref().unwrap();
        let cf = self.cf.as_ref().unwrap();
        let mut key = ArenaVec::new(&arena);
        loop {
            let rs = self.child.next(feedback, zone);
            if rs.is_none() {
                break None;
            }
            let tuple = rs.unwrap();
            key.clear();
            DB::encode_tuple(&tuple, &mut key);

            let rs = storage.get_pinnable(&self.rd_opts, cf, &key);
            if rs.is_ok() {
                continue;
            }
            let status = rs.unwrap_err();
            if status.is_not_found() {
                match storage.insert(&self.wr_opts, cf, &key, &[]) {
                    Ok(_) => (),
                    Err(e) => {
                        feedback.catch_error(e);
                        break None;
                    }
                }
                break Some(tuple);
            } else {
                feedback.catch_error(status);
                break None;
            }
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        arena_vec!(&self.arena, [self.child.clone()])
    }
}

pub struct FilteringOps {
    expr: BytecodeVector,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    interpreter: Interpreter,

    context: Option<Arc<UpstreamContext>>,
}

impl FilteringOps {
    pub fn new(expr: BytecodeVector,
               child: &ArenaBox<dyn PhysicalPlanOps>,
               projected_columns: &ArenaBox<ColumnSet>,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>,
               arena: &ArenaMut<Arena>) -> Self {
        Self {
            expr,
            child: child.clone(),
            arena: arena.clone(),
            projected_columns: projected_columns.clone(),
            prepared_stmt,
            interpreter: Interpreter::new(arena),
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
        //let mut arena = zone.get_mut();

        loop {
            let prev = self.child.next(feedback, zone);
            if prev.is_none() {
                break None;
            }
            let tuple = prev.unwrap();

            let context = self.context.as_ref().unwrap().clone();
            context.attach(&tuple);

            //let rs = evaluator.evaluate(self.expr.deref_mut(), context);
            let rs = self.interpreter.evaluate(&mut self.expr, context);
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
    columns: ArenaVec<BytecodeVector>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    interpreter: Interpreter,

    context: Option<Arc<UpstreamContext>>,
}

impl ProjectingOps {
    pub fn new(columns: ArenaVec<BytecodeVector>,
               child: ArenaBox<dyn PhysicalPlanOps>,
               arena: ArenaMut<Arena>,
               projected_columns: ArenaBox<ColumnSet>,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>) -> Self {
        debug_assert_eq!(columns.len(), projected_columns.len());
        Self {
            columns,
            child,
            interpreter: Interpreter::new(&arena),
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

        let cols = self.child.prepare()?;
        context.add(cols.deref());

        self.context = Some(Arc::new(context));
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.context = None;
        self.child.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let arena = zone.get_mut();

        match self.child.next(feedback, zone) {
            Some(tuple) => {
                //let mut evaluator = Evaluator::new(&mut arena);

                let context = self.context.as_ref().unwrap();
                context.attach(&tuple);

                let mut rv = Tuple::with(&self.projected_columns, &arena);
                for i in 0..self.projected_columns.len() {
                    let expr = &mut self.columns[i];
                    // let rs = evaluator.evaluate(expr.deref_mut(),
                    //                             context.clone());
                    let rs = self.interpreter.evaluate(expr, context.clone());
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
        let mut columns = ColumnSet::new("", 0, arena);
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
        let mut cols = ColumnSet::new(self.schema_as.as_str(), origin.tid, &self.arena);
        debug_assert_eq!(origin.columns.len(), self.columns_as.len());
        for i in 0..origin.columns.len() {
            let col = &origin.columns[i];
            let name = if self.columns_as[i].is_empty() {
                col.name.as_str()
            } else {
                self.columns_as[i].as_str()
            };
            cols.append(name, col.desc.as_str(), col.original_tid, col.id, col.ty.clone());
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

pub struct AggregatorBundle {
    acc: ArenaBox<dyn Aggregator>,
    args: ArenaVec<BytecodeVector>,
    args_expr: ArenaVec<ArenaBox<dyn Expression>>,
    rets: ArenaBox<dyn Writable>,
    rets_ty: Option<ColumnType>,
    bufs: ArenaVec<ArenaBox<dyn Writable>>,
}

impl AggregatorBundle {
    pub fn new(acc: &ArenaBox<dyn Aggregator>,
               args_expr: &[ArenaBox<dyn Expression>],
               arena: &ArenaMut<Arena>) -> Self {
        Self {
            acc: acc.clone(),
            args: BytecodeBuildingVisitor::build_arena_boxes(args_expr, arena),
            args_expr: ArenaVec::with_data(arena, args_expr),
            rets: acc.new_buf(arena),
            rets_ty: None,
            bufs: ArenaVec::with_init(arena, |_| { acc.new_buf(&arena) }, args_expr.len()),
        }
    }

    pub fn returning_ty(&self) -> ColumnType { self.rets_ty.clone().unwrap() }

    pub fn reduce_returning_ty(&mut self, params: &[ColumnType]) {
        self.rets_ty = Some(self.acc.signature(params))
    }

    pub fn args_len(&self) -> usize { self.args.len() }

    pub fn args_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] { &mut self.args_expr }
}

pub struct GroupingAggregatorOps {
    group_keys: ArenaVec<BytecodeVector>,
    aggregators: ArenaVec<AggregatorBundle>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,
    upstream_columns: Option<ArenaBox<ColumnSet>>,
    eof: bool,
    current_key: ArenaVec<u8>,
    interpreter: Interpreter,

    context: Option<Arc<UpstreamContext>>,
    storage: Option<Arc<dyn storage::DB>>,
    cf: Option<Arc<dyn ColumnFamily>>,
    iter: Option<IteratorArc>,
}

impl GroupingAggregatorOps {
    pub fn new(group_keys: ArenaVec<BytecodeVector>,
               aggregators: ArenaVec<AggregatorBundle>,
               child: ArenaBox<dyn PhysicalPlanOps>,
               arena: ArenaMut<Arena>,
               projected_columns: ArenaBox<ColumnSet>,
               storage: Arc<dyn storage::DB>) -> Self {
        let current_key = ArenaVec::new(&arena);
        Self {
            group_keys,
            aggregators,
            child,
            interpreter: Interpreter::new(&arena),
            arena,
            projected_columns,
            upstream_columns: None,
            eof: false,
            current_key,

            context: None,
            storage: Some(storage),
            cf: None,
            iter: None,
        }
    }

    fn group_by_keys(&mut self, cf: &Arc<dyn ColumnFamily>,
                     upstream: &mut dyn PhysicalPlanOps) -> Result<u64> {
        let mut count = 0u64;
        let context = self.context.as_ref().unwrap();

        let mut zone = Arena::new_ref();
        let arena = zone.get_mut();
        let mut feedback = FeedbackImpl::default();

        //let mut evaluator = Evaluator::new(&arena);
        let wr_opts = WriteOptions::default();
        loop {
            if zone.rss_in_bytes > 10 * config::MB {
                zone = Arena::new_ref();
            }

            match upstream.next(&mut feedback, &zone) {
                Some(tuple) => {
                    let mut key = ArenaVec::new(&arena);
                    let mut row = ArenaVec::new(&arena);
                    context.attach(&tuple);
                    for expr in self.group_keys.iter_mut() {
                        // let mut expr = expr_box.clone();
                        // let value = evaluator.evaluate(expr.deref_mut(), context.clone())?;
                        let value = self.interpreter.evaluate(expr, context.clone())?;
                        DB::encode_sort_key(&value, &mut key);
                    }
                    key.write(&count.to_be_bytes()).unwrap();

                    DB::encode_tuple(&tuple, &mut row);
                    self.storage().insert(&wr_opts, cf, &key, &row)?;
                }
                None => break
            }
            count += 1;
        }
        Ok(count)
    }

    fn aggregators_iterate(&mut self, tuple: &Tuple, context: &Arc<UpstreamContext>,
                           zone: &ArenaRef<Arena>) -> Result<()> {
        context.attach(tuple);
        // let arena = zone.get_mut();
        // let mut evaluator = Evaluator::new(&arena);

        debug_assert!(self.aggregators.len() <= self.projected_columns.len());
        for agg in self.aggregators.iter_mut() {
            for i in 0..agg.args.len() {
                //let mut expr = agg.args[i].clone();
                //let value = evaluator.evaluate(expr.deref_mut(), context.clone())?;
                let value = self.interpreter.evaluate(&mut agg.args[i], context.clone())?;
                agg.bufs[i].recv(&value);
            }
            agg.acc.iterate(agg.rets.deref_mut(), &agg.bufs)?;
        }
        Ok(())
    }

    fn aggregators_terminate(&mut self, zone: &ArenaRef<Arena>, group_key: Option<Tuple>) -> Result<Tuple> {
        let arena = zone.get_mut();
        let mut tuple = Tuple::with_filling(&self.projected_columns, |_| { Value::Null }, arena.get_mut());

        debug_assert!(self.aggregators.len() <= self.projected_columns.len());
        for i in 0..self.aggregators.len() {
            let agg = &mut self.aggregators[i];
            let value = agg.acc.terminate(agg.rets.deref_mut())?;
            tuple.set(i, value);
            agg.rets.clear();
        }
        if group_key.is_none() {
            return Ok(tuple);
        }

        let input = group_key.unwrap();
        for i in self.aggregators.len()..self.projected_columns.len() {
            let dest_col = &self.projected_columns[i];
            let rs = input.columns().index_by_name(dest_col.desc.as_str(),
                                                   dest_col.name.as_str());
            match rs {
                Some(col) => tuple.set(i, input.get(col).dup(&arena)),
                None => return corrupted_err!("Column not found: {}", dest_col.name)
            }
        }
        Ok(tuple)
    }

    fn overwrite_current_key(&mut self, key: &[u8]) {
        self.current_key.clear();
        self.current_key.write(&key[..key.len() - size_of::<u64>()]).unwrap();
    }

    fn storage(&self) -> &dyn storage::DB {
        self.storage.as_ref().unwrap().as_ref()
    }
}

// select a, count(*) from t1 group by a;
impl PhysicalPlanOps for GroupingAggregatorOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let upstream_cols = self.child.prepare()?;
        let mut context = UpstreamContext::new(None, &self.arena);
        context.add(upstream_cols.deref());
        self.context = Some(Arc::new(context));

        if !self.group_keys.is_empty() {
            let opts = ColumnFamilyOptions::with()
                .temporary(true)
                .build();
            let cf = self.storage().new_column_family("grouping", opts)?;
            let mut upstream = self.child.clone();
            self.group_by_keys(&cf, upstream.deref_mut())?;

            let rd_opts = ReadOptions::default();
            let iter_box = self.storage().new_iterator(&rd_opts, &cf)?;
            let mut iter = iter_box.borrow_mut();
            iter.seek_to_first();
            if iter.status().is_not_ok() {
                return Err(iter.status().clone());
            }
            if iter.valid() {
                self.overwrite_current_key(iter.key());
            }

            drop(iter);
            self.iter = Some(iter_box);
            self.cf = Some(cf);
        }

        self.eof = false;
        self.upstream_columns = Some(upstream_cols);
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        // drop the temp table
        match &self.cf {
            Some(cf) => self.storage().drop_column_family(cf.clone()).unwrap(),
            None => (),
        }
        self.context = None;
        self.cf = None;
        self.iter = None;
        self.storage = None;
        self.child.finalize();
    }

    #[allow(unused_assignments)]
    fn next(&mut self, _feedback: &mut dyn Feedback, _zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let context = self.context.as_ref().cloned().unwrap();

        if self.group_keys.is_empty() {
            if self.eof {
                return None;
            }

            let mut zone = Arena::new_ref();
            let mut group_tuple = Option::<Tuple>::None;

            debug_assert!(self.aggregators.len() <= self.projected_columns.len());
            loop {
                zone_limit_guard_on!(zone, new_zone, 1, {
                    if let Some(gk) = &group_tuple {
                        group_tuple = Some(gk.dup(&new_zone.get_mut()));
                    }
                });
                match self.child.next(_feedback, &zone) {
                    Some(tuple) => match self.aggregators_iterate(&tuple, &context, &zone) {
                        Err(e) => {
                            _feedback.catch_error(e);
                            return None;
                        }
                        Ok(_) => group_tuple = Some(tuple)
                    }
                    None => {
                        self.eof = true;
                        break;
                    }
                } // match
            } // loop
            match self.aggregators_terminate(&_zone, group_tuple) {
                Err(e) => {
                    _feedback.catch_error(e);
                    None
                }
                Ok(tuple) => Some(tuple)
            }
        } else {
            let iter_box = self.iter.as_ref().cloned().unwrap();
            let mut iter = iter_box.borrow_mut();
            if !iter.valid() {
                return None;
            }

            let mut zone = Arena::new_ref();
            let mut group_tuple = Option::<Tuple>::None;
            while &iter.key()[..iter.key().len() - 8] == self.current_key.as_slice() {
                zone_limit_guard_on!(zone, new_zone, 1, {
                    if let Some(gk) = &group_tuple {
                        group_tuple = Some(gk.dup(&new_zone.get_mut()));
                    }
                });
                let arena = zone.get_mut();

                let tuple = DB::decode_tuple(self.upstream_columns.as_ref().unwrap(),
                                             iter.value(), &arena);
                match self.aggregators_iterate(&tuple, &context, &zone) {
                    Err(e) => {
                        _feedback.catch_error(e);
                        return None;
                    }
                    Ok(_) => ()
                }

                group_tuple = Some(tuple);
                iter.move_next();
                if !iter.valid() {
                    self.eof = true;
                    break;
                }
            }
            if iter.valid() {
                self.overwrite_current_key(iter.key());
            }
            if iter.status().is_not_ok() {
                _feedback.catch_error(iter.status().clone());
                return None;
            }

            match self.aggregators_terminate(&_zone, group_tuple) {
                Err(e) => {
                    _feedback.catch_error(e);
                    None
                }
                Ok(tuple) => Some(tuple)
            }
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::of(self.child.clone(), &self.arena)
    }
}

pub struct SimpleNestedLoopJoinOps {
    driver: ArenaBox<dyn PhysicalPlanOps>,
    matcher: ArenaBox<dyn PhysicalPlanOps>,
    matching: BytecodeVector,
    projected_columns: ArenaBox<ColumnSet>,
    arena: ArenaMut<Arena>,
    join_op: JoinOp,
    interpreter: Interpreter,

    context: Option<Arc<UpstreamContext>>,
}

impl SimpleNestedLoopJoinOps {
    pub fn new(driver: ArenaBox<dyn PhysicalPlanOps>,
               matcher: ArenaBox<dyn PhysicalPlanOps>,
               matching: BytecodeVector,
               projected_columns: ArenaBox<ColumnSet>,
               arena: ArenaMut<Arena>,
               join_op: JoinOp) -> Self {
        Self {
            driver,
            matcher,
            matching,
            projected_columns,
            interpreter: Interpreter::new(&arena),
            arena,
            join_op,

            context: None,
        }
    }

    fn match_left_outer(&mut self, drive: Tuple, inner: bool, owns: &ArenaMut<Arena>) -> Result<Tuple> {
        let mut zone = Arena::new_ref();
        let mut feedback = FeedbackImpl::default();
        let env = self.context.as_ref().unwrap();

        self.matcher.prepare()?;
        loop {
            zone_limit_guard!(zone, 1);
            let arena = zone.get_mut();

            let rs = self.matcher.next(&mut feedback, &zone);
            if feedback.status.is_not_ok() {
                break Err(feedback.status);
            }

            let mut tuple = Tuple::new(&self.projected_columns, arena.get_mut());
            for i in 0..drive.len() {
                tuple.set(i, drive[i].clone());
            }
            if rs.is_none() {
                if inner {
                    break Err(Status::NotFound);
                }
                for i in drive.len()..tuple.len() {
                    tuple.set(i, Value::Null);
                }
                break Ok(tuple.dup(owns));
            }
            let match_ = rs.unwrap();
            debug_assert_eq!(tuple.len(), drive.len() + match_.len());
            for i in drive.len()..tuple.len() {
                tuple.set(i, match_[i - drive.len()].clone());
            }

            //let mut evaluator = Evaluator::new(&arena);
            env.attach(&tuple);
            //let rv = evaluator.evaluate(self.matching.deref_mut(), env.clone())?;
            let rv = self.interpreter.evaluate(&mut self.matching, env.clone())?;
            if Evaluator::normalize_to_bool(&rv) {
                break Ok(Self::finalize_tuple(tuple, drive, match_, feedback.need_row_key(), owns));
            }
        }
    }

    fn match_right_outer(&mut self, match_: Tuple, owns: &ArenaMut<Arena>) -> Result<Tuple> {
        let mut zone = Arena::new_ref();
        let mut feedback = FeedbackImpl::default();
        let env = self.context.as_ref().unwrap();

        self.driver.prepare()?;
        loop {
            zone_limit_guard!(zone, 1);
            let arena = zone.get_mut();

            let rs = self.driver.next(&mut feedback, &zone);
            if feedback.status.is_not_ok() {
                break Err(feedback.status);
            }

            let mut tuple = Tuple::new(&self.projected_columns, arena.get_mut());
            for i in tuple.len() - match_.len()..tuple.len() {
                let d = tuple.len() - match_.len();
                tuple.set(i, match_.get(i - d).clone());
            }
            if rs.is_none() {
                for i in 0..tuple.len() - match_.len() {
                    tuple.set(i, Value::Null);
                }
                break Ok(tuple.dup(owns));
            }
            let drive = rs.unwrap();
            debug_assert_eq!(tuple.len(), drive.len() + match_.len());
            for i in 0..drive.len() {
                tuple.set(i, drive[i].clone());
            }

            //let mut evaluator = Evaluator::new(&arena);
            env.attach(&tuple);
            //let rv = evaluator.evaluate(self.matching.deref_mut(), env.clone())?;
            let rv = self.interpreter.evaluate(&mut self.matching, env.clone())?;
            if Evaluator::normalize_to_bool(&rv) {
                break Ok(Self::finalize_tuple(tuple, drive, match_, feedback.need_row_key(), owns));
            }
        }
    }

    fn finalize_tuple(tuple: Tuple, this: Tuple, that: Tuple, need_row_key: bool, arena: &ArenaMut<Arena>) -> Tuple {
        let mut row = tuple.dup(arena);
        if need_row_key {
            let mut multi_row_key = arena_vec!(arena);
            DB::encode_multi_row_key(&this, &mut multi_row_key);
            DB::encode_multi_row_key(&that, &mut multi_row_key);
            row.attach_row_key(multi_row_key);
        }
        row
    }
}

impl PhysicalPlanOps for SimpleNestedLoopJoinOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let mut env = UpstreamContext::new(None, &self.arena);
        self.driver.prepare()?;
        self.matcher.prepare()?;
        env.add(self.projected_columns.deref());

        self.context = Some(Arc::new(env));
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.context = None;
        self.driver.finalize();
        self.matcher.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let arena = zone.get_mut();
        loop {
            let rs = if matches!(self.join_op, JoinOp::RightOuterJoin) {
                self.matcher.next(feedback, &zone)
            } else {
                self.driver.next(feedback, &zone)
            };
            if rs.is_none() {
                break None;
            }

            let rs = match &self.join_op {
                JoinOp::LeftOuterJoin =>
                    self.match_left_outer(rs.unwrap(), false, &arena),
                JoinOp::RightOuterJoin =>
                    self.match_right_outer(rs.unwrap(), &arena),
                JoinOp::InnerJoin | JoinOp::CrossJoin =>
                    self.match_left_outer(rs.unwrap(), true, &arena)
            };

            match rs {
                Err(e) => {
                    if !e.is_not_found() {
                        feedback.catch_error(e);
                        break None;
                    }
                }
                Ok(tuple) => break Some(tuple)
            }
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        arena_vec!(&self.arena, [self.driver.clone(), self.matcher.clone()])
    }
}

pub struct IndexNestedLoopJoinOps {
    driver: ArenaBox<dyn PhysicalPlanOps>,
    projected_columns: ArenaBox<ColumnSet>,
    arena: ArenaMut<Arena>,
    join_op: JoinOp,
    driving_vals: ArenaVec<u8>,
    driving_columns: ArenaBox<ColumnSet>,
    // expr -> ty
    matching_key_bundle: RefCell<ArenaVec<(BytecodeVector, ColumnType)>>,
    matching_columns: ArenaBox<ColumnSet>,
    matching_key_id: u64,
    matching_key: RefCell<ArenaVec<u8>>,
    need_produce_row_key: bool,
    //interpreter: Interpreter,

    storage: Option<Arc<dyn storage::DB>>,
    iter: Option<IteratorArc>,
    snapshot: Option<Arc<dyn storage::Snapshot>>,
    cf: Option<Arc<dyn ColumnFamily>>,
    context: Option<Arc<UpstreamContext>>,
}

impl IndexNestedLoopJoinOps {
    pub fn new(driver: ArenaBox<dyn PhysicalPlanOps>,
               projected_columns: ArenaBox<ColumnSet>,
               join_op: JoinOp,
               driving_columns: ArenaBox<ColumnSet>,
               matching_key_bundle: ArenaVec<(BytecodeVector, ColumnType)>,
               matching_columns: ArenaBox<ColumnSet>,
               matching_key_id: u64,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>,
               arena: ArenaMut<Arena>,
               storage: Arc<dyn storage::DB>,
               snapshot: Arc<dyn storage::Snapshot>,
               cf: Arc<dyn ColumnFamily>) -> Self {
        let mut env = UpstreamContext::new(prepared_stmt, &arena);
        env.add(projected_columns.deref());
        Self {
            driver,
            projected_columns,
            join_op,
            driving_vals: arena_vec!(&arena),
            driving_columns,
            matching_key_bundle: RefCell::new(matching_key_bundle),
            matching_columns,
            matching_key_id,
            matching_key: RefCell::new(arena_vec!(&arena)),
            need_produce_row_key: false,
            //interpreter: Interpreter::new(&arena),
            arena,
            storage: Some(storage),
            iter: None,
            snapshot: Some(snapshot),
            cf: Some(cf),
            context: Some(Arc::new(env)),
        }
    }

    fn next_driving_tuple(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.driving_vals.is_empty() || self.matching_key.borrow().is_empty() {
            self.next_driving_tuple_directly(feedback, zone)
        } else {
            let iter_box = self.iter.as_ref().unwrap().clone();
            let iter = iter_box.borrow();
            let arena = zone.get_mut();
            if iter.valid() && iter.key().starts_with(&self.matching_key.borrow()) {
                Some(DB::decode_tuple(&self.driving_columns, &self.driving_vals, &arena))
            } else {
                self.next_driving_tuple_directly(feedback, zone)
            }
        }
    }

    fn next_driving_tuple_directly(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let rs = self.driver.next(feedback, zone);
        if rs.is_none() {
            return None;
        }
        let drive = rs.unwrap();
        self.driving_vals.clear();
        DB::encode_tuple(&drive, &mut self.driving_vals);
        Some(drive)
    }

    fn match_tuple(&mut self, drive: &Tuple, arena: &ArenaMut<Arena>) -> Result<Tuple> {
        if DB::is_primary_key(self.matching_key_id) {
            self.match_tuple_from_pk(drive, arena)
        } else {
            self.match_tuple_from_k(drive, arena)
        }
    }

    fn match_tuple_from_pk(&mut self, drive: &Tuple, arena: &ArenaMut<Arena>) -> Result<Tuple> {
        let iter_box = self.iter.clone().unwrap();
        let mut iter = iter_box.borrow_mut(); // iter in primary key
        let key_prefix = self.matching_key.borrow();
        match self.next_matching_tuple_directly(&key_prefix, &self.matching_columns, iter.deref_mut(), arena) {
            Err(e) => {
                if !e.is_not_found() {
                    return Err(e);
                }
            }
            Ok(tuple) => return Ok(tuple)
        }
        drop(key_prefix);

        let key_prefix = self.build_matching_key(drive, arena)?;
        debug_assert!(!key_prefix.is_empty());

        iter.seek(&key_prefix);
        if iter.status().is_not_ok() {
            return Err(iter.status().clone());
        }
        if !iter.valid() {
            return Err(Status::NotFound);
        }
        self.next_matching_tuple_directly(&key_prefix, &self.matching_columns, iter.deref_mut(), arena)
    }

    fn match_tuple_from_k(&mut self, drive: &Tuple, arena: &ArenaMut<Arena>) -> Result<Tuple> {
        let iter_box = self.iter.as_ref().unwrap();
        let mut iter = iter_box.borrow_mut(); // iter in secondary key
        let key_prefix = self.matching_key.borrow();
        match self.next_matching_tuple_indirectly(&key_prefix, &self.matching_columns, iter.deref_mut(), arena) {
            Err(e) => {
                if !e.is_not_found() {
                    return Err(e);
                }
            }
            Ok(tuple) => return Ok(tuple)
        }
        drop(key_prefix);

        let key_prefix = self.build_matching_key(drive, arena)?;
        debug_assert!(!key_prefix.is_empty());

        iter.seek(&key_prefix);
        if iter.status().is_not_ok() {
            return Err(iter.status().clone());
        }
        if !iter.valid() {
            return Err(Status::NotFound);
        }
        self.next_matching_tuple_indirectly(&key_prefix, &self.matching_columns, iter.deref_mut(), arena)
    }

    fn build_matching_key(&self, drive: &Tuple, arena: &ArenaMut<Arena>) -> Result<Ref<ArenaVec<u8>>> {
        let mut key_buf = self.matching_key.borrow_mut();
        key_buf.clear();

        DB::encode_idx_id(self.matching_key_id, key_buf.deref_mut());

        let env = self.context.as_ref().unwrap();
        env.attach(drive);
        let mut interpreter = Interpreter::new(arena);
        for (expr, ty) in self.matching_key_bundle.borrow_mut().iter_mut() {
            let rv = interpreter.evaluate(expr, env.clone())?;

            if DB::is_primary_key(self.matching_key_id) {
                DB::encode_row_key(&rv, ty, key_buf.deref_mut())?;
            } else {
                DB::encode_secondary_index(&rv, ty, key_buf.deref_mut());
            }
        }
        drop(key_buf);
        Ok(self.matching_key.borrow())
    }

    fn next_matching_tuple_indirectly(&self, key_prefix: &[u8], columns: &ArenaBox<ColumnSet>,
                                      iter: &mut dyn storage::Iterator, arena: &ArenaMut<Arena>) -> Result<Tuple> {
        if key_prefix.is_empty() {
            return Err(Status::NotFound);
        }
        if iter.valid() && !iter.key().starts_with(key_prefix) {
            return Err(Status::NotFound);
        }

        let storage = self.storage.as_ref().unwrap();
        let cf = self.cf.as_ref().unwrap();
        let rd_opts = ReadOptions::with()
            .option_snapshot(self.snapshot.clone())
            .build();

        if iter.valid() && iter.key().starts_with(key_prefix) {
            let iter_pk_box = storage.new_iterator(&rd_opts, cf)?;
            let mut iter_pk = iter_pk_box.borrow_mut();
            let row_key = DB::decode_row_key_from_secondary_index(iter.key(), iter.value());
            iter_pk.seek(row_key);
            if !iter_pk.valid() {
                return Err(Status::NotFound);
            }
            if iter_pk.status().is_not_ok() {
                return Err(iter_pk.status().clone());
            }
            return match self.next_matching_tuple_directly(row_key, columns, iter_pk.deref_mut(), arena) {
                Err(e) => Err(e),
                Ok(tuple) => {
                    iter.move_next();
                    Ok(tuple)
                }
            };
        }
        Err(Status::NotFound)
    }

    fn next_matching_tuple_directly(&self, key_prefix: &[u8], columns: &ArenaBox<ColumnSet>,
                                    iter: &mut dyn storage::Iterator, arena: &ArenaMut<Arena>) -> Result<Tuple> {
        if key_prefix.is_empty() {
            return Err(Status::NotFound);
        }
        if iter.valid() && !iter.key().starts_with(key_prefix) {
            return Err(Status::NotFound);
        }
        let mut tuple = Tuple::with_filling(&columns, |_| { Value::Null }, arena.get_mut());

        let mut prev_row_key = arena_vec!(arena);
        while iter.valid() && iter.key().starts_with(key_prefix) {
            let row_key = &iter.key()[..iter.key().len() - DB::COL_ID_LEN];
            if prev_row_key.is_empty() {
                (&mut prev_row_key).write(row_key).unwrap();
                if self.need_produce_row_key {
                    tuple.attach_row_key(ArenaVec::with_data(arena, row_key));
                }
            }
            let (_, col_id) = DB::parse_row_key(iter.key());

            let i = columns.index_by_id(col_id).unwrap();
            let value = DB::decode_column_value(&columns[i].ty, iter.value(), arena.get_mut());
            tuple.set(i, value);

            if prev_row_key.as_slice() != row_key {
                // prev_row_key.clear();
                // (&mut prev_row_key).write(row_key).unwrap();
                // tuple.associate_row_key(&prev_row_key);
                return Ok(tuple);
            }
            iter.move_next();
        }
        if iter.status().is_not_ok() && !iter.status().is_not_found() {
            Err(iter.status().clone())
        } else {
            Ok(tuple)
        }
    }

    fn merge_tuples(&self, drive: Tuple, match_: Tuple, arena: &ArenaMut<Arena>) -> Tuple {
        let mut tuple = Tuple::new(&self.projected_columns, arena.get_mut());
        for i in 0..drive.len() {
            tuple.set(i, drive[i].dup(arena));
        }
        for i in drive.len()..tuple.len() {
            tuple.set(i, match_[i - drive.len()].dup(arena));
        }
        if self.need_produce_row_key {
            let mut multi_row_key = arena_vec!(arena);
            DB::encode_multi_row_key(&drive, &mut multi_row_key);
            DB::encode_multi_row_key(&match_, &mut multi_row_key);
            tuple.attach_row_key(multi_row_key);
        }
        tuple
    }

    fn merge_left_half(&self, drive: Tuple, arena: &ArenaMut<Arena>) -> Tuple {
        let mut tuple = Tuple::new(&self.projected_columns, arena.get_mut());
        debug_assert!(drive.len() < tuple.len());
        for i in 0..drive.len() {
            tuple.set(i, drive[i].dup(arena));
        }
        for i in drive.len()..tuple.len() {
            tuple.set(i, Value::Null);
        }
        if self.need_produce_row_key {
            let mut multi_row_key = arena_vec!(arena);
            DB::encode_multi_row_key(&drive, &mut multi_row_key);
            tuple.attach_row_key(multi_row_key);
        }
        tuple
    }

    fn merge_right_half(&self, drive: Tuple, arena: &ArenaMut<Arena>) -> Tuple {
        let mut tuple = Tuple::new(&self.projected_columns, arena.get_mut());
        debug_assert!(drive.len() < tuple.len());

        let match_len = tuple.len() - drive.len();
        for i in 0..match_len {
            tuple.set(i, Value::Null);
        }
        for i in match_len..tuple.len() {
            tuple.set(i, drive[i - match_len].dup(arena));
        }
        if self.need_produce_row_key {
            let mut multi_row_key = arena_vec!(arena);
            DB::encode_multi_row_key(&drive, &mut multi_row_key);
            tuple.attach_row_key(multi_row_key);
        }
        tuple
    }
}

struct LimitingState {
    pulled_rows: u64,
    delta_rows: u64,
    limit: Option<u64>,
    offset: Option<u64>,
}

impl LimitingState {
    fn new(limit: Option<u64>, offset: Option<u64>) -> Self {
        Self {
            pulled_rows: 0,
            delta_rows: offset.map_or(0, |x| { x }),
            limit,
            offset,
        }
    }

    fn apply_offset_should_skip(&mut self, n: u64) -> bool {
        self.pulled_rows += n;
        if let Some(offset) = &self.offset {
            if self.pulled_rows < *offset {
                return true;
            }
        }
        false
    }

    fn exceeding(&self) -> bool {
        if let Some(limit) = &self.limit {
            if self.pulled_rows - self.delta_rows >= *limit {
                return true;
            }
        }
        false
    }
}

impl PhysicalPlanOps for IndexNestedLoopJoinOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        self.driver.prepare()?;

        self.driving_vals.clear();
        self.matching_key.borrow_mut().clear();

        let storage = self.storage.as_ref().unwrap();
        let cf = self.cf.as_ref().unwrap();
        let rd_opts = ReadOptions::with()
            .option_snapshot(self.snapshot.clone())
            .build();
        let iter = storage.new_iterator(&rd_opts, cf)?;
        iter.borrow_mut().seek_to_first();

        self.iter = Some(iter);
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.storage = None;
        self.iter = None;
        self.snapshot = None;
        self.cf = None;
        self.driver.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let arena = zone.get_mut();
        loop {
            self.need_produce_row_key = feedback.need_row_key();
            let rs = self.next_driving_tuple(feedback, zone);
            if rs.is_none() {
                break None;
            }
            let drive = rs.unwrap();
            match self.match_tuple(&drive, &arena) {
                Ok(match_) => {
                    if matches!(self.join_op, JoinOp::RightOuterJoin) {
                        break Some(self.merge_tuples(match_, drive, &arena));
                    } else {
                        break Some(self.merge_tuples(drive, match_, &arena));
                    }
                }
                Err(e) => {
                    if !e.is_not_found() {
                        feedback.catch_error(e);
                        break None;
                    }
                    match &self.join_op {
                        JoinOp::LeftOuterJoin => break Some(self.merge_left_half(drive, &arena)),
                        JoinOp::RightOuterJoin => break Some(self.merge_right_half(drive, &arena)),
                        JoinOp::InnerJoin | JoinOp::CrossJoin => ()
                    }
                }
            }
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        arena_vec!(&self.arena, [self.driver.clone()])
    }
}

pub struct OrderingInStorageOps {
    ordering_key_bundle: ArenaVec<(BytecodeVector, SqlOrdering)>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    projected_columns: ArenaBox<ColumnSet>,
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    limit: LimitingState,
    arena: ArenaMut<Arena>,

    storage: Option<Arc<dyn storage::DB>>,
    iter: Option<IteratorArc>,
    cf: Option<Arc<dyn ColumnFamily>>,
}

impl OrderingInStorageOps {
    pub fn new(ordering_key_bundle: ArenaVec<(BytecodeVector, SqlOrdering)>,
               child: ArenaBox<dyn PhysicalPlanOps>,
               projected_columns: ArenaBox<ColumnSet>,
               limit: Option<u64>,
               offset: Option<u64>,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>,
               arena: ArenaMut<Arena>,
               storage: Arc<dyn storage::DB>) -> Self {
        Self {
            ordering_key_bundle,
            child,
            projected_columns,
            prepared_stmt,
            limit: LimitingState::new(limit, offset),
            arena,
            storage: Some(storage),
            iter: None,
            cf: None,
        }
    }

    fn ordering_insert(&mut self, up_cols: &ColumnSet, storage: &Arc<dyn storage::DB>, cf: &Arc<dyn ColumnFamily>) -> Result<()> {
        let mut env = UpstreamContext::new(self.prepared_stmt.clone(), &self.arena);
        env.add(up_cols);
        let context = Arc::new(env);

        let mut zone = Arena::new_ref();
        let mut feedback = FeedbackImpl::new(true);
        let mut interpreter = Interpreter::new(&self.arena);
        let mut sequence = 0u64;
        let wr_opts = WriteOptions::default();
        loop {
            zone_limit_guard!(zone, 1);
            let rs = self.child.next(&mut feedback, &zone);
            if rs.is_none() {
                break;
            }
            let tuple = rs.unwrap();
            context.attach(&tuple);

            let mut ordering_key = arena_vec!(&self.arena);

            for (bcv, ordering) in self.ordering_key_bundle.iter_mut() {
                let value = interpreter.evaluate(bcv, context.clone())?;
                match ordering {
                    SqlOrdering::Asc => DB::encode_sort_key(&value, &mut ordering_key),
                    SqlOrdering::Desc => DB::encode_inverse_sort_key(&value, &mut ordering_key),
                }
            }
            // write row key
            if tuple.row_key().is_empty() {
                ordering_key.write(&sequence.to_be_bytes()).unwrap();
                ordering_key.write(&0u32.to_be_bytes()).unwrap();
            } else {
                ordering_key.write(tuple.row_key()).unwrap();
                let len = tuple.row_key().len() as u32;
                ordering_key.write(&len.to_be_bytes()).unwrap();
            }

            sequence += 1;

            let mut row = arena_vec!(&self.arena);
            DB::encode_tuple(&tuple, &mut row);

            storage.insert(&wr_opts, &cf, &ordering_key, &row)?;
        }
        Ok(())
    }

    fn get_row_key(key: &[u8]) -> &[u8] {
        let tail = &key[key.len() - 4..];
        let len = u32::from_be_bytes(tail.try_into().unwrap()) as usize;
        &key[key.len() - 4 - len..key.len() - 4]
    }
}

impl PhysicalPlanOps for OrderingInStorageOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let cols = self.child.prepare()?;
        if let Some(cf) = self.cf.as_ref() {
            let _ = self.storage.as_ref().unwrap().drop_column_family(cf.clone());
            self.cf = None;
        }
        self.limit.pulled_rows = 0;

        let storage = self.storage.clone().unwrap();
        let opts = ColumnFamilyOptions::with()
            .temporary(true)
            .build();
        let cf = storage.new_column_family("ordering", opts)?;

        self.ordering_insert(cols.deref(), &storage, &cf)?;

        let rd_opts = ReadOptions::default();
        let iter_box = storage.new_iterator(&rd_opts, &cf)?;
        let mut iter = iter_box.borrow_mut();
        if iter.status().is_not_ok() {
            return Err(iter.status().clone());
        }
        iter.seek_to_first();
        if !iter.valid() && iter.status().is_corruption() {
            return Err(iter.status().clone());
        }

        if self.limit.offset.is_some() {
            while iter.valid() && self.limit.apply_offset_should_skip(1) {
                iter.move_next();
                if iter.status().is_corruption() {
                    return Err(iter.status().clone());
                }
            }
        }

        drop(iter);
        self.iter = Some(iter_box);
        self.cf = Some(cf);
        Ok(self.projected_columns.clone())
    }

    fn finalize(&mut self) {
        self.child.finalize();
        if let Some(cf) = self.cf.as_ref() {
            let _ = self.storage.as_ref().unwrap().drop_column_family(cf.clone());
        }
        self.storage = None;
        self.cf = None;
        self.iter = None;
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let iter_box = self.iter.as_ref().unwrap();
        let mut iter = iter_box.borrow_mut();
        if !iter.valid() || self.limit.exceeding() {
            return None;
        }
        let arena = zone.get_mut();
        let mut tuple = DB::decode_tuple(&self.projected_columns, iter.value(), &arena);
        if feedback.need_row_key() {
            let row_key = Self::get_row_key(iter.key());
            tuple.attach_row_key(ArenaVec::with_data(&arena, row_key));
        }
        iter.move_next();
        if iter.status().is_not_ok() {
            feedback.catch_error(iter.status().clone());
            None
        } else {
            assert!(!self.limit.apply_offset_should_skip(1));
            //println!("tuple={}", tuple.to_string());
            Some(tuple)
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        arena_vec!(&self.arena, [self.child.clone()])
    }
}

pub struct MockingProducerOps {
    data: ArenaVec<Tuple>,
    current: usize,
    projected_columns: ArenaBox<ColumnSet>,
}

impl MockingProducerOps {
    pub fn new(data: ArenaVec<Tuple>, projected_columns: ArenaBox<ColumnSet>) -> Self {
        Self {
            data,
            current: 0,
            projected_columns,
        }
    }
}

impl PhysicalPlanOps for MockingProducerOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        self.current = 0;
        Ok(self.projected_columns.clone())
    }

    fn next(&mut self, _feedback: &mut dyn Feedback, _zone: &ArenaRef<Arena>) -> Option<Tuple> {
        if self.current >= self.data.len() {
            None
        } else {
            let tuple = self.data[self.current].dup(&self.data.owns);
            self.current += 1;
            Some(tuple)
        }
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        arena_vec!(&self.data.owns)
    }
}

pub struct EmptyOps {
    arena: ArenaMut<Arena>,
}

impl EmptyOps {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self { arena: arena.clone() }
    }
}

impl PhysicalPlanOps for EmptyOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        let mut columns = ColumnSet::new("<empty>", 0, &self.arena);
        columns.append_with_name("_", ColumnType::Int(1));
        Ok(ArenaBox::new(columns, self.arena.get_mut()))
    }

    fn next(&mut self, _feedback: &mut dyn Feedback, _zone: &ArenaRef<Arena>) -> Option<Tuple> {
        None
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        ArenaVec::new(&self.arena)
    }
}

impl<T: PhysicalPlanOps + 'static> From<ArenaBox<T>> for ArenaBox<dyn PhysicalPlanOps> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr())
    }
}

#[cfg(test)]
mod tests {
    use crate::base::Arena;
    use crate::exec::connection::FeedbackImpl;
    use crate::exec::db::ColumnType;
    use crate::exec::evaluator::Value;
    use crate::sql::parse_sql_expr_from_content;
    use crate::storage;
    use crate::storage::{JunkFilesCleaner, open_kv_storage, Options, WriteOptions};

    use super::*;

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

        let mut columns = ArenaBox::new(ColumnSet::new("t1", 0, &arena), arena.get_mut());
        columns.append("id", "", 0, 1, ColumnType::Int(11));
        columns.append("name", "", 0, 2, ColumnType::Varchar(64));

        let mut plan = RangeScanningOps::new(columns,
                                             begin_key, true,
                                             end_key, true, None, None,
                                             arena.clone(), db.clone(),
                                             db.get_snapshot(),
                                             db.default_column_family().clone());
        plan.prepare()?;
        let mut feedback = FeedbackImpl::default();
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

    #[test]
    fn simple_nested_loop_join() -> Result<()> {
        let zone = Arena::new_ref();
        let arena = zone.get_mut();
        let mut t1_cols = ColumnSet::new("t1", 0, &arena);
        t1_cols.append("a", "", 0, 1, ColumnType::Int(11));
        t1_cols.append("b", "", 0, 2, ColumnType::Int(11));
        t1_cols.append("c", "", 0, 3, ColumnType::Int(11));
        let t1 = ArenaBox::new(t1_cols, arena.get_mut());
        let mut data = arena_vec!(&arena);
        for i in 0..3 {
            let mut tuple = Tuple::new(&t1, arena.get_mut());
            tuple.set(0, Value::Int(i));
            tuple.set(1, Value::Int((i + 1) * 10));
            tuple.set(2, Value::Int((i + 1) * 100));
            data.push(tuple);
        }
        let plan_t1 =
            ArenaBox::new(MockingProducerOps::new(data, t1.clone()), arena.get_mut());

        let mut t2_cols = ColumnSet::new("t2", 0, &arena);
        t2_cols.append("d", "", 0, 1, ColumnType::Int(11));
        t2_cols.append("e", "", 0, 2, ColumnType::Int(11));
        t2_cols.append("f", "", 0, 3, ColumnType::Int(11));
        let t2 = ArenaBox::new(t2_cols, arena.get_mut());
        let mut data = arena_vec!(&arena);
        for i in 0..3 {
            let mut tuple = Tuple::new(&t2, arena.get_mut());
            tuple.set(0, Value::Int(i + 1));
            tuple.set(1, Value::Int((i + 1) * 10));
            tuple.set(2, Value::Int((i + 1) * 100));
            data.push(tuple)
        }
        let plan_t2 =
            ArenaBox::new(MockingProducerOps::new(data, t2.clone()), arena.get_mut());

        let mut tt_cols = ColumnSet::new("", 0, &arena);
        for t in &[t1, t2] {
            for i in 0..t.len() {
                let col = &t[i];
                tt_cols.append(col.name.as_str(), t.schema.as_str(), 0, 0, col.ty.clone());
            }
        }
        let tt = ArenaBox::new(tt_cols, arena.get_mut());

        let expr = parse_sql_expr_from_content("t1.a = t2.d", &arena)?;
        let bcv = BytecodeBuildingVisitor::build_arena_box(&expr, &arena);
        let mut plan = SimpleNestedLoopJoinOps::new(plan_t1.into(),
                                                    plan_t2.into(), bcv,
                                                    tt, arena.clone(),
                                                    JoinOp::LeftOuterJoin);
        let data = [
            "(0, 10, 100, NULL, NULL, NULL)",
            "(1, 20, 200, 1, 10, 100)",
            "(2, 30, 300, 2, 20, 200)"
        ];
        check_plan_rows(&mut plan, &data, &zone)?;

        plan.join_op = JoinOp::InnerJoin;
        let data = [
            "(1, 20, 200, 1, 10, 100)",
            "(2, 30, 300, 2, 20, 200)"
        ];
        check_plan_rows(&mut plan, &data, &zone)?;

        plan.join_op = JoinOp::CrossJoin;
        let data = [
            "(1, 20, 200, 1, 10, 100)",
            "(2, 30, 300, 2, 20, 200)"
        ];
        check_plan_rows(&mut plan, &data, &zone)?;

        plan.join_op = JoinOp::RightOuterJoin;
        let data = [
            "(1, 20, 200, 1, 10, 100)",
            "(2, 30, 300, 2, 20, 200)",
            "(NULL, NULL, NULL, 3, 30, 300)"
        ];
        check_plan_rows(&mut plan, &data, &zone)?;
        Ok(())
    }

    fn check_plan_rows<P: PhysicalPlanOps + ?Sized>(plan: &mut P, rows: &[&str], zone: &ArenaRef<Arena>) -> Result<()> {
        plan.prepare()?;
        let mut i = 0;
        loop {
            let mut feedback = FeedbackImpl::default();
            let rs = plan.next(&mut feedback, &zone);
            assert_eq!(Status::Ok, feedback.status);
            if rs.is_none() {
                break;
            }
            assert_eq!(rows[i], rs.unwrap().to_string());
            //println!("{}", rs.unwrap());
            i += 1;
        }
        plan.finalize();
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