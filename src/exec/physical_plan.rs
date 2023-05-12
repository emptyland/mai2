use std::collections::HashMap;
use std::io::Write;
use std::ops::DerefMut;
use std::sync::Arc;
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaRef, ArenaVec};
use crate::exec::executor::{ColumnSet, Tuple};
use crate::{Result, Status, storage};
use crate::exec::db::DB;
use crate::exec::evaluator::{Context, Evaluator};
use crate::sql::ast::Expression;

pub trait PhysicalPlanOps {
    fn prepare(&mut self) -> Result<()> {
        for child in self.children().iter_mut() {
            child.prepare()?;
        }
        Ok(())
    }

    fn finalize(&mut self) {
        for child in self.children().iter_mut() {
            child.finalize();
        }
    }

    fn next(&mut self, feedback: &mut dyn Feedback, arena: &ArenaRef<Arena>) -> Option<Tuple>;
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
    range_end: ArenaVec<u8>,

    arena: ArenaMut<Arena>,
    iter: Option<storage::IteratorArc>,
    col_id_to_order: HashMap<u32, usize>,
}

impl RangeScanOps {
    pub fn new(projected_columns: ArenaBox<ColumnSet>, range_begin: ArenaVec<u8>,
               range_end: ArenaVec<u8>, limit: Option<u64>, offset: Option<u64>,
               arena: ArenaMut<Arena>, iter: &storage::IteratorArc) -> Self {
        let mut col_id_to_order = HashMap::new();
        for i in 0..projected_columns.columns.len() {
            let col = &projected_columns.columns[i];
            col_id_to_order.insert(col.id, i);
        }
        Self {
            projected_columns,
            pulled_rows: 0,
            limit,
            offset,
            range_begin,
            range_end,
            arena,
            iter: Some(iter.clone()),
            col_id_to_order,
        }
    }

    fn next_row<F>(&self, iter: &mut dyn storage::Iterator, mut each_col: F, arena: &ArenaMut<Arena>) -> Result<()>
        where F: FnMut(&[u8], &[u8]) {
        debug_assert!(iter.key().len() >= DB::PRIMARY_KEY_ID_BYTES.len() + DB::COL_ID_LEN);
        let row_key_ref = &iter.key()[..iter.key().len() - DB::COL_ID_LEN];
        if !row_key_ref.starts_with(&DB::PRIMARY_KEY_ID_BYTES) {
            return Ok(());
        }

        let mut row_key = ArenaVec::<u8>::new(arena);
        row_key.write(row_key_ref).unwrap();

        debug_assert!(iter.valid());
        while iter.valid() && iter.key().starts_with(&row_key) {
            each_col(iter.key(), iter.value());
            iter.move_next();
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
    fn prepare(&mut self) -> Result<()> {
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
            Ok(())
        }
    }

    fn finalize(&mut self) {
        self.iter = None;
        self.col_id_to_order = HashMap::default();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
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
            feedback.catch_error(e);
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

pub struct FilteringOps {
    expr: ArenaBox<dyn Expression>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,

    context: Option<Arc<dyn Context>>,
}

impl PhysicalPlanOps for FilteringOps {
    fn finalize(&mut self) {
        self.context = None;
        self.child.finalize();
    }

    fn next(&mut self, feedback: &mut dyn Feedback, zone: &ArenaRef<Arena>) -> Option<Tuple> {
        let mut arena = zone.get_mut();
        let prev = self.child.next(feedback, zone);
        if prev.is_none() {
            return None;
        }
        let tuple = prev.unwrap();
        let mut evaluator = Evaluator::new(&mut arena);

        let rs = evaluator.evaluate(self.expr.deref_mut(), 
                                    self.context.as_ref().cloned().unwrap());
        

        todo!()
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        let mut children = ArenaVec::new(&self.arena);
        children.push(self.child.clone());
        children
    }
}