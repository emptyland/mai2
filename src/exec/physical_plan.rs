use std::collections::HashMap;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaRef, ArenaVec};
use crate::exec::executor::{ColumnSet, Tuple, UpstreamContext};
use crate::{Result, Status, storage};
use crate::exec::db::DB;
use crate::exec::evaluator::{Context, Evaluator};
use crate::sql::ast::Expression;

pub trait PhysicalPlanOps {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>>;

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
    pub fn new(projected_columns: ArenaBox<ColumnSet>,
               range_begin: ArenaVec<u8>,
               range_end: ArenaVec<u8>,
               limit: Option<u64>,
               offset: Option<u64>,
               arena: ArenaMut<Arena>,
               iter: &storage::IteratorArc) -> Self {
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
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
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

pub struct FilteringOps<'a> {
    expr: ArenaBox<dyn Expression>,
    child: ArenaBox<dyn PhysicalPlanOps>,
    arena: ArenaMut<Arena>,
    projected_columns: ArenaBox<ColumnSet>,

    context: Option<UpstreamContext<'a>>,
}

impl <'a> PhysicalPlanOps for FilteringOps<'a> {
    fn prepare(&mut self) -> Result<ArenaBox<ColumnSet>> {
        self.context.as_mut().unwrap().add(self.projected_columns.deref());
        self.child.prepare()
    }

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
        //let mut evaluator = Evaluator::new(&mut arena);

        // let rs = evaluator.evaluate(self.expr.deref_mut(),
        //                             self.context.as_ref().cloned().unwrap());


        todo!()
    }

    fn children(&self) -> ArenaVec<ArenaBox<dyn PhysicalPlanOps>> {
        let mut children = ArenaVec::new(&self.arena);
        children.push(self.child.clone());
        children
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

        let iter = db.new_iterator(&ReadOptions::default(), &db.default_column_family())?;
        let mut plan = RangeScanOps::new(columns, begin_key,
                                         end_key, None, None, arena.clone(),
                                         &iter);
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