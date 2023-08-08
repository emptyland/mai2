use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::mem::{replace, swap};
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;

use crate::{arena_vec, break_visit, corrupted_err, Corrupting, Result, Status, switch, try_visit, visit_fatal};
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaStr, ArenaVec};
use crate::exec::db::{ColumnType, DB, TableHandle, TableRef};
use crate::exec::evaluator::{Evaluator, TypingReducer, Value};
use crate::exec::executor::{ColumnSet, PreparedStatement, TypingStubContext, UniversalContext};
use crate::exec::function;
use crate::exec::function::{Aggregator, ExecutionContext};
use crate::exec::interpreter::{BytecodeBuildingVisitor, BytecodeVector};
use crate::exec::physical_plan::*;
use crate::sql::ast::*;
use crate::storage::storage;

pub struct PlanMaker {
    db: Arc<DB>,
    snapshot: Arc<dyn storage::Snapshot>,
    arena: ArenaMut<Arena>,
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    rs: Status,
    schemas: ArenaVec<ArenaBox<ColumnSet>>,
    current: ArenaVec<ArenaBox<dyn PhysicalPlanOps>>,
    bytecode_builder: RefCell<BytecodeBuildingVisitor>,
}

impl PlanMaker {
    pub fn new(db: &Arc<DB>,
               snapshot: Arc<dyn storage::Snapshot>,
               prepared_stmt: Option<ArenaBox<PreparedStatement>>,
               arena: &ArenaMut<Arena>) -> Self {
        Self {
            db: db.clone(),
            snapshot,
            arena: arena.clone(),
            prepared_stmt,
            rs: Status::Ok,
            schemas: ArenaVec::new(arena),
            current: ArenaVec::new(arena),
            bytecode_builder: RefCell::new(BytecodeBuildingVisitor::new(arena)),
        }
    }

    pub fn make(&mut self, stmt: &mut dyn Relation) -> Result<ArenaBox<dyn PhysicalPlanOps>> {
        self.schemas.clear();
        self.current.clear();
        self.rs = Status::Ok;
        stmt.accept(self);
        if self.rs.is_not_ok() {
            Err(self.rs.clone())
        } else {
            Ok(self.current.pop().unwrap())
        }
    }

    pub fn make_rows_multi_producer(&mut self, relation: &ArenaBox<dyn Relation>,
                                    where_clause: &Option<ArenaBox<dyn Expression>>,
                                    order_by_clause: &ArenaVec<OrderClause>,
                                    limit: &Option<ArenaBox<dyn Expression>>) -> Result<ArenaBox<dyn PhysicalPlanOps>> {
        let mut rel = relation.clone();
        //let physical_table = self.visit_from_clause(rel.deref_mut());
        let limit = self.eval_require_u64_literal(limit)?;
        if let Some(table) = self.visit_from_clause(rel.deref_mut()) {
            // Maybe fast path
            let cols = self.schemas.pop().unwrap();
            let mut wc = where_clause.clone();

            if !order_by_clause.is_empty() {
                self.make_selecting(&table, cols, &mut wc, None, None)?;
            } else {
                self.make_selecting(&table, cols, &mut wc, limit, None)?;
            }

            let cols = self.schemas.pop().unwrap();
            let mut plan = self.current.pop().unwrap();
            if !order_by_clause.is_empty() {
                plan = self.make_ordering(&cols, &plan, order_by_clause, limit, None);
            }
            Ok(plan)
        } else {
            // Fallback (slow) path
            let mut plan = self.make(rel.deref_mut())?;

            let cols = self.schemas.pop().unwrap();
            if let Some(filter) = where_clause {
                let filtering = FilteringOps::new(self.build_bytecode_box(filter),
                                                  &plan,
                                                  &cols,
                                                  self.prepared_stmt.clone(),
                                                  &self.arena);
                plan = ArenaBox::new(filtering, self.arena.get_mut()).into();
            }
            if limit.is_some() {
                let merging = MergingOps::new(arena_vec!(&self.arena, [plan]), limit, None);
                plan = ArenaBox::new(merging, self.arena.get_mut()).into();
            }
            Ok(plan)
        }
    }

    pub fn make_rows_simple_producer(&mut self, name: &str,
                                     where_clause: &Option<ArenaBox<dyn Expression>>,
                                     order_by_clause: &[OrderClause],
                                     limit: &Option<ArenaBox<dyn Expression>>) -> Result<ArenaBox<dyn PhysicalPlanOps>> {
        let (table, cols) = self.resolve_physical_table(name, "")?;
        let limit = self.eval_require_u64_literal(limit)?;
        if where_clause.is_none() {
            let mut plan = self.make_scan_table(&table, &cols,
                                                switch!(!order_by_clause.is_empty(), None, limit), None)?;
            if !order_by_clause.is_empty() {
                plan = self.make_ordering(&cols, &plan, order_by_clause, limit, None);
            }
            return Ok(plan);
        }

        let mut expr = where_clause.clone().unwrap().clone();
        let mut analyzer =
            PhysicalSelectionAnalyzer::new(&table, &self.arena, self.prepared_stmt.clone());
        match analyzer.analyze(expr.deref_mut()) {
            Err(e) => {
                self.rs = e.clone();
                Err(e)
            }
            Ok(analyzed_val) => {
                match self.make_by_physical_selection_analyzed_val(analyzed_val, &table, &cols,
                                                                   switch!(!order_by_clause.is_empty(), None, limit),
                                                                   None) {
                    Err(e) => {
                        self.rs = e.clone();
                        Err(e)
                    }
                    Ok(_) => {
                        let cols = self.schemas.pop().unwrap();
                        let plan = self.current.pop().unwrap();
                        if !order_by_clause.is_empty() {
                            Ok(self.make_ordering(&cols, &plan, order_by_clause, limit, None))
                        } else {
                            Ok(plan)
                        }
                    }
                }
            }
        }
    }

    pub fn make_full_scan_producer(&mut self, table: &TableRef) -> Result<ArenaBox<dyn PhysicalPlanOps>> {
        let mut columns = ColumnSet::new(table.name().as_str(), table.metadata.id, &self.arena);
        for col in &table.metadata.columns {
            columns.append_physical(col.name.as_str(), table.metadata.id, col.id, col.ty.clone());
        }
        let cols = ArenaBox::new(columns, self.arena.deref_mut());
        self.make_scan_table(table, &cols, None, None)
    }

    fn arena_of_ast(&self) -> &ArenaMut<Arena> {
        match &self.prepared_stmt {
            Some(stmt) => stmt.owns_arena(),
            None => &self.arena,
        }
    }

    fn visit_from_clause(&mut self, from: &mut dyn Relation) -> Option<Arc<TableHandle>> {
        if let Some(table_ref) = from.as_any().downcast_ref::<FromClause>() {
            match self.resolve_physical_table(table_ref.name.as_str(), table_ref.alias.as_str()) {
                Err(e) => {
                    self.rs = e;
                    None
                }
                Ok((table, cols)) => {
                    self.schemas.push(cols);
                    Some(table)
                }
            }
        } else {
            from.accept(self);
            None
        }
    }

    fn resolve_physical_table(&self, name: &str, alias: &str) -> Result<(TableRef, ArenaBox<ColumnSet>)> {
        let tables = self.db.lock_tables();
        let rs = tables.get(name);
        if rs.is_none() {
            return corrupted_err!("Table: {} not found", name);
        }
        let table = rs.unwrap().clone();
        let schema = if alias.is_empty() {
            name
        } else {
            alias
        };
        let mut columns = ColumnSet::new(schema, table.metadata.id, &self.arena);
        for col in &table.metadata.columns {
            columns.append_physical(col.name.as_str(), table.metadata.id, col.id, col.ty.clone());
        }
        Ok((table, ArenaBox::new(columns, self.arena.get_mut())))
    }

    fn top_schema(&self) -> Option<&ArenaBox<ColumnSet>> {
        self.schemas.back()
    }

    fn make_by_physical_selection_analyzed_val(&mut self, analyzed: AnalyzedVal, table: &TableRef,
                                               columns: &ArenaBox<ColumnSet>, limit: Option<u64>, offset: Option<u64>)
                                               -> Result<()> {
        let ops = match analyzed {
            AnalyzedVal::And(sets, expr) => {
                let ops = self.make_merging_by_sets(&sets, table, columns,
                                                    None, None)?;
                self.make_filtering(&expr, columns, &ops, limit, offset)
            }
            AnalyzedVal::Set(sets) => {
                self.make_merging_by_sets(&sets, table, columns, limit, offset)?
            }
            AnalyzedVal::NeedEval(expr) => {
                let ops = self.make_scan_table(table, columns,
                                               None, None)?;
                self.make_filtering(&expr, columns, &ops, limit, offset)
            }
            _ => unreachable!()
        };
        self.schemas.push(columns.clone());
        self.current.push(ops);
        Ok(())
    }

    fn make_scan_table(&self, table: &TableRef, columns: &ArenaBox<ColumnSet>, limit: Option<u64>, offset: Option<u64>)
                       -> Result<ArenaBox<dyn PhysicalPlanOps>> {
        let mut begin_key = ArenaVec::new(&self.arena);
        DB::encode_idx_id(DB::PRIMARY_KEY_ID as u64, &mut begin_key);
        let mut end_key = ArenaVec::new(&self.arena);
        DB::encode_idx_id(DB::PRIMARY_KEY_ID as u64 + 1, &mut end_key);

        let ops = RangeScanningOps::new(columns.clone(),
                                        begin_key, true,
                                        end_key, true,
                                        limit, offset,
                                        self.arena.clone(),
                                        self.db.storage.clone(),
                                        self.snapshot.clone(),
                                        table.column_family.clone());
        Ok(ArenaBox::new(ops, self.arena.get_mut()).into())
    }

    fn make_filtering(&self, expr: &ArenaBox<dyn Expression>, columns: &ArenaBox<ColumnSet>,
                      child: &ArenaBox<dyn PhysicalPlanOps>,
                      limit: Option<u64>, offset: Option<u64>) -> ArenaBox<dyn PhysicalPlanOps> {
        let ops = FilteringOps::new(self.build_bytecode_box(expr), child, columns,
                                    self.prepared_stmt.clone(), &self.arena);
        let child = ArenaBox::new(ops, self.arena.get_mut());
        if limit.is_some() || offset.is_some() {
            let rv = MergingOps::new(ArenaVec::of(child.into(), &self.arena), limit, offset);
            ArenaBox::new(rv, self.arena.get_mut()).into()
        } else {
            child.into()
        }
    }

    fn make_merging_by_sets(&self, sets: &[SelectionSet], table: &TableRef,
                            columns: &ArenaBox<ColumnSet>,
                            limit: Option<u64>, offset: Option<u64>) -> Result<ArenaBox<dyn PhysicalPlanOps>> {
        debug_assert!(sets.len() > 0);
        let mut children = ArenaVec::new(&self.arena);
        if sets.len() == 1 {
            self.make_physical_ops_by_set(sets.first().unwrap(), table, columns, limit, offset,
                                          &mut children)?;
        } else {
            for set in sets {
                self.make_physical_ops_by_set(set, table, columns, None, None,
                                              &mut children)?;
            }
        }
        debug_assert!(children.len() > 0);
        if children.len() == 1 {
            Ok(children.first().unwrap().clone().into())
        } else {
            let ops = MergingOps::new(children, limit, offset);
            Ok(ArenaBox::new(ops, self.arena.get_mut()).into())
        }
    }

    fn make_physical_ops_by_set(&self, set: &SelectionSet, table: &TableRef, columns: &ArenaBox<ColumnSet>,
                                limit: Option<u64>, offset: Option<u64>,
                                receiver: &mut ArenaVec<ArenaBox<dyn PhysicalPlanOps>>) -> Result<()> {
        // let has_points = set.segments.iter().filter(|x| {x.is_point()}).count() > 0;
        //
        // if has_points {
        //     for range in set.segments.iter().filter(|x| {x.is_point()}) {
        //
        //     }
        // }
        let mut points = arena_vec!(&self.arena);
        let mut ranges = arena_vec!(&self.arena);
        set.segments.iter().for_each(|x| {
            if x.is_point() {
                points.push(x);
            } else {
                ranges.push(x);
            }
        });

        if !points.is_empty() {
            let mut keys = arena_vec!(&self.arena);

            for point in &points {
                let mut key = arena_vec!(&self.arena);
                DB::encode_idx_id(set.key_id, &mut key);
                if DB::is_primary_key(set.key_id) {
                    DB::encode_row_key(&point.min, &set.ty, &mut key)?;
                } else {
                    DB::encode_secondary_index(&point.min, &set.ty, &mut key);
                }
                keys.push(key);
            }

            let ops = RowsGettingOps::new(columns.clone(), keys, self.arena.clone(),
                                          self.db.storage.clone(), self.db.get_snapshot(),
                                          table.column_family.clone());
            receiver.push(ArenaBox::new(ops, self.arena.get_mut()).into())
        }


        for range in ranges.iter() {
            let mut begin_key = ArenaVec::new(&self.arena);
            DB::encode_idx_id(set.key_id, &mut begin_key);

            let mut end_key = ArenaVec::new(&self.arena);
            if range.max.is_inf() {
                DB::encode_idx_id(set.key_id + 1, &mut end_key);
            } else {
                DB::encode_idx_id(set.key_id, &mut end_key);
            }

            if DB::is_primary_key(set.key_id) { // is primary key
                DB::encode_row_key(&range.min, &set.ty, &mut begin_key)?;
                DB::encode_row_key(&range.max, &set.ty, &mut end_key)?;
            } else {
                DB::encode_secondary_index(&range.min, &set.ty, &mut begin_key);
                DB::encode_secondary_index(&range.max, &set.ty, &mut end_key);
            }

            let ops = RangeScanningOps::new(columns.clone(),
                                            begin_key, range.left_close,
                                            end_key, range.right_close,
                                            switch!(set.segments.len() == 1, limit, None),
                                            switch!(set.segments.len() == 1, offset, None),
                                            self.arena.clone(),
                                            self.db.storage.clone(),
                                            self.snapshot.clone(),
                                            table.column_family.clone());
            receiver.push(ArenaBox::new(ops, self.arena.get_mut()).into())
        }
        Ok(())
    }

    fn make_ordering(&self,
                     cols: &ArenaBox<ColumnSet>,
                     child: &ArenaBox<dyn PhysicalPlanOps>,
                     order_by_clause: &[OrderClause],
                     limit: Option<u64>,
                     offset: Option<u64>) -> ArenaBox<dyn PhysicalPlanOps> {
        let mut ordering_key_bundle = arena_vec!(&self.arena);
        for clause in order_by_clause {
            ordering_key_bundle.push((self.build_bytecode_box(&clause.key), clause.ordering.clone()));
        }
        let ordering = OrderingInStorageOps::new(ordering_key_bundle, child.clone(),
                                                 cols.clone(),
                                                 limit,
                                                 offset,
                                                 self.prepared_stmt.clone(),
                                                 self.arena.clone(),
                                                 self.db.storage.clone());
        ArenaBox::new(ordering, self.arena.get_mut()).into()
    }

    fn build_column_set(&self, table: &TableRef, alias: &ArenaStr, names: &[(ArenaStr, ArenaStr)])
                        -> ArenaBox<ColumnSet> {
        let mut columns =
            ColumnSet::new(switch!(alias.is_empty(), table.metadata.name.as_str(), alias.as_str()),
                           table.metadata.id,
                           &self.arena);
        for (_, name) in names {
            let col = table.get_col_by_name(&name.to_string()).unwrap();
            columns.append(name.as_str(), alias.as_str(), table.metadata.id, col.id, col.ty.clone());
        }
        ArenaBox::new(columns, self.arena.get_mut())
    }

    fn merge_joining_columns_set(&self, driver: &ColumnSet, matcher: &ColumnSet) -> ArenaBox<ColumnSet> {
        let mut cols = ColumnSet::new("", 0, &self.arena);
        for cols_set in [&driver, &matcher] {
            for col in &cols_set.columns {
                cols.append(col.name.as_str(), cols_set.schema.as_str(), col.original_tid, col.id,
                            col.ty.clone());
            }
        }
        ArenaBox::new(cols, self.arena.get_mut())
    }

    fn merge_aggregators_columns_set(&self, aggregators: &[AggregatorBundle],
                                     up_cols: &ColumnSet) -> ArenaBox<ColumnSet> {
        let mut cols = ColumnSet::new(up_cols.schema.as_str(), up_cols.tid, &self.arena);
        let mut i = 0;
        for agg in aggregators {
            i += 1;
            let name = format!("_a_{i}");
            cols.append_with_name(name.as_str(), agg.returning_ty().clone());
        }
        drop(i);

        for col in &up_cols.columns {
            cols.append(col.name.as_str(), col.desc.as_str(), col.original_tid, col.id, col.ty.clone());
        }
        ArenaBox::new(cols, self.arena.get_mut())
    }

    fn reduce_aggregators_returning_ty(&self, aggregators: &mut [AggregatorBundle],
                                       up_cols: &ArenaBox<ColumnSet>) -> Result<()> {
        let context =
            Arc::new(TypingStubContext::new(self.prepared_stmt.clone(), up_cols));
        let mut reducer = TypingReducer::new(&self.arena);
        let mut params = ArenaVec::new(&self.arena);
        for agg in aggregators {
            for expr in agg.args_mut() {
                params.push(reducer.reduce(expr.deref_mut(), context.clone())?);
            }
            agg.reduce_returning_ty(&params);
            params.clear();
        }
        Ok(())
    }

    fn choose_join_plan(&mut self, driver_cols: ArenaBox<ColumnSet>,
                        driver_plan: ArenaBox<dyn PhysicalPlanOps>,
                        physical_table: Option<TableRef>,
                        op: JoinOp,
                        on_clause: &mut ArenaBox<dyn Expression>) {
        if let Some(table) = physical_table {
            let matcher_cols = self.schemas.pop().unwrap();
            let rs = self.choose_join_plan_with_physical_table(driver_cols, driver_plan, matcher_cols,
                                                               table, op, on_clause);
            if let Err(e) = rs {
                self.rs = e;
                return;
            }
        } else {
            let (
                lhs_cols,
                lhs_plan,
                rhs_cols,
                rhs_plan,
            ) = if op == JoinOp::RightOuterJoin {
                (self.schemas.pop().unwrap(), self.current.pop().unwrap(), driver_cols, driver_plan)
            } else {
                (driver_cols, driver_plan, self.schemas.pop().unwrap(), self.current.pop().unwrap())
            };
            self.make_simple_nested_loop_join(lhs_cols.deref(), lhs_plan, rhs_cols.deref(), rhs_plan,
                                              op, on_clause.clone());
        }
    }

    fn choose_join_plan_with_physical_table(&mut self, driver_cols: ArenaBox<ColumnSet>,
                                            driver_plan: ArenaBox<dyn PhysicalPlanOps>,
                                            matcher_cols: ArenaBox<ColumnSet>,
                                            matcher_table: TableRef,
                                            op: JoinOp,
                                            on_clause: &mut ArenaBox<dyn Expression>) -> Result<()> {
        let mut analyzer =
            PhysicalJoinConditionAnalyzer::new(&driver_cols, &matcher_cols, &matcher_table,
                                               &self.arena);
        let jcv = analyzer.visit(on_clause.deref_mut())?;
        if let Some(JoinExplainedVal {
                        matching_key_id,
                        matching_key_bundle,
                        filtering
                    }) = analyzer.explain(jcv) {
            self.make_index_nested_loop_join(driver_cols, driver_plan, matcher_cols,
                                             &matcher_table, matching_key_id, matching_key_bundle, op);
            return Ok(());
        }

        let rhs_plan = self.make_scan_table(&matcher_table, &matcher_cols, None, None)?;
        self.make_simple_nested_loop_join(driver_cols.deref(), driver_plan, matcher_cols.deref(), rhs_plan,
                                          op, on_clause.clone());
        Ok(())
    }

    fn make_simple_nested_loop_join(&mut self, lhs_cols: &ColumnSet, lhs_plan: ArenaBox<dyn PhysicalPlanOps>,
                                    rhs_cols: &ColumnSet, rhs_plan: ArenaBox<dyn PhysicalPlanOps>, join_op: JoinOp,
                                    on_clause: ArenaBox<dyn Expression>) {
        let cols = self.merge_joining_columns_set(lhs_cols, rhs_cols);
        let bcv = self.build_bytecode_box(&on_clause);
        let join_plan = SimpleNestedLoopJoinOps::new(lhs_plan, rhs_plan,
                                                     bcv, cols.clone(),
                                                     self.arena.clone(), join_op);
        let plan = ArenaBox::new(join_plan, self.arena.get_mut());
        self.schemas.push(cols);
        self.current.push(plan.into());
    }

    fn make_index_nested_loop_join(&mut self, lhs_cols: ArenaBox<ColumnSet>, lhs_plan: ArenaBox<dyn PhysicalPlanOps>,
                                   rhs_cols: ArenaBox<ColumnSet>, matcher: &TableRef, matching_key_id: u64,
                                   matching_key_bundle: ArenaVec<(ArenaBox<dyn Expression>, ColumnType)>,
                                   join_op: JoinOp) {
        let cols = self.merge_joining_columns_set(lhs_cols.deref(), rhs_cols.deref());
        let mut matching_bcv_bundle = arena_vec!(&self.arena);
        for (expr, ty) in matching_key_bundle.iter() {
            matching_bcv_bundle.push((self.build_bytecode_box(expr), ty.clone()));
        }
        let join_plan = IndexNestedLoopJoinOps::new(lhs_plan, cols.clone(),
                                                    join_op, lhs_cols, matching_bcv_bundle,
                                                    rhs_cols, matching_key_id,
                                                    self.prepared_stmt.clone(), self.arena.clone(),
                                                    self.db.storage.clone(), self.snapshot.clone(),
                                                    matcher.column_family.clone());
        let plan = ArenaBox::new(join_plan, self.arena.get_mut());
        self.schemas.push(cols);
        self.current.push(plan.into());
    }

    fn make_projecting(&mut self, columns: &[SelectColumnItem], alias: &str) -> Result<()> {
        let up_cols = self.schemas.pop().unwrap();
        let up_plan = self.current.pop().unwrap();
        let mut cols = ColumnSet::new(
            switch!(alias.is_empty(), up_cols.schema.as_str(), alias),
            up_cols.tid,
            &self.arena,
        );

        let context =
            Arc::new(TypingStubContext::new(self.prepared_stmt.clone(), &up_cols));
        let mut reducer = TypingReducer::new(&self.arena);

        let mut exprs = ArenaVec::new(&self.arena);
        let mut i = 0;
        for col_item in columns {
            debug_assert!(matches!(col_item.expr, SelectColumn::Expr(_)));
            let mut expr;
            if let SelectColumn::Expr(e) = &col_item.expr {
                expr = e.clone();
            } else {
                unreachable!()
            }

            let certain_col =
                if let Some((prefix, suffix)) = self.analyze_require_id_literal(expr.deref()) {
                    up_cols.find_by_name(prefix.as_str(), suffix.as_str())
                } else {
                    None
                };

            let (tid, id, ty) = if let Some(col) = &certain_col {
                (col.original_tid, col.id, col.ty.clone())
            } else {
                (0, 0, reducer.reduce(expr.deref_mut(), context.clone())?)
            };
            //let ty = reducer.reduce(expr.deref_mut(), context.clone())?;
            let mut desc = ArenaStr::default();
            let name = if !col_item.alias.is_empty() {
                col_item.alias.to_string()
            } else if let Some((prefix, suffix)) = self.analyze_require_id_literal(expr.deref()) {
                if prefix != cols.schema {
                    desc = prefix;
                }
                suffix.to_string()
            } else {
                i += 1;
                format!("_{i}")
            };
            cols.append(name.as_str(), desc.as_str(), tid, id, ty);
            exprs.push(expr);
        }

        let projected_cols = ArenaBox::new(cols, self.arena.get_mut());
        let bcs = self.build_bytecode_boxes(&exprs);
        let plan = ProjectingOps::new(bcs,
                                      up_plan,
                                      self.arena.clone(),
                                      projected_cols.clone(),
                                      self.prepared_stmt.clone());
        self.schemas.push(projected_cols.clone());
        self.current.push(ArenaBox::new(plan, self.arena.get_mut()).into());
        Ok(())
    }

    fn make_selecting(&mut self,
                      table: &TableRef,
                      cols: ArenaBox<ColumnSet>,
                      where_clause: &mut Option<ArenaBox<dyn Expression>>,
                      limit: Option<u64>,
                      offset: Option<u64>) -> Result<()> {
        if let Some(selection) = where_clause {
            let mut analyzer =
                PhysicalSelectionAnalyzer::new(&table, &self.arena, self.prepared_stmt.clone());
            let analyzed_val = analyzer.analyze(selection.deref_mut())?;
            self.make_by_physical_selection_analyzed_val(analyzed_val, table, &cols, limit, offset)
        } else {
            // just scan all rows of table
            let plan = self.make_scan_table(table, &cols, limit, offset)?;
            self.schemas.push(cols);
            self.current.push(plan);
            Ok(())
        }
    }

    fn eval_require_u64_literal(&self, may_expr: &Option<ArenaBox<dyn Expression>>) -> Result<Option<u64>> {
        match may_expr {
            Some(expr) => {
                let mut evaluator = Evaluator::new(&self.arena);
                let context = Arc::new(UniversalContext::new(self.prepared_stmt.clone(), &self.arena));
                let mut owned_expr = expr.clone();
                let val = evaluator.evaluate(owned_expr.deref_mut(), context)?;
                match val {
                    Value::Int(n) => Ok(Some(n as u64)),
                    _ => Err(Status::corrupted("Require integral literal by limit of offset clause"))
                }
            }
            None => Ok(None)
        }
    }

    fn analyze_require_id_literal(&self, expr: &dyn Expression) -> Option<(ArenaStr, ArenaStr)> {
        if let Some(id) = expr.as_any().downcast_ref::<Identifier>() {
            Some((ArenaStr::default(), id.symbol.clone()))
        } else if let Some(full_name) = expr.as_any().downcast_ref::<FullyQualifiedName>() {
            Some((full_name.prefix.clone(), full_name.suffix.clone()))
        } else {
            None
        }
    }

    fn build_bytecode_boxes(&self, boxes: &[ArenaBox<dyn Expression>]) -> ArenaVec<BytecodeVector> {
        let mut rv = arena_vec!(&self.arena);
        for it in boxes {
            rv.push(self.build_bytecode_box(it));
        }
        rv
    }

    fn build_bytecode_box(&self, expr: &ArenaBox<dyn Expression>) -> BytecodeVector {
        let mut copied = expr.clone();
        self.build_bytecode(copied.deref_mut())
    }

    fn build_bytecode(&self, expr: &mut dyn Expression) -> BytecodeVector {
        let mut chunk = arena_vec!(&self.arena);
        let mut builder = self.bytecode_builder.borrow_mut();
        builder.build(expr, &mut chunk).unwrap();
        BytecodeVector::new(chunk, &self.arena)
    }
}

macro_rules! try_make_plan {
    ($self:ident, $node:expr) => {
        {
            try_visit!($self, $node);
            ($self.schemas.pop().unwrap(), $self.current.pop().unwrap())
        }
    };
}


impl Visitor for PlanMaker {
    fn visit_collection(&mut self, this: &mut Collection) {
        let (lhs_cols, lhs_plan) = try_make_plan!(self, this.lhs);
        let (rhs_cols, rhs_plan) = try_make_plan!(self, this.rhs);

        if lhs_cols.len() != rhs_cols.len() {
            visit_fatal!(self, "Different number of columns, left is {}, but right is {}",
                lhs_cols.len(), rhs_cols.len());
        }

        for i in 0..lhs_cols.len() {
            let lhs_col = &lhs_cols[i];
            let rhs_col = &rhs_cols[i];
            if lhs_col.ty.is_not_compatible_of(&rhs_col.ty) {
                visit_fatal!(self, "Column not compatible in column[{i}], left is {:?}, but right is {:?}",
                lhs_col.ty, rhs_col.ty);
            }
        }

        let merging_plan = MergingOps::new(arena_vec!(&self.arena, [lhs_plan, rhs_plan]),
                                           None, None);
        let mut plan: ArenaBox<dyn PhysicalPlanOps> = ArenaBox::new(merging_plan,
                                                                    self.arena.get_mut()).into();
        if matches!(this.op, SetOp::Union) {
            let distinct_plan = DistinctOps::new(lhs_cols.clone(),
                                                 plan.clone(),
                                                 self.arena.clone(),
                                                 self.db.storage.clone());
            plan = ArenaBox::new(distinct_plan, self.arena.get_mut()).into();
        }

        self.schemas.push(lhs_cols);
        self.current.push(plan);
    }

    fn visit_select(&mut self, this: &mut Select) {
        let maybe_table = match &mut this.from_clause {
            Some(from) => self.visit_from_clause(from.deref_mut()),
            None => None
        };
        if self.rs.is_not_ok() {
            return;
        }
        let mut projection_col_visitor =
            ProjectionColumnsVisitor::new(self.top_schema().unwrap(),
                                          self.arena_of_ast(),
                                          &self.arena);
        match projection_col_visitor.try_rewrite(&mut this.columns) {
            Err(e) => {
                self.rs = e;
                return;
            }
            Ok(_) => ()
        }

        let limit;
        match self.eval_require_u64_literal(&this.limit_clause) {
            Err(e) => {
                self.rs = e;
                return;
            }
            Ok(val) => limit = val,
        }

        let offset;
        match self.eval_require_u64_literal(&this.offset_clause) {
            Err(e) => {
                self.rs = e;
                return;
            }
            Ok(val) => offset = val,
        }

        let outer_limit = !this.order_by_clause.is_empty() ||
            this.distinct ||
            !this.group_by_clause.is_empty() ||
            maybe_table.is_none();

        if let Some(table) = &maybe_table {
            let low_columns =
                self.build_column_set(&table, this.from_clause.as_ref().unwrap().alias(),
                                      &projection_col_visitor.projection_fields);
            match self.make_selecting(table, low_columns, &mut this.where_clause,
                                      if outer_limit { None } else { limit },
                                      if outer_limit { None } else { offset }) {
                Err(e) => {
                    self.rs = e;
                    return;
                }
                Ok(_) => ()
            }
        }

        if maybe_table.is_none() {
            if let Some(filter) = &mut this.where_clause {
                let up_cols = self.schemas.pop().unwrap();
                let up_plan = self.current.pop().unwrap();
                let filtering = FilteringOps::new(self.build_bytecode_box(filter),
                                                  &up_plan, &up_cols,
                                                  self.prepared_stmt.clone(), &self.arena);
                self.schemas.push(up_cols);
                self.current.push(ArenaBox::new(filtering, self.arena.get_mut()).into());
            }
        }

        if let Some(having) = &mut this.having_clause {
            debug_assert!(!this.group_by_clause.is_empty());
            match projection_col_visitor.try_rewrite_expr(having) {
                Err(e) => {
                    self.rs = e;
                    return;
                }
                Ok(_) => ()
            }
        }

        if !projection_col_visitor.aggregators.is_empty() {
            let up_cols = self.schemas.pop().unwrap();
            let mut aggregators = projection_col_visitor.grab_aggregators();
            match self.reduce_aggregators_returning_ty(&mut aggregators, &up_cols) {
                Err(e) => {
                    self.rs = e;
                    return;
                }
                Ok(_) => ()
            }

            let up_plan = self.current.pop().unwrap();
            let cols =
                self.merge_aggregators_columns_set(&aggregators, &up_cols);

            let group_keys = self.build_bytecode_boxes(&this.group_by_clause);
            let grouping_plan =
                GroupingAggregatorOps::new(group_keys,
                                           aggregators,
                                           up_plan,
                                           self.arena.clone(),
                                           cols.clone(),
                                           self.db.storage.clone());
            self.schemas.push(cols);
            self.current.push(ArenaBox::new(grouping_plan, self.arena.get_mut()).into());
        } else if !this.group_by_clause.is_empty() {
            self.rs = Status::corrupted("Group by clause without aggregator");
            return;
        }

        if let Some(having) = &this.having_clause {
            let up_cols = self.schemas.pop().unwrap();
            let up_plan = self.current.pop().unwrap();

            debug_assert!(!this.group_by_clause.is_empty());
            let filtering_plan = FilteringOps::new(
                self.build_bytecode_box(having),
                &up_plan,
                &up_cols,
                self.prepared_stmt.clone(),
                &self.arena);

            self.schemas.push(up_cols);
            self.current.push(ArenaBox::new(filtering_plan, self.arena.get_mut()).into());
        }

        if !this.order_by_clause.is_empty() {
            let up_cols = self.schemas.pop().unwrap();
            let up_plan = self.current.pop().unwrap();
            let plan = self.make_ordering(&up_cols, &up_plan, &this.order_by_clause,
                                          None, None);
            self.schemas.push(up_cols);
            self.current.push(plan);
        }

        match self.make_projecting(&this.columns, this.alias.as_str()) {
            Err(e) => {
                self.rs = e;
                return;
            }
            Ok(_) => ()
        }

        if this.distinct {
            todo!()
        }

        if outer_limit && limit.is_some() {
            let up_plan = self.current.pop().unwrap();

            let merging = MergingOps::new(ArenaVec::of(up_plan, &self.arena), limit, offset);
            let plan = ArenaBox::new(merging, self.arena.get_mut());
            self.current.push(plan.into());
        }
    }

    fn visit_from_clause(&mut self, this: &mut FromClause) {
        let rs = self.resolve_physical_table(this.name.as_str(), this.alias.as_str());
        if let Err(e) = rs {
            self.rs = e;
            return;
        }

        let (table, cols) = rs.unwrap();
        match self.make_scan_table(&table, &cols, None, None) {
            Err(e) => {
                self.rs = e;
                return;
            }
            Ok(plan) => {
                self.schemas.push(cols);
                self.current.push(plan);
            }
        }
    }

    fn visit_join_clause(&mut self, this: &mut JoinClause) {
        if matches!(this.op, JoinOp::RightOuterJoin) {
            let lhs = self.visit_from_clause(this.lhs.deref_mut());
            if self.rs.is_not_ok() {
                return;
            }
            let (rhs_cols, rhs_plan) = try_make_plan!(self, this.rhs);
            self.choose_join_plan(rhs_cols, rhs_plan, lhs, this.op.clone(),
                                  &mut this.on_clause);
        } else {
            let (lhs_cols, lhs_plan) = try_make_plan!(self, this.lhs);
            let rhs = self.visit_from_clause(this.rhs.deref_mut());
            if self.rs.is_not_ok() {
                return;
            }
            self.choose_join_plan(lhs_cols, lhs_plan, rhs, this.op.clone(),
                                  &mut this.on_clause);
        }
    }
}


// a > 0 and a < 0
//
// a > 0 and a < 100
//
// (0, +) & (-, 100)
// = (0, 100)
//
// a < 0 or a > 100 or a = 200
// (0, +) | (-, 100) | [200, 200]
// = (-, 0) (100, +) [200, 200]
struct PhysicalSelectionAnalyzer {
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    table: Arc<TableHandle>,
    analyzing_vals: ArenaVec<AnalyzedVal>,
    arena: ArenaMut<Arena>,
    rs: Status,
}

// pk > 0 and pk < 100 => scan(pk)
// pk > 0 and k1 > 0 => filter(k1) <- scan(pk)
// pk > 0 or k1 > 0 => scan(pk) merge scan(k1)
#[derive(Debug, Clone)]
struct SelectionSet {
    key_id: u64,
    part_of: usize,
    total: usize,
    ty: ColumnType,
    segments: ArenaVec<SelectionRange>,
}

impl SelectionSet {
    fn intersect(&mut self, other: &ArenaVec<SelectionRange>) {
        let mut rv = ArenaVec::new(&self.segments.owns);
        for a in &self.segments {
            for b in other {
                let r = a.intersect(b);
                if !r.is_empty() {
                    rv.push(r);
                }
            }
        }
        self.segments = Self::merge_overlapped_range(&mut rv)
    }

    fn union(&mut self, other: &ArenaVec<SelectionRange>) {
        for b in other {
            self.segments.push(b.clone());
        }
        self.segments = Self::merge_overlapped_range(&mut self.segments);
    }

    fn merge_overlapped_range(ranges: &mut ArenaVec<SelectionRange>) -> ArenaVec<SelectionRange> {
        ranges.sort_by(|a, b| { a.min.partial_cmp(&b.min).unwrap() });

        match ranges.len() {
            0 | 1 => ranges.clone(), // fast path
            _ => {
                let mut rv = ArenaVec::new(&ranges.owns);
                match ranges[0].union(&ranges[1]) {
                    Some(r) => rv.push(r),
                    None => {
                        rv.push(ranges[0].clone());
                        rv.push(ranges[1].clone());
                    }
                }
                for i in 2..ranges.len() {
                    let mut merged = false;
                    let b = &ranges[i];
                    for j in 0..rv.len() {
                        let a = &rv[j];
                        if let Some(r) = a.union(b) {
                            rv[j] = r.clone();
                            merged = true;
                            break;
                        }
                    }
                    if !merged {
                        rv.push(b.clone());
                    }
                }
                rv
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct SelectionRange {
    min: Value,
    max: Value,
    left_close: bool,
    right_close: bool,
}

impl SelectionRange {
    fn from_min_to_max(min: Value, max: Value, left_close: bool, right_close: bool) -> Self {
        Self {
            min,
            max,
            left_close,
            right_close,
        }
    }

    fn from_min_to_positive_infinity(min: Value, close: bool) -> Self {
        Self {
            min,
            max: Value::PositiveInf,
            left_close: close,
            right_close: false,
        }
    }

    fn from_negative_infinity_to_max(max: Value, close: bool) -> Self {
        Self {
            min: Value::NegativeInf,
            max,
            left_close: false,
            right_close: close,
        }
    }

    fn from_point(point: Value) -> Self {
        Self {
            min: point.clone(),
            max: point,
            left_close: true,
            right_close: true,
        }
    }

    fn inf() -> Self {
        Self {
            min: Value::NegativeInf,
            max: Value::PositiveInf,
            left_close: false,
            right_close: false,
        }
    }

    fn empty() -> Self {
        Self {
            min: Value::PositiveInf,
            max: Value::NegativeInf,
            left_close: true,
            right_close: true,
        }
    }

    fn is_empty(&self) -> bool {
        matches!(self.min, Value::PositiveInf) && matches!(self.max, Value::NegativeInf)
    }

    fn is_inf(&self) -> bool {
        matches!(self.min, Value::NegativeInf) && matches!(self.max, Value::PositiveInf)
    }

    fn is_point(&self) -> bool { self.min == self.max && self.left_close && self.right_close }

    // (-∞, 1) ∩ [1, +∞) => Ø
    // (-∞, 1] ∩ [1, +∞) => [1, 1]
    // (-∞, 100] ∩ [12, +∞) => [12, 100]
    // (0, 100) ∩ (1,99) => (1, 99)
    fn intersect(&self, other: &Self) -> Self {
        let mut rv = Self::empty();
        if self.is_contain_of(other) {
            rv = other.clone();
            rv.left_close = self.left_should_close(other);
            rv.right_close = self.right_should_close(other);
        } else if other.is_contain_of(self) {
            rv = self.clone();
            rv.left_close = other.left_should_close(self);
            rv.right_close = other.right_should_close(self);
        } else if self.min <= other.min && self.is_overlapped_of(other) {
            rv.min = other.min.clone();
            rv.left_close = other.left_close;
            rv.max = self.max.clone();
            rv.right_close = self.right_close;
        } else if self.min >= other.min && other.is_overlapped_of(self) {
            rv.min = self.min.clone();
            rv.left_close = self.left_close;
            rv.max = other.max.clone();
            rv.right_close = other.right_close;
        }
        rv
    }

    // (-∞, 1) ∪ (1, +∞) => (-∞, 1),(1, +∞)
    // (-∞, 1] ∪ [1, +∞) => (-∞, +∞)
    // (0, 100) ∪ (1,99) => (0, 100)
    fn union(&self, other: &Self) -> Option<Self> {
        if self.is_contain_of(other) {
            Some(self.clone())
        } else if other.is_contain_of(self) {
            Some(other.clone())
        } else if self.min <= other.min && self.is_continuous_of(other) {
            Some(Self::from_min_to_max(self.min.clone(), other.max.clone(),
                                       self.left_close, other.right_close))
        } else if self.min >= other.min && other.is_continuous_of(self) {
            Some(Self::from_min_to_max(other.min.clone(), self.max.clone(),
                                       other.left_close, self.right_close))
        } else {
            None
        }
    }

    fn left_should_close(&self, other: &Self) -> bool {
        if self.min == other.min {
            self.left_close && other.left_close
        } else {
            other.left_close
        }
    }

    fn right_should_close(&self, other: &Self) -> bool {
        if self.max == other.max {
            self.right_close && other.right_close
        } else {
            other.right_close
        }
    }

    fn is_continuous_of(&self, other: &Self) -> bool {
        other.min < self.max || (other.min == self.max && (other.left_close || self.right_close))
    }

    fn is_overlapped_of(&self, other: &Self) -> bool {
        other.min < self.max || (other.min == self.max && other.left_close && self.right_close)
    }

    fn is_contain_of(&self, other: &Self) -> bool {
        other.min >= self.min && other.max <= self.max
    }

    fn to_string(&self) -> String { format!("{self}") }
}

impl Display for SelectionRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{},{}{}",
               if self.left_close { '[' } else { '(' },
               self.min,
               self.max,
               if self.right_close { ']' } else { ')' })
    }
}

type AnalyzedVal = GenericAnalyzedVal<SelectionSet>;

//#[derive(Debug, PartialEq)]
enum GenericAnalyzedVal<T> {
    Set(ArenaVec<T>),
    PrimaryKey,
    PartOfPrimaryKey(usize),
    Index(u64),
    PartOfIndex(u64, usize),
    NeedEval(ArenaBox<dyn Expression>),
    Integral(i64),
    Floating(f64),
    String(ArenaStr),
    And(ArenaVec<T>, ArenaBox<dyn Expression>),
    Or(ArenaVec<T>, ArenaBox<dyn Expression>),
    Null,
}

impl<T> GenericAnalyzedVal<T> {
    fn unwrap_set(self) -> ArenaVec<T> {
        if let Self::Set(set) = self {
            set
        } else {
            panic!("called `AnalyzedVal::unwrap_set()` on a non-`Set` value")
        }
    }

    fn unwrap_and(self) -> (ArenaVec<T>, ArenaBox<dyn Expression>) {
        if let Self::And(set, expr) = self {
            (set, expr)
        } else {
            panic!("called `AnalyzedVal::unwrap_and()` on a non-`And` value")
        }
    }

    fn unwrap_need_eval(self) -> ArenaBox<dyn Expression> {
        if let Self::NeedEval(expr) = self {
            expr
        } else {
            panic!("called `AnalyzedVal::unwrap_need_eval()` on a non-`NeedEval` value")
        }
    }

    fn need_eval<U: Expression + 'static>(expr: &mut U) -> Self {
        Self::NeedEval(ArenaBox::from(expr).into())
    }

    fn is_key(&self) -> bool {
        self.full_cover_key_id().is_some() || self.partial_cover_key_id().is_some()
    }

    fn full_cover_key_id(&self) -> Option<u64> {
        match self {
            Self::PrimaryKey => Some(0),
            Self::Index(key_id) => Some(*key_id),
            _ => None
        }
    }

    fn partial_cover_key_id(&self) -> Option<(u64, usize)> {
        match self {
            Self::PartOfPrimaryKey(order) => Some((0, *order)),
            Self::PartOfIndex(key_id, order) => Some((*key_id, *order)),
            _ => None
        }
    }

    fn part_of_key(&self) -> Option<(u64, usize)> {
        match self {
            Self::PrimaryKey => Some((0, 0)),
            Self::Index(key_id) => Some((*key_id, 0)),
            Self::PartOfPrimaryKey(order) => Some((0, *order)),
            Self::PartOfIndex(key_id, order) => Some((*key_id, *order)),
            _ => None
        }
    }

    fn is_constant(&self) -> bool {
        match self {
            Self::Integral(_) | Self::Floating(_) | Self::String(_) => true,
            _ => false
        }
    }

    fn is_set(&self) -> bool {
        match self {
            Self::Set(_) => true,
            _ => false
        }
    }

    fn is_logic(&self) -> bool {
        match self {
            Self::And(_, _) | Self::Or(_, _) => true,
            _ => false
        }
    }

    fn is_null(&self) -> bool { matches!(self, Self::Null) }
}


impl PhysicalSelectionAnalyzer {
    fn new(table: &Arc<TableHandle>, arena: &ArenaMut<Arena>,
           prepared_stmt: Option<ArenaBox<PreparedStatement>>) -> Self {
        Self {
            prepared_stmt,
            table: table.clone(),
            analyzing_vals: ArenaVec::new(arena),
            arena: arena.clone(),
            rs: Status::Ok,
        }
    }

    fn analyze(&mut self, expr: &mut dyn Expression) -> Result<AnalyzedVal> {
        self.rs = Status::Ok;
        self.analyzing_vals.clear();
        expr.accept(self);
        if self.rs.is_not_ok() {
            return Err(self.rs.clone());
        }
        Ok(self.analyzing_vals.pop().unwrap())
    }

    fn ret(&mut self, kind: AnalyzedVal) { self.analyzing_vals.push(kind); }

    fn analyzer_expr(&mut self, expr: &mut dyn Expression) -> Result<AnalyzedVal> {
        expr.accept(self);
        if self.rs.is_not_ok() {
            Err(self.rs.clone())
        } else {
            Ok(self.analyzing_vals.pop().unwrap())
        }
    }

    fn merge_selection_sets(&mut self, a: &mut ArenaVec<SelectionSet>, b: &ArenaVec<SelectionSet>,
                            op: &Operator) -> bool {
        let mut processed_pairs = ArenaVec::new(&self.arena);
        for dst in a.iter_mut() {
            for src in b {
                if dst.key_id == src.key_id && dst.part_of == src.part_of {
                    match op {
                        Operator::And => dst.intersect(&src.segments),
                        Operator::Or => dst.union(&src.segments),
                        _ => unreachable!()
                    }
                    processed_pairs.push((dst.key_id, dst.part_of));
                }
            }
        }
        for x in b.iter().filter(|x| {
            processed_pairs.iter()
                .find(|(key_id, part_of)| {
                    x.key_id == *key_id && x.part_of == *part_of
                }).is_none()
        }) {
            if op == &Operator::And {
                return false;
            }
            a.push(x.clone());
        }
        true
    }

    fn build_selection_set(&mut self,
                           key: AnalyzedVal,
                           constant: AnalyzedVal,
                           op: &Operator,
                           reserve: bool) -> Option<SelectionSet> {
        let (key_id, order) = match key {
            AnalyzedVal::PrimaryKey => (0, None),
            AnalyzedVal::Index(key_id) => (key_id, None),
            AnalyzedVal::PartOfPrimaryKey(order) => (0, Some(order)),
            AnalyzedVal::PartOfIndex(key_id, order) => (key_id, Some(order)),
            _ => unreachable!()
        };
        let col = if key_id == 0 {
            let col_id = match order {
                Some(pos) => self.table.metadata.primary_keys[pos],
                None => self.table.metadata.primary_keys[0],
            };
            self.table.get_col_by_id(col_id).unwrap()
        } else {
            let idx = self.table.get_2rd_idx_by_id(key_id).unwrap();
            let col_id = match order {
                Some(pos) => idx.key_parts[pos],
                None => idx.key_parts[0],
            };
            self.table.get_col_by_id(col_id).unwrap()
        };

        let boundary = match Self::require_val(&constant, &col.ty) {
            Some(val) => val,
            None => return None
        };

        let segments = match op {
            Operator::Lt | Operator::Le => {
                let close = op == &Operator::Le;
                ArenaVec::of(if reserve {
                    SelectionRange::from_min_to_positive_infinity(boundary, close)
                } else {
                    SelectionRange::from_negative_infinity_to_max(boundary, close)
                }, &self.arena)
            }
            Operator::Gt | Operator::Ge => {
                let close = op == &Operator::Ge;
                ArenaVec::of(if reserve {
                    SelectionRange::from_negative_infinity_to_max(boundary, close)
                } else {
                    SelectionRange::from_min_to_positive_infinity(boundary, close)
                }, &self.arena)
            }
            Operator::Eq => {
                ArenaVec::of(SelectionRange::from_point(boundary), &self.arena)
            }
            Operator::Ne => {
                let mut ranges = ArenaVec::new(&self.arena);
                ranges.push(SelectionRange::from_negative_infinity_to_max(boundary.clone(), false));
                ranges.push(SelectionRange::from_min_to_positive_infinity(boundary, false));
                ranges
            }
            _ => unreachable!()
        };
        Some(SelectionSet {
            key_id,
            part_of: order.map_or(0, |x| { x }),
            total: 0, // TODO:
            ty: col.ty.clone(),
            segments,
        })
    }

    fn reduce_maybe_all_constant(&mut self, input: &[ArenaBox<dyn Expression>]) -> Result<(ArenaVec<AnalyzedVal>, bool)> {
        let mut rv = arena_vec!(&self.arena);
        let mut all_constant = true;
        for it in input {
            let mut expr = it.clone();
            let val = self.analyzer_expr(expr.deref_mut())?;
            if !val.is_constant() {
                all_constant = false;
            }
            rv.push(val);
        }
        Ok((rv, all_constant))
    }

    fn require_val(constant: &AnalyzedVal, ty: &ColumnType) -> Option<Value> {
        if ty.is_integral() {
            Self::require_i64(&constant).map_or(None, |x| { Some(x.into()) })
        } else if ty.is_floating() {
            Self::require_f64(&constant).map_or(None, |x| { Some(x.into()) })
        } else if ty.is_string() {
            Self::require_str(&constant).map_or(None, |x| { Some(x.into()) })
        } else {
            unreachable!()
        }
    }

    fn require_i64(val: &AnalyzedVal) -> Option<i64> {
        match val {
            AnalyzedVal::Integral(n) => Some(*n),
            AnalyzedVal::Floating(n) => Some(*n as i64),
            AnalyzedVal::String(s) => {
                match i64::from_str_radix(s.as_str(), 10) {
                    Ok(n) => return Some(n),
                    Err(_) => ()
                }
                match f64::from_str(s.as_str()) {
                    Ok(n) => Some(n as i64),
                    Err(_) => None
                }
            }
            _ => None
        }
    }

    fn require_f64(val: &AnalyzedVal) -> Option<f64> {
        match val {
            AnalyzedVal::Integral(n) => Some(*n as f64),
            AnalyzedVal::Floating(n) => Some(*n),
            AnalyzedVal::String(s) => {
                match i64::from_str_radix(s.as_str(), 10) {
                    Ok(n) => return Some(n as f64),
                    Err(_) => ()
                }
                match f64::from_str(s.as_str()) {
                    Ok(n) => Some(n),
                    Err(_) => None
                }
            }
            _ => None
        }
    }

    fn require_str(val: &AnalyzedVal) -> Option<ArenaStr> {
        match val {
            AnalyzedVal::String(s) => Some(s.clone()),
            _ => None
        }
    }
}

impl Visitor for PhysicalSelectionAnalyzer {
    fn visit_identifier(&mut self, this: &mut Identifier) {
        let maybe_col = self.table.get_col_by_name(&this.symbol.to_string());
        if maybe_col.is_none() {
            visit_fatal!(self, "Column: {} not found in {}", this.symbol, self.table.metadata.name);
        }
        let col_id = maybe_col.unwrap().id;
        let is_col_be_part_of_primary_key =
            self.table.is_col_be_part_of_primary_key_by_name(&this.symbol.to_string());
        if is_col_be_part_of_primary_key {
            if self.table.metadata.primary_keys.len() == 1 {
                self.ret(AnalyzedVal::PrimaryKey);
            } else {
                for i in 0..self.table.metadata.primary_keys.len() {
                    if self.table.metadata.primary_keys[i] == col_id {
                        self.ret(AnalyzedVal::PartOfPrimaryKey(i));
                        break;
                    }
                }
            }
        } else {
            let rs =
                self.table.get_col_be_part_of_2rd_idx_by_name(&this.symbol.to_string());
            match rs.cloned() {
                Some(idx) => if idx.key_parts.len() == 1 {
                    debug_assert_eq!(&col_id, idx.key_parts.first().unwrap());
                    self.ret(AnalyzedVal::Index(idx.id));
                } else {
                    for i in 0..idx.key_parts.len() {
                        if idx.key_parts[i] == col_id {
                            self.ret(AnalyzedVal::PartOfIndex(idx.id, i));
                        }
                    }
                }
                None => self.ret(AnalyzedVal::need_eval(this)),
            }
        }
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        self.ret(AnalyzedVal::need_eval(this));
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        match this.op() {
            Operator::IsNull => {
                todo!()
            }
            Operator::IsNotNull => {
                todo!()
            }
            Operator::Not => {
                todo!()
            }
            _ => self.ret(AnalyzedVal::need_eval(this))
        }
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        let lhs;
        let rhs;
        match self.analyzer_expr(this.lhs_mut().deref_mut()) {
            Ok(kind) => lhs = kind,
            Err(_) => return
        }
        match self.analyzer_expr(this.rhs_mut().deref_mut()) {
            Ok(kind) => rhs = kind,
            Err(_) => return
        }


        match this.op() {
            Operator::Lt | Operator::Le | Operator::Gt | Operator::Ge | Operator::Eq | Operator::Ne => {
                let op = this.op().clone();
                let default_val = AnalyzedVal::need_eval(this);
                let analyzed = if lhs.is_key() && rhs.is_constant() {
                    self.build_selection_set(lhs, rhs, &op, false)
                        .map_or(default_val, |x| {
                            AnalyzedVal::Set(ArenaVec::of(x, &self.arena))
                        })
                } else if lhs.is_constant() && rhs.is_key() {
                    self.build_selection_set(rhs, lhs, &op, true)
                        .map_or(default_val, |x| {
                            AnalyzedVal::Set(ArenaVec::of(x, &self.arena))
                        })
                } else {
                    default_val
                };
                self.ret(analyzed);
            }
            Operator::And | Operator::Or => {
                if lhs.is_set() && rhs.is_set() {
                    let mut a = match lhs {
                        AnalyzedVal::Set(a) => a,
                        _ => unreachable!()
                    };
                    let b = match rhs {
                        AnalyzedVal::Set(b) => b,
                        _ => unreachable!()
                    };
                    if !self.merge_selection_sets(&mut a, &b, this.op()) {
                        self.rs = Status::NotFound;
                        return;
                    }
                    self.ret(AnalyzedVal::Set(a));
                } else if lhs.is_set() && !rhs.is_set() {
                    let a = match lhs {
                        AnalyzedVal::Set(a) => a,
                        _ => unreachable!()
                    };
                    if this.op() == &Operator::And {
                        self.ret(AnalyzedVal::And(a, this.rhs_mut().clone()));
                    } else {
                        self.ret(AnalyzedVal::need_eval(this))
                    }
                } else if !lhs.is_set() && rhs.is_set() {
                    let b = match rhs {
                        AnalyzedVal::Set(b) => b,
                        _ => unreachable!()
                    };
                    if this.op() == &Operator::And {
                        self.ret(AnalyzedVal::And(b, this.lhs_mut().clone()));
                    } else {
                        self.ret(AnalyzedVal::need_eval(this))
                    }
                } else {
                    self.ret(AnalyzedVal::need_eval(this));
                }
            }
            _ => self.ret(AnalyzedVal::need_eval(this))
        }
    }

    fn visit_in_literal_set(&mut self, this: &mut InLiteralSet) {
        if this.not_in {
            self.ret(AnalyzedVal::need_eval(this));
            return;
        }

        let lhs;
        match self.analyzer_expr(this.lhs_mut().deref_mut()) {
            Ok(kind) => lhs = kind,
            Err(_) => return
        }
        let rhs;
        let all_constant;
        match self.reduce_maybe_all_constant(&this.set) {
            Err(e) => {
                self.rs = e;
                return;
            }
            Ok(rv) => {
                rhs = rv.0;
                all_constant = rv.1;
            }
        }
        if !lhs.is_key() || !all_constant {
            self.ret(AnalyzedVal::need_eval(this));
            return;
        }

        let mut ranges = SelectionSet {
            key_id: 0,
            part_of: 0,
            total: 0,
            ty: ColumnType::Null,
            segments: arena_vec!(&self.arena),
        };
        let ty = match lhs {
            AnalyzedVal::PrimaryKey => {
                ranges.key_id = 0;
                ranges.part_of = 0;
                ranges.total = 1;
                let idx = self.table.get_index_refs_by_id(0).unwrap();
                idx.key_parts[0].ty.clone()
            }
            AnalyzedVal::PartOfPrimaryKey(i) => {
                let idx = self.table.get_index_refs_by_id(0).unwrap();
                ranges.key_id = 0;
                ranges.part_of = i;
                ranges.total = idx.key_parts.len();
                idx.key_parts[i].ty.clone()
            }
            AnalyzedVal::Index(key_id) => {
                let idx = self.table.get_index_refs_by_id(key_id).unwrap();
                ranges.key_id = idx.id;
                ranges.part_of = 0;
                ranges.total = 1;
                idx.key_parts[0].ty.clone()
            }
            AnalyzedVal::PartOfIndex(key_id, i) => {
                let idx = self.table.get_index_refs_by_id(key_id).unwrap();
                ranges.key_id = idx.id;
                ranges.part_of = i;
                ranges.total = idx.key_parts.len();
                idx.key_parts[i].ty.clone()
            }
            _ => unreachable!()
        };
        ranges.ty = ty.clone();

        for val in &rhs {
            let point = Self::require_val(val, &ty).unwrap();
            ranges.segments.push(SelectionRange::from_point(point));
        }
        self.ret(AnalyzedVal::Set(ArenaVec::of(ranges, &self.arena)))
    }

    fn visit_between_and(&mut self, this: &mut BetweenAnd) {
        let lhs;
        match self.analyzer_expr(this.matched_mut().deref_mut()) {
            Ok(kind) => lhs = kind,
            Err(_) => return
        }

        if !lhs.is_key() {
            self.ret(AnalyzedVal::need_eval(this));
            return;
        }

        let lower;
        match self.analyzer_expr(this.lower_mut().deref_mut()) {
            Ok(kind) => lower = kind,
            Err(_) => return
        }
        let upper;
        match self.analyzer_expr(this.upper_mut().deref_mut()) {
            Ok(kind) => upper = kind,
            Err(_) => return
        }
        if upper.is_null() || lower.is_null() {
            self.ret(AnalyzedVal::Null);
            return;
        }
        if !upper.is_constant() || !lower.is_constant() {
            self.ret(AnalyzedVal::need_eval(this));
            return;
        }

        let (key_id, part_of) = lhs.part_of_key().unwrap();
        let index = self.table.get_index_refs_by_id(key_id).unwrap();
        let col = index.key_parts[part_of];
        let mut upper = Self::require_val(&upper, &col.ty).unwrap();
        let mut lower = Self::require_val(&lower, &col.ty).unwrap();
        if lower > upper {
            swap(&mut lower, &mut upper);
        }

        let mut sel = SelectionSet {
            key_id,
            part_of,
            total: index.key_parts.len(),
            ty: col.ty.clone(),
            segments: arena_vec!(&self.arena),
        };
        if this.not_between {
            sel.segments.push(SelectionRange::from_negative_infinity_to_max(lower, false));
            sel.segments.push(SelectionRange::from_min_to_positive_infinity(upper, false));
        } else {
            sel.segments.push(SelectionRange::from_min_to_max(lower, upper, true, true));
        }

        self.ret(AnalyzedVal::Set(ArenaVec::of(sel, &self.arena)));
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        self.ret(AnalyzedVal::Integral(this.data));
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        self.ret(AnalyzedVal::Floating(this.data));
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        self.ret(AnalyzedVal::String(this.data.clone()));
    }

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {
        self.ret(AnalyzedVal::Null);
    }

    fn visit_placeholder(&mut self, this: &mut Placeholder) {
        if self.prepared_stmt.is_none() {
            visit_fatal!(self, "Not prepared statement for '?' placeholder");
        }
        let prepared_stmt = self.prepared_stmt.as_ref().unwrap().clone();
        if !prepared_stmt.all_bound() {
            visit_fatal!(self, "Not all '?' placeholder has been bound in prepared statement");
        }
        match &prepared_stmt.parameters[this.order] {
            Value::Int(n) => self.ret(AnalyzedVal::Integral(*n)),
            Value::Float(n) => self.ret(AnalyzedVal::Floating(*n)),
            Value::Str(s) => self.ret(AnalyzedVal::String(s.clone())),
            Value::Null => self.ret(AnalyzedVal::Null),
            _ => unreachable!()
        }
    }
}

struct PhysicalJoinConditionAnalyzer {
    analyzed_vals: ArenaVec<JoinConditionVal>,
    driver_cols: ArenaBox<ColumnSet>,
    matcher_cols: ArenaBox<ColumnSet>,
    matcher: TableRef,
    arena: ArenaMut<Arena>,
    rs: Status,
}

struct JoinExplainedVal {
    matching_key_id: u64,
    matching_key_bundle: ArenaVec<(ArenaBox<dyn Expression>, ColumnType)>,
    filtering: Option<ArenaBox<dyn Expression>>,
}

impl JoinExplainedVal {
    fn new(matching_key_id: u64,
           matching_key_bundle: ArenaVec<(ArenaBox<dyn Expression>, ColumnType)>,
           filtering: Option<ArenaBox<dyn Expression>>) -> Self {
        Self {
            matching_key_id,
            matching_key_bundle,
            filtering,
        }
    }
}

#[derive(Clone)]
struct JoinConditionKeyPart {
    key_id: u64,
    part_of: usize,
    total: usize,
    matching: ArenaBox<dyn Expression>,
    ty: ColumnType,
}

impl JoinConditionKeyPart {
    fn with_primary_key(table: &TableRef, part_of: usize, matching: &ArenaBox<dyn Expression>) -> Self {
        let col_id = table.metadata.primary_keys[part_of];
        let col = table.get_col_by_id(col_id).unwrap();
        Self {
            key_id: 0,
            part_of,
            total: table.metadata.primary_keys.len(),
            matching: matching.clone(),
            ty: col.ty.clone(),
        }
    }

    fn with_2rd_index(table: &TableRef, key_id: u64, part_of: usize, matching: &ArenaBox<dyn Expression>) -> Self {
        let idx = table.get_2rd_idx_by_id(key_id).unwrap();
        let col_id = idx.key_parts[part_of];
        let col = table.get_col_by_id(col_id).unwrap();
        Self {
            key_id,
            part_of,
            total: idx.key_parts.len(),
            matching: matching.clone(),
            ty: col.ty.clone(),
        }
    }
}

type JoinConditionVal = GenericAnalyzedVal<JoinConditionKeyPart>;

impl PhysicalJoinConditionAnalyzer {
    fn new(driver_cols: &ArenaBox<ColumnSet>,
           matcher_cols: &ArenaBox<ColumnSet>,
           matcher: &TableRef,
           arena: &ArenaMut<Arena>) -> Self {
        Self {
            analyzed_vals: arena_vec!(arena),
            driver_cols: driver_cols.clone(),
            matcher_cols: matcher_cols.clone(),
            matcher: matcher.clone(),
            arena: arena.clone(),
            rs: Status::Ok,
        }
    }

    fn visit(&mut self, expr: &mut dyn Expression) -> Result<JoinConditionVal> {
        self.analyzed_vals.clear();
        self.rs = Status::Ok;
        expr.accept(self);
        if self.rs.is_not_ok() {
            Err(self.rs.clone())
        } else {
            Ok(self.analyzed_vals.pop().unwrap())
        }
    }

    fn explain(&self, val: JoinConditionVal) -> Option<JoinExplainedVal> {
        // ArenaVec<(ArenaBox<dyn Expression>, ColumnType)>
        // expr
        match val {
            JoinConditionVal::Set(set) => {
                let mut bundle = arena_vec!(&self.arena);
                if let Some(key_id) = self.explain_key_parts(set, &mut bundle) {
                    Some(JoinExplainedVal::new(key_id, bundle, None))
                } else {
                    None
                }
            }
            JoinConditionVal::And(set, filtering) => {
                let mut bundle = arena_vec!(&self.arena);
                if let Some(key_id) = self.explain_key_parts(set, &mut bundle) {
                    Some(JoinExplainedVal::new(key_id, bundle, Some(filtering)))
                } else {
                    None
                }
            }
            _ => None
        }
    }

    fn explain_key_parts(&self, parts: ArenaVec<JoinConditionKeyPart>,
                         bundle: &mut ArenaVec<(ArenaBox<dyn Expression>, ColumnType)>) -> Option<u64> {
        debug_assert!(!parts.is_empty());
        debug_assert!(bundle.is_empty());
        let key_id = parts.first().unwrap().key_id;
        let mut order = 0;
        for part in &parts {
            if part.key_id != key_id { // different key id
                return None;
            }
            if part.part_of != order { // hole in key parts
                return None;
            }
            bundle.push((part.matching.clone(), part.ty.clone()));
            order += 1;
        }
        Some(key_id)
    }

    fn ret(&mut self, value: JoinConditionVal) {
        self.analyzed_vals.push(value);
    }

    fn try_merge_key_parts(&mut self, a: ArenaVec<JoinConditionKeyPart>, other: JoinConditionVal, origin: &mut BinaryExpression) {
        match other {
            JoinConditionVal::Set(b) => {
                if let Some(parts) = Self::merge_key_parts(a, b) {
                    self.ret(JoinConditionVal::Set(parts));
                } else {
                    self.ret(JoinConditionVal::need_eval(origin));
                }
            }
            JoinConditionVal::NeedEval(expr) =>
                self.ret(JoinConditionVal::And(a, expr)),
            _ => self.ret(JoinConditionVal::need_eval(origin))
        }
    }

    fn merge_key_parts(mut a: ArenaVec<JoinConditionKeyPart>, b: ArenaVec<JoinConditionKeyPart>)
                       -> Option<ArenaVec<JoinConditionKeyPart>> {
        let mut rv = arena_vec!(&a.owns);
        for part in &b {
            for dest in &a {
                if part.key_id == dest.key_id && part.part_of == dest.part_of {
                    return None;
                }
            }
            rv.push(part.clone());
        }
        rv.append(&mut a);
        rv.sort_by(|a, b| {
            if a.key_id == b.key_id {
                a.part_of.cmp(&b.part_of)
            } else {
                a.key_id.cmp(&b.key_id)
            }
        });
        Some(rv)
    }
}

impl Visitor for PhysicalJoinConditionAnalyzer {
    fn visit_identifier(&mut self, this: &mut Identifier) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        if this.prefix.as_str() != self.matcher_cols.schema.as_str() {
            self.ret(JoinConditionVal::need_eval(this));
            return;
        }
        let rs = self.matcher.get_col_by_name(&this.suffix.to_string());
        if rs.is_none() {
            visit_fatal!(self, "Column not found by name: {}.{}", this.prefix, this.suffix);
        }
        let col = rs.unwrap();
        if self.matcher.is_col_be_part_of_primary_key_by_name(&this.suffix.to_string()) {
            if self.matcher.metadata.primary_keys.len() == 1 {
                self.ret(JoinConditionVal::PrimaryKey);
            } else {
                for i in 0..self.matcher.metadata.primary_keys.len() {
                    if self.matcher.metadata.primary_keys[i] == col.id {
                        self.ret(JoinConditionVal::PartOfPrimaryKey(i));
                        return;
                    }
                }
                unreachable!();
            }
        } else {
            match self.matcher.get_col_be_part_of_2rd_idx_by_name(&this.suffix.to_string()) {
                Some(idx) => {
                    if idx.key_parts.len() == 1 {
                        self.ret(JoinConditionVal::Index(idx.id));
                    } else {
                        for i in 0..idx.key_parts.len() {
                            if idx.key_parts[i] == col.id {
                                self.ret(JoinConditionVal::PartOfIndex(idx.id, i));
                                return;
                            }
                        }
                        unreachable!()
                    }
                }
                None => self.ret(JoinConditionVal::need_eval(this))
            }
        }
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        try_visit!(self, this.lhs_mut());
        let lhs = self.analyzed_vals.pop().unwrap();
        try_visit!(self, this.rhs_mut());
        let rhs = self.analyzed_vals.pop().unwrap();

        match this.op() {
            Operator::Eq => {
                let matching;
                let expr;
                if lhs.is_key() && !rhs.is_key() {
                    matching = lhs;
                    expr = rhs.unwrap_need_eval();
                } else if !lhs.is_key() && rhs.is_key() {
                    matching = rhs;
                    expr = lhs.unwrap_need_eval();
                } else {
                    return self.ret(JoinConditionVal::need_eval(this));
                }

                let key_part = match matching {
                    JoinConditionVal::PrimaryKey =>
                        JoinConditionKeyPart::with_primary_key(&self.matcher, 0, &expr),
                    JoinConditionVal::PartOfPrimaryKey(order) =>
                        JoinConditionKeyPart::with_primary_key(&self.matcher, order, &expr),
                    JoinConditionVal::Index(key_id) =>
                        JoinConditionKeyPart::with_2rd_index(&self.matcher, key_id, 0, &expr),
                    JoinConditionVal::PartOfIndex(key_id, order) =>
                        JoinConditionKeyPart::with_2rd_index(&self.matcher, key_id, order, &expr),
                    _ => unreachable!()
                };
                self.ret(JoinConditionVal::Set(arena_vec!(&self.arena, [key_part])));
            }
            Operator::And => {
                if let JoinConditionVal::Set(a) = lhs {
                    self.try_merge_key_parts(a, rhs, this);
                } else if let JoinConditionVal::Set(a) = rhs {
                    self.try_merge_key_parts(a, lhs, this);
                } else {
                    self.ret(JoinConditionVal::need_eval(this));
                }
            }
            _ => self.ret(JoinConditionVal::need_eval(this))
        }
    }

    fn visit_in_literal_set(&mut self, this: &mut InLiteralSet) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_in_relation(&mut self, this: &mut InRelation) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_null_literal(&mut self, this: &mut Literal<()>) {
        self.ret(JoinConditionVal::need_eval(this));
    }

    fn visit_fast_access_hint(&mut self, this: &mut FastAccessHint) {
        self.ret(JoinConditionVal::need_eval(this));
    }
}

struct ProjectionColumnsVisitor {
    schema: ArenaBox<ColumnSet>,
    rewriting: ArenaVec<Option<ArenaBox<dyn Expression>>>,
    aggregators: ArenaVec<AggregatorBundle>,
    in_agg_calling: i32,
    projection_fields: ArenaVec<(ArenaStr, ArenaStr)>,
    arena: ArenaMut<Arena>,
    arena_of_ast: ArenaMut<Arena>,
    rs: Status,
}

impl ProjectionColumnsVisitor {
    fn new(schema: &ArenaBox<ColumnSet>, arena_of_ast: &ArenaMut<Arena>, arena: &ArenaMut<Arena>) -> Self {
        Self {
            schema: schema.clone(),
            rewriting: ArenaVec::new(arena),
            aggregators: ArenaVec::new(arena),
            in_agg_calling: 0,
            projection_fields: ArenaVec::new(arena),
            arena: arena.clone(),
            arena_of_ast: arena_of_ast.clone(),
            rs: Status::Ok,
        }
    }

    fn try_rewrite(&mut self, cols: &mut ArenaVec<SelectColumnItem>) -> Result<()> {
        let mut i = 0;
        while i < cols.len() {
            let col = &mut cols[i];
            match &mut col.expr {
                SelectColumn::Expr(expr) => {
                    self.try_rewrite_expr(expr)?;
                    i += 1;
                }
                SelectColumn::Star => {
                    let schema = self.schema.clone();
                    let arena = self.arena.clone();
                    let mut incremental = ArenaVec::new(&arena);
                    self.install_all_schema_fields(schema.schema.as_str(),
                                                   schema.deref(),
                                                   |prefix, name| {
                                                       let id: ArenaBox<dyn Expression> = if prefix.is_empty() {
                                                           Identifier::new(name, &arena).into()
                                                       } else {
                                                           FullyQualifiedName::new(prefix, name, &arena).into()
                                                       };
                                                       incremental.push(SelectColumnItem {
                                                           expr: SelectColumn::Expr(id.into()),
                                                           alias: ArenaStr::default(),
                                                       });
                                                   });
                    i += cols.replace_more(i, &mut incremental);
                }
                SelectColumn::SuffixStar(prefix) => {
                    let schema = self.schema.clone();
                    let arena = self.arena.clone();
                    let mut incremental = ArenaVec::new(&arena);
                    self.install_all_schema_fields(prefix.as_str(),
                                                   schema.deref(),
                                                   |prefix, suffix| {
                                                       let id = FullyQualifiedName::new(prefix, suffix, &arena);
                                                       incremental.push(SelectColumnItem {
                                                           expr: SelectColumn::Expr(id.into()),
                                                           alias: ArenaStr::default(),
                                                       });
                                                   });
                    i += cols.replace_more(i, &mut incremental);
                }
            }
        }
        if self.rs.is_not_ok() {
            Err(self.rs.clone())
        } else {
            Ok(())
        }
    }

    fn try_rewrite_expr(&mut self, expr: &mut ArenaBox<dyn Expression>) -> Result<()> {
        expr.accept(self);
        if self.rs.is_not_ok() {
            return Err(self.rs.clone());
        }
        if let Some(ast) = self.rewrite() {
            utils::replace_expr(expr, ast);
        }
        Ok(())
    }

    fn grab_aggregators(&mut self) -> ArenaVec<AggregatorBundle> {
        replace(&mut self.aggregators, ArenaVec::new(&self.arena))
    }

    fn add_projection_field(&mut self, prefix: ArenaStr, suffix: ArenaStr) {
        let full_name = (prefix, suffix);
        for pair in &self.projection_fields {
            if pair.eq(&full_name) {
                return;
            }
        }
        self.projection_fields.push(full_name);
    }

    fn install_all_schema_fields<F>(&mut self, schema: &str, columns: &ColumnSet, mut callback: F)
        where F: FnMut(&str, &str) {
        if schema == columns.schema.as_str() {
            columns.columns.iter()
                .for_each(|x| {
                    self.add_projection_field(x.desc.clone(), x.name.clone());
                    callback(x.desc.as_str(), x.name.as_str());
                })
        } else {
            columns.columns.iter()
                .filter(|x| { x.desc.as_str() == schema })
                .for_each(|x| {
                    self.add_projection_field(x.desc.clone(), x.name.clone());
                    callback(x.desc.as_str(), x.name.as_str());
                })
        }
    }

    fn enter_aggregator_calling(&mut self, agg: &ArenaBox<dyn Aggregator>,
                                args: &[ArenaBox<dyn Expression>]) -> usize {
        self.in_agg_calling += 1;
        let bundle = AggregatorBundle::new(agg, args, &self.arena);
        let rv = self.aggregators.len();
        self.aggregators.push(bundle);
        rv
    }

    fn exit_aggregator_calling(&mut self) {
        self.in_agg_calling -= 1;
    }

    fn rewrite(&mut self) -> Option<ArenaBox<dyn Expression>> {
        match self.rewriting.pop() {
            Some(top) => top,
            None => None,
        }
    }

    fn dont_rewrite(&mut self) { self.rewriting.push(None); }

    fn want_rewrite(&mut self, ast: ArenaBox<dyn Expression>) { self.rewriting.push(Some(ast)); }
}

pub fn resolve_physical_tables(rel: &ArenaBox<dyn Relation>) -> Result<HashMap<ArenaStr, ArenaBox<FromClause>>> {
    let mut resolver = PhysicalTablesResolver {
        tables: HashMap::default(),
        rs: Status::Ok,
    };
    let mut node = rel.clone();
    node.accept(&mut resolver);
    if resolver.rs.is_not_ok() {
        Err(resolver.rs)
    } else {
        Ok(resolver.tables)
    }
}

struct PhysicalTablesResolver {
    tables: HashMap<ArenaStr, ArenaBox<FromClause>>,
    rs: Status,
}

impl Visitor for PhysicalTablesResolver {
    fn visit_from_clause(&mut self, this: &mut FromClause) {
        let key = if this.alias.is_empty() {
            this.name.clone()
        } else {
            this.alias.clone()
        };

        if self.tables.contains_key(&key) {
            visit_fatal!(self, "Duplicated reference name: {}", key);
        }
        self.tables.insert(key, ArenaBox::from(this));
    }

    fn visit_join_clause(&mut self, this: &mut JoinClause) {
        try_visit!(self, this.lhs);
        try_visit!(self, this.rhs);
    }
}

macro_rules! try_visit {
    ($self:ident, $node:expr) => {
        {
            $node.accept($self);
            if let Some(ast) = $self.rewrite() {
                utils::replace_expr($node, ast);
            }
            if $self.rs.is_not_ok() {
                return;
            }
        }
    }
}

macro_rules! break_visit {
    ($self:ident, $node:expr) => {
        {
            $node.accept($self);
            if let Some(ast) = $self.rewrite() {
                utils::replace_expr($node, ast);
            }
            if $self.rs.is_not_ok() {
                break;
            }
        }
    }
}

impl Visitor for ProjectionColumnsVisitor {
    fn visit_identifier(&mut self, this: &mut Identifier) {
        self.add_projection_field(ArenaStr::default(), this.symbol.clone());
        self.dont_rewrite();
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        self.add_projection_field(this.prefix.clone(), this.suffix.clone());
        self.dont_rewrite();
    }
    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        try_visit!(self, &mut this.operands_mut()[0]);
        self.dont_rewrite();
    }
    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        try_visit!(self, this.lhs_mut());
        try_visit!(self, this.rhs_mut());
        self.dont_rewrite();
    }
    fn visit_in_literal_set(&mut self, this: &mut InLiteralSet) {
        try_visit!(self, this.lhs_mut());
        for literal in this.set.iter_mut() {
            try_visit!(self, literal);
        }
        self.dont_rewrite();
    }
    fn visit_in_relation(&mut self, this: &mut InRelation) {
        try_visit!(self, this.lhs_mut());
        self.dont_rewrite();
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        let ctx = ExecutionContext::new(this.distinct, &self.arena);
        match function::new_udaf(this.callee_name.as_str(), &ctx) {
            Some(udaf) => {
                if self.in_agg_calling > 0 {
                    visit_fatal!(self, "Nested aggregator calling: {}", this.callee_name);
                }

                let fast_access_hint;
                if this.in_args_star {
                    let schema = self.schema.clone();
                    let mut args = ArenaVec::new(&self.arena);
                    self.install_all_schema_fields(schema.schema.as_str(),
                                                   schema.deref(),
                                                   |prefix, name| {
                                                       if prefix.is_empty() {
                                                           args.push(Identifier::new(name, &args.owns).into());
                                                       } else {
                                                           args.push(FullyQualifiedName::new(prefix, name, &args.owns).into());
                                                       }
                                                   });
                    fast_access_hint = self.enter_aggregator_calling(&udaf, &args);
                } else {
                    fast_access_hint = self.enter_aggregator_calling(&udaf, &this.args);
                    for arg in this.args.iter_mut() {
                        break_visit!(self, arg);
                    }
                }
                self.exit_aggregator_calling();
                let this_box = ArenaBox::from(this);
                debug_assert!(self.arena_of_ast.contains_box(&this_box));
                let hint = FastAccessHint::new(fast_access_hint,
                                               this_box.into(),
                                               &self.arena_of_ast); // NOTICE: only for ast!
                self.want_rewrite(hint.into());
            }
            None => {
                for arg in this.args.iter_mut() {
                    break_visit!(self, arg);
                }
                self.dont_rewrite();
            }
        }
    }

    fn visit_int_literal(&mut self, _this: &mut Literal<i64>) { self.dont_rewrite(); }

    fn visit_float_literal(&mut self, _this: &mut Literal<f64>) { self.dont_rewrite(); }

    fn visit_str_literal(&mut self, _this: &mut Literal<ArenaStr>) { self.dont_rewrite(); }

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) { self.dont_rewrite(); }

    fn visit_placeholder(&mut self, _this: &mut Placeholder) { self.dont_rewrite(); }

    fn visit_fast_access_hint(&mut self, this: &mut FastAccessHint) {
        let mut origin = this.origin.clone();
        debug_assert!(!origin.as_any().is::<FastAccessHint>());
        try_visit!(self, &mut origin);

        match origin.as_mut_any().downcast_ref::<FastAccessHint>() {
            Some(hint) => this.offset = hint.offset,
            None => self.want_rewrite(origin),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::exec::db::ColumnType;
    use crate::exec::from_sql_result;
    use crate::sql::ast::*;
    use crate::sql::parser::Parser;
    use crate::sql::serialize::serialize_expr_to_string;
    use crate::storage::{JunkFilesCleaner, MemorySequentialFile};
    use crate::suite::testing::*;

    use super::*;

    #[test]
    fn selection_range_sanity() {
        assert!(SelectionRange::empty().is_empty());
        assert!(SelectionRange::inf().is_inf());
    }

    #[test]
    fn selection_range_intersect_to_point() {
        let a = SelectionRange::from_min_to_max(Value::NegativeInf, Value::Int(1), false, true);
        let b = SelectionRange::from_min_to_max(Value::Int(1), Value::PositiveInf, true, false);
        let r = a.intersect(&b);
        assert_eq!(Value::Int(1), r.min);
        assert_eq!(Value::Int(1), r.max);
        assert!(r.left_close && r.right_close);
    }

    #[test]
    fn selection_range_intersect_to_empty() {
        let a = SelectionRange::from_min_to_max(Value::NegativeInf, Value::Int(1), false, false);
        let b = SelectionRange::from_min_to_max(Value::Int(1), Value::PositiveInf, true, false);
        let r = a.intersect(&b);
        assert!(r.is_empty());
    }

    #[test]
    fn selection_range_intersect_to_range() {
        // (-∞, 100) ∩ [12, +∞) => [12, 100)
        let a = SelectionRange::from_negative_infinity_to_max(Value::Int(100), false);
        let b = SelectionRange::from_min_to_positive_infinity(Value::Int(12), true);
        let r = a.intersect(&b);
        assert_eq!(Value::Int(12), r.min);
        assert!(r.left_close);
        assert_eq!(Value::Int(100), r.max);
        assert!(!r.right_close);
    }

    #[test]
    fn selection_range_intersect_issue_0() {
        // (0, 100) ∩ (99,100] => (99, 100)
        let a = SelectionRange::from_min_to_max(Value::Int(0), Value::Int(100), false, false);
        let b = SelectionRange::from_min_to_max(Value::Int(99), Value::Int(100), false, true);
        let r = a.intersect(&b);
        assert_eq!(Value::Int(99), r.min);
        assert!(!r.left_close);
        assert_eq!(Value::Int(100), r.max);
        assert!(!r.right_close);

        let o = b.intersect(&a);
        assert_eq!(r, o);
    }

    #[test]
    fn selection_range_intersect_issue_1() {
        // (0, 100) ∩ [0,1) => (0, 1)
        let a = SelectionRange::from_min_to_max(Value::Int(0), Value::Int(100), false, false);
        let b = SelectionRange::from_min_to_max(Value::Int(0), Value::Int(1), true, false);
        let r = a.intersect(&b);
        assert_eq!(Value::Int(0), r.min);
        assert!(!r.left_close);
        assert_eq!(Value::Int(1), r.max);
        assert!(!r.right_close);

        let o = b.intersect(&a);
        assert_eq!(r, o);
    }

    #[test]
    fn selection_range_union_to_inf() {
        // (-∞, 1] ∪ [1, +∞) => (-∞, +∞)
        let a = SelectionRange::from_negative_infinity_to_max(Value::Int(1), true);
        let b = SelectionRange::from_min_to_positive_infinity(Value::Int(1), true);
        let r = a.union(&b).unwrap();
        assert!(r.is_inf());
        let r = b.union(&a).unwrap();
        assert!(r.is_inf());
    }

    #[test]
    fn selection_range_union_to_inf2() {
        // (-∞, 1] ∪ (1, +∞) => (-∞, +∞)
        let a = SelectionRange::from_negative_infinity_to_max(Value::Int(1), true);
        let b = SelectionRange::from_min_to_positive_infinity(Value::Int(1), false);
        let r = a.union(&b).unwrap();
        assert!(r.is_inf());
        let r = b.union(&a).unwrap();
        assert!(r.is_inf());
    }

    #[test]
    fn selection_range_union_to_2parts() {
        // (-∞, 1) ∪ (1, +∞) => (-∞, 1), (1, +∞)
        let a = SelectionRange::from_negative_infinity_to_max(Value::Int(1), false);
        let b = SelectionRange::from_min_to_positive_infinity(Value::Int(1), false);
        assert!(a.union(&b).is_none());
        assert!(b.union(&a).is_none());
    }

    #[test]
    fn selection_range_union_to_concat() {
        // (0, 100) ∪ [99, 200) => (0, 200)
        let a = SelectionRange::from_min_to_max(Value::Int(0), Value::Int(100), false, false);
        let b = SelectionRange::from_min_to_max(Value::Int(99), Value::Int(200), true, false);
        let r = a.union(&b).unwrap();
        assert_eq!(Value::Int(0), r.min);
        assert!(!r.left_close);
        assert_eq!(Value::Int(200), r.max);
        assert!(!r.right_close);
        let o = b.union(&a).unwrap();
        assert_eq!(r, o);
    }

    #[test]
    fn selection_range_union_to_contain() {
        // (-∞, +∞) ∪ [-1, 1] => (-∞, +∞)
        let a = SelectionRange::inf();
        let b = SelectionRange::from_min_to_max(Value::Int(-1), Value::Int(1), true, true);
        assert!(a.union(&b).unwrap().is_inf());
        assert!(b.union(&a).unwrap().is_inf());
    }

    #[test]
    fn projecting_field_names_analyzing() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut ast = parse_sql_as_select("select a,b,c;", &arena)?;
        let cols = ArenaBox::new(ColumnSet::new("default", 0, &arena), arena.get_mut());
        let mut visitor = ProjectionColumnsVisitor::new(&cols, &arena, &arena);
        visitor.try_rewrite(&mut ast.columns)?;
        assert!(visitor.aggregators.is_empty());
        assert_eq!(3, visitor.projection_fields.len());
        assert_eq!("a", visitor.projection_fields[0].1.as_str());
        assert_eq!("b", visitor.projection_fields[1].1.as_str());
        assert_eq!("c", visitor.projection_fields[2].1.as_str());
        Ok(())
    }

    #[test]
    fn projecting_aggregator_analyzing() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut ast = parse_sql_as_select("select a + count(a), count(*);", &arena)?;
        let mut cols = ColumnSet::new("default", 0, &arena);
        cols.append("a", "", 0, 1, ColumnType::Int(11));
        cols.append("b", "", 0, 2, ColumnType::Char(9));
        cols.append("c", "", 0, 3, ColumnType::TinyInt(1));
        let mut visitor =
            ProjectionColumnsVisitor::new(&ArenaBox::new(cols, arena.get_mut()), &arena, &arena);
        visitor.try_rewrite(&mut ast.columns)?;

        assert_eq!(2, visitor.aggregators.len());
        assert_eq!(1, visitor.aggregators[0].args_len());
        assert_eq!(3, visitor.aggregators[1].args_len());
        let yaml = if let SelectColumn::Expr(expr) = &mut ast.columns[0].expr {
            serialize_expr_to_string(expr.deref_mut())
        } else {
            String::default()
        };
        assert_eq!("BinaryExpression:
  op: Add(+)
  lhs:
    Identifier: a
  rhs:
    FastAccessHint: 0
", yaml);

        let yaml = if let SelectColumn::Expr(expr) = &mut ast.columns[1].expr {
            serialize_expr_to_string(expr.deref_mut())
        } else {
            String::default()
        };
        assert_eq!("FastAccessHint: 1\n", yaml);

        assert_eq!(3, visitor.projection_fields.len());
        assert_eq!("", visitor.projection_fields[0].0.as_str());
        assert_eq!("a", visitor.projection_fields[0].1.as_str());
        assert_eq!("", visitor.projection_fields[1].0.as_str());
        assert_eq!("b", visitor.projection_fields[1].1.as_str());
        assert_eq!("", visitor.projection_fields[2].0.as_str());
        assert_eq!("c", visitor.projection_fields[2].1.as_str());
        Ok(())
    }

    #[test]
    fn projecting_star_rewriting() -> Result<()> {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut ast = parse_sql_as_select("select a, *, b;", &arena)?;
        let mut cols = ColumnSet::new("default", 0, &arena);
        cols.append("a", "", 0, 1, ColumnType::Int(11));
        cols.append("b", "", 0, 2, ColumnType::Char(9));
        cols.append("c", "", 0, 3, ColumnType::TinyInt(1));
        let mut visitor =
            ProjectionColumnsVisitor::new(&ArenaBox::new(cols, arena.get_mut()), &arena, &arena);

        assert_eq!(3, ast.columns.len());
        visitor.try_rewrite(&mut ast.columns)?;
        assert_eq!(5, ast.columns.len());

        Ok(())
    }

    #[test]
    fn physical_selection_analyzing_simple() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db300");
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let db = DB::open("tests".to_string(), "db300".to_string())?;
        //let n = 10000;
        let conn = db.connect();
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
                index idx_b(b)\n\
            };\n\
            insert into table t1(b) values (\"aaa\"),(\"bbb\"),(\"ccc\");\n\
            ";
        assert_eq!(3, conn.execute_str(sql, &arena)?);
        let table = db._test_get_table_ref("t1").unwrap();
        let stmt = parse_sql_as_select("select * from t1 where a > 100", &arena)?;
        let mut analyzer = PhysicalSelectionAnalyzer::new(&table, &arena, None);
        let mut expr = stmt.where_clause.as_ref().cloned().unwrap();
        let rs = analyzer.analyze(expr.deref_mut())?.unwrap_set();
        assert_eq!(1, rs.len());
        assert_eq!(0, rs[0].key_id);
        assert_eq!(Value::PositiveInf, rs[0].segments[0].max);
        assert_eq!(Value::Int(100), rs[0].segments[0].min);
        assert!(!rs[0].segments[0].left_close);
        assert!(!rs[0].segments[0].right_close);

        let stmt = parse_sql_as_select("select * from t1 where a <> 100", &arena)?;
        let mut expr = stmt.where_clause.as_ref().cloned().unwrap();
        let rs = analyzer.analyze(expr.deref_mut())?.unwrap_set();
        //dbg!(&rs[0]);
        assert_eq!(1, rs.len());
        assert_eq!(0, rs[0].key_id);
        assert_eq!(2, rs[0].segments.len());

        assert_eq!(Value::NegativeInf, rs[0].segments[0].min);
        assert_eq!(Value::Int(100), rs[0].segments[0].max);
        assert!(!rs[0].segments[0].left_close);
        assert!(!rs[0].segments[0].right_close);

        assert_eq!(Value::Int(100), rs[0].segments[1].min);
        assert_eq!(Value::PositiveInf, rs[0].segments[1].max);
        assert!(!rs[0].segments[1].left_close);
        assert!(!rs[0].segments[1].right_close);
        Ok(())
    }

    #[test]
    fn physical_selection_analyzing_issue_0() -> Result<()> {
        let junk = JunkFilesCleaner::new("tests/db301");
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let db = DB::open(junk.ensure().path, junk.ensure().name)?;
        let conn = db.connect();
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            ";
        assert_eq!(0, conn.execute_str(sql, &arena)?);
        let table = db._test_get_table_ref("t1").unwrap();
        let stmt = parse_sql_as_select("select * from t1 where a > 100 and b < \"aaa\"", &arena)?;
        let mut analyzer = PhysicalSelectionAnalyzer::new(&table, &arena, None);
        let mut expr = stmt.where_clause.as_ref().cloned().unwrap();
        let (rs, mut expr) = analyzer.analyze(expr.deref_mut())?.unwrap_and();

        assert_eq!(1, rs.len());
        assert_eq!(0, rs[0].key_id);
        assert_eq!("(100,+∞)", rs[0].segments[0].to_string());

        let yaml = serialize_expr_to_string(expr.deref_mut());
        //println!("{yaml}");
        assert_eq!("BinaryExpression:
  op: Lt(<)
  lhs:
    Identifier: b
  rhs:
    StrLiteral: aaa
", yaml);


        let stmt = parse_sql_as_select("select * from t1 where a > 100 and a <= \"200\"", &arena)?;
        let mut expr = stmt.where_clause.as_ref().cloned().unwrap();
        let rs = analyzer.analyze(expr.deref_mut())?.unwrap_set();
        assert_eq!(1, rs.len());
        assert_eq!(0, rs[0].key_id);
        assert_eq!("(100,200]", rs[0].segments[0].to_string());


        let stmt = parse_sql_as_select("select * from t1 where a <= -100 or a > \"200\"", &arena)?;
        let mut expr = stmt.where_clause.as_ref().cloned().unwrap();
        let rs = analyzer.analyze(expr.deref_mut())?.unwrap_set();
        assert_eq!(1, rs.len());
        assert_eq!(0, rs[0].key_id);
        assert_eq!(2, rs[0].segments.len());
        assert_eq!("(-∞,-100]", rs[0].segments[0].to_string());
        assert_eq!("(200,+∞)", rs[0].segments[1].to_string());
        Ok(())
    }

    #[test]
    fn physical_join_analyzing() -> Result<()> {
        let suite = SqlSuite::new("tests/db302")?;
        suite.execute_file(Path::new("testdata/t1_t2_t3_small_table_without_data_for_join.sql"), &suite.arena)?;
        let t1_cols = suite.get_table_cols_set("t1", "");
        let t2_cols = suite.get_table_cols_set("t2", "");
        let t2 = suite.db._test_get_table_ref("t2").unwrap();

        let mut join = PhysicalJoinConditionAnalyzer::new(&t1_cols, &t2_cols, &t2, &suite.arena);
        let mut expr = suite.parse_expr("t1.id = t2.id")?;
        let rv = join.visit(expr.deref_mut())?;
        assert!(matches!(rv, JoinConditionVal::Set(_)));
        let mut set = rv.unwrap_set();
        assert_eq!(1, set.len());
        assert_eq!(0, set[0].key_id);
        assert_eq!(0, set[0].part_of);
        assert_eq!(1, set[0].total);
        assert_eq!("FullQualifiedName: t1.id\n", serialize_expr_to_string(set[0].matching.deref_mut()));
        assert_eq!(ColumnType::BigInt(11), set[0].ty);

        let mut expr = suite.parse_expr("t1.dc = t2.df")?;
        let rv = join.visit(expr.deref_mut())?;
        assert!(matches!(rv, JoinConditionVal::Set(_)));
        let mut set = rv.unwrap_set();
        assert_eq!(1, set.len());
        assert_eq!(3, set[0].key_id);
        assert_eq!(0, set[0].part_of);
        assert_eq!(1, set[0].total);
        assert_eq!("FullQualifiedName: t1.dc\n", serialize_expr_to_string(set[0].matching.deref_mut()));
        assert_eq!(ColumnType::Int(11), set[0].ty);

        let mut expr = suite.parse_expr("t1.dc = t2.df and t1.dd = t2.de")?;
        let rv = join.visit(expr.deref_mut())?;
        assert!(matches!(rv, JoinConditionVal::And(_, _)));
        let (mut set, mut filter) = rv.unwrap_and();
        assert_eq!(1, set.len());
        assert_eq!(3, set[0].key_id);
        assert_eq!(0, set[0].part_of);
        assert_eq!(1, set[0].total);
        assert_eq!("FullQualifiedName: t1.dc\n", serialize_expr_to_string(set[0].matching.deref_mut()));
        //println!("{}", serialize_expr_to_string(filter.deref_mut()));
        assert_eq!("BinaryExpression:
  op: Eq(=)
  lhs:
    FullQualifiedName: t1.dd
  rhs:
    FullQualifiedName: t2.de
", serialize_expr_to_string(filter.deref_mut()));
        assert_eq!(ColumnType::Int(11), set[0].ty);
        Ok(())
    }

    #[test]
    fn physical_join_multiple_cols_index_analyzing() -> Result<()> {
        let suite = SqlSuite::new("tests/db303")?;
        suite.execute_file(Path::new("testdata/t1_t2_t3_small_table_without_data_for_join.sql"), &suite.arena)?;
        let t1_cols = suite.get_table_cols_set("t1", "");
        let t3_cols = suite.get_table_cols_set("t3", "");
        let t3 = suite.db._test_get_table_ref("t3").unwrap();

        let mut join = PhysicalJoinConditionAnalyzer::new(&t1_cols, &t3_cols, &t3, &suite.arena);
        let mut expr = suite.parse_expr("t1.id = t3.part1")?;
        let rv = join.visit(expr.deref_mut())?;
        assert!(matches!(rv, JoinConditionVal::Set(_)));
        let mut set = rv.unwrap_set();
        assert_eq!(1, set.len());
        assert_eq!(0, set[0].key_id);
        assert_eq!(0, set[0].part_of);
        assert_eq!(3, set[0].total);
        assert_eq!("FullQualifiedName: t1.id\n", serialize_expr_to_string(set[0].matching.deref_mut()));
        assert_eq!(ColumnType::BigInt(11), set[0].ty);

        let mut expr = suite.parse_expr("t1.id = t3.part1 and t1.dd = t3.part2")?;
        let rv = join.visit(expr.deref_mut())?;
        assert!(matches!(rv, JoinConditionVal::Set(_)));
        let mut set = rv.unwrap_set();
        assert_eq!(2, set.len());
        assert_eq!(0, set[0].key_id);
        assert_eq!(0, set[0].part_of); // key part 1
        assert_eq!(3, set[0].total);
        assert_eq!("FullQualifiedName: t1.id\n", serialize_expr_to_string(set[0].matching.deref_mut()));
        assert_eq!(ColumnType::BigInt(11), set[0].ty);
        assert_eq!(0, set[1].key_id);
        assert_eq!(1, set[1].part_of); // key part 2
        assert_eq!(3, set[1].total);
        assert_eq!("FullQualifiedName: t1.dd\n", serialize_expr_to_string(set[1].matching.deref_mut()));
        assert_eq!(ColumnType::Int(11), set[1].ty);

        let rv = join.visit(expr.deref_mut())?;
        let explained = join.explain(rv).unwrap();
        assert!(explained.filtering.is_none());
        assert_eq!(2, explained.matching_key_bundle.len());
        Ok(())
    }

    fn parse_sql_as_select(sql: &str, arena: &ArenaMut<Arena>) -> Result<ArenaBox<Select>> {
        let stmt = parse_sql(sql, arena)?;
        if !stmt.as_any().is::<Select>() {
            return Err(Status::corrupted("Not select statement"));
        }
        Ok(stmt.into())
    }

    fn parse_sql(sql: &str, arena: &ArenaMut<Arena>) -> Result<ArenaBox<dyn Statement>> {
        let mut rd = MemorySequentialFile::new(sql.to_string().into());
        let factory = Factory::new(arena);
        let mut parser = from_sql_result(Parser::new(&mut rd, factory))?;
        let stmts = from_sql_result(parser.parse())?;
        Ok(stmts[0].clone())
    }
}