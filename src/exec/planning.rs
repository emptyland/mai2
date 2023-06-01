use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;
use crate::sql::ast::*;
use crate::{break_visit, Corrupting, Result, Status, try_visit, visit_fatal};
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaStr, ArenaVec};
use crate::exec::db::{DB, TableHandle};
use crate::exec::evaluator::Value;
use crate::exec::executor::{Column, ColumnSet};
use crate::exec::function;
use crate::exec::function::{Aggregator, ExecutionContext};
use crate::exec::physical_plan::{PhysicalPlanOps, RangeScanOps};


struct PlanMaker {
    db: Arc<DB>,
    arena: ArenaMut<Arena>,
    rs: Status,
    schemas: ArenaVec<ArenaBox<ColumnSet>>,
    current: ArenaVec<ArenaBox<dyn PhysicalPlanOps>>,
}

impl PlanMaker {
    pub fn new(db: &Arc<DB>, arena: &ArenaMut<Arena>) -> Self {
        Self {
            db: db.clone(),
            arena: arena.clone(),
            rs: Status::Ok,
            schemas: ArenaVec::new(arena),
            current: ArenaVec::new(arena),
        }
    }


    fn visit_from_clause(&mut self, from: &mut dyn Relation) -> Option<Arc<TableHandle>> {
        if let Some(table_ref) = from.as_any().downcast_ref::<FromClause>() {
            let tables = self.db.lock_tables();
            let rs = tables.get(table_ref.name.as_str());
            if rs.is_none() {
                self.rs = Status::corrupted(format!("Table: {} not found", table_ref.name));
                return None;
            }
            let table = rs.unwrap().clone();
            let schema = if table_ref.alias.is_empty() {
                table_ref.name.as_str()
            } else {
                table_ref.alias.as_str()
            };
            let mut columns = ColumnSet::new(schema, &self.arena);
            for col in &table.metadata.columns {
                columns.append_with_name(col.name.as_str(), col.ty.clone());
            }
            self.schemas.push(ArenaBox::new(columns, self.arena.deref_mut()));
            Some(table)
        } else {
            from.accept(self);
            None
        }
    }

    fn top_schema(&self) -> Option<&ArenaBox<ColumnSet>> {
        self.schemas.back()
    }
}


impl Visitor for PlanMaker {
    fn visit_select(&mut self, this: &mut Select) {
        let maybe_table = match &mut this.from_clause {
            Some(from) => self.visit_from_clause(from.deref_mut()),
            None => None
        };
        if self.rs.is_not_ok() {
            return;
        }
        let mut projection_col_visitor =
            ProjectionColumnVisitor::new(self.top_schema().unwrap(), &self.arena);
        for col in this.columns.iter_mut() {
            projection_col_visitor.visit(col);
        }
        if let Some(table) = maybe_table {
            if this.where_clause.is_none() {
                //let plan = RangeScanOps::new()
            }
        }

        match &mut this.where_clause {
            Some(filter) => {}
            None => ()
        }
        todo!()
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
    table: Arc<TableHandle>,
    analyzing_vals: ArenaVec<AnalyzedVal>,
    arena: ArenaMut<Arena>,
    rs: Status,
}

// pk > 0 and pk < 100 => scan(pk)
// pk > 0 and k1 > 0 => filter(k1) <- scan(pk)
// pk > 0 or k1 > 0 => scan(pk) merge scan(k1)
struct SelectionSet {
    key_id: u64,
    part_of: usize,
    total: usize,
    segments: ArenaVec<SelectionRange>,
}

struct SelectionRange {
    min: Value,
    max: Value,
    left_close: bool,
    right_close: bool,
}

impl SelectionRange {
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
}

//#[derive(Debug, PartialEq)]
enum AnalyzedVal {
    Set(ArenaVec<SelectionSet>),
    PrimaryKey,
    PartOfPrimaryKey(usize),
    Index(u64),
    PartOfIndex(u64, usize),
    NeedEval,
    Integral(i64),
    Floating(f64),
    String(ArenaStr),
    Null,
}

impl PhysicalSelectionAnalyzer {

    fn analyze(&mut self, expr: &mut dyn Expression) -> Result<ArenaVec<SelectionSet>> {
        self.rs = Status::Ok;
        self.analyzing_vals.clear();
        expr.accept(self);
        if self.rs.is_not_ok() {
            return Err(self.rs.clone());
        }
        let analyzed_val = self.analyzing_vals.pop().unwrap();
        match analyzed_val {
            AnalyzedVal::Set(set) => Ok(set),
            _ => Err(Status::NotFound)
        }
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

        let boundary = if col.ty.is_integral() {
            let rs = Self::require_i64(&constant);
            if rs.is_none() {
                return None;
            }
            Value::Int(rs.unwrap())
        } else if col.ty.is_floating() {
            let rs = Self::require_f64(&constant);
            if rs.is_none() {
                return None;
            }
            Value::Float(rs.unwrap())
        } else if col.ty.is_string() {
            let rs = Self::require_str(&constant);
            if rs.is_none() {
                return None;
            }
            Value::Str(rs.unwrap())
        } else {
            unreachable!()
        };

        let segments = match op {
            Operator::Lt | Operator::Le => {
                let close = matches!(op, Operator::Le);
                ArenaVec::of(if reserve {
                    SelectionRange::from_min_to_positive_infinity(boundary, close)
                } else {
                    SelectionRange::from_negative_infinity_to_max(boundary, close)
                }, &self.arena)
            }
            Operator::Gt | Operator::Ge => {
                let close = matches!(op, Operator::Ge);
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
            part_of: order.map_or(0, |x|{x}),
            total: 0, // TODO:
            segments,
        })
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

impl AnalyzedVal {
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

    fn is_constant(&self) -> bool {
        match self {
            Self::Integral(_) | Self::Floating(_) | Self::String(_) => true,
            _ => false
        }
    }

    fn should_ignore(&self) -> bool {
        match self {
            Self::NeedEval | Self::Null => true,
            _ => false
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
                None => self.ret(AnalyzedVal::NeedEval),
            }
        }
    }

    fn visit_full_qualified_name(&mut self, _this: &mut FullyQualifiedName) {
        self.ret(AnalyzedVal::NeedEval);
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        match this.op() {
            Operator::Lt | Operator::Le | Operator::Gt | Operator::Ge | Operator::Eq | Operator::Ne => {
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
                let analyzed = if lhs.is_key() && rhs.is_constant() {
                    self.build_selection_set(lhs, rhs, this.op(), false)
                        .map_or(AnalyzedVal::NeedEval, |x| {
                            AnalyzedVal::Set(ArenaVec::of(x, &self.arena))
                        })
                } else if lhs.is_constant() && rhs.is_key() {
                    self.build_selection_set(rhs, lhs, this.op(), true)
                        .map_or(AnalyzedVal::NeedEval, |x| {
                            AnalyzedVal::Set(ArenaVec::of(x, &self.arena))
                        })
                } else {
                    AnalyzedVal::NeedEval
                };
                self.ret(analyzed);
            }
            Operator::And => todo!(),
            Operator::Or => todo!(),
            _ => self.ret(AnalyzedVal::NeedEval)
        }
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

    fn visit_placeholder(&mut self, _this: &mut Placeholder) {}
}

struct ProjectionColumnVisitor {
    schema: ArenaBox<ColumnSet>,
    aggregators: ArenaVec<(ArenaStr, ArenaBox<dyn Aggregator>)>,
    in_agg_calling: i32,
    projection_fields: ArenaVec<(ArenaStr, ArenaStr)>,
    arena: ArenaMut<Arena>,
    rs: Status,
}

impl ProjectionColumnVisitor {
    fn new(schema: &ArenaBox<ColumnSet>, arena: &ArenaMut<Arena>) -> Self {
        Self {
            schema: schema.clone(),
            aggregators: ArenaVec::new(arena),
            in_agg_calling: 0,
            projection_fields: ArenaVec::new(arena),
            arena: arena.clone(),
            rs: Status::Ok,
        }
    }

    fn visit(&mut self, col: &mut SelectColumnItem) -> Status {
        match &mut col.expr {
            SelectColumn::Expr(expr) => expr.accept(self),
            SelectColumn::Star => {
                let schema = self.schema.clone();
                self.install_all_schema_fields(schema.schema.as_str(), schema.deref());
            }
            SelectColumn::SuffixStar(prefix) => {
                let schema = self.schema.clone();
                self.install_all_schema_fields(prefix.as_str(), schema.deref());
            }
        }
        self.rs.clone()
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

    fn install_all_schema_fields(&mut self, schema: &str, columns: &ColumnSet) {
        if schema == columns.schema.as_str() {
            for col in &columns.columns {
                self.add_projection_field(ArenaStr::default(), col.name.clone());
            }
        } else {
            columns.columns.iter()
                .filter(|x| { x.desc.as_str() == schema })
                .for_each(|x| {
                    self.add_projection_field(x.desc.clone(), x.name.clone());
                })
        }
    }

    fn enter_aggregator_calling(&mut self, name: &ArenaStr, agg: &ArenaBox<dyn Aggregator>) {
        self.in_agg_calling += 1;
        for (dest, _) in &self.aggregators {
            if dest == name {
                return;
            }
        }
        self.aggregators.push((name.clone(), agg.clone()));
    }

    fn exit_aggregator_calling(&mut self) {
        self.in_agg_calling -= 1;
    }
}

impl Visitor for ProjectionColumnVisitor {
    fn visit_identifier(&mut self, this: &mut Identifier) {
        self.add_projection_field(ArenaStr::default(), this.symbol.clone());
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        self.add_projection_field(this.prefix.clone(), this.suffix.clone());
    }
    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        this.operands_mut()[0].accept(self);
    }
    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        try_visit!(self, this.lhs_mut());
        this.rhs_mut().accept(self);
    }
    fn visit_in_literal_set(&mut self, this: &mut InLiteralSet) {
        try_visit!(self, this.lhs);
        for literal in this.set.iter_mut() {
            try_visit!(self, literal);
        }
    }
    fn visit_in_relation(&mut self, this: &mut InRelation) {
        this.lhs.accept(self);
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        let ctx = ExecutionContext::new(this.distinct, &self.arena);
        match function::new_udaf(this.callee_name.as_str(), &ctx) {
            Some(udaf) => {
                if self.in_agg_calling > 0 {
                    visit_fatal!(self, "Nested aggregator calling: {}", this.callee_name);
                }
                self.enter_aggregator_calling(&this.callee_name, &udaf);
                if this.in_args_star {
                    let schema = self.schema.clone();
                    self.install_all_schema_fields(schema.schema.as_str(), schema.deref());
                } else {
                    for arg in this.args.iter_mut() {
                        break_visit!(self, arg);
                    }
                }
                self.exit_aggregator_calling();
            }
            None => {
                for arg in this.args.iter_mut() {
                    break_visit!(self, arg);
                }
            }
        }
    }

    fn visit_int_literal(&mut self, _this: &mut Literal<i64>) {}

    fn visit_float_literal(&mut self, _this: &mut Literal<f64>) {}

    fn visit_str_literal(&mut self, _this: &mut Literal<ArenaStr>) {}

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {}

    fn visit_placeholder(&mut self, _this: &mut Placeholder) {}
}

struct AggregatorCallingScope<'a> {
    owns: &'a mut ProjectionColumnVisitor,
}

impl<'a> AggregatorCallingScope<'a> {
    fn new(owns: &'a mut ProjectionColumnVisitor, name: &ArenaStr, agg: &ArenaBox<dyn Aggregator>) -> Self {
        owns.enter_aggregator_calling(name, agg);
        Self { owns }
    }

    fn nested_calling(&self) -> bool {
        self.owns.in_agg_calling > 1
    }
}

impl<'a> Drop for AggregatorCallingScope<'a> {
    fn drop(&mut self) {
        self.owns.exit_aggregator_calling();
    }
}