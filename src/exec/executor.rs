use std::alloc::Layout;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::intrinsics::copy_nonoverlapping;
use std::io::{Read, Write};
use std::iter::repeat;
use std::{mem, ptr};
use std::ops::{AddAssign, DerefMut, Index};
use std::ptr::{addr_of, NonNull, slice_from_raw_parts_mut};
use std::sync::{Arc, MutexGuard, Weak};

use crate::{arena_vec, corrupted, corrupted_err, Corrupting, Result, Status, visit_fatal};
use crate::base::{Allocator, Arena, ArenaBox, ArenaMut, ArenaStr, ArenaVec};
use crate::exec::{from_sql_result, function};
use crate::exec::connection::ResultSet;
use crate::exec::db::{ColumnMetadata, ColumnType, DB, OrderBy, SecondaryIndexMetadata, TableHandle, TableMetadata};
use crate::exec::evaluator::{Context, Evaluator, expr_typing_reduce, Value};
use crate::exec::function::{ExecutionContext, new_udf, UDF};
use crate::exec::interpreter::{BytecodeArray, BytecodeBuildingVisitor, Interpreter};
use crate::exec::physical_plan::{ProjectingOps, ReturningOneDummyOps};
use crate::exec::planning::{PlanMaker, resolve_physical_tables};
use crate::map::ArenaMap;
use crate::sql::ast::*;
use crate::sql::lexer::Token;
use crate::sql::parser::Parser;
use crate::sql::serialize::serialize_yaml_to_string;

pub struct Executor {
    pub db: Weak<DB>,
    arena: ArenaMut<Arena>,
    prepared_stmts: ArenaVec<ArenaBox<PreparedStatement>>,
    affected_rows: u64,
    result_set: Option<ResultSet>,
    rs: Status,
}

impl Executor {
    pub fn new(db: &Weak<DB>) -> Self {
        let zone = Arena::new_val();
        Self {
            db: db.clone(),
            arena: zone.get_mut(),
            prepared_stmts: ArenaVec::new(&zone.get_mut()),
            affected_rows: 0,
            result_set: None,
            rs: Status::Ok,
        }
    }

    pub fn execute_query<R: Read + ?Sized>(&mut self, reader: &mut R, arena: &ArenaMut<Arena>) -> Result<ResultSet> {
        let affected_rows = self.execute(reader, arena)?;
        Ok(self.returning_result_set(affected_rows))
    }

    pub fn execute<R: Read + ?Sized>(&mut self, reader: &mut R, arena: &ArenaMut<Arena>) -> Result<u64> {
        // clear latest result;
        self.rs = Status::Ok;
        self.arena = arena.clone();

        let factory = Factory::new(arena);
        let mut parser = from_sql_result(Parser::new(reader, factory))?;
        let mut stmts = from_sql_result(parser.parse())?;
        for i in 0..stmts.len() {
            let stmt = &mut stmts[i];
            stmt.accept(self);
            if self.rs.is_not_ok() {
                return Err(self.rs.clone());
            }
        }
        Ok(self.affected_rows)
    }

    pub fn execute_prepared_statement(&mut self, prepared: &mut ArenaBox<PreparedStatement>,
                                      arena: &ArenaMut<Arena>) -> Result<u64> {
        if !prepared.all_bound() {
            return Err(Status::corrupted("Not all value bound in PreparedStatement."));
        }
        // clear latest result;
        self.rs = Status::Ok;
        self.arena = arena.clone();
        self.prepared_stmts = ArenaVec::new(arena);
        self.prepared_stmts.push(prepared.clone());
        prepared.statement.accept(self);
        self.prepared_stmts.pop();
        if self.rs.is_not_ok() {
            Err(self.rs.clone())
        } else {
            Ok(self.affected_rows)
        }
    }

    pub fn execute_query_prepared_statement(&mut self, prepared: &mut ArenaBox<PreparedStatement>,
                                            arena: &ArenaMut<Arena>) -> Result<ResultSet> {
        let affected_rows = self.execute_prepared_statement(prepared, arena)?;
        Ok(self.returning_result_set(affected_rows))
    }

    pub fn prepare<R: Read + ?Sized>(&mut self, reader: &mut R, arena: &ArenaMut<Arena>)
                                     -> Result<ArenaVec<ArenaBox<PreparedStatement>>> {
        // clear latest result;
        self.rs = Status::Ok;
        self.arena = arena.clone();

        let factory = Factory::new(arena);
        let mut parser = from_sql_result(Parser::new(reader, factory))?;
        let stmts = from_sql_result(
            parser.parse_with_processor(|stmt, n_params| {
                let ps = PreparedStatement {
                    statement: stmt,
                    parameters: ArenaVec::with_init(arena, |_| { Value::Undefined }, n_params),
                };
                ArenaBox::new(ps, arena.clone().deref_mut())
            }))?;
        Ok(stmts)
    }

    fn returning_result_set(&mut self, affected_rows: u64) -> ResultSet {
        if self.result_set.is_none() {
            ResultSet::from_affected_rows(affected_rows)
        } else {
            mem::replace(&mut self.result_set, None).unwrap()
        }
    }

    fn convert_to_type(ast: &TypeDeclaration) -> Result<ColumnType> {
        match ast.token {
            Token::TinyInt => Ok(ColumnType::TinyInt(ast.len as u32)),
            Token::SmallInt => Ok(ColumnType::SmallInt(ast.len as u32)),
            Token::Int => Ok(ColumnType::Int(ast.len as u32)),
            Token::BigInt => Ok(ColumnType::BigInt(ast.len as u32)),
            Token::Char => Ok(ColumnType::Char(ast.len as u32)),
            Token::Varchar => Ok(ColumnType::Varchar(ast.len as u32)),
            _ => Err(Status::corrupted("Bad type token!"))
        }
    }

    fn build_insertion_tuples<'a>(&self,
                                  table: &'a Arc<TableHandle>,
                                  values: &mut ArenaVec<ArenaVec<ArenaBox<dyn Expression>>>,
                                  columns: &ArenaBox<ColumnSet>,
                                  anonymous_row_key_counter: &mut Option<MutexGuard<u64>>,
                                  auto_increment_counter: &mut Option<MutexGuard<u64>>,
                                  name_to_order: ArenaMap<&'a str, usize>,
                                  ignore_cols_bca: ArenaMap<u32, BytecodeArray>)
                                  -> Result<(ArenaVec<Tuple>, ArenaVec<SecondaryIndexBundle>)> {
        let mut evaluator = Evaluator::new(&self.arena);
        let mut interpreter = Interpreter::new(&self.arena);
        let context = Arc::new(UniversalContext::new(self.prepared_stmts.back().cloned(), &self.arena));
        let mut tuples = ArenaVec::new(&self.arena); // Vec::new();
        let mut secondary_indices = ArenaVec::new(&self.arena);
        let mut unique_row_keys = HashSet::new();
        let mut unique_keys = Vec::from_iter(repeat(HashSet::<Vec<u8>>::new())
            .take(table.metadata.secondary_indices.len()));
        for row in values.iter_mut() {
            let mut row_key = ArenaVec::new(&self.arena);
            if let Some(counter) = auto_increment_counter.as_mut() {
                counter.add_assign(1);
            }
            if let Some(counter) = anonymous_row_key_counter.as_mut() {
                counter.add_assign(1);
                DB::encode_anonymous_row_key(counter.clone(), &mut row_key);
            }
            let mut tuple = Tuple::with(columns, &self.arena);
            for col in &table.metadata.columns {
                if let Some(order) = name_to_order.get(&col.name.as_str()) {
                    let expr = &mut row[*order];
                    let value = evaluator.evaluate(expr.deref_mut(), context.clone())?;
                    tuple.set(col.order, value);
                } else if col.auto_increment {
                    let value = auto_increment_counter.as_ref().unwrap();
                    tuple.set(col.order, Value::Int(**value.clone() as i64));
                } else if !col.default_value.is_empty() {
                    let bca = ignore_cols_bca.get_mut(&col.id).unwrap();
                    let value = interpreter.evaluate(bca, context.clone())?;
                    tuple.set(col.order, value);
                } else {
                    debug_assert!(!col.not_null);
                    tuple.set(col.order, Value::Null);
                }
            }

            if !table.metadata.primary_keys.is_empty() {
                assert!(row_key.is_empty());
                row_key.write(&DB::PRIMARY_KEY_ID_BYTES).unwrap();
                for id in &table.metadata.primary_keys {
                    let col = table.get_col_by_id(*id).unwrap();
                    DB::encode_row_key(tuple.get(col.order), &col.ty, &mut row_key)?;
                }
                tuple.associate_row_key(&row_key);
                if !unique_row_keys.insert(row_key.clone()) { // already exists
                    Err(Status::corrupted("Duplicated primary key, must be unique."))?;
                }
            } else {
                tuple.associate_row_key(&row_key);
            }

            if !table.metadata.secondary_indices.is_empty() {
                let mut bundle = SecondaryIndexBundle::new(&tuple, &self.arena);
                // The secondary index format:
                // [key_id, (NULL-byte, key-part)+, row-key]
                for i in 0..table.metadata.secondary_indices.len() {
                    let index = &table.metadata.secondary_indices[i];
                    let mut key = ArenaVec::<u8>::new(&self.arena);
                    //key.write(&(index.id as u32).to_be_bytes()).unwrap();
                    DB::encode_idx_id(index.id, &mut key);
                    for id in &index.key_parts {
                        let col = table.get_col_by_id(*id).unwrap();
                        DB::encode_secondary_index(tuple.get(col.order), &col.ty, &mut key);
                    }
                    if index.unique && !unique_keys[i].insert(key.to_vec()) {
                        corrupted_err!("Duplicated secondary key, index {} is unique.",
                            table.metadata.secondary_indices[i].name)?
                    }
                    key.write(&row_key).unwrap(); // tail row key
                    bundle.index_keys.push(key);
                }
                secondary_indices.push(bundle);
            }

            tuples.push(tuple);
        }

        Ok((tuples, secondary_indices))
    }

    fn build_standalone_projecting(&mut self, this: &mut Select) -> Result<ResultSet> {
        let mut columns_expr = ArenaVec::new(&self.arena);
        let mut columns = ColumnSet::new("", 0, &self.arena);
        let mut i = 0;
        let context = Arc::new(UniversalContext::new(self.prepared_stmts.back().cloned(), &self.arena));
        for col in this.columns.iter_mut() {
            match col.expr {
                SelectColumn::Expr(ref mut expr) => {
                    let ty = expr_typing_reduce(expr.deref_mut(), context.clone(), &self.arena)?;
                    if col.alias.is_empty() {
                        columns.append_with_name(format!("_{i}").as_str(), ty);
                    } else {
                        columns.append_with_name(col.alias.as_str(), ty);
                    }
                    columns_expr.push(expr.clone());
                }
                _ => {
                    return Err(Status::corrupted("No columns for star(*)"));
                }
            }
            i += 1;
        }

        let stub = ArenaBox::new(ReturningOneDummyOps::new(&self.arena),
                                 self.arena.deref_mut());
        let projected_columns = ArenaBox::new(columns, self.arena.deref_mut());

        let bcs = BytecodeBuildingVisitor::build_arena_boxes(&columns_expr, &self.arena);
        let plan = ProjectingOps::new(bcs, stub.into(),
                                      self.arena.clone(), projected_columns,
                                      self.prepared_stmts.back().cloned());
        ResultSet::from_dcl_stmt(ArenaBox::new(plan, self.arena.deref_mut()).into())
    }

    fn process_dql(&mut self, this: &mut dyn Relation) {
        let db = self.db.upgrade().unwrap();
        let _locking = db.lock_tables();
        let mut planner =
            PlanMaker::new(&db, db.get_snapshot(), self.prepared_stmts.back().cloned(),
                           &self.arena);
        match planner.make(this) {
            Err(e) => self.rs = e,
            Ok(root) => {
                match ResultSet::from_dcl_stmt(root) {
                    Err(e) => self.rs = e,
                    Ok(rs) => self.result_set = Some(rs)
                }
            }
        }
    }
}

pub struct PreparedStatement {
    statement: ArenaBox<dyn Statement>,
    pub parameters: ArenaVec<Value>,
}

impl PreparedStatement {
    pub fn owns_arena(&self) -> &ArenaMut<Arena> { &self.parameters.owns }

    pub fn parameters_len(&self) -> usize { self.parameters.len() }

    pub fn all_bound(&self) -> bool {
        for i in 0..self.parameters_len() {
            if !self.is_bound(i) {
                return false;
            }
        }
        true
    }

    pub fn is_bound(&self, i: usize) -> bool { !self.parameters[i].is_undefined() }

    pub fn bind_i64(&mut self, i: usize, value: i64) { self.bind(i, Value::Int(value)); }
    pub fn bind_f64(&mut self, i: usize, value: f64) { self.bind(i, Value::Float(value)); }

    pub fn bind_string<A: Allocator>(&mut self, i: usize, value: String, arena: &mut A) {
        self.bind_str(i, value.as_str(), arena);
    }

    pub fn bind_str<A: Allocator>(&mut self, i: usize, value: &str, arena: &mut A) {
        let str = ArenaStr::new(value, arena);
        self.bind_arena_str(i, str);
    }

    pub fn bind_arena_str(&mut self, i: usize, value: ArenaStr) { self.bind(i, Value::Str(value)) }
    pub fn bind(&mut self, i: usize, value: Value) { self.parameters[i] = value; }
    pub fn bind_null(&mut self, i: usize) { self.bind(i, Value::Null); }
}

// macro_rules! visit_error {
//     ($self:ident, $($arg:tt)+) => {
//         {
//             let message = format!($($arg)+);
//             $self.rs = Err(Status::corrupted(message));
//             return;
//         }
//     }
// }

impl Visitor for Executor {
    fn visit_create_table(&mut self, this: &mut CreateTable) {
        let db = self.db.upgrade().unwrap();
        let mut locking_tables = db.lock_tables_mut();
        if locking_tables.contains_key(&this.table_name.to_string()) {
            if !this.if_not_exists {
                self.rs = corrupted!("Duplicated table name: {}", this.table_name);
            }
            return;
        }

        let rs = db.next_table_id();
        if rs.is_err() {
            self.rs = rs.err().unwrap();
            return;
        }
        let mut table = TableMetadata {
            name: this.table_name.to_string(),
            id: rs.unwrap(),
            created_at: "".to_string(),
            updated_at: "".to_string(),
            raw_ast: serialize_yaml_to_string(this),
            rows: 0,
            primary_keys: Vec::default(),
            secondary_indices: Vec::default(),
            auto_increment_keys: Vec::default(),
            columns: Vec::default(),
        };

        let mut col_name_to_id = HashMap::new();
        let mut bc_visitor = BytecodeBuildingVisitor::new(&self.arena);
        for i in 0..this.columns.len() {
            let col_decl = &this.columns[i];
            let ty = Self::convert_to_type(&col_decl.type_decl);
            if ty.is_err() {
                self.rs = ty.err().unwrap();
                return;
            }
            let mut def_val = vec![];
            match col_decl.default_val.clone() {
                Some(mut expr) => {
                    bc_visitor.build(expr.deref_mut(), &mut def_val).unwrap();
                }
                None => ()
            }
            let col = ColumnMetadata {
                name: col_decl.name.to_string(),
                order: i,
                id: i as u32,
                ty: ty.unwrap(),
                primary_key: col_decl.primary_key,
                auto_increment: col_decl.auto_increment,
                not_null: col_decl.not_null,
                default_value: def_val,
            };
            if col_decl.primary_key {
                table.primary_keys.push(col.id);
            }
            if col_decl.auto_increment {
                table.auto_increment_keys.push(col.id);
            }
            col_name_to_id.insert(col.name.clone(), col.id);
            table.columns.push(col);
        }

        let mut key_part_set = HashSet::new();
        for key in &this.primary_keys {
            match col_name_to_id.get(&key.to_string()) {
                Some(col_id) => {
                    let exists = table.primary_keys.iter()
                        .cloned()
                        .find(|x| { *x == *col_id });
                    if exists.is_some() {
                        visit_fatal!(self, "Primary key: `{}` not found in declaration", key.as_str());
                    }
                    table.primary_keys.push(*col_id);
                    key_part_set.insert(key.as_str());
                }
                None =>
                    visit_fatal!(self, "Primary key: `{}` not found in declaration", key.as_str())
            }
        }

        let mut idx_name_to_id = HashMap::new();
        for index_decl in &this.secondary_indices {
            if idx_name_to_id.contains_key(&index_decl.name.to_string()) {
                visit_fatal!(self, "Duplicated index name: {}", index_decl.name.as_str());
            }

            let mut index = SecondaryIndexMetadata {
                name: index_decl.name.to_string(),
                id: 0,
                key_parts: Vec::default(),
                unique: index_decl.unique,
                order_by: OrderBy::Asc,
            };
            for key_part in &index_decl.key_parts {
                if key_part_set.contains(key_part.as_str()) {
                    visit_fatal!(self, "In index {}, duplicated column name: {}", index.name, key_part.as_str());
                }
                key_part_set.insert(key_part.as_str());
                if !col_name_to_id.contains_key(&key_part.to_string()) {
                    visit_fatal!(self, "In index {}, column name: {} not found", index.name, key_part.as_str());
                }
                index.key_parts.push(*col_name_to_id.get(&key_part.to_string()).unwrap());
            }

            let rs = db.next_index_id();
            if rs.is_err() {
                self.rs = rs.err().unwrap();
                return;
            }
            index.id = rs.unwrap();
            idx_name_to_id.insert(index.name.clone(), index.id);
            table.secondary_indices.push(index);
        }
        match db.create_table(table, &mut locking_tables) {
            Err(e) => self.rs = e,
            Ok(_) => self.affected_rows = 0,
        }
    }

    fn visit_drop_table(&mut self, this: &mut DropTable) {
        let db = self.db.upgrade().unwrap();
        let mut locking_tables = db.lock_tables_mut();
        if !locking_tables.contains_key(&this.table_name.to_string()) {
            if !this.if_exists {
                self.rs = corrupted!("Table `{}` not found", this.table_name);
            }
            return;
        }
        match db.drop_table(&this.table_name.to_string(), &mut locking_tables) {
            Err(e) => self.rs = e,
            Ok(_) => self.affected_rows = 0,
        }
    }

    fn visit_create_index(&mut self, this: &mut CreateIndex) {
        let db = self.db.upgrade().unwrap();
        let mut locking_tables = db.lock_tables_mut();
        let may_table = locking_tables.get(&this.table_name.to_string()).cloned();
        if may_table.is_none() {
            visit_fatal!(self, "Table {} not found", this.table_name);
        }
        let table = may_table.unwrap();
        let ddl_mutex = table.mutex.clone();
        let _locking_ddl = ddl_mutex.lock().unwrap();

        if table.get_2rd_idx_by_name(&this.name.to_string()).is_some() {
            visit_fatal!(self, "Duplicated index name: {} in table {}", this.name, this.table_name);
        }

        for name_str in &this.key_parts {
            let name = name_str.to_string();
            if table.is_col_be_part_of_primary_key_by_name(&name) {
                visit_fatal!(self, "Key part: {} has been part of primary key.", name_str);
            }

            if let Some(idx) = table.get_col_be_part_of_2rd_idx_by_name(&name) {
                visit_fatal!(self, "Key part: {} has been part of secondary index: {}.", name_str,
                    idx.name);
            }
        }

        let idx = SecondaryIndexMetadata {
            name: this.name.to_string(),
            id: db.next_index_id().unwrap(),
            key_parts: this.key_parts.iter()
                .map(|x| {
                    table.get_col_by_name(&x.to_string()).unwrap().id
                })
                .collect(),
            unique: this.unique,
            order_by: OrderBy::Asc,
        };
        let mut metadata = (*table.metadata).clone();
        let idx_id = idx.id;
        metadata.secondary_indices.push(idx);
        let shadow_table = Arc::new(table.update(metadata));

        locking_tables.insert(shadow_table.metadata.name.clone(), shadow_table.clone());
        drop(locking_tables);

        match db.build_secondary_index(&shadow_table, idx_id) {
            Err(e) => {
                let mut locking_tables = db.lock_tables_mut();
                db.remove_secondary_index(table.metadata.id, &table.column_family,
                                          idx_id, this.unique, false).unwrap();

                locking_tables.insert(table.metadata.name.clone(), table); // fall back
                self.rs = e;
            }
            Ok(affected_rows) => {
                db.write_table_metadata(&shadow_table.metadata).unwrap();
                // TODO:
                self.affected_rows = affected_rows;
            }
        }
    }

    fn visit_drop_index(&mut self, this: &mut DropIndex) {
        let db = self.db.upgrade().unwrap();
        let mut locking_tables = db.lock_tables_mut();
        let may_table = locking_tables.get(&this.table_name.to_string()).cloned();
        if may_table.is_none() {
            visit_fatal!(self, "Table {} not found", this.table_name);
        }
        let table = may_table.unwrap();
        let ddl_mutex = table.mutex.clone();
        let _locking_ddl = ddl_mutex.lock().unwrap();

        let idx = table.get_2rd_idx_by_name(&this.name.to_string());
        if idx.is_none() {
            visit_fatal!(self, "Index {} not found in table: {}", this.name, this.table_name);
        }
        let secondary_index = idx.unwrap();
        let secondary_index_id = secondary_index.id;
        let unique = secondary_index.unique;
        let shadow_table = Arc::new({
            let mut metadata = (*table.metadata).clone();
            for i in 0..metadata.secondary_indices.len() {
                if metadata.secondary_indices[i].id == secondary_index.id {
                    metadata.secondary_indices.remove(i);
                    break;
                }
            }
            debug_assert_eq!(table.metadata.secondary_indices.len() - 1, metadata.secondary_indices.len());
            table.update(metadata)
        });

        locking_tables.insert(this.table_name.to_string(), shadow_table.clone());
        drop(locking_tables);

        match db.remove_secondary_index(shadow_table.metadata.id,
                                        &shadow_table.column_family,
                                        secondary_index_id, unique, true) {
            Ok(affected_rows) => self.affected_rows = affected_rows,
            Err(_) => {
                // Eat error
                self.affected_rows = 0;
                // TODO: record warning message
            }
        }
    }

    fn visit_insert_into_table(&mut self, this: &mut InsertIntoTable) {
        let db = self.db.upgrade().unwrap();
        let tables = db.lock_tables();
        if !tables.contains_key(&this.table_name.to_string()) {
            visit_fatal!(self, "Table `{}` not found", this.table_name);
        }
        let table = tables.get(&this.table_name.to_string()).unwrap().clone();
        drop(tables);

        let mut insertion_cols = Vec::new();
        let mut columns = {
            let tmp = ColumnSet::new(table.metadata.name.as_str(), table.metadata.id,
                                     &mut self.arena);
            ArenaBox::new(tmp, self.arena.deref_mut())
        };

        let mut ignore_cols = ArenaMap::new(&self.arena);
        for col in &table.metadata.columns {
            ignore_cols.insert(col.name.as_str(), col.id);
            columns.append_physical(col.name.as_str(), table.metadata.id, col.id, col.ty.clone());
        }

        for col_name in &this.columns_name {
            match table.get_col_by_name(&col_name.to_string()) {
                Some(col) => {
                    ignore_cols.remove(&col.name.as_str());
                    insertion_cols.push(col.id);
                }
                None => visit_fatal!(self, "Column: `{}` not found in table: `{}`", col_name.as_str(), this.table_name)
            }
        }

        let mut ignore_cols_bca = ArenaMap::new(&self.arena);
        for (_, col_id) in ignore_cols.iter() {
            let col = table.get_col_by_id(*col_id).unwrap();
            if col.not_null {
                if col.auto_increment {
                    // ok: has auto increment value
                } else if !col.default_value.is_empty() {
                    // ok: has default value
                } else {
                    visit_fatal!(self, "Column: `{}` is not null, but no default value", col.name);
                }
            }
            if !col.default_value.is_empty() {
                let bca = BytecodeArray::new(&col.default_value, &self.arena).unwrap();
                ignore_cols_bca.insert(col.id, bca);
            }
        }

        let use_anonymous_row_key = table.metadata.primary_keys.is_empty();
        let cf = table.column_family.clone();

        for row in &this.values {
            if row.len() < insertion_cols.len() {
                visit_fatal!(self, "Not enough number of row values for insertion, need: {}", insertion_cols.len());
            }
        }

        let mut anonymous_row_key_counter = if use_anonymous_row_key {
            Some(table.anonymous_row_key_counter.lock().unwrap())
        } else {
            None
        };
        let mut auto_increment_counter = if table.has_auto_increment_fields() {
            Some(table.auto_increment_counter.lock().unwrap())
        } else {
            None
        };

        let name_to_order = {
            let mut tmp = ArenaMap::new(&self.arena);
            for i in 0..this.columns_name.len() {
                tmp.insert(this.columns_name[i].as_str(), i);
            }
            tmp
        };

        let interpreter = Interpreter::new(&self.arena);
        let rs = self.build_insertion_tuples(&table, &mut this.values, &columns,
                                             &mut anonymous_row_key_counter, &mut auto_increment_counter,
                                             name_to_order, ignore_cols_bca);
        if let Err(e) = rs {
            self.rs = e;
            return;
        }
        let (tuples, secondary_indices) = rs.unwrap();
        let anonymous_row_key_value = anonymous_row_key_counter.map(|x| { *x });
        let auto_increment_value = auto_increment_counter.map(|x| { *x });

        match db.insert_rows(&cf, &table.metadata, &tuples, &secondary_indices,
                             anonymous_row_key_value, auto_increment_value,
                             !use_anonymous_row_key) {
            Ok(affected_rows) => self.affected_rows = affected_rows,
            Err(e) => self.rs = e
        }
    }

    fn visit_collection(&mut self, this: &mut Collection) {
        self.process_dql(this);
    }

    fn visit_select(&mut self, this: &mut Select) {
        match &this.from_clause {
            None => { // fast path
                match self.build_standalone_projecting(this) {
                    Err(e) => self.rs = e,
                    Ok(rs) => self.result_set = Some(rs),
                }
            }
            Some(_) => self.process_dql(this)
        }
    }

    fn visit_delete(&mut self, this: &mut Delete) {
        debug_assert!(!this.names.is_empty());
        let db = self.db.upgrade().unwrap();
        let mut tables_will_be_delete = arena_vec!(&self.arena);
        let tables = db.lock_tables();
        for name in &this.names {
            match tables.get(&name.to_string()) {
                Some(table) => tables_will_be_delete.push(table.clone()),
                None => visit_fatal!(self, "Table name: {name} not found."),
            }
        }
        drop(tables);

        let mut maker = PlanMaker::new(&db, db.get_snapshot(), self.prepared_stmts.back().cloned(), &self.arena);
        let rs = if this.names.len() == 1 && this.relation.is_none() {
            // Simple delete
            maker.make_rows_simple_producer(this.names[0].as_str(), &this.where_clause, &this.limit_clause)
        } else {
            maker.make_rows_multi_producer(this.relation.as_ref().unwrap(), &this.where_clause,
                                           &this.order_by_clause, &this.limit_clause)
        };
        if let Err(e) = rs {
            self.rs = e;
            return;
        }
        let mut producer = rs.unwrap().clone();
        match db.delete_rows(&tables_will_be_delete, producer.clone(), &self.arena) {
            Ok(affected_rows) => self.affected_rows = affected_rows,
            Err(e) => self.rs = e
        }
        producer.finalize();
    }

    fn visit_update(&mut self, this: &mut Update) {
        let db = self.db.upgrade().unwrap();
        let tables = db.lock_tables();
        let rs = resolve_physical_tables(&this.relation);
        if let Err(e) = rs {
            self.rs = e;
            return;
        }
        let mut tables_will_be_update = HashMap::new();
        let refs = rs.unwrap();
        for (key, from) in refs {
            match tables.get(&from.name.to_string()) {
                Some(table) => { tables_will_be_update.insert(key, table.clone()); }
                None => visit_fatal!(self, "Table name: {} not found.", from.name),
            }
        }
        drop(tables);

        for assignment in &this.assignments {
            if assignment.lhs.prefix.is_empty() {
                if tables_will_be_update.len() > 1 {
                    visit_fatal!(self, "Obscure assignment name: {}", assignment.lhs);
                }
                for table in tables_will_be_update.values() {
                    if table.get_col_by_name(&assignment.lhs.suffix.to_string()).is_none() {
                        visit_fatal!(self, "Unresolved assignment name: {}", assignment.lhs);
                    }
                    break;
                }
            } else {
                if let Some(table) = tables_will_be_update.get(&assignment.lhs.prefix) {
                    if table.get_col_by_name(&assignment.lhs.suffix.to_string()).is_none() {
                        visit_fatal!(self, "Unresolved assignment name: {}", assignment.lhs);
                    }
                } else {
                    visit_fatal!(self, "Unresolved assignment name: {}", assignment.lhs);
                }
            }
        }

        let mut maker = PlanMaker::new(&db, db.get_snapshot(),
                                       self.prepared_stmts.back().cloned(), &self.arena);
        let rs = maker.make_rows_multi_producer(&this.relation, &this.where_clause,
                                                &this.order_by_clause, &this.limit_clause);
        if let Err(e) = rs {
            self.rs = e;
            return;
        }

        let mut producer = rs.unwrap().clone();
        match db.update_rows(&tables_will_be_update,
                             &this.assignments,
                             self.prepared_stmts.back().cloned(),
                             producer.clone(),
                             &self.arena) {
            Ok(affected_rows) => self.affected_rows = affected_rows,
            Err(e) => self.rs = e
        }
        producer.finalize();
    }
}

pub struct ColumnsAuxResolver {
    id_to_idx: ArenaMap<(u64, u32), usize>,
    //row_keys: ArenaMap<u64, (usize, usize)>,
    cols: *const ColumnSet,
}

impl ColumnsAuxResolver {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            id_to_idx: ArenaMap::new(arena),
            //row_keys: ArenaMap::new(arena),
            cols: ptr::null(),
        }
    }

    pub fn attach(&mut self, tuple: &Tuple) {
        if self.cols == tuple.columns_set.as_ptr() {
            return;
        }
        self.id_to_idx.clear();
        //self.row_keys.clear();
        if let Some(tid) = tuple.columns().original_table_id() {
            for col in &tuple.columns().columns {
                self.id_to_idx.insert((tid, col.id), col.order);
            }
        } else {
            for col in &tuple.columns().columns {
                self.id_to_idx.insert((col.original_tid, col.id), col.order);
            }
            // Self::iterate_multi_row_key(tuple.row_key(), |tid, slice| {
            //     self.row_keys.insert(tid, slice);
            // });
        }
        self.cols = tuple.columns_set.as_ptr();
    }

    pub fn get_column_by_id(&self, tid: u64, id: u32) -> Option<usize> {
        self.id_to_idx.get(&(tid, id)).cloned()
    }

    // pub fn get_row_key<'a>(&self, tid: u64, buf: &'a [u8]) -> &'a [u8] {
    //     if self.row_keys.is_empty() {
    //         buf
    //     } else {
    //         let (pos, len) = self.row_keys.get(&tid).cloned().unwrap();
    //         &buf[pos..pos + len]
    //     }
    // }
    //
    // pub fn iterate_multi_row_key<F>(row_key: &[u8], mut callback: F) where F: FnMut(u64, (usize, usize)) {
    //     let mut pos = 0;
    //     while pos < row_key.len() {
    //         let id_part = &row_key[pos..pos + size_of::<u64>()];
    //         let id = u64::from_be_bytes(id_part.try_into().unwrap());
    //         pos += size_of::<u64>();
    //
    //         let len_part = &row_key[pos..pos + size_of::<u32>()];
    //         pos += size_of::<u32>();
    //         let len = u32::from_be_bytes(len_part.try_into().unwrap()) as usize;
    //
    //         //let part = &row_key[pos..pos + len];
    //         callback(id, (pos, len));
    //         pos += len;
    //     }
    // }
}

#[derive(Debug)]
pub struct ColumnSet {
    pub schema: ArenaStr,
    pub columns: ArenaVec<Column>,
    pub tid: u64,
    arena: ArenaMut<Arena>,
}

impl ColumnSet {
    pub fn new(schema: &str, tid: u64, arena_mut: &ArenaMut<Arena>) -> Self {
        let mut arena = arena_mut.clone();
        Self {
            schema: ArenaStr::from_arena(schema, &mut arena),
            columns: ArenaVec::new(&arena),
            tid,
            arena,
        }
    }

    pub fn original_table_id(&self) -> Option<u64> {
        if self.tid > 0 {
            Some(self.tid)
        } else {
            None
        }
    }

    pub fn append_with_name(&mut self, name: &str, ty: ColumnType) {
        self.append(name, "", 0, 0, ty);
    }

    pub fn append_physical(&mut self, name: &str, table_id: u64, id: u32, ty: ColumnType) {
        self.append(name, "", table_id, id, ty)
    }

    pub fn append(&mut self, name: &str, desc: &str, table_id: u64, id: u32, ty: ColumnType) {
        let order = self.columns.len();
        self.columns.push(Column {
            name: ArenaStr::from_arena(name, &mut self.arena),
            desc: ArenaStr::from_arena(desc, &mut self.arena),
            id,
            original_tid: table_id,
            ty,
            order,
        });
    }

    pub fn find_by_id(&self, col_id: u32) -> Option<&Column> {
        self.index_by_id(col_id).map(|x| {
            &self.columns[x]
        })
    }

    pub fn index_by_id(&self, col_id: u32) -> Option<usize> {
        for i in 0..self.columns.len() {
            let col = &self.columns[i];
            if col.id == col_id {
                return Some(i);
            }
        }
        None
    }

    pub fn index_by_original_and_id(&self, table_id: u64, col_id: u32) -> Option<usize> {
        for i in 0..self.columns.len() {
            let col = &self.columns[i];
            if col.original_tid == table_id && col.id == col_id {
                return Some(i);
            }
        }
        None
    }

    pub fn find_by_name(&self, prefix: &str, suffix: &str) -> Option<&Column> {
        self.index_by_name(prefix, suffix).map(|x| {
            &self.columns[x]
        })
    }

    pub fn index_by_name(&self, prefix: &str, suffix: &str) -> Option<usize> {
        if prefix == self.schema.as_str() {
            for i in 0..self.columns.len() {
                if self.columns[i].name.as_str() == suffix {
                    return Some(i);
                }
            }
        } else {
            for i in 0..self.columns.len() {
                let col = &self.columns[i];
                if col.name.as_str() == suffix && col.desc.as_str() == prefix {
                    return Some(i);
                }
            }
        }
        None
    }

    pub fn len(&self) -> usize { self.columns.len() }
}

impl Index<usize> for ColumnSet {
    type Output = Column;

    fn index(&self, index: usize) -> &Self::Output { &self.columns[index] }
}

#[derive(Debug)]
pub struct Column {
    pub name: ArenaStr,
    pub desc: ArenaStr,
    pub id: u32,
    // physical column id
    pub original_tid: u64,
    // original physical table id
    pub ty: ColumnType,
    pub order: usize,
}

pub struct Tuple {
    row_key: NonNull<[u8]>,
    pub items: NonNull<[Value]>,
    columns_set: NonNull<ColumnSet>,
}

static EMPTY_ARRAY_DUMMY: [u8; 0] = [0; 0];

impl Tuple {
    pub fn new<A: Allocator + ?Sized>(columns: &ArenaBox<ColumnSet>, arena: &mut A) -> Self {
        Self::with_filling(columns, |_| { Value::Undefined }, arena)
    }

    pub fn with_filling<A: Allocator + ?Sized, F>(columns: &ArenaBox<ColumnSet>, callback: F, arena: &mut A) -> Self
        where F: Fn(usize) -> Value {
        let len = columns.columns.len();
        let value_layout = Layout::new::<Value>();
        let layout = Layout::from_size_align(value_layout.size() * len, value_layout.align()).unwrap();
        let mut items = {
            let chunk = arena.allocate(layout).unwrap();
            let addr = chunk.as_ptr() as *mut Value;
            let ptr = slice_from_raw_parts_mut(addr, len);
            NonNull::new(ptr).unwrap()
        };
        let row_key = unsafe {
            for i in 0..len {
                items.as_mut()[i] = callback(i);
            }
            let ptr = addr_of!(EMPTY_ARRAY_DUMMY) as *mut u8;
            NonNull::new(slice_from_raw_parts_mut(ptr, 0)).unwrap()
        };
        Self {
            row_key,
            items,
            columns_set: columns.ptr(),
        }
    }

    pub fn dup(&self, arena: &ArenaMut<Arena>) -> Self {
        let cols = ArenaBox::from_ptr(self.columns_set);
        let mut this = if self.row_key().is_empty() {
            Self::with(&cols, arena)
        } else {
            Self::with_row_key(&cols, self.row_key(), arena)
        };
        for i in 0..self.as_slice().len() {
            this.set(i, self.get(i).dup(arena))
        }
        this
    }

    pub fn with(columns: &ArenaBox<ColumnSet>, arena: &ArenaMut<Arena>) -> Self {
        Self::new(columns, arena.get_mut())
    }

    pub fn with_row_key(columns: &ArenaBox<ColumnSet>, key: &[u8], arena: &ArenaMut<Arena>) -> Self {
        let mut this = Self::new(columns, arena.get_mut());
        this.row_key = Self::new_row_key(key, arena.get_mut());
        this
    }

    pub fn attach_row_key(&mut self, key: ArenaVec<u8>) { self.associate_row_key(&key) }

    pub fn associate_row_key(&mut self, key: &ArenaVec<u8>) {
        unsafe {
            let raw_ptr = key.raw_ptr();
            let ptr = raw_ptr.as_ptr() as *mut u8;
            self.row_key = NonNull::new(slice_from_raw_parts_mut(ptr, key.len())).unwrap();
        }
    }

    fn new_row_key(key: &[u8], arena: &mut dyn Allocator) -> NonNull<[u8]> {
        let row_key: NonNull<[u8]> = if key.is_empty() {
            let ptr = addr_of!(EMPTY_ARRAY_DUMMY) as *mut u8;
            NonNull::new(slice_from_raw_parts_mut(ptr, 0)).unwrap()
        } else {
            let layout = Layout::from_size_align(key.len(), 4).unwrap();
            arena.allocate(layout).unwrap()
        };
        unsafe {
            if !key.is_empty() {
                let dst = row_key.as_ptr() as *mut u8;
                copy_nonoverlapping(addr_of!(key[0]), dst, key.len());
            }
        }
        row_key
    }

    pub fn columns(&self) -> &ColumnSet { unsafe { self.columns_set.as_ref() } }

    pub fn rename(&mut self, cols: &ArenaBox<ColumnSet>) { self.columns_set = cols.ptr(); }

    pub fn get_null(&self, i: usize) -> bool {
        self.get(i).is_null()
    }

    pub fn get_i64(&self, i: usize) -> Option<i64> {
        match self.get(i) {
            Value::Int(n) => Some(*n),
            _ => None
        }
    }

    pub fn get_arena_str(&self, i: usize) -> Option<ArenaStr> {
        match self.get(i) {
            Value::Str(s) => Some(s.clone()),
            _ => None
        }
    }

    pub fn get_str(&self, i: usize) -> Option<&str> {
        match self.get(i) {
            Value::Str(s) => Some(s.as_str()),
            _ => None
        }
    }

    pub fn get_string(&self, i: usize) -> Option<String> {
        match self.get(i) {
            Value::Str(s) => Some(s.to_string()),
            _ => None
        }
    }

    pub fn get(&self, i: usize) -> &Value { &self.as_slice()[i] }

    pub fn set(&mut self, i: usize, value: Value) { self.as_mut_slice()[i] = value; }

    pub fn len(&self) -> usize { self.as_slice().len() }

    pub fn as_slice(&self) -> &[Value] { unsafe { self.items.as_ref() } }

    fn as_mut_slice(&mut self) -> &mut [Value] { unsafe { self.items.as_mut() } }

    pub fn row_key(&self) -> &[u8] { unsafe { self.row_key.as_ref() } }
}

impl Index<usize> for Tuple {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index)
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if !self.row_key().is_empty() {
            write!(f, "rowkey={:x?}", self.row_key())?;
        }
        f.write_str("(")?;
        for i in 0..self.columns().columns.len() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}", self[i])?
        }
        f.write_str(")")
    }
}

pub struct SecondaryIndexBundle {
    row_key: NonNull<[u8]>,
    pub index_keys: ArenaVec<ArenaVec<u8>>,
}

impl SecondaryIndexBundle {
    fn new(tuple: &Tuple, arena: &ArenaMut<Arena>) -> Self {
        Self {
            row_key: tuple.row_key,
            index_keys: ArenaVec::new(arena),
        }
    }

    pub fn row_key(&self) -> &[u8] {
        unsafe { self.row_key.as_ref() }
    }

    pub fn index_with_row_key(&self, i: usize) -> &[u8] {
        self.index_keys[i].as_slice()
    }

    pub fn index(&self, i: usize) -> &[u8] {
        let full_key = self.index_with_row_key(i);
        &full_key[..full_key.len() - self.row_key().len()]
    }
}

macro_rules! bound_param_impl {
    ($self:ident, $order:ident) => {
        match $self.prepared_stmt.as_ref() {
            Some(ps) => {
                if $order >= ps.parameters_len() {
                    &Value::Undefined
                } else {
                    &ps.parameters[$order]
                }
            }
            None => &Value::Undefined
        }
    };
}

pub struct UpstreamContext {
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    arena: ArenaMut<Arena>,
    symbols: HashMap<&'static str, usize>,
    fully_qualified_symbols: HashMap<(&'static str, &'static str), usize>,
    udfs_cache: RefCell<HashMap<&'static str, ArenaBox<dyn UDF>>>,
    tuple: Cell<*const [Value]>,
}

impl UpstreamContext {
    pub fn new(prepared_stmt: Option<ArenaBox<PreparedStatement>>, arena: &ArenaMut<Arena>) -> Self {
        static DUMMY: [Value; 0] = [];
        Self {
            prepared_stmt,
            arena: arena.clone(),
            symbols: HashMap::default(),
            fully_qualified_symbols: HashMap::default(),
            udfs_cache: RefCell::new(HashMap::default()),
            tuple: Cell::new(addr_of!(DUMMY[..])),
        }
    }

    pub fn add(&mut self, column_set: &ColumnSet) {
        let schema = self.dup_str(column_set.schema.as_str());
        for i in 0..column_set.columns.len() {
            let col = &column_set.columns[i];
            let name = self.dup_str(col.name.as_str());
            self.symbols.insert(name, i);
            if !col.desc.is_empty() {
                let prefix = self.dup_str(col.desc.as_str());
                self.fully_qualified_symbols.insert((prefix, name), i);
            } else if !column_set.schema.is_empty() {
                self.fully_qualified_symbols.insert((schema, name), i);
            }
        }
    }

    pub fn dup_str<'a>(&'a self, input: &str) -> &'static str {
        let layout = Layout::from_size_align(input.len(), 4).unwrap();
        let chunk = self.arena.get_mut().allocate(layout).unwrap();
        unsafe {
            copy_nonoverlapping(input.as_ptr(), chunk.as_ptr() as *mut u8, input.len());
            std::str::from_utf8_unchecked(chunk.as_ref())
        }
    }

    pub fn attach(&self, tuple: &Tuple) {
        self.tuple.set(tuple.items.as_ptr());
    }
}

impl Context for UpstreamContext {
    fn fast_access(&self, i: usize) -> &Value {
        unsafe { &(*self.tuple.get())[i] }
    }

    fn resolve(&self, name: &str) -> Value {
        match self.symbols.get(name) {
            Some(index) => self.fast_access(*index).clone(),
            None => Value::Undefined
        }
    }

    fn resolve_fully_qualified(&self, prefix: &str, suffix: &str) -> Value {
        match self.fully_qualified_symbols.get(&(prefix, suffix)) {
            Some(index) => self.fast_access(*index).clone(),
            None => Value::Undefined
        }
    }

    fn bound_param(&self, order: usize) -> &Value {
        bound_param_impl!(self, order)
    }

    fn get_udf(&self, name: &str) -> Option<ArenaBox<dyn UDF>> {
        let mut udfs = self.udfs_cache.borrow_mut();
        match udfs.get(name) {
            Some(udf) => Some(udf.clone()),
            None => {
                let ctx = ExecutionContext::new(false, &self.arena);
                match function::new_udf(name, &ctx) {
                    Some(udf) => {
                        udfs.insert(self.dup_str(name), udf.clone());
                        Some(udf)
                    }
                    None => None
                }
            }
        }
    }
}

pub struct UniversalContext {
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    arena: ArenaMut<Arena>,
}

impl UniversalContext {
    pub fn new(prepared_stmt: Option<ArenaBox<PreparedStatement>>, arena: &ArenaMut<Arena>) -> Self {
        Self {
            prepared_stmt,
            arena: arena.clone()
        }
    }
}

impl Context for UniversalContext {
    fn bound_param(&self, order: usize) -> &Value {
        bound_param_impl!(self, order)
    }
    fn get_udf(&self, name: &str) -> Option<ArenaBox<dyn UDF>> {
        let ctx = ExecutionContext::new(false, &self.arena);
        new_udf(name, &ctx)
    }
}

pub struct MockContent {
    vals: ArenaVec<Value>,
    bind_vals: ArenaVec<Value>,
    names: ArenaMap<(ArenaStr, ArenaStr), usize>,
    arena: ArenaMut<Arena>,
}

impl MockContent {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            vals: arena_vec!(arena),
            bind_vals: arena_vec!(arena),
            names: ArenaMap::new(arena),
            arena: arena.clone(),
        }
    }

    pub fn with_env<const N: usize>(mut self, env: [(&str, Value); N]) -> Self {
        for (name, value) in env {
            let i = self.vals.len();
            self.vals.push(value);
            let name = if let Some(pos) = name.find(".") {
                let (prefix, part) = name.split_at(pos);
                let suffix = std::str::from_utf8(&part.as_bytes()[1..]).unwrap();
                (
                    ArenaStr::new(prefix, self.arena.deref_mut()),
                    ArenaStr::new(suffix, self.arena.deref_mut()),
                )
            } else {
                (
                    ArenaStr::default(),
                    ArenaStr::new(name, self.arena.deref_mut())
                )
            };
            self.names.insert(name, i);
        }
        self
    }

    pub fn with_binds<const N: usize>(mut self, binds: [Value; N]) -> Self {
        for val in binds {
            self.bind_vals.push(val);
        }
        self
    }
}

impl Context for MockContent {
    fn fast_access(&self, i: usize) -> &Value {
        &self.vals[i]
    }

    fn resolve(&self, name: &str) -> Value {
        self.resolve_fully_qualified("", name)
    }

    fn resolve_fully_qualified(&self, prefix: &str, suffix: &str) -> Value {
        let a = ArenaStr::new(prefix, self.arena.get_mut());
        let b = ArenaStr::new(suffix, self.arena.get_mut());
        self.names.get(&(a, b)).map_or(Value::Undefined, |x| {
            self.vals[*x].clone()
        })
    }

    fn bound_param(&self, order: usize) -> &Value {
        &self.bind_vals[order]
    }

    fn get_udf(&self, name: &str) -> Option<ArenaBox<dyn UDF>> {
        let ctx = ExecutionContext::new(false, &self.arena);
        new_udf(name, &ctx)
    }
}

pub struct TypingStubContext {
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
    cols: ArenaBox<ColumnSet>,
    int_ty: Value,
    float_ty: Value,
    str_ty: Value,
}

impl TypingStubContext {
    pub fn new(prepared_stmt: Option<ArenaBox<PreparedStatement>>, cols: &ArenaBox<ColumnSet>) -> Self {
        Self {
            prepared_stmt,
            cols: cols.clone(),
            int_ty: Value::Int(i64::default()),
            float_ty: Value::Float(f64::default()),
            str_ty: Value::Str(ArenaStr::default()),
        }
    }

    pub fn ty_to_stub_value(&self, ty: &ColumnType) -> &Value {
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => &self.int_ty,
            ColumnType::Float(_, _)
            | ColumnType::Double(_, _) => &self.float_ty,
            ColumnType::Char(_)
            | ColumnType::Varchar(_) => &self.str_ty,
        }
    }
}

impl Context for TypingStubContext {
    fn fast_access(&self, i: usize) -> &Value { self.ty_to_stub_value(&self.cols[i].ty) }

    fn resolve(&self, name: &str) -> Value {
        self.resolve_fully_qualified("", name)
    }

    fn resolve_fully_qualified(&self, prefix: &str, suffix: &str) -> Value {
        match self.cols.find_by_name(prefix, suffix) {
            Some(col) => self.ty_to_stub_value(&col.ty).clone(),
            None => Value::Undefined
        }
    }

    fn bound_param(&self, order: usize) -> &Value {
        bound_param_impl!(self, order)
    }

    fn get_udf(&self, name: &str) -> Option<ArenaBox<dyn UDF>> {
        let ctx = ExecutionContext::new(false, &self.cols.arena);
        function::new_udf(name, &ctx)
    }
}