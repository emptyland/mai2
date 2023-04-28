use std::alloc::{alloc, Layout};
use std::arch::x86_64::_tzcnt_u32;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::intrinsics::copy_nonoverlapping;
use std::io::Read;
use std::ops::{Deref, DerefMut, Index};
use std::ptr::{addr_of, addr_of_mut, NonNull, slice_from_raw_parts_mut};
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::sync::atomic::Ordering;

use crate::base::{Allocator, Arena, ArenaBox, ArenaStr, ArenaVec};
use crate::exec::db::{ColumnMetadata, ColumnType, DB, TableMetadata};
use crate::exec::from_sql_result;
use crate::sql::ast::{BinaryExpression, CallFunction, CreateTable, DropTable, Factory, FullyQualifiedName, Identifier, InsertIntoTable, Literal, Placeholder, Statement, TypeDeclaration, UnaryExpression, Visitor};
use crate::sql::parser::Parser;
use crate::{Corrupting, Result, Status};
use crate::exec::evaluator::{Context, Evaluator, Value};
use crate::exec::evaluator::Value::Str;
use crate::sql::lexer::Token;
use crate::sql::serialize::serialize_yaml_to_string;

pub struct Executor {
    pub db: Weak<DB>,
    arena: Rc<RefCell<Arena>>,
    prepared_stmts: VecDeque<ArenaBox<PreparedStatement>>,
    rs: Result<()>,
}

impl Executor {
    pub fn new(db: &Weak<DB>) -> Self {
        Self {
            db: db.clone(),
            arena: Arena::new_rc(),
            prepared_stmts: VecDeque::default(),
            rs: Ok(()),
        }
    }

    pub fn execute(&mut self, reader: &mut dyn Read, arena: &Rc<RefCell<Arena>>) -> Result<()> {
        // clear latest result;
        self.rs = Ok(());
        self.arena = arena.clone();

        let factory = Factory::from(arena);
        let mut parser = from_sql_result(Parser::new(reader, factory))?;
        let mut stmts = from_sql_result(parser.parse())?;
        for i in 0..stmts.len() {
            let stmt = &mut stmts[i];
            stmt.accept(self);
            self.rs.clone()?;
        }
        Ok(())
    }

    pub fn execute_prepared_statement(&mut self, prepared: &mut ArenaBox<PreparedStatement>,
                                      arena: &Rc<RefCell<Arena>>) -> Result<()> {
        if !prepared.all_bound() {
            return Err(Status::corrupted("Not all value bound in PreparedStatement."));
        }
        // clear latest result;
        self.rs = Ok(());
        self.arena = arena.clone();
        self.prepared_stmts.push_back(prepared.clone());
        prepared.statement.accept(self);
        self.prepared_stmts.pop_back();
        self.rs.clone()
    }

    pub fn prepare(&mut self, reader: &mut dyn Read, arena: &Rc<RefCell<Arena>>)
                   -> Result<ArenaVec<ArenaBox<PreparedStatement>>> {
        // clear latest result;
        self.rs = Ok(());
        self.arena = arena.clone();

        let factory = Factory::from(arena);
        let mut parser = from_sql_result(Parser::new(reader, factory))?;
        let stmts = from_sql_result(
            parser.parse_with_processor(|stmt, n_params| {
                let ps = PreparedStatement {
                    statement: stmt,
                    parameters: ArenaVec::with_init(arena, |_| { Value::Unit }, n_params),
                };
                ArenaBox::new(ps, arena)
            }))?;
        Ok(stmts)
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
}

pub struct PreparedStatement {
    statement: ArenaBox<dyn Statement>,
    parameters: ArenaVec<Value>,
}

impl PreparedStatement {
    pub fn parameters_len(&self) -> usize { self.parameters.len() }

    pub fn all_bound(&self) -> bool {
        for i in 0..self.parameters_len() {
            if !self.is_bound(i) {
                return false;
            }
        }
        true
    }

    pub fn is_bound(&self, i: usize) -> bool { !self.parameters[i].is_unit() }

    pub fn bind_i64(&mut self, i: usize, value: i64) { self.bind(i,Value::Int(value)); }
    pub fn bind_f64(&mut self, i: usize, value: f64) { self.bind(i, Value::Float(value)); }
    pub fn bind_str(&mut self, i: usize, value: &str) { todo!() }
    pub fn bind(&mut self, i: usize, value: Value) { self.parameters[i] = value; }
}

macro_rules! visit_error {
    ($self:ident, $($arg:tt)+) => {
        {
            let message = format!($($arg)+);
            $self.rs = Err(Status::corrupted(message));
            return;
        }
    }
}

impl Visitor for Executor {
    fn visit_create_table(&mut self, this: &mut CreateTable) {
        let db = self.db.upgrade().unwrap();
        let mut locking_tables = db.lock_tables();
        if locking_tables.contains_key(&this.table_name.to_string()) {
            if !this.if_not_exists {
                self.rs = Err(Status::corrupted(format!("Duplicated type name: {}",
                                                        this.table_name.as_str())));
            }
            return;
        }

        let rs = db.next_table_id();
        if rs.is_err() {
            self.rs = Err(rs.err().unwrap());
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
            auto_increment_keys: Vec::default(),
            columns: Vec::default(),
        };

        let mut col_name_to_id = HashMap::new();
        for i in 0..this.columns.len() {
            let col_decl = &this.columns[i];
            let ty = Self::convert_to_type(&col_decl.type_decl);
            if ty.is_err() {
                self.rs = Err(ty.err().unwrap());
                return;
            }
            let col = ColumnMetadata {
                name: col_decl.name.to_string(),
                id: i as u32,
                ty: ty.unwrap(),
                primary_key: col_decl.primary_key,
                auto_increment: col_decl.auto_increment,
                not_null: col_decl.not_null,
                default_value: "".to_string(),
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

        for key in &this.primary_keys {
            match col_name_to_id.get(&key.to_string()) {
                Some(col_id) => {
                    let exists = table.primary_keys.iter()
                        .cloned()
                        .find(|x| { *x == *col_id });
                    if exists.is_some() {
                        visit_error!(self, "Primary key: `{}` not found in declaration", key.as_str());
                    }
                    table.primary_keys.push(*col_id);
                }
                None =>
                    visit_error!(self, "Primary key: `{}` not found in declaration", key.as_str())
            }
        }
        match db.create_table(table, &mut locking_tables) {
            Err(e) => self.rs = Err(e),
            Ok(_) => ()
        }
    }

    fn visit_drop_table(&mut self, this: &mut DropTable) {
        let db = self.db.upgrade().unwrap();
        let mut locking_tables = db.lock_tables();
        if !locking_tables.contains_key(&this.table_name.to_string()) {
            if !this.if_exists {
                self.rs = Err(Status::corrupted(format!("Table `{}` not found",
                                                        this.table_name.as_str())));
            }
            return;
        }
        match db.drop_table(&this.table_name.to_string(), &mut locking_tables) {
            Err(e) => self.rs = Err(e),
            Ok(_) => ()
        }
    }

    fn visit_insert_into_table(&mut self, this: &mut InsertIntoTable) {
        let db = self.db.upgrade().unwrap();
        let tables = db.lock_tables();
        if !tables.contains_key(&this.table_name.to_string()) {
            visit_error!(self, "Table `{}` not found", this.table_name);
        }
        let table_ref = tables.get(&this.table_name.to_string()).unwrap().clone();
        drop(tables);


        let mut insertion_cols = Vec::new();
        let table = table_ref.lock().unwrap();
        let mut columns = {
            let tmp = ColumnSet::new(
                table.metadata.borrow().name.as_str(), &self.arena);
            ArenaBox::new(tmp, &self.arena)
        };
        for col_name in &this.columns_name {
            match table.get_col_by_name(&col_name.to_string()) {
                Some(col) => {
                    columns.append(col.name.as_str(),
                                   "",
                                   col.ty.clone());
                    insertion_cols.push(col.id)
                }
                None => visit_error!(self, "Column: `{}` not found in table: `{}`",
                    col_name.as_str(), this.table_name)
            }
        }

        let use_anonymous_row_key = table.metadata.borrow().primary_keys.is_empty();
        let anonymous_row_key_counter =  addr_of!(table.anonymous_row_key_counter);
        let cf = table.column_family.clone();
        drop(table);

        for row in &this.values {
            if row.len() < insertion_cols.len() {
                visit_error!(self, "Not enough number of row values for insertion, need: {}",
                    insertion_cols.len());
            }
        }

        let mut evaluator = Evaluator::new(&self.arena);
        let context = Arc::new(TupleContext::new(self.prepared_stmts.back().cloned()));
        let mut tuples = Vec::new();
        for row in this.values.iter_mut() {
            let mut row_key = Vec::new();
            if use_anonymous_row_key {
                let n = unsafe { &*anonymous_row_key_counter }.fetch_add(1, Ordering::AcqRel) + 1;
                DB::encode_anonymous_row_key(n, &mut row_key);
            }
            let mut tuple = Tuple::new(&columns, &row_key, &[], &self.arena);
            for i in 0..row.len() {
                let expr = &mut row[i];
                match evaluator.evaluate(expr.deref_mut(), context.clone()) {
                    Err(e) => {
                        self.rs = Err(e);
                        return;
                    }
                    Ok(value) => tuple.set(i, value)
                }
            }
            tuples.push(tuple);
        }

        match db.insert_rows(&cf, &this.table_name.to_string(), &tuples) {
            Ok(_) => (),
            Err(e) => self.rs = Err(e)
        }
    }

    fn visit_identifier(&mut self, _: &mut Identifier) { unreachable!() }
    fn visit_full_qualified_name(&mut self, _: &mut FullyQualifiedName) { unreachable!() }
    fn visit_unary_expression(&mut self, _: &mut UnaryExpression) { unreachable!() }
    fn visit_binary_expression(&mut self, _: &mut BinaryExpression) { unreachable!() }
    fn visit_call_function(&mut self, _: &mut CallFunction) { unreachable!() }
    fn visit_int_literal(&mut self, _: &mut Literal<i64>) { unreachable!() }
    fn visit_float_literal(&mut self, _: &mut Literal<f64>) { unreachable!() }
    fn visit_str_literal(&mut self, _: &mut Literal<ArenaStr>) { unreachable!() }
    fn visit_null_literal(&mut self, _: &mut Literal<()>) { unreachable!() }
    fn visit_placeholder(&mut self, _: &mut Placeholder) { unreachable!() }
}

pub struct ColumnSet {
    arena: Rc<RefCell<Arena>>,
    pub schema: ArenaStr,
    pub columns: ArenaVec<Column>,
}

impl ColumnSet {
    pub fn new(schema: &str, arena: &Rc<RefCell<Arena>>) -> Self {
        Self {
            arena: arena.clone(),
            schema: ArenaStr::from_arena(schema, arena),
            columns: ArenaVec::new(arena),
        }
    }

    pub fn append(&mut self, name: &str, desc: &str, ty: ColumnType) {
        let order = self.columns.len();
        self.columns.push(Column {
            name: ArenaStr::from_arena(name, &self.arena),
            desc: ArenaStr::from_arena(desc, &self.arena),
            ty,
            order,
        });
    }
}


pub struct Column {
    pub name: ArenaStr,
    pub desc: ArenaStr,
    pub ty: ColumnType,
    pub order: usize,
}

pub struct Tuple {
    row_key: NonNull<[u8]>,
    items: NonNull<[Value]>,
    columns_set: ArenaBox<ColumnSet>,
}

static EMPTY_ARRAY_DUMMY: [u8;0] = [0;0];

impl Tuple {
    pub fn new(columns: &ArenaBox<ColumnSet>, key: &[u8], suffix: &[u8], arena: &Rc<RefCell<Arena>>) -> Tuple {
        let len = columns.columns.len();
        let value_layout = Layout::new::<Value>();
        let layout = Layout::from_size_align(value_layout.size() * len, value_layout.align()).unwrap();
        let mut items = {
            let chunk = arena.borrow_mut().allocate(layout).unwrap();
            let addr = chunk.as_ptr() as *mut Value;
            let ptr = slice_from_raw_parts_mut(addr, len);
            NonNull::new(ptr).unwrap()
        };
        let row_key: NonNull<[u8]> = if key.is_empty() {
            let ptr = addr_of!(EMPTY_ARRAY_DUMMY) as *mut u8;
            NonNull::new(slice_from_raw_parts_mut(ptr, 0)).unwrap()
        } else {
            let layout = Layout::from_size_align(key.len() + suffix.len(), 4).unwrap();
            arena.borrow_mut().allocate(layout).unwrap()
        };
        unsafe {
            if !key.is_empty() {
                let dst = row_key.as_ptr() as *mut u8;
                copy_nonoverlapping(addr_of!(key[0]), dst, key.len());
                if !suffix.is_empty() {
                    let dst  = dst.add(key.len());
                    copy_nonoverlapping(addr_of!(suffix[0]), dst, suffix.len());
                }
            }
            for i in 0..len {
                items.as_mut()[i] = Value::Unit;
            }
        }
        Self {
            row_key,
            items,
            columns_set: columns.clone(),
        }
    }

    pub fn columns(&self) -> &ArenaBox<ColumnSet> { &self.columns_set }

    pub fn get_null(&self, i: usize) -> bool {
        self.get(i).is_null()
    }

    pub fn get_i64(&self, i: usize) -> Option<i64> {
        match self.get(i) {
            Value::Int(n) => Some(*n),
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

struct TupleContext {
    prepared_stmt: Option<ArenaBox<PreparedStatement>>,
}

impl TupleContext {
    pub fn new(prepared_stmt: Option<ArenaBox<PreparedStatement>>) -> Self {
        Self {prepared_stmt}
    }
}

impl Context for TupleContext {
    fn resolve(&self, _name: &str) -> Value {
        Value::Unit
    }

    fn resolve_fully_qualified(&self, _prefix: &str, _suffix: &str) -> Value {
        Value::Unit
    }

    fn invoke(&self, _callee: &str, _args: &[Value]) -> Value {
        Value::Unit
    }

    fn bound_param(&self, order: usize) -> Value {
        match self.prepared_stmt.as_ref() {
            Some(ps) => {
                if order >= ps.parameters_len() {
                    Value::Unit
                } else {
                    ps.parameters[order].clone()
                }
            }
            None => Value::Unit
        }
    }
}