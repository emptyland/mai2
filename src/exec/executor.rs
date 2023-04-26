use std::cell::RefCell;
use std::io::Read;
use std::rc::Rc;
use std::sync::Weak;

use crate::base::{Arena, ArenaStr};
use crate::exec::db::{ColumnMetadata, ColumnType, DB, TableMetadata};
use crate::exec::from_sql_result;
use crate::sql::ast::{BinaryExpression, CallFunction, CreateTable, DropTable, Factory, FullyQualifiedName, Identifier, InsertIntoTable, Literal, TypeDeclaration, UnaryExpression, Visitor};
use crate::sql::parser::Parser;
use crate::{Corrupting, Result, Status};
use crate::sql::lexer::Token;
use crate::sql::serialize::serialize_yaml_to_string;

pub struct Executor {
    pub db: Weak<DB>,
    arena: Rc<RefCell<Arena>>,
    rs: Result<()>
}

impl Executor {
    pub fn new(db: &Weak<DB>) -> Self {
        Self {
            db: db.clone(),
            arena: Arena::new_rc(),
            rs: Ok(())
        }
    }

    pub fn execute(&mut self, reader: &mut dyn Read, arena: &Rc<RefCell<Arena>>) -> Result<()> {
        // clear latest result;
        self.rs = Ok(());

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
            primary_keys: vec![],
            columns: vec![],
        };
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
                not_null: col_decl.not_null,
                default_value: "".to_string(),
            };
            table.columns.push(col);
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
        todo!()
    }

    fn visit_identifier(&mut self, this: &mut Identifier) {
        todo!()
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        todo!()
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        todo!()
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        todo!()
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        todo!()
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        todo!()
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        todo!()
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        todo!()
    }

    fn visit_null_literal(&mut self, this: &mut Literal<()>) {
        todo!()
    }
}