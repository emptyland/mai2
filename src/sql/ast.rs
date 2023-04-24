use std::any::Any;
use std::cell::RefCell;
use std::ops::DerefMut;
use std::ptr::NonNull;
use std::rc::Rc;
use crate::base::{Arena, ArenaStr, ArenaVec, ArenaBox};
use crate::sql::lexer::Token;

macro_rules! ast_nodes_impl {
    [$(($name:ident, $call:ident)),+ $(,)?] => {
        pub trait Visitor {
            $(fn $call(&mut self, this: &mut $name);)+
        }
        $(statement_impl!($name, $call);)+
    }
}

macro_rules! statement_impl {
    ($name:ident, $call:ident) => {
        impl Statement for $name {
            fn as_any(&self) -> &dyn Any { self }
            fn as_mut_any(&mut self) -> &mut dyn Any { self }
            fn accept(&mut self, visitor: &mut dyn Visitor) {
                visitor.$call(self)
            }
        }
    }
}

ast_nodes_impl![
    (CreateTable, visit_create_table),
    (DropTable, visit_drop_table),
    (BinaryExpression, visit_binary_expression)
];


pub trait Statement {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn accept(&mut self, visitor: &mut dyn Visitor);
}

pub enum Operator {
    Plus
}

pub trait Expression: Statement {
    fn op(&self) -> &Operator;
    fn operands(&self) -> &[ArenaBox<dyn Expression>];
    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>];
    fn lhs(&self) -> &ArenaBox<dyn Expression> { &self.operands()[0] }
    fn lhs_mut(&mut self) -> &mut ArenaBox<dyn Expression> { &mut self.operands_mut()[0] }
    fn rhs(&self) -> &ArenaBox<dyn Expression> { &self.operands()[1] }
    fn rhs_mut(&mut self) -> &mut ArenaBox<dyn Expression> { &mut self.operands_mut()[1] }
}

pub struct CreateTable {
    pub table_name: ArenaStr,
    pub if_not_exists: bool,
    pub columns: ArenaVec<ArenaBox<ColumnDeclaration>>,
    pub primary_keys: ArenaVec<ArenaStr>
}

pub struct ColumnDeclaration {
    pub name: ArenaStr,
    pub auto_increment: bool,
    pub not_null: bool,
    pub primary_key: bool,
    pub type_decl: ArenaBox<TypeDeclaration>,
    pub default_val: Option<ArenaBox<dyn Expression>>,
}

#[derive(Debug)]
pub struct TypeDeclaration {
    pub token: Token,
    pub len: usize,
    pub len_part: usize,
}

#[derive(Debug)]
pub struct DropTable {
    pub table_name: ArenaStr,
    pub if_exists: bool,
}

pub struct BinaryExpression {
    op: Operator,
    operands: [ArenaBox<dyn Expression>; 2]
}

impl Expression for BinaryExpression {
    fn op(&self) -> &Operator {
        &self.op
    }

    fn operands(&self) -> &[ArenaBox<dyn Expression>] {
        &self.operands
    }

    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] {
        &mut self.operands
    }
}

pub struct Factory {
    pub arena: Rc<RefCell<Arena>>,
}

impl Factory {
    pub fn new() -> Self {
        Self {
            arena: Arena::new_rc()
        }
    }

    pub fn new_create_table(&self, table_name: ArenaStr, if_not_exists: bool) -> ArenaBox<CreateTable> {
        ArenaBox::new(CreateTable {
            table_name,
            if_not_exists,
            columns: ArenaVec::new(&self.arena),
            primary_keys: ArenaVec::new(&self.arena),
        }, &self.arena)
    }

    pub fn new_drop_table(&self, table_name: ArenaStr, if_exists: bool) -> ArenaBox<DropTable> {
        ArenaBox::new(DropTable {
            table_name,
            if_exists,
        }, &self.arena)
    }

    pub fn new_column_decl(&self, name: ArenaStr,
                           auto_increment: bool,
                           not_null: bool,
                           primary_key: bool,
                           type_decl: ArenaBox<TypeDeclaration>,
                           default_val: Option<ArenaBox<dyn Expression>>) -> ArenaBox<ColumnDeclaration> {
        ArenaBox::new(ColumnDeclaration {
            name,
            auto_increment,
            not_null,
            primary_key,
            type_decl,
            default_val,
        }, &self.arena)
    }

    pub fn new_type_decl(&self, token: Token, len: usize, len_part: usize) -> ArenaBox<TypeDeclaration> {
        ArenaBox::new(TypeDeclaration {
            token,
            len,
            len_part,
        }, &self.arena)
    }

    pub fn str(&self, raw: &str) -> ArenaStr {
        ArenaStr::new(raw, self.arena.borrow_mut().deref_mut())
    }
}

impl <T: Statement + 'static> From<ArenaBox<T>> for ArenaBox<dyn Statement> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr(), value.owns())
    }
}

impl <T: Statement + 'static> From<ArenaBox<dyn Statement>> for ArenaBox<T> {
    fn from(value: ArenaBox<dyn Statement>) -> Self {
        let ptr = NonNull::from(value.as_any().downcast_ref::<T>().unwrap());
        Self::from_ptr(ptr, value.owns())
    }
}


#[cfg(test)]
mod tests {
    use std::ops::DerefMut;
    use std::ptr::NonNull;
    use super::*;

    #[test]
    fn sanity() {
        let factory = Factory::new();
        let name = ArenaStr::new("a", factory.arena.borrow_mut().deref_mut());
        let mut node = factory.new_create_table(name, false);
        assert!(!node.if_not_exists);
        assert_eq!("a", node.table_name.as_str());

        let name = ArenaStr::new("col1", factory.arena.borrow_mut().deref_mut());
        let type_decl = factory.new_type_decl(Token::Varchar, 256, 0);
        let col_decl = factory.new_column_decl(name, false,
                                               true, false, type_decl,
                                               None);
        node.columns.push(col_decl);

        assert_eq!(1, node.columns.len());
    }

    #[test]
    fn trait_cast() {
        let factory = Factory::new();
        let name = ArenaStr::new("a", factory.arena.borrow_mut().deref_mut());
        let node = factory.new_create_table(name, false);
        let ast: ArenaBox<dyn Statement> = node.into();
    }
}