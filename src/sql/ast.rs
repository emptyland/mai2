use std::cell::RefCell;
use std::rc::Rc;
use crate::arena::{Arena, ArenaStr, ArenaVec, Handle};
use crate::mai2::Snapshot;
use crate::sql::lexer::Token;

// macro_rules! ast_nodes {
//     () => {
//         [
//             (CreateTable, visit_create_table),
//             (DropTable, visit_drop_table),
//         ]
//     }
// }

//($($val:expr),+ $(,)?)
macro_rules! ast_nodes_impl {
    [$(($name:ident, $call:ident)),+ $(,)?] => {
        pub trait Visitor {
            $(fn $call(&self, this: &$name);)+
        }
        $(statement_impl!($name, $call);)+
    }
}

macro_rules! statement_impl {
    ($name:ident, $call:ident) => {
        impl Statement for $name {
            fn accept(&self, visitor: &dyn Visitor) {
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
    fn accept(&self, visitor: &dyn Visitor);
}

pub enum Operator {
    Plus
}

pub trait Expression: Statement {
    fn op(&self) -> &Operator;
    fn operands(&self) -> &[Handle<dyn Expression>];
    fn lhs(&self) -> &Handle<dyn Expression> { &self.operands()[0] }
    fn rhs(&self) -> &Handle<dyn Expression> { &self.operands()[1] }
}

pub struct CreateTable {
    pub table_name: ArenaStr,
    pub if_exists: bool,
    pub columns: ArenaVec<Handle<ColumnDeclaration>>,
}

pub struct ColumnDeclaration {
    pub name: ArenaStr,
    pub auto_increment: bool,
    pub type_decl: Handle<TypeDeclaration>,
    pub default_val: Option<Handle<dyn Expression>>,
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
}

pub struct BinaryExpression {
    op: Operator,
    operands: [Handle<dyn Expression>; 2]
}

impl Expression for BinaryExpression {
    fn op(&self) -> &Operator {
        &self.op
    }

    fn operands(&self) -> &[Handle<dyn Expression>] {
        &self.operands
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

    pub fn new_create_table(&self, table_name: ArenaStr, if_exists: bool) -> Handle<CreateTable> {
        Handle::new(CreateTable {
            table_name,
            if_exists,
            columns: ArenaVec::new(&self.arena),
        }, &self.arena)
    }

    // pub fn new_drop_table(&self, table_name: ArenaStr) -> Handle<dyn Statement> {
    //     Handle::new(DropTable {
    //         table_name
    //     }, &self.arena)
    // }

    pub fn new_column_decl(&self, name: ArenaStr,
                           auto_increment: bool,
                           type_decl: Handle<TypeDeclaration>,
                           default_val: Option<Handle<dyn Expression>>) -> Handle<ColumnDeclaration> {
        Handle::new(ColumnDeclaration {
            name,
            auto_increment,
            type_decl,
            default_val,
        }, &self.arena)
    }

    pub fn new_type_decl(&self, token: Token, len: usize, len_part: usize) -> Handle<TypeDeclaration> {
        Handle::new(TypeDeclaration {
            token,
            len,
            len_part,
        }, &self.arena)
    }
}

// impl <T: Statement> From<Handle<T>> for Handle<dyn Statement> {
//     fn from(value: Handle<T>) -> Self {
//         Self {
//             naked:
//             owns: value.owns.clone()
//         }
//     }
// }

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
        assert!(!node.if_exists);
        assert_eq!("a", node.table_name.as_str());

        let name = ArenaStr::new("col1", factory.arena.borrow_mut().deref_mut());
        let type_decl = factory.new_type_decl(Token::Varchar, 256, 0);
        let col_decl = factory.new_column_decl(name, false, type_decl, None);
        node.columns.push(col_decl);

        assert_eq!(1, node.columns.len());
    }

    #[test]
    fn trait_cast() {
        let factory = Factory::new();
        let name = ArenaStr::new("a", factory.arena.borrow_mut().deref_mut());
        let node = factory.new_create_table(name, false);
        let ast: Handle<dyn Statement> = node;
    }
}