use std::any::Any;
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::ops::DerefMut;
use std::ptr::{addr_of, addr_of_mut, NonNull};
use std::rc::Rc;
use std::slice;
use num_enum::TryFromPrimitive;
use crate::base::{Arena, ArenaStr, ArenaVec, ArenaBox};
use crate::sql::lexer::Token;

macro_rules! ast_nodes_impl {
    [$(($name:ty, $call:ident)),+ $(,)?] => {
        pub trait Visitor {
            $(fn $call(&mut self, this: &mut $name);)+
        }
        $(statement_impl!($name, $call);)+
    }
}

macro_rules! statement_impl {
    ($name:ty, $call:ident) => {
        impl Statement for $name {
            fn as_any(&self) -> &dyn Any { self }
            fn as_mut_any(&mut self) -> &mut dyn Any { self }
            fn accept(&mut self, visitor: &mut dyn Visitor) {
                visitor.$call(self)
            }
        }
    }
}

macro_rules! expression_impl {
    ($name:ty) => {
        impl Expression for $name {
            fn op( & self ) -> & Operator { & Operator::Lit }
            fn operands( & self ) -> & [ArenaBox< dyn Expression > ] { & [] }
            fn operands_mut( & mut self ) -> & mut [ArenaBox < dyn Expression > ] { & mut [] }
        }
    }
}

ast_nodes_impl![
    (CreateTable, visit_create_table),
    (DropTable, visit_drop_table),
    (InsertIntoTable, visit_insert_into_table),
    (Identifier, visit_identifier),
    (FullyQualifiedName, visit_full_qualified_name),
    (UnaryExpression, visit_unary_expression),
    (BinaryExpression, visit_binary_expression),
    (CallFunction, visit_call_function),
    (Literal<i64>, visit_int_literal),
    (Literal<f64>, visit_float_literal),
    (Literal<ArenaStr>, visit_str_literal),
    (Literal<()>, visit_null_literal),
];


pub trait Statement {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn accept(&mut self, visitor: &mut dyn Visitor);
}

macro_rules! ast_ops_impl {
    [$(($name:ident, $op:expr, $prio:expr)),+ $(,)?] => {
        #[repr(u32)]
        #[derive(Clone, Eq, PartialEq, TryFromPrimitive)]
        pub enum Operator {
            $($name,)+
        }

        static OPS_META: [OpMeta; 15] = [
            $(OpMeta {name: stringify!($name), literal: $op, priority: $prio},)+
        ];
    }
}

ast_ops_impl! [
    (Lit, "<lit>", 110),
    (Minus, "-", 110),
    (Not, "not", 110),

    (Add, "+", 90),
    (Sub, "-", 90),

    (Mul, "*", 100),
    (Div, "/", 100),

    (Eq, "=", 60),
    (Ne, "<>", 60),
    (Lt, "<", 60),
    (Le, "<=", 60),
    (Gt, ">", 60),
    (Ge, ">=", 60),

    (And, "and", 20),
    (Or, "or", 10),
];

impl Operator {
    pub fn priority(&self) -> i32 { self.metadata().priority }

    pub fn name(&self) -> &'static str { self.metadata().name }

    pub fn metadata(&self) -> &'static OpMeta {
        let index = self.clone() as u32;
        &OPS_META[index as usize]
    }

    pub fn is_unary(&self) -> bool {
        match self {
            Self::Minus | Self::Not | Self::Lit => true,
            _ => false
        }
    }

    pub fn to_unary(&self) -> Self {
        match self {
            Self::Sub => Self::Minus,
            Self::Not => Self::Not,
            _ => unreachable!()
        }
    }

    pub fn is_binary(&self) -> bool { !self.is_unary() }

    pub fn from_token(token: &Token) -> Option<Self> {
        match token {
            Token::Plus => Some(Self::Add),
            Token::Minus => Some(Self::Sub),
            Token::Star => Some(Self::Mul),
            Token::Div => Some(Self::Div),
            Token::Eq => Some(Self::Eq),
            Token::Ne => Some(Self::Ne),
            Token::Lt => Some(Self::Lt),
            Token::Le => Some(Self::Le),
            Token::Gt => Some(Self::Gt),
            Token::Ge => Some(Self::Ge),
            Token::And => Some(Self::And),
            Token::Or => Some(Self::Or),
            Token::Not => Some(Self::Not),
            _ => None
        }
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({})", self.name(), self.metadata().literal)
    }
}

pub struct OpMeta {
    pub name: &'static str,
    pub literal: &'static str,
    pub priority: i32,
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

pub struct InsertIntoTable {
    pub table_name: ArenaStr,
    pub columns_name: ArenaVec<ArenaStr>,
    pub values: ArenaVec<ArenaVec<ArenaBox<dyn Expression>>>
}

pub struct Identifier {
    pub symbol: ArenaStr,
}

expression_impl!(Identifier);

pub struct Literal<T> {
    pub data: T,
}

expression_impl!(Literal<i64>);
expression_impl!(Literal<f64>);
expression_impl!(Literal<ArenaStr>);
expression_impl!(Literal<()>);

pub struct UnaryExpression {
    op: Operator,
    operand: ArenaBox<dyn Expression>
}

impl Expression for UnaryExpression {
    fn op(&self) -> &Operator { &self.op }

    fn operands(&self) -> &[ArenaBox<dyn Expression>] {
        let ptr = addr_of!(self.operand);
        unsafe { slice::from_raw_parts(ptr, 1) }
    }

    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] {
        let ptr = addr_of_mut!(self.operand);
        unsafe { slice::from_raw_parts_mut(ptr, 1) }
    }
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

// fully-qualified
pub struct FullyQualifiedName {
    pub prefix: ArenaStr,
    pub suffix: ArenaStr,
}

expression_impl!(FullyQualifiedName);

pub struct CallFunction {
    pub callee_name: ArenaStr,
    pub distinct: bool,
    pub in_args_star: bool,
    pub args: ArenaVec<ArenaBox<dyn Expression>>
}

impl Expression for CallFunction {
    fn op(&self) -> &Operator { &Operator::Lit }
    fn operands(&self) -> &[ArenaBox<dyn Expression>] { self.args.as_slice() }
    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] { self.args.as_slice_mut() }
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

    pub fn from(arena: &Rc<RefCell<Arena>>) -> Self {
        Self {
            arena: arena.clone(),
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

    pub fn new_insert_into_table(&self, table_name: ArenaStr) -> ArenaBox<InsertIntoTable> {
        ArenaBox::new(InsertIntoTable {
            table_name,
            columns_name: ArenaVec::new(&self.arena),
            values: ArenaVec::new(&self.arena),
        }, &self.arena)
    }

    pub fn new_unary_expr(&self, op: Operator, operand: ArenaBox<dyn Expression>) -> ArenaBox<UnaryExpression> {
        ArenaBox::new(UnaryExpression {
            op,
            operand
        }, &self.arena)
    }

    pub fn new_binary_expr(&self, op: Operator, lhs: ArenaBox<dyn Expression>,
                           rhs: ArenaBox<dyn Expression>) -> ArenaBox<BinaryExpression> {
        ArenaBox::new(BinaryExpression {
            op,
            operands: [lhs, rhs]
        }, &self.arena)
    }

    pub fn new_call_function(&self, callee_name: ArenaStr, distinct: bool) -> ArenaBox<CallFunction> {
        ArenaBox::new(CallFunction {
            callee_name,
            distinct,
            in_args_star: false,
            args: ArenaVec::new(&self.arena),
        }, &self.arena)
    }

    pub fn new_identifier(&self, symbol: ArenaStr) -> ArenaBox<Identifier> {
        ArenaBox::new(Identifier { symbol }, &self.arena)
    }

    pub fn new_fully_qualified_name(&self, prefix: ArenaStr, suffix: ArenaStr) -> ArenaBox<FullyQualifiedName> {
        ArenaBox::new(FullyQualifiedName{
            prefix,
            suffix
        }, &self.arena)
    }

    pub fn new_literal<T>(&self, data: T) -> ArenaBox<Literal<T>> {
        ArenaBox::new(Literal {
            data,
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

impl <T: Expression + 'static> From<ArenaBox<T>> for ArenaBox<dyn Expression> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr(), value.owns())
    }
}

impl <T: Expression + 'static> From<ArenaBox<dyn Expression>> for ArenaBox<T> {
    fn from(value: ArenaBox<dyn Expression>) -> Self {
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