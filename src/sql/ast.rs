use std::any::Any;
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::ops::DerefMut;
use std::ptr::{addr_of, addr_of_mut, NonNull};
use std::rc::Rc;
use std::slice;
use num_enum::TryFromPrimitive;
use crate::base::{Arena, ArenaStr, ArenaVec, ArenaBox, ArenaMut, ArenaRef};
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
    (CreateIndex, visit_create_index),
    (DropIndex, visit_drop_index),
    (InsertIntoTable, visit_insert_into_table),
    (Collection, visit_collection),
    (Select, visit_select),
    (FromClause, visit_from_clause),
    (JoinClause, visit_join_clause),
    (Identifier, visit_identifier),
    (FullyQualifiedName, visit_full_qualified_name),
    (UnaryExpression, visit_unary_expression),
    (BinaryExpression, visit_binary_expression),
    (InLiteralSet, visit_in_literal_set),
    (InRelation, visit_in_relation),
    (CallFunction, visit_call_function),
    (Literal<i64>, visit_int_literal),
    (Literal<f64>, visit_float_literal),
    (Literal<ArenaStr>, visit_str_literal),
    (Literal<()>, visit_null_literal),
    (Placeholder, visit_placeholder),
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

        static OPS_META: [OpMeta; 17] = [
            $(OpMeta {name: stringify!($name), literal: $op, priority: $prio},)+
        ];
    }
}

ast_ops_impl![
    (Lit, "<lit>", 110),

    (In, "in", 110),
    (NotIn, "not in", 110),

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
    pub primary_keys: ArenaVec<ArenaStr>,
    pub secondary_indices: ArenaVec<ArenaBox<IndexDeclaration>>,
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
pub struct IndexDeclaration {
    pub name: ArenaStr,
    pub unique: bool,
    pub key_parts: ArenaVec<ArenaStr>,
}

#[derive(Debug)]
pub struct DropTable {
    pub table_name: ArenaStr,
    pub if_exists: bool,
}

#[derive(Debug)]
pub struct CreateIndex {
    pub name: ArenaStr,
    pub unique: bool,
    pub table_name: ArenaStr,
    pub key_parts: ArenaVec<ArenaStr>,
}

#[derive(Debug)]
pub struct DropIndex {
    pub name: ArenaStr,
    pub primary_key: bool,
    pub table_name: ArenaStr,
}

pub struct InsertIntoTable {
    pub table_name: ArenaStr,
    pub columns_name: ArenaVec<ArenaStr>,
    pub values: ArenaVec<ArenaVec<ArenaBox<dyn Expression>>>,
}

pub trait Relation: Statement {
    fn alias(&self) -> &ArenaStr;
    fn alias_as(&mut self, name: ArenaStr);
}

// DCL
#[derive(Debug)]
pub enum SetOp {
    UnionDistinct,
    UnionAll,
}

pub struct Collection {
    pub op: SetOp,
    pub lhs: ArenaBox<dyn Relation>,
    pub rhs: ArenaBox<dyn Relation>,
    pub alias: ArenaStr,
}

impl Relation for Collection {
    fn alias(&self) -> &ArenaStr { &self.alias }
    fn alias_as(&mut self, name: ArenaStr) { self.alias = name; }
}

#[derive(Debug)]
pub enum JoinOp {
    LeftOuterJoin,
    RightOuterJoin,
    InnerJoin,
    CrossJoin,
}

impl Display for JoinOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinOp::LeftOuterJoin => f.write_str("LEFT OUTER JOIN"),
            JoinOp::RightOuterJoin => f.write_str("RIGHT OUTER JOIN"),
            JoinOp::InnerJoin => f.write_str("INNER JOIN"),
            JoinOp::CrossJoin => f.write_str("CROSS JOIN"),
        }
    }
}

pub struct JoinClause {
    pub op: JoinOp,
    pub lhs: ArenaBox<dyn Relation>,
    pub rhs: ArenaBox<dyn Relation>,
    pub on_clause: ArenaBox<dyn Expression>,
    pub alias: ArenaStr,
}

impl Relation for JoinClause {
    fn alias(&self) -> &ArenaStr { &self.alias }
    fn alias_as(&mut self, name: ArenaStr) { self.alias = name; }
}

// select * from b t1
// left join b t2 on (t1.id = t2.id)
// left join c t3 on (t2.id = t3.id)
// where
pub struct Select {
    pub distinct: bool,
    pub columns: ArenaVec<SelectColumnItem>,
    pub from_clause: Option<ArenaBox<dyn Relation>>,
    pub where_clause: Option<ArenaBox<dyn Expression>>,
    pub group_by_clause: ArenaVec<ArenaBox<dyn Expression>>,
    pub order_by_clause: ArenaVec<ArenaBox<dyn Expression>>,
    pub limit_clause: Option<ArenaBox<dyn Expression>>,
    pub offset_clause: Option<ArenaBox<dyn Expression>>,
    pub alias: ArenaStr,
}

impl Relation for Select {
    fn alias(&self) -> &ArenaStr { &self.alias }
    fn alias_as(&mut self, name: ArenaStr) { self.alias = name; }
}

pub enum SelectColumn {
    Expr(ArenaBox<dyn Expression>),
    Star,
    SuffixStar(ArenaStr),
}

pub struct SelectColumnItem {
    pub expr: SelectColumn,
    pub alias: ArenaStr,
}

pub struct FromClause {
    pub name: ArenaStr,
    pub alias: ArenaStr,
}

impl Relation for FromClause {
    fn alias(&self) -> &ArenaStr { &self.alias }
    fn alias_as(&mut self, name: ArenaStr) { self.alias = name; }
}

// Expression:

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
    operand: ArenaBox<dyn Expression>,
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
    operands: [ArenaBox<dyn Expression>; 2],
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
    pub args: ArenaVec<ArenaBox<dyn Expression>>,
}

impl Expression for CallFunction {
    fn op(&self) -> &Operator { &Operator::Lit }
    fn operands(&self) -> &[ArenaBox<dyn Expression>] { self.args.as_slice() }
    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] { self.args.as_slice_mut() }
}

pub struct InLiteralSet {
    pub not_in: bool,
    pub lhs: ArenaBox<dyn Expression>,
    pub set: ArenaVec<ArenaBox<dyn Expression>>,
}

impl Expression for InLiteralSet {
    fn op(&self) -> &Operator { if self.not_in { &Operator::NotIn } else { &Operator::In } }

    fn operands(&self) -> &[ArenaBox<dyn Expression>] {
        &[]
    }

    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] {
        &mut []
    }
}

pub struct InRelation {
    pub not_in: bool,
    pub lhs: ArenaBox<dyn Expression>,
    pub set: ArenaVec<ArenaBox<dyn Relation>>,
}

impl Expression for InRelation {
    fn op(&self) -> &Operator { if self.not_in { &Operator::NotIn } else { &Operator::In } }

    fn operands(&self) -> &[ArenaBox<dyn Expression>] {
        &[]
    }

    fn operands_mut(&mut self) -> &mut [ArenaBox<dyn Expression>] {
        &mut []
    }
}

pub struct Placeholder {
    pub order: usize,
}

expression_impl!(Placeholder);

pub struct Factory {
    pub arena: ArenaMut<Arena>,
}

impl Factory {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            arena: arena.clone()
        }
    }

    pub fn new_create_table(&self, table_name: ArenaStr, if_not_exists: bool)
                            -> ArenaBox<CreateTable> {
        let mut arena = self.arena.clone();
        ArenaBox::new(CreateTable {
            table_name,
            if_not_exists,
            columns: ArenaVec::new(&mut arena),
            primary_keys: ArenaVec::new(&mut arena),
            secondary_indices: ArenaVec::new(&mut arena),
        }, arena.deref_mut())
    }

    pub fn new_drop_table(&self, table_name: ArenaStr, if_exists: bool) -> ArenaBox<DropTable> {
        let mut arena = self.arena.clone();
        ArenaBox::new(DropTable {
            table_name,
            if_exists,
        }, arena.deref_mut())
    }

    pub fn new_create_index(&self, name: ArenaStr, unique: bool, table_name: ArenaStr)
                            -> ArenaBox<CreateIndex> {
        let mut arena = self.arena.clone();
        ArenaBox::new(CreateIndex {
            name,
            table_name,
            unique,
            key_parts: ArenaVec::new(&mut arena),
        }, arena.deref_mut())
    }

    pub fn new_drop_index(&self, name: ArenaStr, primary_key: bool, table_name: ArenaStr)
                          -> ArenaBox<DropIndex> {
        let mut arena = self.arena.clone();
        ArenaBox::new(DropIndex {
            name,
            primary_key,
            table_name,
        }, arena.deref_mut())
    }

    pub fn new_collection(&self, op: SetOp, lhs: ArenaBox<dyn Relation>, rhs: ArenaBox<dyn Relation>)
                          -> ArenaBox<Collection> {
        let mut arena = self.arena.clone();
        ArenaBox::new(Collection {
            op,
            lhs,
            rhs,
            alias: ArenaStr::default(),
        }, arena.deref_mut())
    }

    pub fn new_join_clause(&self, op: JoinOp, lhs: ArenaBox<dyn Relation>, rhs: ArenaBox<dyn Relation>,
                           on_clause: ArenaBox<dyn Expression>) -> ArenaBox<JoinClause> {
        let mut arena = self.arena.clone();
        ArenaBox::new(JoinClause {
            op,
            lhs,
            rhs,
            on_clause,
            alias: ArenaStr::default(),
        }, arena.deref_mut())
    }

    pub fn new_from_clause(&self, name: ArenaStr) -> ArenaBox<FromClause> {
        let mut arena = self.arena.clone();
        ArenaBox::new(FromClause {
            name,
            alias: ArenaStr::default(),
        }, arena.deref_mut())
    }

    pub fn new_select(&self, distinct: bool) -> ArenaBox<Select> {
        let mut arena = self.arena.clone();
        ArenaBox::new(Select {
            distinct,
            columns: ArenaVec::new(&mut arena),
            from_clause: None,
            where_clause: None,
            group_by_clause: ArenaVec::new(&mut arena),
            order_by_clause: ArenaVec::new(&mut arena),
            limit_clause: None,
            offset_clause: None,
            alias: ArenaStr::default(),
        }, arena.deref_mut())
    }

    pub fn new_column_decl(&self, name: ArenaStr,
                           auto_increment: bool,
                           not_null: bool,
                           primary_key: bool,
                           type_decl: ArenaBox<TypeDeclaration>,
                           default_val: Option<ArenaBox<dyn Expression>>)
                           -> ArenaBox<ColumnDeclaration> {
        let mut arena = self.arena.clone();
        ArenaBox::new(ColumnDeclaration {
            name,
            auto_increment,
            not_null,
            primary_key,
            type_decl,
            default_val,
        }, arena.deref_mut())
    }

    pub fn new_type_decl(&self, token: Token, len: usize, len_part: usize)
                         -> ArenaBox<TypeDeclaration> {
        let mut arena = self.arena.clone();
        ArenaBox::new(TypeDeclaration {
            token,
            len,
            len_part,
        }, arena.deref_mut())
    }

    pub fn new_index_decl(&self, name: ArenaStr, unique: bool) -> ArenaBox<IndexDeclaration> {
        let mut arena = self.arena.clone();
        ArenaBox::new(IndexDeclaration {
            name,
            unique,
            key_parts: ArenaVec::new(&mut arena),
        }, arena.deref_mut())
    }

    pub fn new_insert_into_table(&self, table_name: ArenaStr) -> ArenaBox<InsertIntoTable> {
        let mut arena = self.arena.clone();
        ArenaBox::new(InsertIntoTable {
            table_name,
            columns_name: ArenaVec::new(&mut arena),
            values: ArenaVec::new(&mut arena),
        }, arena.deref_mut())
    }

    pub fn new_unary_expr(&self, op: Operator, operand: ArenaBox<dyn Expression>) -> ArenaBox<UnaryExpression> {
        let mut arena = self.arena.clone();
        ArenaBox::new(UnaryExpression {
            op,
            operand,
        }, arena.deref_mut())
    }

    pub fn new_binary_expr(&self, op: Operator, lhs: ArenaBox<dyn Expression>,
                           rhs: ArenaBox<dyn Expression>) -> ArenaBox<BinaryExpression> {
        let mut arena = self.arena.clone();
        ArenaBox::new(BinaryExpression {
            op,
            operands: [lhs, rhs],
        }, arena.deref_mut())
    }

    pub fn new_call_function(&self, callee_name: ArenaStr, distinct: bool) -> ArenaBox<CallFunction> {
        let mut arena = self.arena.clone();
        ArenaBox::new(CallFunction {
            callee_name,
            distinct,
            in_args_star: false,
            args: ArenaVec::new(&self.arena),
        }, arena.deref_mut())
    }

    pub fn new_identifier(&self, symbol: ArenaStr) -> ArenaBox<Identifier> {
        let mut arena = self.arena.clone();
        ArenaBox::new(Identifier { symbol }, arena.deref_mut())
    }

    pub fn new_fully_qualified_name(&self, prefix: ArenaStr, suffix: ArenaStr) -> ArenaBox<FullyQualifiedName> {
        let mut arena = self.arena.clone();
        ArenaBox::new(FullyQualifiedName {
            prefix,
            suffix,
        }, arena.deref_mut())
    }

    pub fn new_literal<T>(&self, data: T) -> ArenaBox<Literal<T>> {
        let mut arena = self.arena.clone();
        ArenaBox::new(Literal {
            data,
        }, arena.deref_mut())
    }

    pub fn new_placeholder(&self, order: usize) -> ArenaBox<Placeholder> {
        let mut arena = self.arena.clone();
        ArenaBox::new(Placeholder {
            order,
        }, arena.deref_mut())
    }

    pub fn str(&self, raw: &str) -> ArenaStr {
        let mut arena = self.arena.clone();
        ArenaStr::new(raw, arena.deref_mut())
    }
}

// impl From<ArenaBox<dyn Relation + 'static>> for ArenaBox<dyn Statement> {
//     fn from(value: ArenaBox<dyn Relation>) -> Self {
//         Self::from_ptr(value.ptr(), value.owns())
//     }
// }

impl<T: Statement + 'static> From<ArenaBox<T>> for ArenaBox<dyn Statement> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr())
    }
}

impl<T: Statement + 'static> From<ArenaBox<dyn Statement>> for ArenaBox<T> {
    fn from(value: ArenaBox<dyn Statement>) -> Self {
        let ptr = NonNull::from(value.as_any().downcast_ref::<T>().unwrap());
        Self::from_ptr(ptr)
    }
}

impl<T: Expression + 'static> From<ArenaBox<T>> for ArenaBox<dyn Expression> {
    fn from(value: ArenaBox<T>) -> Self {
        Self::from_ptr(value.ptr())
    }
}

impl<T: Expression + 'static> From<ArenaBox<dyn Expression>> for ArenaBox<T> {
    fn from(value: ArenaBox<dyn Expression>) -> Self {
        let ptr = NonNull::from(value.as_any().downcast_ref::<T>().unwrap());
        Self::from_ptr(ptr)
    }
}

macro_rules! try_cast_to_relation {
    ($value:ident, $ty:ty) => {
        if $value.as_any().downcast_ref::<$ty>().is_some() {
            let core = ArenaBox::<$ty>::from($value);
            return ArenaBox::<dyn Relation>::from_ptr(core.ptr());
        }
    }
}

impl From<ArenaBox<dyn Statement>> for ArenaBox<dyn Relation> {
    fn from(value: ArenaBox<dyn Statement>) -> Self {
        try_cast_to_relation!(value, Select);
        try_cast_to_relation!(value, Collection);
        try_cast_to_relation!(value, JoinClause);
        try_cast_to_relation!(value, FromClause);
        unreachable!()
    }
}


#[cfg(test)]
mod tests {
    use std::ops::DerefMut;
    use std::ptr::NonNull;
    use super::*;

    #[test]
    fn sanity() {
        let mut arena = Arena::new_ref();
        let factory = Factory::new(&arena.get_mut());
        let name = factory.str("a");
        let mut node = factory.new_create_table(name, false);
        assert!(!node.if_not_exists);
        assert_eq!("a", node.table_name.as_str());

        let name = factory.str("col1");
        let type_decl = factory.new_type_decl(Token::Varchar, 256, 0);
        let col_decl = factory.new_column_decl(name, false,
                                               true, false, type_decl,
                                               None);
        node.columns.push(col_decl);

        assert_eq!(1, node.columns.len());
    }

    #[test]
    fn trait_cast() {
        let mut arena = Arena::new_ref();
        let factory = Factory::new(&arena.get_mut());
        let name = factory.str("a");
        let node = factory.new_create_table(name, false);
        let ast: ArenaBox<dyn Statement> = node.into();
    }
}