use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::ops::{Add, DerefMut, Sub, Mul};
use std::str::FromStr;
use std::sync::Arc;
use crate::{Corrupting, Result, Status};
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaStr, ArenaVec};
use crate::exec::db::ColumnType;
use crate::exec::function::{AnyFn, ExecutionContext, new_any_fn, Signature, UDF};
use crate::sql::ast::*;
use crate::sql::lexer::Token::Or;

pub struct Reducer<T> {
    arena: ArenaMut<Arena>,
    rs: Status,
    env: ArenaVec<Arc<dyn Context>>,
    stack: ArenaVec<T>,
}

pub type Evaluator = Reducer<Value>;
pub type TypingReducer = Reducer<ColumnType>;

pub trait Context {
    fn fast_access(&self, _i: usize) -> &Value { &Value::Undefined }
    fn resolve(&self, _name: &str) -> Value {
        Value::Undefined
    }
    fn resolve_fully_qualified(&self, _prefix: &str, _suffix: &str) -> Value {
        Value::Undefined
    }
    fn bound_param(&self, order: usize) -> &Value;
    fn get_udf(&self, _name: &str) -> Option<ArenaBox<dyn UDF>> { None }
    // aggregate
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum Value {
    #[default]
    Undefined,
    NegativeInf, // -∞
    PositiveInf, // +∞
    Null,
    Int(i64),
    Float(f64),
    Str(ArenaStr),
}

impl Value {
    pub fn is_null(&self) -> bool { self.eq(&Value::Null) }
    pub fn is_not_null(&self) -> bool { !self.is_null() && !self.is_undefined() }
    pub fn is_undefined(&self) -> bool { self.eq(&Value::Undefined) }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Undefined => f.write_str("<undefined>"),
            Self::NegativeInf => f.write_str("-∞"),
            Self::PositiveInf => f.write_str("+∞"),
            Self::Null => f.write_str("NULL"),
            Self::Int(n) => write!(f, "{n}"),
            Self::Float(n) => write!(f, "{n}"),
            Self::Str(s) => write!(f, "\"{s}\"")
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        debug_assert!(!self.is_undefined() && !other.is_undefined());
        if self.is_null() || other.is_null() {
            return None;
        }
        match self {
            Self::NegativeInf => Some(Ordering::Less),
            Self::PositiveInf => Some(Ordering::Greater),
            Self::Int(a) => match other {
                Self::Int(b) => a.partial_cmp(b),
                Self::NegativeInf => Some(Ordering::Greater),
                Self::PositiveInf => Some(Ordering::Less),
                _ => None
            }
            Self::Float(a) => match other {
                Self::Float(b) => a.partial_cmp(b),
                Self::NegativeInf => Some(Ordering::Greater),
                Self::PositiveInf => Some(Ordering::Less),
                _ => None
            }
            Self::Str(a) => match other {
                Self::Str(b) => a.as_str().partial_cmp(b.as_str()),
                Self::NegativeInf => Some(Ordering::Greater),
                Self::PositiveInf => Some(Ordering::Less),
                _ => None
            }
            _ => unreachable!()
        }
    }
}

unsafe impl Sync for Value {}

impl <T: Clone> Reducer<T> {
    pub fn new(arena: &ArenaMut<Arena>) -> Self {
        Self {
            arena: arena.clone(),
            rs: Status::Ok,
            env: ArenaVec::new(arena),
            stack: ArenaVec::new(arena),
        }
    }

    pub fn env_enter(&mut self, env: Arc<dyn Context>) {
        self.env.push(env);
    }

    pub fn env_exit(&mut self) {
        self.env.truncate(self.env.len() - 1);
    }

    pub fn env(&self) -> &Arc<dyn Context> {
        self.env.back().unwrap()
    }

    fn ret(&mut self, elem: T) {
        self.stack.push(elem);
    }

    fn take_top(&mut self) -> T { self.stack.pop().unwrap() }
}

impl Evaluator {
    pub fn evaluate(&mut self, expr: &mut dyn Expression, env: Arc<dyn Context>) -> Result<Value> {
        self.rs = Status::Ok;
        self.stack.clear();
        self.env_enter(env);
        expr.accept(self);
        self.env_exit();
        match self.rs {
            Status::Ok => Ok(self.take_top()),
            _ => Err(self.rs.clone())
        }
    }

    fn evaluate_returning(&mut self, expr: &mut dyn Expression) -> Value {
        expr.accept(self);
        self.take_top()
    }

    pub fn normalize_to_bool(value: &Value) -> bool {
        match value {
            Value::Int(n) => *n != 0,
            Value::Float(n) => *n != 0f64,
            Value::Str(s) => !s.is_empty(),
            _ => false
        }
    }

    fn require_int(origin: &Value) -> Value {
        match origin {
            Value::Undefined => Value::Undefined,
            Value::Null => Value::Null,
            Value::Int(n) => Value::Int(*n),
            Value::Float(n) => Value::Int(*n as i64),
            Value::Str(_) => Value::Int(1),
            _ => unreachable!()
        }
    }

    fn require_number(origin: &Value) -> Value {
        match origin {
            Value::Str(s) => {
                let n = i64::from_str_radix(s.as_str(), 10);
                if n.is_err() {
                    let f = f64::from_str(s.as_str());
                    if f.is_err() {
                        Value::Null
                    } else {
                        Value::Float(f.unwrap())
                    }
                } else {
                    Value::Int(n.unwrap())
                }
            }
            _ => origin.clone()
        }
    }
}


macro_rules! process_airth_op {
    ($self:ident, $this:ident, $call:ident) => {
        {
            let lhs = Self::require_number(&$self.evaluate_returning($this.lhs_mut().deref_mut()));
            let rhs = Self::require_number(&$self.evaluate_returning($this.rhs_mut().deref_mut()));
            match lhs {
                Value::Int(a) => match rhs {
                    Value::Int(b) => $self.ret(Value::Int(a.$call(b))),
                    Value::Float(b) => $self.ret(Value::Float((a as f64).$call(b))),
                    _ => $self.ret(rhs)
                }
                Value::Float(a) => match rhs {
                    Value::Int(b) => $self.ret(Value::Float(a.$call(b as f64))),
                    Value::Float(b) => $self.ret(Value::Float(a.$call(b))),
                    _ => $self.ret(rhs)
                }
                _ => $self.ret(lhs)
            }
        }
    }
}

impl Visitor for Evaluator {
    fn visit_identifier(&mut self, this: &mut Identifier) {
        let value = self.env().resolve(this.symbol.as_str());
        if value.is_undefined() {
            self.rs = Status::corrupted(format!("Unresolved symbol: {}", this.symbol.as_str()));
        }
        self.ret(value);
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        let value = self.env().resolve_fully_qualified(this.prefix.as_str(),
                                                       this.suffix.as_str());
        if value.is_undefined() {
            self.rs = Status::corrupted(format!("Unresolved symbol: {}.{}",
                                                this.prefix.as_str(), this.suffix.as_str()));
        }
        self.ret(value);
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        match this.op() {
            Operator::Not => {
                let operand = self.evaluate_returning(this.operands_mut()[0].deref_mut());
                match Self::require_int(&operand) {
                    Value::Int(n) => self.ret(Value::Int(if n != 0 {1} else {0})),
                    _ => self.ret(operand)
                }
            },
            Operator::Minus => {
                let operand = self.evaluate_returning(this.operands_mut()[0].deref_mut());
                match Self::require_number(&operand) {
                    Value::Int(n) => self.ret(Value::Int(-n)),
                    Value::Float(n) => self.ret(Value::Float(-n)),
                    _ => self.ret(operand)
                }
            },
            _ => unreachable!()
        }
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        match this.op() {
            Operator::Add => process_airth_op!(self, this, add),
            Operator::Sub => process_airth_op!(self, this, sub),
            Operator::Mul => process_airth_op!(self, this, mul),
            Operator::Div => {
                let lhs = Self::require_number(&self.evaluate_returning(this.lhs_mut().deref_mut()));
                let rhs = Self::require_number(&self.evaluate_returning(this.rhs_mut().deref_mut()));
                match lhs {
                    Value::Int(a) => match rhs {
                        Value::Int(b) => self.ret(if b == 0 {Value::Null} else {Value::Int(a / b)}),
                        Value::Float(b) => self.ret(Value::Float((a as f64) / b)),
                        _ => self.ret(rhs)
                    }
                    Value::Float(a) => match rhs {
                        Value::Int(b) => self.ret(Value::Float(a / b as f64)),
                        Value::Float(b) => self.ret(Value::Float(a / b)),
                        _ => self.ret(rhs)
                    }
                    _ => self.ret(lhs)
                }
            }
            Operator::And => {
                let lhs = Self::require_int(&self.evaluate_returning(this.lhs_mut().deref_mut()));
                let rhs = Self::require_int(&self.evaluate_returning(this.rhs_mut().deref_mut()));
                match lhs {
                    Value::Int(a) => match rhs {
                        Value::Int(b) => self.ret(Value::Int(if a != 0 && b != 0 {1} else {0})),
                        _ => self.ret(rhs)
                    }
                    _ => self.ret(lhs)
                }
            }
            Operator::Or => {
                let lhs = Self::require_int(&self.evaluate_returning(this.lhs_mut().deref_mut()));
                let rhs = Self::require_int(&self.evaluate_returning(this.rhs_mut().deref_mut()));
                match lhs {
                    Value::Int(a) => match rhs {
                        Value::Int(b) => self.ret(Value::Int(if a != 0 || b != 0 {1} else {0})),
                        _ => self.ret(rhs)
                    }
                    _ => self.ret(lhs)
                }
            }
            _ => unreachable!()
        }
    }

    fn visit_in_literal_set(&mut self, this: &mut InLiteralSet) {
        todo!()
    }

    fn visit_in_relation(&mut self, this: &mut InRelation) {
        todo!()
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        match self.env().get_udf(this.callee_name.as_str()) {
            Some(udf) => {
                let mut args = ArenaVec::new(&self.arena);
                for expr in this.args.iter_mut() {
                    args.push(self.evaluate_returning(expr.deref_mut()));
                    if self.rs.is_not_ok() {
                        self.ret(Value::Undefined);
                        break;
                    }
                }
                match udf.evaluate(&args, &self.arena) {
                    Ok(value) => self.ret(value),
                    Err(e) => {
                        self.ret(Value::Undefined);
                        self.rs = e;
                    }
                }
            }
            None => {
                self.rs = Status::corrupted(format!("Unresolved function: {}", this.callee_name));
                self.ret(Value::Undefined);
            }
        }
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        self.ret(Value::Int(this.data));
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        self.ret(Value::Float(this.data));
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        self.ret(Value::Str(this.data.clone()))
    }

    fn visit_null_literal(&mut self, this: &mut Literal<()>) {
        self.ret(Value::Null);
    }

    fn visit_placeholder(&mut self, this: &mut Placeholder) {
        self.ret(self.env().bound_param(this.order).clone())
    }
}


pub fn expr_typing_reduce(expr: &mut dyn Expression, env: Arc<dyn Context>, arena: &ArenaMut<Arena>) -> Result<ColumnType> {
    let mut reducer = TypingReducer::new(arena);
    reducer.reduce(expr, env)
}

impl TypingReducer {
    pub fn reduce(&mut self, expr: &mut dyn Expression, env: Arc<dyn Context>) -> Result<ColumnType> {
        self.rs = Status::Ok;
        self.stack.clear();
        self.env_enter(env);
        expr.accept(self);
        self.env_exit();
        match self.rs {
            Status::Ok => Ok(self.take_top()),
            _ => Err(self.rs.clone())
        }
    }

    fn reduce_returning(&mut self, expr: &mut dyn Expression) -> ColumnType {
        expr.accept(self);
        self.take_top()
    }

    fn reduce_value_type(value: &Value) -> ColumnType {
        match value {
            Value::Int(_) => ColumnType::BigInt(11),
            Value::Float(_) => ColumnType::Double(0, 0),
            Value::Str(_) => ColumnType::Varchar(255),
            Value::Null => ColumnType::Varchar(255),
            _ => unreachable!()
        }
    }
}


impl Visitor for TypingReducer {
    fn visit_identifier(&mut self, this: &mut Identifier) {
        let value = self.env().resolve(this.symbol.as_str());
        if value.is_undefined() {
            self.rs = Status::corrupted(format!("Unresolved symbol: {}", this.symbol.as_str()));
        }
        self.ret(Self::reduce_value_type(&value));
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        let value = self.env().resolve_fully_qualified(this.prefix.as_str(),
                                                       this.suffix.as_str());
        if value.is_undefined() {
            self.rs = Status::corrupted(format!("Unresolved symbol: {}.{}",
                                                this.prefix.as_str(), this.suffix.as_str()));
        }
        self.ret(Self::reduce_value_type(&value));
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        match this.op() {
            Operator::Not => self.ret(ColumnType::Int(11)),
            Operator::Minus => {
                let ty = match self.reduce_returning(this.operands_mut()[0].deref_mut()) {
                    ColumnType::Float(m, n) => ColumnType::Float(m, n),
                    ColumnType::Double(m, n) => ColumnType::Double(m, n),
                    ColumnType::TinyInt(n) => ColumnType::Int(n),
                    ColumnType::SmallInt(n) => ColumnType::Int(n),
                    ColumnType::Int(n) => ColumnType::Int(n),
                    ColumnType::BigInt(n) => ColumnType::BigInt(n),
                    ColumnType::Char(_) => ColumnType::Int(11),
                    ColumnType::Varchar(_) => ColumnType::Int(11),
                    //_ => unreachable!()
                };
                self.ret(ty);
            }
            _ => unreachable!()
        }
    }

    fn visit_binary_expression(&mut self, this: &mut BinaryExpression) {
        let lhs = self.reduce_returning(this.lhs_mut().deref_mut());
        let rhs = self.reduce_returning(this.rhs_mut().deref_mut());
        match this.op() {
            Operator::Add
            | Operator::Sub
            | Operator::Mul
            | Operator::Div => {
                if lhs.is_integral() && rhs.is_integral() {
                    if this.op() == &Operator::Div {
                        self.ret(ColumnType::Float(0, 0))
                    } else {
                        self.ret(ColumnType::BigInt(11))
                    }
                } else if lhs.is_number() && rhs.is_number() {
                    self.ret(ColumnType::Float(0, 0))
                } else {
                    debug_assert!(lhs.is_string() || rhs.is_string());
                    self.ret(ColumnType::Float(0, 0))
                }
            }
            Operator::And
            | Operator::Or
            | Operator::Like => self.ret(ColumnType::Int(11)),
            _ => unreachable!()
        }
    }

    fn visit_in_literal_set(&mut self, _this: &mut InLiteralSet) {
        self.ret(ColumnType::Int(11));
    }

    fn visit_in_relation(&mut self, _this: &mut InRelation) {
        self.ret(ColumnType::Int(11));
    }

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        let ctx = ExecutionContext::new(this.distinct, &self.arena);
        let rs = new_any_fn(this.callee_name.as_str(), &ctx);
        if rs.is_none() {
            self.rs = Status::corrupted(format!("Unresolved function: {}", this.callee_name));
            self.ret(ColumnType::Int(11));
            return;
        }

        match rs.unwrap() {
            AnyFn::Udf(udf) => {
                let sig = Signature::parse(udf.signatures()[0], &self.arena).unwrap();
                self.ret(sig.ret_val);
            }
            AnyFn::Udaf(udaf) => self.ret(udaf.signature())
        }
    }

    fn visit_int_literal(&mut self, this: &mut Literal<i64>) {
        self.ret(ColumnType::BigInt(11));
    }

    fn visit_float_literal(&mut self, this: &mut Literal<f64>) {
        self.ret(ColumnType::Double(0, 0));
    }

    fn visit_str_literal(&mut self, this: &mut Literal<ArenaStr>) {
        self.ret(ColumnType::Varchar(255));
    }

    fn visit_null_literal(&mut self, this: &mut Literal<()>) {
        self.ret(ColumnType::Varchar(255));
    }

    fn visit_placeholder(&mut self, this: &mut Placeholder) {
        let ty = TypingReducer::reduce_value_type(self.env().bound_param(this.order));
        self.ret(ty);
    }
}