use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::ops::{Add, DerefMut, Mul, Sub};
use std::str::FromStr;
use std::sync::Arc;

use crate::{Corrupting, Result, Status, switch, try_eval};
use crate::base::{Arena, ArenaBox, ArenaMut, ArenaStr, ArenaVec};
use crate::exec::db::ColumnType;
use crate::exec::function::{AnyFn, ExecutionContext, new_any_fn, Signature, UDF};
use crate::sql::ast::*;

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
    NegativeInf,
    // -∞
    PositiveInf,
    // +∞
    Null,
    Int(i64),
    Float(f64),
    Str(ArenaStr),
}

impl Value {
    pub fn is_null(&self) -> bool { self.eq(&Value::Null) }
    pub fn is_not_null(&self) -> bool { !self.is_null() && !self.is_undefined() }
    pub fn is_certain(&self) -> bool { !self.is_null() && !self.is_undefined() && !self.is_inf() }
    pub fn is_undefined(&self) -> bool { self.eq(&Value::Undefined) }
    pub fn is_inf(&self) -> bool {
        match self {
            Self::NegativeInf | Self::PositiveInf => true,
            _ => false,
        }
    }

    pub fn is_str(&self) -> bool { matches!(self, Self::Str(_)) }
    pub fn unwrap_str(&self) -> &ArenaStr {
        match self {
            Self::Str(s) => s,
            _ => panic!("Not Str(ArenaStr) value")
        }
    }

    pub fn is_int(&self) -> bool { matches!(self, Self::Int(_)) }
    pub fn unwrap_int(&self) -> i64 {
        match self {
            Self::Int(n) => *n,
            _ => panic!("Not Int(i64) value")
        }
    }

    pub fn is_float(&self) -> bool { matches!(self, Self::Float(_)) }
    pub fn unwrap_float(&self) -> f64 {
        match self {
            Self::Float(n) => *n,
            _ => panic!("Not Float(f64) value")
        }
    }

    pub fn dup(&self, arena: &ArenaMut<Arena>) -> Value {
        match self {
            Self::Str(s) => Value::Str(ArenaStr::new(s.as_str(), arena.get_mut())),
            _ => self.clone()
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Int(switch!(value, 1, 0))
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<ArenaStr> for Value {
    fn from(value: ArenaStr) -> Self {
        Self::Str(value)
    }
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

impl<T: Clone> Reducer<T> {
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

    pub fn require_int(origin: &Value) -> Value {
        match origin {
            Value::Undefined => Value::Undefined,
            Value::Null => Value::Null,
            Value::Int(n) => Value::Int(*n),
            Value::Float(n) => Value::Int(*n as i64),
            Value::Str(_) => Value::Int(1),
            _ => unreachable!()
        }
    }

    pub fn require_number(origin: &Value) -> Value {
        match origin {
            Value::Str(s) => Self::require_number_from_str(s),
            _ => origin.clone()
        }
    }

    pub fn require_number_from_str(origin: &ArenaStr) -> Value {
        let n = i64::from_str_radix(origin.as_str(), 10);
        if n.is_err() {
            let f = f64::from_str(origin.as_str());
            if f.is_err() {
                Value::Null
            } else {
                Value::Float(f.unwrap())
            }
        } else {
            Value::Int(n.unwrap())
        }
    }

    pub fn compare_vals(lhs: &Value, rhs: &Value, op: &Operator) -> Value {
        match lhs {
            Value::Int(_) =>
                Self::eval_compare_number(lhs, &Self::require_number(rhs), op),
            Value::Float(_) =>
                Self::eval_compare_number(lhs, &Self::require_number(rhs), op),
            Value::Str(s) => match &rhs {
                Value::Str(b) =>
                    Self::eval_compare_op(s, b, op),
                _ =>
                    Self::eval_compare_number(&Self::require_number(lhs), rhs, op),
            }
            Value::Null => Value::Null,
            _ => unreachable!()
        }
    }

    pub fn eval_compare_number(lhs: &Value, rhs: &Value, op: &Operator) -> Value {
        match lhs {
            Value::Int(a) => match rhs {
                Value::Int(b) => Self::eval_compare_op(*a, *b, op),
                Value::Float(b) => Self::eval_compare_op(*a as f64, *b, op),
                Value::Null => Value::Null,
                _ => unreachable!()
            }
            Value::Float(a) => match rhs {
                Value::Int(b) => Self::eval_compare_op(*a, *b as f64, op),
                Value::Float(b) => Self::eval_compare_op(*a, *b, op),
                Value::Null => Value::Null,
                _ => unreachable!()
            }
            Value::Null => Value::Null,
            _ => unreachable!()
        }
    }

    pub fn eval_compare_op<T: PartialOrd>(lhs: T, rhs: T, op: &Operator) -> Value {
        match op {
            Operator::Lt => lhs < rhs,
            Operator::Le => lhs <= rhs,
            Operator::Gt => lhs > rhs,
            Operator::Ge => lhs >= rhs,
            Operator::Eq => lhs == rhs,
            Operator::Ne => lhs != rhs,
            _ => unreachable!()
        }.into()
    }

    pub fn migrate_to(input: &Value, ty: &ColumnType, not_null: bool, arena: &ArenaMut<Arena>) -> Value {
        match input {
            Value::Int(n) => if ty.is_integral() {
                Value::Int(*n)
            } else if ty.is_floating() {
                Value::Float(*n as f64)
            } else {
                assert!(ty.is_string());
                Value::Str(ArenaStr::new(n.to_string().as_str(), arena.get_mut()))
            }
            Value::Float(f) => if ty.is_integral() {
                Value::Int(f.trunc() as i64)
            } else if ty.is_floating() {
                Value::Float(*f)
            } else {
                assert!(ty.is_string());
                Value::Str(ArenaStr::new(f.to_string().as_str(), arena.get_mut()))
            }
            Value::Str(s) => if ty.is_integral() {
                match i64::from_str_radix(s.as_str(), 10) {
                    Err(e) => switch!(not_null, Value::Int(0), Value::Null),
                    Ok(n) => Value::Int(n)
                }
            } else if ty.is_floating() {
                match f64::from_str(s.as_str()) {
                    Err(e) => switch!(not_null, Value::Float(0f64), Value::Null),
                    Ok(n) => Value::Float(n)
                }
            } else {
                assert!(ty.is_string());
                Value::Str(s.clone())
            }
            _ => unreachable!()
        }
    }
}


macro_rules! process_airth_op {
    ($self:ident, $this:ident, $call:ident) => {
        {
            let lhs = Self::require_number(&$self.evaluate_returning($this.lhs_mut().deref_mut()));
            if $self.rs.is_not_ok() {
                return;
            }
            let rhs = Self::require_number(&$self.evaluate_returning($this.rhs_mut().deref_mut()));
            if $self.rs.is_not_ok() {
                return;
            }
            if lhs.is_null() || rhs.is_null() {
                $self.ret(Value::Null);
                return;
            }
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
                    Value::Int(n) => self.ret((n != 0).into()),
                    _ => self.ret(operand)
                }
            }
            Operator::Minus => {
                let operand = self.evaluate_returning(this.operands_mut()[0].deref_mut());
                match Self::require_number(&operand) {
                    Value::Int(n) => self.ret(Value::Int(-n)),
                    Value::Float(n) => self.ret(Value::Float(-n)),
                    _ => self.ret(operand)
                }
            }
            Operator::IsNull => {
                let operand = self.evaluate_returning(this.operands_mut()[0].deref_mut());
                self.ret(operand.is_null().into());
            }
            Operator::IsNotNull => {
                let operand = self.evaluate_returning(this.operands_mut()[0].deref_mut());
                self.ret((!operand.is_null()).into());
            }
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
                if self.rs.is_not_ok() {
                    return;
                }
                let rhs = Self::require_number(&self.evaluate_returning(this.rhs_mut().deref_mut()));
                if self.rs.is_not_ok() {
                    return;
                }
                if lhs.is_null() || rhs.is_null() {
                    self.ret(Value::Null);
                    return;
                }
                match lhs {
                    Value::Int(a) => match rhs {
                        Value::Int(b) => self.ret(if b == 0 { Value::Null } else { (a / b).into() }),
                        Value::Float(b) => self.ret(((a as f64) / b).into()),
                        _ => self.ret(rhs)
                    }
                    Value::Float(a) => match rhs {
                        Value::Int(b) => self.ret((a / b as f64).into()),
                        Value::Float(b) => self.ret((a / b).into()),
                        _ => self.ret(rhs)
                    }
                    _ => self.ret(lhs)
                }
            }
            Operator::Lt | Operator::Le | Operator::Gt | Operator::Ge | Operator::Eq | Operator::Ne => {
                let lhs = try_eval!(self, self.evaluate_returning(this.lhs_mut().deref_mut()));
                let rhs = try_eval!(self, self.evaluate_returning(this.rhs_mut().deref_mut()));
                let rv = Self::compare_vals(&lhs, &rhs, this.op());
                self.ret(rv);
            }
            Operator::And => {
                let lhs = Self::require_int(&self.evaluate_returning(this.lhs_mut().deref_mut()));
                if self.rs.is_not_ok() {
                    return;
                }
                let rhs = Self::require_int(&self.evaluate_returning(this.rhs_mut().deref_mut()));
                if self.rs.is_not_ok() {
                    return;
                }
                match lhs {
                    Value::Int(a) => match rhs {
                        Value::Int(b) => self.ret((a != 0 && b != 0).into()),
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
                        Value::Int(b) => self.ret((a != 0 || b != 0).into()),
                        _ => self.ret(rhs)
                    }
                    _ => self.ret(lhs)
                }
            }
            _ => unreachable!()
        }
    }

    fn visit_in_literal_set(&mut self, this: &mut InLiteralSet) {
        let lhs = self.evaluate_returning(this.lhs_mut().deref_mut());
        for expr in this.set.iter_mut() {
            let val = self.evaluate_returning(expr.deref_mut());
            if Self::normalize_to_bool(&Self::compare_vals(&lhs, &val, &Operator::Eq)) {
                return self.ret((!this.not_in).into());
            }
        }
        self.ret(this.not_in.into());
    }

    fn visit_in_relation(&mut self, _this: &mut InRelation) {
        todo!()
    }

    fn visit_between_and(&mut self, this: &mut BetweenAnd) {
        let matched = self.evaluate_returning(this.matched_mut().deref_mut());
        let lower = self.evaluate_returning(this.lower_mut().deref_mut());

        if Evaluator::normalize_to_bool(&Evaluator::compare_vals(&matched, &lower, &Operator::Lt)) {
            return self.ret(this.not_between.into());
        }
        let upper = self.evaluate_returning(this.upper_mut().deref_mut());
        let b = Evaluator::normalize_to_bool(&Evaluator::compare_vals(&matched, &upper, &Operator::Gt));
        self.ret(if this.not_between { b } else { !b }.into());
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

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {
        self.ret(Value::Null);
    }

    fn visit_placeholder(&mut self, this: &mut Placeholder) {
        self.ret(self.env().bound_param(this.order).clone())
    }

    fn visit_fast_access_hint(&mut self, this: &mut FastAccessHint) {
        self.ret(self.env().fast_access(this.offset).clone())
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
            return;
        }
        self.ret(Self::reduce_value_type(&value));
    }

    fn visit_full_qualified_name(&mut self, this: &mut FullyQualifiedName) {
        let value = self.env().resolve_fully_qualified(this.prefix.as_str(), this.suffix.as_str());
        if value.is_undefined() {
            self.rs = Status::corrupted(format!("Unresolved symbol: {}.{}", this.prefix.as_str(),
                                                this.suffix.as_str()));
            return;
        }
        self.ret(Self::reduce_value_type(&value));
    }

    fn visit_unary_expression(&mut self, this: &mut UnaryExpression) {
        match this.op() {
            Operator::Not => self.ret(ColumnType::Int(11)),
            Operator::Minus => {
                let ty = match self.reduce_returning(this.operands_mut()[0].deref_mut()) {
                    ColumnType::Null => ColumnType::Null,
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
            AnyFn::Udaf(udaf) => {
                let mut params = ArenaVec::new(&self.arena);
                for arg in this.args.iter_mut() {
                    params.push(self.reduce_returning(arg.deref_mut()));
                }
                self.ret(udaf.signature(&params))
            }
        }
    }

    fn visit_int_literal(&mut self, _this: &mut Literal<i64>) {
        self.ret(ColumnType::BigInt(11));
    }

    fn visit_float_literal(&mut self, _this: &mut Literal<f64>) {
        self.ret(ColumnType::Double(0, 0));
    }

    fn visit_str_literal(&mut self, _this: &mut Literal<ArenaStr>) {
        self.ret(ColumnType::Varchar(255));
    }

    fn visit_null_literal(&mut self, _this: &mut Literal<()>) {
        self.ret(ColumnType::Varchar(255));
    }

    fn visit_placeholder(&mut self, this: &mut Placeholder) {
        let ty = TypingReducer::reduce_value_type(self.env().bound_param(this.order));
        self.ret(ty);
    }

    fn visit_fast_access_hint(&mut self, this: &mut FastAccessHint) {
        let ty = TypingReducer::reduce_value_type(self.env().fast_access(this.offset));
        self.ret(ty);
    }
}