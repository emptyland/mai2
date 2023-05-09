use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::{Add, DerefMut, Sub, Mul};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use crate::{Corrupting, Result, Status};
use crate::base::{Arena, ArenaStr};
use crate::sql::ast::{BinaryExpression, CallFunction, CreateIndex, CreateTable, DropIndex, DropTable, Expression, FullyQualifiedName, Identifier, InsertIntoTable, Literal, Operator, Placeholder, UnaryExpression, Visitor};

pub struct Evaluator {
    arena: Rc<RefCell<Arena>>,
    rs: Status,
    env: VecDeque<Arc<dyn Context>>,
    stack: VecDeque<Value>,
}

pub trait Context {
    fn resolve(&self, name: &str) -> Value;
    fn resolve_fully_qualified(&self, prefix: &str, suffix: &str) -> Value;
    fn invoke(&self, callee: &str, args: &[Value]) -> Value;
    fn bound_param(&self, order: usize) -> Value;
    // aggregate
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Undefined,
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

impl Evaluator {
    pub fn new(arena: &Rc<RefCell<Arena>>) -> Self {
        Self {
            arena: arena.clone(),
            rs: Status::Ok,
            env: VecDeque::default(),
            stack: VecDeque::default(),
        }
    }

    pub fn evaluate(&mut self, expr: &mut dyn Expression, env: Arc<dyn Context>) -> Result<Value> {
        self.rs = Status::Ok;
        self.stack.clear();
        self.env_enter(env);
        expr.accept(self);
        self.env_exit();
        match self.rs {
            Status::Ok => Ok(self.take_value()),
            _ => Err(self.rs.clone())
        }
    }

    fn evaluate_returning(&mut self, expr: &mut dyn Expression) -> Value {
        expr.accept(self);
        self.take_value()
    }

    pub fn env_enter(&mut self, env: Arc<dyn Context>) {
        self.env.push_back(env);
    }

    pub fn env_exit(&mut self) {
        self.env.pop_back();
    }

    pub fn env(&self) -> &Arc<dyn Context> {
        self.env.back().unwrap()
    }

    fn ret(&mut self, value: Value) {
        self.stack.push_back(value);
    }

    fn take_value(&mut self) -> Value {
        self.stack.pop_back().unwrap()
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
    fn visit_create_table(&mut self, this: &mut CreateTable) { unreachable!() }
    fn visit_drop_table(&mut self, this: &mut DropTable) { unreachable!() }
    fn visit_create_index(&mut self, this: &mut CreateIndex) { unreachable!() }
    fn visit_drop_index(&mut self, this: &mut DropIndex) { unreachable!() }
    fn visit_insert_into_table(&mut self, this: &mut InsertIntoTable) { unreachable!() }

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

    fn visit_call_function(&mut self, this: &mut CallFunction) {
        todo!()
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
        self.ret(self.env().bound_param(this.order))
    }
}