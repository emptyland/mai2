#![allow(unused_variables)]
#![allow(dead_code)]

pub use executor::ColumnSet;

use crate::{Corrupting, sql, Status};

pub mod db;
pub mod connection;
mod executor;
mod evaluator;
mod locking;
mod relational_plan;
mod physical_plan;
mod function;
mod planning;

#[inline]
pub fn from_sql_result<T>(input: sql::Result<T>) -> crate::Result<T> {
    match input {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}