use crate::{Corrupting, sql, Status};

mod db;
mod executor;
mod connection;
mod evaluator;
mod locking;
mod relational_plan;
mod physical_plan;

#[inline]
pub fn from_sql_result<T>(input: sql::Result<T>) -> crate::Result<T> {
    match input {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}