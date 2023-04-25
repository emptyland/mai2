use crate::{Corrupting, sql, Status};

mod db;
mod executor;
mod connection;

#[inline]
pub fn from_sql_result<T>(input: sql::Result<T>) -> crate::Result<T> {
    match input {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}