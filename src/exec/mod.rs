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
mod interpreter;
mod field;

#[macro_export]
macro_rules! zone_limit_guard {
    ($zone:ident, $mb:expr) => {
        if $zone.rss_in_bytes >= $mb * (1024 * 1024) {
            drop($zone);
            $zone = Arena::new_ref();
        }
    };
}

#[macro_export]
macro_rules! zone_limit_guard_on {
    ($zone:ident, $new_zone:ident, $mb:expr, {$on:stmt}) => {
        if $zone.rss_in_bytes >= $mb * (1024 * 1024) {
            let $new_zone = Arena::new_ref();
            $on
            drop($zone);
            $zone = $new_zone;
        }
    };
}

#[inline]
pub fn from_sql_result<T>(input: sql::Result<T>) -> crate::Result<T> {
    match input {
        Ok(v) => Ok(v),
        Err(e) => Err(Status::corrupted(e.to_string()))
    }
}