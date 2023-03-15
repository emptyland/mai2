use std::cell::RefCell;
use std::cmp::max;
use std::sync::Arc;

use crate::column_family::ColumnFamilySet;
use crate::mai2::Options;

pub struct VersionSet {

    abs_db_path: String,
    block_size: u64,

    last_sequence_number: u64,
    next_file_number: u64,
    prev_log_number: u64,
    redo_log_number: u64,
    manifest_file_number: u64,
    column_families: Arc<RefCell<ColumnFamilySet>>
}

impl VersionSet {
    pub fn new_dummy(abs_db_path: String, options: Options) -> VersionSet {
        Self {
            abs_db_path,
            block_size: options.block_size,

            last_sequence_number: 0,
            next_file_number: 0,
            prev_log_number: 0,
            redo_log_number: 0,
            manifest_file_number: 0,
            column_families: ColumnFamilySet::new_dummy(),
        }
    }

    pub fn abs_db_path(&self) -> &String {
        &self.abs_db_path
    }

    pub fn column_families(&self) -> &Arc<RefCell<ColumnFamilySet>> {
        &self.column_families
    }

    pub const fn last_sequence_number(&self) -> u64 {
        self.last_sequence_number
    }

    pub fn add_sequence_number(&mut self, add: u64) -> u64 {
        self.last_sequence_number += add;
        self.last_sequence_number
    }

    pub fn update_sequence_number(&mut self, new_val: u64) -> u64 {
        self.last_sequence_number = max(new_val, self.last_sequence_number);
        self.last_sequence_number
    }

    pub const fn next_file_number(&self) -> u64 { self.next_file_number }

    pub fn reuse_file_number(&mut self, file_number: u64) {
        if file_number + 1 == self.next_file_number {
            self.next_file_number = file_number;
        }
    }

    pub fn generate_file_number(&mut self) -> u64 {
        let rs = self.next_file_number;
        self.next_file_number += 1;
        rs
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn sanity() {
        let mutex = Arc::new(Mutex::new(VersionSet::new_dummy(String::from("/db/demo"), Options::default())));
        let mut ver = mutex.lock().unwrap();

        assert_eq!(0, ver.last_sequence_number());
        assert_eq!("/db/demo", ver.abs_db_path());

        assert_eq!(0, ver.next_file_number());
        assert_eq!(0, ver.generate_file_number());
        assert_eq!(1, ver.next_file_number());
        ver.reuse_file_number(0);
        assert_eq!(0, ver.next_file_number());

        assert_eq!(4096, ver.block_size);
    }
}