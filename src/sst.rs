use std::cell::RefCell;
use std::io;
use std::sync::Arc;

use crate::env::WritableFile;
use crate::key::InternalKeyComparator;
use crate::marshal::FileWriter;

pub struct SSTBuilder<'a> {
    internal_key_cmp: &'a InternalKeyComparator,
    writer: FileWriter,
    block_size: u64,
    n_restart: usize,
    approximated_n_entries: usize,

    has_seen_first_key: bool,
    is_last_level: bool,
    properties: TableProperties,
    block_builder: DataBlockBuilder,
    index_builder: DataBlockBuilder,
    filter_builder: FilterBlockBuilder,
}

#[derive(Clone, Debug, Default)]
struct TableProperties {
    last_level: bool,
    block_size: u32,
    n_entries: usize,
    index_position: u64,
    index_size: usize,
    filter_position: u64,
    filter_size: usize,
    last_version: u64,
    smallest_key: Vec<u8>,
    largest_key: Vec<u8>,
}

impl<'a> SSTBuilder<'a> {
    pub fn new(internal_key_cmp: &'a InternalKeyComparator, file: Arc<RefCell<dyn WritableFile>>,
               block_size: u64, n_restart: usize, approximated_n_entries: usize) -> Self {
        Self {
            internal_key_cmp,
            writer: FileWriter::new(file),
            block_size,
            n_restart,
            approximated_n_entries,
            has_seen_first_key: false,
            is_last_level: false,
            properties: Default::default(),
            block_builder: DataBlockBuilder {},
            index_builder: DataBlockBuilder {},
            filter_builder: FilterBlockBuilder {},
        }
    }

    pub fn add(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        todo!()
    }

    pub fn finish(&self) -> io::Result<()> {
        todo!()
    }

    pub fn abandon(&self) {
        todo!()
    }

    pub fn file_size(&self) -> u64 {
        todo!()
    }
}

struct DataBlockBuilder;

struct FilterBlockBuilder;