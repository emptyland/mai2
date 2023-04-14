use std::sync::Arc;
use crate::column_family::ColumnFamilyImpl;
use crate::iterator;
use crate::version::{FileMetadata, Version, VersionPatch};

pub struct Compaction {
    cfi: Arc<ColumnFamilyImpl>,
    target_level: usize,
    smallest_snapshot: u64,
    compaction_point: Vec<u8>,
    original_inputs: Vec<Box<dyn iterator::Iterator<Item=(Vec<u8>, Vec<u8>)>>>,
    input_version: Arc<Version>
}

pub struct State {
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub deletion_keys_count: usize,
    pub deletion_keys_in_bytes: u64,
    pub compacted_entries_count: usize,
    pub compacted_in_bytes: u64
}

#[derive(Debug)]
pub struct Compact {
    pub level: usize,
    pub input_version: Arc<Version>,
    pub patch: VersionPatch,
    pub inputs: [Vec<Arc<FileMetadata>>;2]
}