use std::cmp::Ordering;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::base::Logger;
use crate::storage::{Iterator, IteratorRc, MergingIterator};
use crate::storage::cache::TableCache;
use crate::storage::column_family::ColumnFamilyImpl;
use crate::storage::Comparator;
use crate::storage::config;
use crate::storage::key::{InternalKey, InternalKeyComparator, MAX_SEQUENCE_NUMBER, Tag};
use crate::storage::memory_table::MemoryTable;
use crate::storage::sst_builder::SSTBuilder;
use crate::storage::version::{FileMetadata, Version, VersionPatch};

pub struct Compaction {
    abs_db_path: PathBuf,
    internal_key_cmp: InternalKeyComparator,
    logger: Arc<dyn Logger>,
    cfi: Arc<ColumnFamilyImpl>,
    memory_tables: Vec<Arc<MemoryTable>>,
    pub target_level: usize,
    pub target_file_number: u64,
    pub smallest_snapshot: u64,
    compaction_point: Vec<u8>,
    pub original_inputs: Vec<IteratorRc>,
    input_version: Arc<Version>,
}

#[derive(Default, Debug)]
pub struct State {
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub deletion_keys_count: usize,
    pub deletion_keys_in_bytes: u64,
    pub compacted_entries_count: usize,
    pub compacted_in_bytes: u64,
}

#[derive(Debug)]
pub struct Compact {
    pub level: usize,
    pub input_version: Arc<Version>,
    pub patch: VersionPatch,
    pub inputs: [Vec<Arc<FileMetadata>>; 2],
}

impl Compaction {
    pub fn new(abs_db_path: &Path,
               internal_key_cmp: &InternalKeyComparator,
               logger: &Arc<dyn Logger>,
               cfi: &Arc<ColumnFamilyImpl>,
               input_version: &Arc<Version>,
               target_level: usize,
               target_file_number: u64,
               compaction_point: &Vec<u8>) -> Self {
        let mut memory_tables = Vec::new();
        memory_tables.push(cfi.mutable().clone());
        memory_tables.append(&mut cfi.immutable_pipeline.peek_all());

        Self {
            abs_db_path: abs_db_path.to_path_buf(),
            internal_key_cmp: internal_key_cmp.clone(),
            logger: logger.clone(),
            cfi: cfi.clone(),
            memory_tables,
            target_level,
            target_file_number,
            smallest_snapshot: 0,
            compaction_point: compaction_point.clone(),
            original_inputs: vec![],
            input_version: input_version.clone(),
        }
    }

    pub fn run(&self, table_cache: &TableCache, builder: &mut SSTBuilder) -> io::Result<State> {
        let mut state = State::default();
        let mut merger = MergingIterator::new(self.internal_key_cmp.clone(),
                                              self.original_inputs.clone());
        merger.seek_to_first();

        let to_last_level = self.target_level == config::MAX_LEVEL - 1;
        let mut current_user_key: Option<Vec<u8>> = None;
        let mut last_sequence_number_for_key = 0u64;
        while merger.valid() {
            let internal_key = InternalKey::parse(merger.key());
            let mut should_drop = false;

            if current_user_key.is_none() ||
                self.internal_key_cmp.user_cmp.compare(&current_user_key.as_ref().unwrap(),
                                                       internal_key.user_key) != Ordering::Equal {

                // First occurrence of this user key
                current_user_key = Some(internal_key.user_key.to_vec());
                last_sequence_number_for_key = MAX_SEQUENCE_NUMBER;
            }

            if self.is_base_memory_for_key(internal_key.user_key) {
                // Has newer versions, can drop old versions.
                should_drop = true;
            } else if last_sequence_number_for_key < self.smallest_snapshot {
                should_drop = true;
            } else if internal_key.tag == Tag::Deletion &&
                internal_key.sequence_number < self.smallest_snapshot {
                // If key flag is deletion and has no oldest versions,
                // Can drop it.
                should_drop = self.is_base_level_for_key(self.target_level + 1,
                                                         merger.key(), table_cache)?;
            }
            last_sequence_number_for_key = internal_key.sequence_number;

            if to_last_level {
                if should_drop {
                    state.deletion_keys_count += 1;
                    state.deletion_keys_in_bytes += merger.key().len() as u64;
                    state.deletion_keys_in_bytes += merger.value().len() as u64;
                } else {
                    let key = InternalKey::from_key(internal_key.user_key,
                                                    0, Tag::Key);
                    builder.add(&key, merger.value())?;
                    state.compacted_entries_count += 1;
                    state.compacted_in_bytes += (key.len() + merger.value().len()) as u64;
                }
            } else {
                if should_drop {
                    state.deletion_keys_count += 1;
                    state.deletion_keys_in_bytes += merger.key().len() as u64;
                    state.deletion_keys_in_bytes += merger.value().len() as u64;
                } else {
                    builder.add(merger.key(), merger.value())?;
                    state.compacted_entries_count += 1;
                    state.compacted_in_bytes += (merger.key().len() + merger.value().len()) as u64;
                }
            }

            if !should_drop {
                if state.smallest_key.is_empty() ||
                    self.internal_key_cmp.lt(merger.key(), &state.smallest_key) {
                    state.smallest_key = merger.key().to_vec();
                }
                if state.largest_key.is_empty() ||
                    self.internal_key_cmp.gt(merger.key(), &state.largest_key) {
                    state.largest_key = merger.key().to_vec();
                }
            }
            merger.move_next();
        }
        dbg!(&state);
        if state.compacted_entries_count > 0 {
            builder.finish()?;
        }
        Ok(state)
    }

    fn is_base_level_for_key(&self, start_level: usize, key: &[u8], table_cache: &TableCache)
                             -> io::Result<bool> {
        if start_level == config::MAX_LEVEL - 1 {
            return Ok(false);
        }

        for i in start_level..config::MAX_LEVEL {
            for file in self.input_version.level_files(i) {
                if self.internal_key_cmp.gt(key, &file.largest_key) ||
                    self.internal_key_cmp.lt(key, &file.smallest_key) {
                    continue;
                }

                let rd = table_cache.get_reader(&self.cfi, file.number)?;
                if rd.keys_filter().may_exists(key) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn is_base_memory_for_key(&self, key: &[u8]) -> bool {
        for table in &self.memory_tables {
            if table.get(key, MAX_SEQUENCE_NUMBER).is_ok() {
                return true;
            }
        }
        false
    }
}