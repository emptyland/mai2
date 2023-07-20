use std::{array, io};
use std::cell::RefCell;
use std::cmp::{max, Ordering};
use std::collections::{BTreeSet, HashMap};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};

use num_enum::TryFromPrimitive;

use patch::CFCreation;
use patch::FileCreation;

use crate::{log_warn, Result};
use crate::base::{Decoder, Logger, VarintDecode, VarintEncode};
use crate::storage::{config, files, wal};
use crate::storage::{Env, WritableFile};
use crate::storage::{ColumnFamilyOptions, Options, PinnableValue, ReadOptions};
use crate::storage::cache::TableCache;
use crate::storage::column_family::{ColumnFamilyImpl, ColumnFamilySet};
use crate::storage::Comparator;
use crate::storage::config::max_size_for_level;
use crate::storage::key::{InternalKey, InternalKeyComparator, Tag};
use crate::storage::Status;
use crate::storage::wal::{LogReader, LogWriter};

pub struct VersionSet {
    env: Arc<dyn Env>,
    pub logger: Arc<dyn Logger>,

    abs_db_path: PathBuf,
    block_size: u64,

    last_sequence_number: u64,
    next_file_number: u64,
    pub prev_log_number: u64,
    pub redo_log_number: u64,
    pub manifest_file_number: u64,
    column_families: Arc<RefCell<ColumnFamilySet>>,
    log: Option<LogWriter>,
    log_file: Option<Arc<RefCell<dyn WritableFile>>>,
}

unsafe impl Sync for VersionSet {}

impl VersionSet {
    pub fn new(abs_db_path: PathBuf, options: &Options) -> Arc<Mutex<VersionSet>> {
        Arc::new_cyclic(|weak| {
            Mutex::new(Self {
                env: options.env.clone(),
                logger: options.logger.clone(),
                abs_db_path: abs_db_path.clone(),
                block_size: options.core.block_size,
                last_sequence_number: 0,
                next_file_number: 0,
                prev_log_number: 0,
                redo_log_number: 0,
                manifest_file_number: 0,
                column_families: ColumnFamilySet::new_dummy(weak.clone(), abs_db_path),
                log: None,
                log_file: None,
            })
        })
    }

    pub fn should_migrate_redo_logs(&self, incoming_cf_id: u32) -> bool {
        let mut n_entries = 0;
        let cfs = self.column_families.borrow();
        for cfi in cfs.column_family_impls() {
            if cfi.id() != incoming_cf_id {
                n_entries += cfi.mutable().number_of_entries();
            }
        }
        n_entries > 0
    }

    pub fn recover(&mut self, desc: &HashMap<String, ColumnFamilyOptions>, file_number: u64)
                   -> io::Result<BTreeSet<u64>> {
        let manifest_file_path = files::paths::manifest_file(&self.abs_db_path, file_number);
        let file = self.env.new_sequential_file(&manifest_file_path)?;
        let mut reader = LogReader::new(file, true, wal::DEFAULT_BLOCK_SIZE);

        let mut history = BTreeSet::new();
        loop {
            let record = reader.read()?;
            if record.is_empty() {
                break;
            }
            //dbg!(&record);

            let (read_in_bytes, patch) = VersionPatch::from_unmarshal(&record)?;
            assert_eq!(read_in_bytes, record.len());

            if patch.has_column_family_creation() {
                let temporary = patch.cf_creation.temporary;
                if !temporary && !desc.contains_key(&patch.cf_creation.name) {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              format!("column family options: {} not found",
                                                      patch.cf_creation.name)));
                }

                let temporary_opts = ColumnFamilyOptions::with()
                    .temporary(true)
                    .build();
                let opts = if temporary {
                    &temporary_opts
                } else {
                    desc.get(&patch.cf_creation.name).unwrap()
                };
                if patch.cf_creation.comparator_name != opts.user_comparator.name() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData,
                                              format!("difference comparator: {} vs {}",
                                                      patch.cf_creation.comparator_name,
                                                      opts.user_comparator.name())));
                }

                ColumnFamilySet::new_column_family(self.column_families(),
                                                   patch.cf_creation.cf_id,
                                                   patch.cf_creation.name.clone(),
                                                   opts.clone());
                if temporary {
                    log_warn!(self.logger, "Temporary column family: {} not dropped yet!",
                        patch.cf_creation.name);
                }
            }

            if patch.has_column_family_deletion() {
                let id = patch.cf_deletion;
                assert_ne!(0, id);
                let cfi = self.ensure_column_family_impl(id);
                cfi.drop_it();
            }

            if patch.has_file_creation() || patch.has_file_deletion() {
                let mut builder = Version::with(self.column_families.clone());
                let mut version = builder
                    .prepare(&patch)
                    .apply(&patch)
                    .build();
                Self::finalize(&mut version);
                builder.cf.unwrap().append(version);
            }

            if patch.has_compaction_point() {
                let id = patch.compaction_point.cf_id;
                let level = patch.compaction_point.level as usize;
                let compaction_key = &patch.compaction_point.key;
                let cfi = self.ensure_column_family_impl(id);
                cfi.set_compaction_point(level, compaction_key.clone());
            }

            if patch.has_redo_log() {
                let id = patch.redo_log.cf_id;
                let cfi = self.ensure_column_family_impl(id);
                cfi.set_redo_log_number(patch.redo_log.number);
            }

            if patch.has_last_sequence_number() {
                self.last_sequence_number = patch.last_sequence_number;
            }

            if patch.has_redo_log_number() {
                self.redo_log_number = patch.redo_log_number;
                history.insert(self.redo_log_number);
            }

            if patch.has_prev_log_number() {
                self.prev_log_number = patch.prev_log_number;
            }

            if patch.has_next_file_number() {
                self.next_file_number = patch.next_file_number;
            }

            if patch.has_max_column_family() {
                unsafe { &mut *self.column_families().as_ptr() }
                    .update_max_column_family_id(patch.max_column_family);
            }
        }

        self.manifest_file_number = file_number;
        Ok(history)
    }

    fn ensure_column_family_impl(&self, id: u32) -> Arc<ColumnFamilyImpl> {
        let cfs = self.column_families().borrow();
        let cfi = cfs.get_column_family_by_id(id);
        cfi.unwrap().clone()
    }

    fn finalize(version: &mut Version) {
        // Precomputed best level for next compaction
        let mut best_level = -1i32;
        let mut best_score = -1.0f64;

        for level in 0..config::MAX_LEVEL - 1 {
            let score = if level == 0 {
                // We treat level-0 specially by bounding the number of files
                // instead of number of bytes for two reasons:
                //
                // (1) With larger write-buffer sizes, it is nice not to do too
                // many level-0 compactions.
                //
                // (2) The files in level-0 are merged on every read and
                // therefore we wish to avoid too many files when the individual
                // file size is small (perhaps because of a small write-buffer
                // setting, or very high compression ratios, or lots of
                // overwrites/deletions).
                version.files[level].len() as f64 / (config::MAX_NUMBER_OF_LEVEL_0_FILES as f64)
            } else {
                // Compute the ratio of current size to size limit.
                let size_in_bytes = version.size_of_level_files(level);
                (size_in_bytes as f64) / max_size_for_level(level) as f64
            };
            //dbg!(score);
            if score > best_score {
                best_level = level as i32;
                best_score = score;
            }
        }
        version.compaction_score = best_score;
        version.compaction_level = best_level;
    }

    pub fn create_manifest_file(&mut self) -> io::Result<()> {
        self.manifest_file_number = self.generate_file_number();
        let file_path = files::paths::manifest_file(self.abs_db_path(), self.manifest_file_number);
        let rs = self.env().new_writable_file(file_path.as_path(), true);
        if let Err(e) = rs {
            self.reuse_file_number(self.manifest_file_number);
            return Err(e);
        }
        let file = rs.unwrap();
        self.log_file = Some(file.clone());
        self.log = Some(LogWriter::new(file.clone(), self.block_size as usize));
        self.write_current_snapshot()
    }

    pub fn write_current_snapshot(&mut self) -> io::Result<()> {
        let cfs = self.column_families().clone();
        let borrowed_cfs = cfs.borrow_mut();

        for cfi in borrowed_cfs.column_family_impls() {
            let mut patch = VersionPatch::default();
            patch.create_column_family(cfi.id(), cfi.name().clone(),
                                       cfi.options().temporary,
                                       cfi.internal_key_cmp.user_cmp.name());
            patch.set_redo_log(cfi.id(), cfi.redo_log_number());

            for i in 0..config::MAX_LEVEL {
                for file in cfi.current().level_files(i) {
                    patch.create_file_by_file_metadata(cfi.id(), i as i32,
                                                       file.clone());
                }
            }

            self.write_patch(&patch)?;
        }

        let mut patch = VersionPatch::default();
        patch.set_max_column_family(borrowed_cfs.max_column_family_id());
        patch.set_last_sequence_number(self.last_sequence_number());
        patch.set_next_file_number(self.next_file_number());
        patch.set_redo_log_number(self.redo_log_number);
        patch.set_prev_log_number(self.prev_log_number);

        let current_file_path = files::paths::current_file(self.abs_db_path());
        self.env().write_all(current_file_path.as_path(),
                             self.manifest_file_number.to_string().as_bytes())?;
        self.write_patch(&patch)
    }

    pub fn log_and_apply(&mut self, cf_options: ColumnFamilyOptions, mut patch: VersionPatch)
                         -> io::Result<()> {
        if patch.has_redo_log_number() {
            assert!(patch.redo_log_number >= self.redo_log_number);
            assert!(patch.redo_log_number < self.next_file_number);
        } else {
            patch.set_redo_log_number(self.redo_log_number);
        }

        if !patch.has_prev_log_number() {
            patch.set_prev_log_number(self.prev_log_number);
        }

        patch.set_next_file_number(self.next_file_number);
        patch.set_last_sequence_number(self.last_sequence_number);

        //
        if patch.has_column_family_creation() {
            ColumnFamilySet::new_column_family(&self.column_families().clone(),
                                               patch.column_family_creation().cf_id,
                                               patch.column_family_creation().name.clone(),
                                               cf_options);

            let column_families = self.column_families().borrow_mut();
            patch.set_max_column_family(column_families.max_column_family_id());
        }

        if patch.has_redo_log() {
            let column_families = self.column_families().borrow();
            let id = patch.redo_log.cf_id;
            let cfi = column_families.get_column_family_by_id(id).unwrap();
            assert!(patch.redo_log.number >= cfi.redo_log_number());
            assert!(patch.redo_log.number < self.next_file_number);
            cfi.set_redo_log_number(patch.redo_log.number);
        }

        if patch.has_column_family_deletion() {
            let cfi = {
                let column_families = self.column_families().borrow();
                let id = patch.cf_deletion;
                assert_ne!(id, 0);
                column_families.get_column_family_by_id(id).unwrap().clone()
            };
            cfi.drop_it();
        }

        if self.log.is_none() {
            self.create_manifest_file()?;
            patch.set_next_file_number(self.next_file_number);
        }

        self.write_patch(&patch)?;

        if patch.has_file_creation() || patch.has_file_creation() {
            let mut builder = Version::with(self.column_families.clone());
            builder.prepare(&patch);
            builder.apply(&patch);
            let mut version = builder.build();
            Self::finalize(&mut version);
            builder.cf.unwrap().append(version);
        }

        self.redo_log_number = patch.redo_log_number();
        self.prev_log_number = patch.prev_log_number();
        Ok(())
    }

    fn write_patch(&mut self, patch: &VersionPatch) -> io::Result<()> {
        let mut buf = Vec::new();
        patch.marshal(&mut buf);

        let log = self.log.as_mut().unwrap();
        //dbg!(buf.as_slice());
        log.append(buf.as_slice())?;
        // log.flush()?;
        // log.sync()
        log.flush()
    }

    pub fn abs_db_path(&self) -> &Path {
        self.abs_db_path.as_path()
    }

    pub fn env(&self) -> &Arc<dyn Env> { &self.env }

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

#[derive(Debug, Default, Clone, PartialEq)]
pub struct FileMetadata {
    pub number: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub size: u64,
    pub ctime: u64,
}

mod patch {
    use std::mem::size_of;
    use std::sync::Arc;

    use num_enum::TryFromPrimitive;

    use crate::base::VarintEncode;
    use crate::storage::version::FileMetadata;

    #[derive(Debug, Default, Clone)]
    pub struct PrepareRedoLog {
        number: u64,
        last_sequence_number: u64,
    }

    #[derive(Debug, Default, Clone)]
    pub struct RedoLog {
        pub cf_id: u32,
        pub number: u64,
    }

    #[derive(Debug, Default, Clone)]
    pub struct CompactionPoint {
        pub cf_id: u32,
        pub level: i32,
        pub key: Vec<u8>,
    }

    #[derive(Debug, Default, Clone)]
    pub struct CFCreation {
        pub cf_id: u32,
        pub name: String,
        pub temporary: bool,
        pub comparator_name: String,
    }

    #[derive(Debug, Default, Clone, PartialEq)]
    pub struct FileDeletion {
        pub cf_id: u32,
        pub level: i32,
        pub number: u64,
    }

    #[derive(Debug, Default, Clone, PartialEq)]
    pub struct FileCreation {
        pub cf_id: u32,
        pub level: i32,
        pub metadata: Arc<FileMetadata>,
    }

    #[repr(u8)]
    #[derive(PartialEq, Debug, Clone, Copy, TryFromPrimitive)]
    pub enum Field {
        LastSequenceNumber,
        NextFileNumber,
        RedoLogNumber,
        RedoLog,
        PrevLogNumber,
        CompactionPoint,
        FileDeletion,
        FileCreation,
        MaxColumnFamily,
        AddColumnFamily,
        DropColumnFamily,
        MaxFields,
    }

    pub const MAX_FIELDS: usize = Field::MaxFields as usize;

    impl VarintEncode<RedoLog> for RedoLog {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.number.write_to(buf);
            size
        }
    }

    impl VarintEncode<CFCreation> for CFCreation {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.name.write_to(buf);
            size += self.temporary.write_to(buf);
            size += self.comparator_name.write_to(buf);
            size
        }
    }

    impl VarintEncode<CompactionPoint> for CompactionPoint {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.level.write_to(buf);
            size += self.key.write_to(buf);
            size
        }
    }

    impl VarintEncode<FileCreation> for FileCreation {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.level.write_to(buf);
            size += self.metadata.number.write_to(buf);
            size += self.metadata.smallest_key.write_to(buf);
            size += self.metadata.largest_key.write_to(buf);
            size += self.metadata.size.write_to(buf);
            size += self.metadata.ctime.write_to(buf);
            size
        }
    }

    impl VarintEncode<FileDeletion> for FileDeletion {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = self.cf_id.write_to(buf);
            size += self.level.write_to(buf);
            size += self.number.write_to(buf);
            size
        }
    }

    impl VarintEncode<Field> for Field {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            buf.push(self.clone() as u8);
            size_of::<u8>()
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct VersionPatch {
    max_column_family: u32,
    cf_deletion: u32,
    cf_creation: patch::CFCreation,
    last_sequence_number: u64,
    next_file_number: u64,
    redo_log: patch::RedoLog,
    prev_log_number: u64,
    compaction_point: patch::CompactionPoint,
    redo_log_number: u64,
    file_creation: Vec<patch::FileCreation>,
    file_deletion: Vec<patch::FileDeletion>,
    field_bits: [u32; (patch::MAX_FIELDS + 31) / 32],
}

impl VersionPatch {
    pub fn from_unmarshal(bytes: &[u8]) -> io::Result<(usize, VersionPatch)> {
        let mut patch = VersionPatch::default();
        let mut decoder = Decoder::new();
        while decoder.offset() < bytes.len() {
            let field_tag = patch::Field::try_from_primitive(decoder.read_from(bytes)?);
            if let Err(_e) = field_tag {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            match field_tag.unwrap() {
                patch::Field::LastSequenceNumber =>
                    patch.set_last_sequence_number(decoder.read_from(bytes)?),
                patch::Field::NextFileNumber =>
                    patch.set_next_file_number(decoder.read_from(bytes)?),
                patch::Field::RedoLogNumber =>
                    patch.set_redo_log_number(decoder.read_from(bytes)?),
                patch::Field::RedoLog =>
                    patch.set_redo_log(decoder.read_from(bytes)?,
                                       decoder.read_from(bytes)?),
                patch::Field::PrevLogNumber =>
                    patch.set_prev_log_number(decoder.read_from(bytes)?),
                patch::Field::CompactionPoint =>
                    patch.set_compaction_point(decoder.read_from(bytes)?,
                                               decoder.read_from(bytes)?,
                                               decoder.read_slice(bytes)?),
                patch::Field::FileDeletion =>
                    patch.delete_file(decoder.read_from(bytes)?,
                                      decoder.read_from(bytes)?,
                                      decoder.read_from(bytes)?),
                patch::Field::FileCreation =>
                    patch.create_file(decoder.read_from(bytes)?,
                                      decoder.read_from(bytes)?,
                                      decoder.read_from(bytes)?,
                                      decoder.read_slice(bytes)?,
                                      decoder.read_slice(bytes)?,
                                      decoder.read_from(bytes)?,
                                      decoder.read_from(bytes)?),
                patch::Field::MaxColumnFamily =>
                    patch.set_max_column_family(decoder.read_from(bytes)?),
                patch::Field::AddColumnFamily =>
                    patch.create_column_family(decoder.read_from(bytes)?,
                                               decoder.read_from(bytes)?,
                                               decoder.read_from(bytes)?,
                                               decoder.read_from(bytes)?),
                patch::Field::DropColumnFamily =>
                    patch.drop_column_family(decoder.read_from(bytes)?),
                patch::Field::MaxFields => break,
            }
        }

        Ok((decoder.offset(), patch))
    }

    pub fn set_last_sequence_number(&mut self, version: u64) {
        self.set_field(patch::Field::LastSequenceNumber);
        self.last_sequence_number = version;
    }

    pub const fn last_sequence_number(&self) -> u64 { self.last_sequence_number }

    pub const fn has_last_sequence_number(&self) -> bool {
        self.has_field(patch::Field::LastSequenceNumber)
    }

    pub fn set_redo_log_number(&mut self, value: u64) {
        self.set_field(patch::Field::RedoLogNumber);
        self.redo_log_number = value;
    }

    pub const fn has_redo_log_number(&self) -> bool { self.has_field(patch::Field::RedoLogNumber) }

    pub const fn redo_log_number(&self) -> u64 { self.redo_log_number }

    pub fn set_next_file_number(&mut self, value: u64) {
        self.set_field(patch::Field::NextFileNumber);
        self.next_file_number = value;
    }

    pub const fn has_next_file_number(&self) -> bool {
        self.has_field(patch::Field::NextFileNumber)
    }

    pub const fn next_file_number(&self) -> u64 { self.next_file_number }

    pub fn set_redo_log(&mut self, cf_id: u32, number: u64) {
        self.set_field(patch::Field::RedoLog);
        self.redo_log = patch::RedoLog {
            cf_id,
            number,
        }
    }

    pub const fn has_redo_log(&self) -> bool { self.has_field(patch::Field::RedoLog) }

    pub const fn redo_log(&self) -> &patch::RedoLog { &self.redo_log }

    pub fn set_prev_log_number(&mut self, value: u64) {
        self.set_field(patch::Field::PrevLogNumber);
        self.prev_log_number = value;
    }

    pub const fn has_prev_log_number(&self) -> bool { self.has_field(patch::Field::PrevLogNumber) }

    pub const fn prev_log_number(&self) -> u64 { self.prev_log_number }

    pub fn set_max_column_family(&mut self, value: u32) {
        self.set_field(patch::Field::MaxColumnFamily);
        self.max_column_family = value;
    }

    pub const fn has_max_column_family(&self) -> bool { self.has_field(patch::Field::MaxColumnFamily) }

    pub const fn max_column_family(&self) -> u32 { self.max_column_family }

    pub fn set_compaction_point(&mut self, cf_id: u32, level: i32, key: &[u8]) {
        self.set_field(patch::Field::CompactionPoint);
        self.compaction_point = patch::CompactionPoint {
            cf_id,
            level,
            key: Vec::from(key),
        }
    }

    pub const fn has_compaction_point(&self) -> bool { self.has_field(patch::Field::CompactionPoint) }

    pub const fn compaction_point(&self) -> &patch::CompactionPoint { &self.compaction_point }

    pub fn delete_file(&mut self, cf_id: u32, level: i32, number: u64) {
        self.set_field(patch::Field::FileDeletion);
        self.file_deletion.push(patch::FileDeletion {
            cf_id,
            level,
            number,
        });
    }

    pub const fn has_file_deletion(&self) -> bool { self.has_field(patch::Field::FileDeletion) }

    pub fn file_deletion(&self) -> &[patch::FileDeletion] { self.file_deletion.as_slice() }

    pub fn create_file(&mut self, cf_id: u32, level: i32, file_number: u64, smallest_key: &[u8],
                       largest_key: &[u8], file_size: u64, ctime: u64) {
        let file_metadata = Arc::new(FileMetadata {
            number: file_number,
            smallest_key: Vec::from(smallest_key),
            largest_key: Vec::from(largest_key),
            size: file_size,
            ctime,
        });
        self.create_file_by_file_metadata(cf_id, level, file_metadata);
    }

    pub fn create_file_by_file_metadata(&mut self, cf_id: u32, level: i32,
                                        metadata: Arc<FileMetadata>) {
        self.set_field(patch::Field::FileCreation);
        self.file_creation.push(patch::FileCreation {
            cf_id,
            level,
            metadata,
        });
    }

    pub const fn has_file_creation(&self) -> bool { self.has_field(patch::Field::FileCreation) }

    pub fn file_creation(&self) -> &[FileCreation] { self.file_creation.as_slice() }

    pub fn drop_column_family(&mut self, cf_id: u32) {
        self.set_field(patch::Field::DropColumnFamily);
        self.cf_deletion = cf_id;
    }

    pub const fn has_column_family_deletion(&self) -> bool { self.has_field(patch::Field::DropColumnFamily) }

    pub const fn column_family_deletion(&self) -> u32 { self.cf_deletion }

    pub fn create_column_family(&mut self, cf_id: u32, name: String, temporary: bool, comparator_name: String) {
        self.set_field(patch::Field::AddColumnFamily);
        self.cf_creation = patch::CFCreation {
            cf_id,
            name,
            temporary,
            comparator_name,
        };
    }

    pub const fn has_column_family_creation(&self) -> bool {
        self.has_field(patch::Field::AddColumnFamily)
    }

    pub const fn column_family_creation(&self) -> &CFCreation { &self.cf_creation }

    pub fn marshal(&self, buf: &mut Vec<u8>) {
        if self.has_prev_log_number() {
            patch::Field::PrevLogNumber.write_to(buf);
            self.prev_log_number.write_to(buf);
        }

        if self.has_redo_log() {
            patch::Field::RedoLog.write_to(buf);
            self.redo_log.write_to(buf);
        }

        if self.has_next_file_number() {
            patch::Field::NextFileNumber.write_to(buf);
            self.next_file_number.write_to(buf);
        }

        if self.has_last_sequence_number() {
            patch::Field::LastSequenceNumber.write_to(buf);
            self.last_sequence_number.write_to(buf);
        }

        if self.has_redo_log_number() {
            patch::Field::RedoLogNumber.write_to(buf);
            self.redo_log_number.write_to(buf);
        }

        if self.has_max_column_family() {
            patch::Field::MaxColumnFamily.write_to(buf);
            self.max_column_family.write_to(buf);
        }

        if self.has_column_family_creation() {
            patch::Field::AddColumnFamily.write_to(buf);
            self.column_family_creation().write_to(buf);
        }

        if self.has_column_family_deletion() {
            patch::Field::DropColumnFamily.write_to(buf);
            self.column_family_deletion().write_to(buf);
        }

        if self.has_compaction_point() {
            patch::Field::CompactionPoint.write_to(buf);
            self.compaction_point.write_to(buf);
        }

        if self.has_file_creation() {
            for item in self.file_creation.iter() {
                patch::Field::FileCreation.write_to(buf);
                item.write_to(buf);
            }
        }
        if self.has_file_deletion() {
            for item in self.file_deletion.iter() {
                patch::Field::FileDeletion.write_to(buf);
                item.write_to(buf);
            }
        }
        patch::Field::MaxFields.write_to(buf); // End of version patch
    }

    pub fn reset(&mut self) {
        self.field_bits.fill(0);
    }

    fn set_field(&mut self, field: patch::Field) {
        assert!(!matches!(field, patch::Field::MaxFields));
        let i = field as usize;
        self.field_bits[i / 32] |= 1u32 << (i % 32);
    }

    const fn has_field(&self, field: patch::Field) -> bool {
        assert!(!matches!(field, patch::Field::MaxFields));
        let i = field as usize;
        self.field_bits[i / 32] & (1u32 << (i % 32)) != 0
    }
}

#[derive(Debug)]
pub struct Version {
    pub owns: Weak<ColumnFamilyImpl>,
    pub compaction_level: i32,
    pub compaction_score: f64,
    files: [Vec<Arc<FileMetadata>>; config::MAX_LEVEL],
}

impl Version {
    pub fn new(owns: Weak<ColumnFamilyImpl>) -> Self {
        Self {
            owns,
            compaction_level: -1,
            compaction_score: -1.0f64,
            files: array::from_fn(|_| Vec::new()),
        }
    }

    pub fn with(cfs: Arc<RefCell<ColumnFamilySet>>) -> VersionBuilder {
        VersionBuilder {
            cfs,
            levels: array::from_fn(|_| FileEntry::default()),
            cf: None,
        }
    }

    pub fn level_files(&self, level: usize) -> &Vec<Arc<FileMetadata>> {
        &self.files[level]
    }

    pub fn size_of_level_files(&self, level: usize) -> u64 {
        let mut size_in_bytes = 0;
        for file_metadata in self.level_files(level) {
            size_in_bytes += file_metadata.size;
        }
        size_in_bytes
    }

    pub fn number_of_level_files(&self, level: usize) -> usize {
        self.level_files(level).len()
    }

    pub fn get(&self, read_opts: &ReadOptions, key: &[u8], sequence_number: u64, cache: &TableCache) ->
    Result<(PinnableValue, Tag)> {
        let internal_key = InternalKey::from_key(key, sequence_number, Tag::Key);
        let owns = self.owns.upgrade().unwrap();
        let internal_key_cmp = &owns.internal_key_cmp;
        let mut level0_files: Vec<Arc<FileMetadata>> = self.level_files(0)
            .iter()
            .cloned()
            .filter(|x| {
                internal_key_cmp.ge(&internal_key, &x.smallest_key) ||
                    internal_key_cmp.le(&internal_key, &x.largest_key)
            })
            .collect();
        level0_files.sort_by_key(|x| { x.ctime });
        level0_files.reverse();

        // Try find key in level-0 files
        for target_file in &level0_files {
            let rs = cache.get(read_opts, owns.deref(), target_file.number,
                               &internal_key);
            if let Err(status) = &rs {
                if !status.is_not_found() {
                    rs?;
                }
                continue;
            }
            return rs;
        }

        // Try find key in level-1~MAX_LEVEL files
        for i in 1..config::MAX_LEVEL {
            for target_file in self.level_files(i) {
                if internal_key_cmp.ge(&internal_key, &target_file.smallest_key) &&
                    internal_key_cmp.le(&internal_key, &target_file.largest_key) {
                    let rs = cache.get(read_opts, owns.deref(), target_file.number,
                                       &internal_key);
                    if let Err(status) = &rs {
                        if !status.is_not_found() {
                            rs?;
                        }
                        continue;
                    }
                    return rs;
                }
            }
        }
        Err(Status::NotFound)
    }

    pub fn get_overlapping_inputs(&self, level: usize, begin: &[u8], end: &[u8], inputs: &mut Vec<Arc<FileMetadata>>) {
        assert!(level < config::MAX_LEVEL);

        let mut user_begin_key = InternalKey::extract_user_key(begin);
        let mut user_end_key = InternalKey::extract_user_key(end);
        let user_cmp = self.owns.upgrade().unwrap().internal_key_cmp.user_cmp.clone();

        let mut i = 0;
        /*for file in &self.files[level]*/
        while i < self.files[level].len() {
            let file = &self.files[level][i];
            i += 1;
            let file_start_key = InternalKey::extract_user_key(&file.smallest_key);
            let file_limit_key = InternalKey::extract_user_key(&file.largest_key);

            if user_cmp.lt(file_limit_key, user_begin_key) {
                // skip it
            } else if user_cmp.gt(file_start_key, user_end_key) {
                // skip it
            } else {
                inputs.push(file.clone());
                if level == 0 {
                    // Level-0 files may overlap each other.  So check if the newly
                    // added file has expanded the range.  If so, restart search.
                    if user_cmp.lt(file_start_key, user_begin_key) {
                        user_begin_key = file_start_key;
                        inputs.clear();
                        i = 0;
                    } else if user_cmp.gt(file_limit_key, user_end_key) {
                        user_end_key = file_limit_key;
                        inputs.clear();
                        i = 0;
                    }
                }
            }
        }
    }
}

pub struct VersionBuilder {
    cfs: Arc<RefCell<ColumnFamilySet>>,
    levels: [FileEntry; config::MAX_LEVEL],
    pub cf: Option<Arc<ColumnFamilyImpl>>,
}

#[derive(Debug, Default, Clone)]
struct FileEntry {
    deletion: BTreeSet<u64>,
    creation: Vec<Arc<FileMetadata>>,
}

impl VersionBuilder {
    pub fn build(&mut self) -> Version {
        let cfi = self.cf.clone().unwrap().clone();

        let mut version = Version::new(Arc::downgrade(&cfi));
        for i in 0..config::MAX_LEVEL {
            for file_metadata in cfi.current().level_files(i) {
                if !self.levels[i].deletion.contains(&file_metadata.number) {
                    version.files[i].push(file_metadata.clone())
                }
            }
            self.levels[i].deletion.clear();

            Self::sort_files_metadata(&mut self.levels[i].creation, &cfi.internal_key_cmp);
            for file_metadata in &self.levels[i].creation {
                version.files[i].push(file_metadata.clone());
            }
            self.levels[i].creation.clear();
        }

        version
    }

    pub fn apply(&mut self, patch: &VersionPatch) -> &mut Self {
        for item in patch.file_deletion() {
            self.levels[item.level as usize].deletion.insert(item.number);
        }

        for item in patch.file_creation() {
            self.levels[item.level as usize].deletion.remove(&item.metadata.number);
            self.levels[item.level as usize].creation.push(item.metadata.clone());
        }
        self
    }

    pub fn prepare(&mut self, patch: &VersionPatch) -> &mut Self {
        for item in patch.file_creation() {
            self.try_get_column_family_impl(item.cf_id);
        }
        for item in patch.file_deletion() {
            self.try_get_column_family_impl(item.cf_id);
        }
        self
    }

    fn sort_files_metadata(this: &mut Vec<Arc<FileMetadata>>, internal_key_cmp: &InternalKeyComparator) {
        this.sort_by(|left, right| {
            let ord = internal_key_cmp.compare(left.smallest_key.as_slice(),
                                               right.smallest_key.as_slice());
            match ord {
                Ordering::Equal => left.number.cmp(&right.number),
                _ => ord
            }
        });
    }

    fn try_get_column_family_impl(&mut self, cf_id: u32) {
        match self.cf.as_ref() {
            Some(cfi) => assert_eq!(cfi.id(), cf_id),
            None => self.cf = Some(self.cfs
                .borrow().get_column_family_by_id(cf_id)
                .unwrap().clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanity() {
        let opts = Options::default();
        let vs = VersionSet::new(PathBuf::from("/tests/demo"), &opts);
        let mut ver = vs.lock().unwrap();

        assert_eq!(0, ver.last_sequence_number());
        assert_eq!(Path::new("/tests/demo"), ver.abs_db_path());

        assert_eq!(0, ver.next_file_number());
        assert_eq!(0, ver.generate_file_number());
        assert_eq!(1, ver.next_file_number());
        ver.reuse_file_number(0);
        assert_eq!(0, ver.next_file_number());

        assert_eq!(4096, ver.block_size);
    }

    #[test]
    fn version_patch() {
        let mut patch = VersionPatch::default();

        assert!(!patch.has_last_sequence_number());
        patch.set_last_sequence_number(1);
        assert!(patch.has_last_sequence_number());
        assert_eq!(1, patch.last_sequence_number());

        assert!(!patch.has_redo_log());
        patch.set_redo_log(1, 100);
        assert!(patch.has_redo_log());
        assert_eq!(1, patch.redo_log().cf_id);
        assert_eq!(100, patch.redo_log().number);

        assert!(!patch.has_prev_log_number());
        patch.set_prev_log_number(3);
        assert!(patch.has_prev_log_number());
        assert_eq!(3, patch.prev_log_number());

        assert!(!patch.has_file_creation());
        patch.create_file(0, 1, 1, "aaa".as_bytes(), "bbb".as_bytes(), 1000, 0);
        assert!(patch.has_file_creation());
        let item = &patch.file_creation()[0];
        assert_eq!(0, item.cf_id);
        assert_eq!(1, item.level);
        assert_eq!("aaa".as_bytes(), item.metadata.smallest_key);
        assert_eq!("bbb".as_bytes(), item.metadata.largest_key);
        assert_eq!(1, item.metadata.number);
        assert_eq!(1000, item.metadata.size);
        assert_eq!(0, item.metadata.ctime);

        assert!(!patch.has_file_deletion());
        patch.delete_file(1, 1, 3);
        assert!(patch.has_file_creation());
        let item = &patch.file_deletion()[0];
        assert_eq!(1, item.cf_id);
        assert_eq!(1, item.level);
        assert_eq!(3, item.number);
    }

    #[test]
    fn version_patch_marshal() -> io::Result<()> {
        let mut patch = VersionPatch::default();
        patch.set_redo_log(1, 99);
        patch.set_redo_log_number(2);

        let mut buf = Vec::<u8>::new();
        patch.marshal(&mut buf);
        assert_eq!(6, buf.len());

        let (loaded, b) = VersionPatch::from_unmarshal(buf.as_slice())?;
        assert_eq!(6, loaded);
        assert_eq!(patch.redo_log().cf_id, b.redo_log().cf_id);
        assert_eq!(patch.redo_log().number, b.redo_log().number);
        assert_eq!(2, patch.redo_log_number());

        Ok(())
    }

    #[test]
    fn marshal_create_file() -> io::Result<()> {
        let mut a = VersionPatch::default();
        a.create_file(11, 1, 99, "aaa".as_bytes(), "bbb".as_bytes(), 996, 7000000);
        a.create_file(12, 0, 100, "ccc".as_bytes(), "ddd".as_bytes(), 1000, 1100000);
        let mut buf = Vec::<u8>::new();
        a.marshal(&mut buf);
        assert_eq!(36, buf.len());

        let (loaded, b) = VersionPatch::from_unmarshal(buf.as_slice())?;
        assert_eq!(36, loaded);
        assert_eq!(a.file_creation().len(), b.file_creation().len());
        assert_eq!(a.file_creation()[0], b.file_creation()[0]);
        assert_eq!(a.file_creation()[1], b.file_creation()[1]);
        Ok(())
    }

    #[test]
    fn marshal_delete_file() -> io::Result<()> {
        let mut a = VersionPatch::default();
        a.delete_file(1, 0, 100);
        a.delete_file(2, 1, 700);
        a.delete_file(3, 0, 996);
        let mut buf = Vec::<u8>::new();
        a.marshal(&mut buf);
        assert_eq!(15, buf.len());

        let (loaded, b) = VersionPatch::from_unmarshal(buf.as_slice())?;
        assert_eq!(15, loaded);
        assert_eq!(a.file_deletion().len(), b.file_deletion().len());
        assert_eq!(a.file_deletion()[0], b.file_deletion()[0]);
        assert_eq!(a.file_deletion()[1], b.file_deletion()[1]);
        assert_eq!(a.file_deletion()[2], b.file_deletion()[2]);
        Ok(())
    }

    #[test]
    fn version_building() {
        let opts = Options::default();
        let refs = VersionSet::new(PathBuf::from("/tests/demo"), &opts);
        //let versions = refs.borrow_mut();

        Version::with(refs.lock().unwrap().column_families.clone());
    }
}