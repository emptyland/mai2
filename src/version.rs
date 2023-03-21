use std::cell::RefCell;
use std::cmp::max;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use num_enum::TryFromPrimitive;
use patch::CFCreation;
use patch::FileCreation;

use crate::column_family::ColumnFamilySet;
use crate::env::{Env, WritableFile};
use crate::files;
use crate::mai2::Options;
use crate::marshal::{Decode, Decoder, Encode};
use crate::wal::LogWriter;

pub struct VersionSet {
    env: Arc<dyn Env>,

    abs_db_path: PathBuf,
    block_size: u64,

    last_sequence_number: u64,
    next_file_number: u64,
    prev_log_number: u64,
    redo_log_number: u64,
    manifest_file_number: u64,
    column_families: Arc<RefCell<ColumnFamilySet>>,
    log: Option<LogWriter>,
    log_file: Option<Rc<RefCell<dyn WritableFile>>>,
}


impl VersionSet {
    pub fn new_dummy(abs_db_path: PathBuf, options: &Options) -> Arc<RefCell<VersionSet>> {
        Arc::new_cyclic(|weak| {
            RefCell::new(Self {
                env: options.env.clone(),
                abs_db_path,
                block_size: options.core.block_size,
                last_sequence_number: 0,
                next_file_number: 0,
                prev_log_number: 0,
                redo_log_number: 0,
                manifest_file_number: 0,
                column_families: ColumnFamilySet::new_dummy(weak.clone()),
                log: None,
                log_file: None,
            })
        })
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
            let borrowed_cif = cfi.borrow();
            patch.create_column_family(borrowed_cif.name().clone(), borrowed_cif.id(),
                                       borrowed_cif.internal_key_cmp().name());
            patch.set_redo_log(borrowed_cif.id(), borrowed_cif.redo_log_number());

            // TODO: write level files

            self.write_patch(patch)?;
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
        self.write_patch(patch)
    }

    fn write_patch(&mut self, patch: VersionPatch) -> io::Result<()> {
        let mut buf = Vec::new();
        patch.marshal(&mut buf);

        let log = self.log.as_mut().unwrap();
        log.append(buf.as_slice())?;
        log.flush()?;
        log.sync()
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
    use std::io::Write;
    use std::mem::size_of;
    use std::slice;
    use std::sync::Arc;

    use num_enum::TryFromPrimitive;

    use crate::marshal::Encode;
    use crate::version::FileMetadata;

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

    #[repr(u32)]
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

    impl Encode<RedoLog> for RedoLog {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.number.write_to(buf);
            size
        }
    }

    impl Encode<CFCreation> for CFCreation {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.name.write_to(buf);
            size += self.comparator_name.write_to(buf);
            size
        }
    }

    impl Encode<CompactionPoint> for CompactionPoint {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = 0;
            size += self.cf_id.write_to(buf);
            size += self.level.write_to(buf);
            size += self.key.write_to(buf);
            size
        }
    }

    impl Encode<FileCreation> for FileCreation {
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

    impl Encode<FileDeletion> for FileDeletion {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            let mut size = self.cf_id.write_to(buf);
            size += self.level.write_to(buf);
            size += self.number.write_to(buf);
            size
        }
    }

    impl Encode<Field> for Field {
        fn write_to(&self, buf: &mut Vec<u8>) -> usize {
            buf.push(self.clone() as u8);
            size_of::<Self>()
        }
    }
}

#[derive(Debug, Default, Clone)]
struct VersionPatch {
    max_column_family: u32,
    cf_deletion: u32,
    cf_creation: patch::CFCreation,
    last_sequence_number: u64,
    next_file_number: u64,
    redo_log: patch::RedoLog,
    prev_redo_log: u64,
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
        self.prev_redo_log = value;
    }

    pub const fn has_prev_log_number(&self) -> bool { self.has_field(patch::Field::PrevLogNumber) }

    pub const fn prev_log_number(&self) -> u64 { self.prev_redo_log }

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
        self.create_file_by_file_metadata(cf_id, level, &file_metadata);
    }

    pub fn create_file_by_file_metadata(&mut self, cf_id: u32, level: i32,
                                        metadata: &Arc<FileMetadata>) {
        self.set_field(patch::Field::FileCreation);
        self.file_creation.push(patch::FileCreation {
            cf_id,
            level,
            metadata: metadata.clone(),
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

    pub fn create_column_family(&mut self, name: String, cf_id: u32, comparator_name: String) {
        self.set_field(patch::Field::AddColumnFamily);
        self.cf_creation = patch::CFCreation {
            cf_id,
            name,
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
            self.prev_redo_log.write_to(buf);
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn sanity() {
        let opts = Options::default();
        let vs = VersionSet::new_dummy(PathBuf::from("/db/demo"), &opts);
        let mut ver = vs.borrow_mut();

        assert_eq!(0, ver.last_sequence_number());
        assert_eq!(Path::new("/db/demo"), ver.abs_db_path());

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

        let mut buf = Vec::<u8>::new();
        patch.marshal(&mut buf);
        assert_eq!(4, buf.len());

        let (loaded, b) = VersionPatch::from_unmarshal(buf.as_slice())?;
        assert_eq!(4, loaded);
        assert_eq!(patch.redo_log().cf_id, b.redo_log().cf_id);
        assert_eq!(patch.redo_log().number, b.redo_log().number);

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
}