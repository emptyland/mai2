use std::cell::RefCell;
use std::io;
use std::io::ErrorKind::UnexpectedEof;
use std::rc::Rc;
use std::sync::Arc;

use crc::{Crc, CRC_32_ISCSI};
use num_enum::TryFromPrimitive;

use crate::env::{SequentialFile, WritableFile};
use crate::marshal;
use crate::marshal::FileReader;

#[repr(u32)]
#[derive(PartialEq, Debug, Clone, Copy, TryFromPrimitive)]
pub enum RecordType {
    Zero,
    Full,

    // for fragments
    First,
    Middle,
    Last,
}

pub const MAX_RECORD_TYPE: usize = RecordType::Last as usize;
pub const HEADER_SIZE: usize = 4 + 2 + 1;
pub const DEFAULT_BLOCK_SIZE: usize = 32768;

/*
 * +---------+-------+
 * |         | crc32 | 4 bytes
 * |         +-------+
 * | header  | len   | 2 bytes
 * |         +-------+
 * |         | type  | 1 bytes
 * +---------+-------+
 * | payload | data  | len bytes
 * +---------+-------+
 */
pub struct LogWriter {
    file_writer: marshal::FileWriter,
    block_size: usize,
    block_offset: usize,
    record_ty_crc32_sums: [u32; MAX_RECORD_TYPE + 1],
}

impl LogWriter {
    pub fn new(file: Arc<RefCell<dyn WritableFile>>, block_size: usize) -> Self {
        let mut sums: [u32; MAX_RECORD_TYPE + 1] = [0; MAX_RECORD_TYPE + 1];
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);

        for i in 0..sums.len() {
            let mut digest = crc.digest();
            digest.update(&[i as u8]);
            sums[i] = digest.finalize();
        }

        Self {
            file_writer: marshal::FileWriter::new(file),
            block_size,
            block_offset: 0,
            record_ty_crc32_sums: sums,
        }
    }

    pub fn append(&mut self, record: &[u8]) -> io::Result<usize> {
        let mut start = 0;
        let mut left = record.len();
        let mut begin = true;
        let mut written_bytes = 0;

        loop {
            let left_over = self.block_size - self.block_offset;
            if left_over < HEADER_SIZE {
                if left_over > 0 {
                    self.file_writer.write_pad(left_over)?;
                }
                self.block_offset = 0;
            }

            let avail = self.block_size - self.block_offset - HEADER_SIZE;
            let fragment_len = if left < avail { left } else { avail };

            let end = left == fragment_len;
            let record_ty = if begin && end {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if end {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            written_bytes += self.emit_physical_record(&record[start..start + fragment_len], record_ty)?;
            start += fragment_len;
            left -= fragment_len;
            begin = false;

            if left <= 0 {
                break;
            }
        }

        Ok(written_bytes)
    }

    pub fn flush(&self) -> io::Result<()> {
        self.file_writer.flush()
    }

    pub fn sync(&self) -> io::Result<()> {
        self.file_writer.sync()
    }

    fn emit_physical_record(&mut self, data: &[u8], record_ty: RecordType) -> io::Result<usize> {
        assert!(data.len() <= u16::MAX as usize);
        assert!(self.block_offset + HEADER_SIZE + data.len() <= self.block_size);

        let crc = Crc::<u32>::new(&CRC_32_ISCSI);
        let mut digest = crc.digest();
        digest.update(&[record_ty as u8]);
        digest.update(data);

        self.file_writer.write_fixed_u32(digest.finalize())?;
        self.file_writer.write_fixed_u16(data.len() as u16)?;
        self.file_writer.write_byte(record_ty as u8)?;
        self.file_writer.write(data)?;

        self.block_offset += HEADER_SIZE + data.len();
        Ok(HEADER_SIZE + data.len())
    }
}

pub struct LogReader {
    file_reader: FileReader,
    verify_checksum: bool,
    block_size: usize,
    block_offset: usize,
}

impl LogReader {
    pub fn new(file: Rc<RefCell<dyn SequentialFile>>, verify_checksum: bool, block_size: usize) -> Self {
        Self {
            file_reader: marshal::FileReader::new(file),
            verify_checksum,
            block_size,
            block_offset: 0,
        }
    }

    pub fn read(&mut self) -> io::Result<Vec<u8>> {
        //let mut segment = 0;
        let mut scratch = Vec::<u8>::new();
        loop {
            let left_over = self.block_size - self.block_offset;

            if left_over < HEADER_SIZE {
                if left_over > 0 {
                    self.file_reader.skip(left_over)?;
                }
                self.block_offset = 0;
            }

            let record_ty = self.read_physical_record(&mut scratch)?;
            //segment += 1;

            if record_ty != RecordType::Middle && record_ty != RecordType::First {
                break;
            }
        }

        Ok(scratch)
    }

    fn read_physical_record(&mut self, scratch: &mut Vec<u8>) -> io::Result<RecordType> {
        let mut buf: [u8;4] = [0;4];
        let rs = self.file_reader.read(buf.as_mut_slice());
        if let Err(e) = rs {
            return if e.kind() == io::ErrorKind::UnexpectedEof && self.file_reader.eof() {
                Ok(RecordType::Zero)
            } else {
                Err(e)
            };
        }
        let checksum = u32::from_le_bytes(buf);
        let len = self.file_reader.read_fixed_u16()? as usize;
        let raw_rd_ty = self.file_reader.read_byte()?;
        let record_ty_rs = RecordType::try_from_primitive(raw_rd_ty as u32);
        if let Err(_) = record_ty_rs {
            let msg = format!("incorrect record type: {}", raw_rd_ty);
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }

        let start = scratch.len();
        scratch.extend(std::iter::repeat(0).take(len));
        self.file_reader.read(&mut scratch.as_mut_slice()[start..start + len])?;

        if self.verify_checksum {
            let crc = Crc::<u32>::new(&CRC_32_ISCSI);
            let mut digest = crc.digest();
            digest.update(&[raw_rd_ty]);
            digest.update(&scratch.as_slice()[start..start + len]);

            let checked_sum = digest.finalize();
            if checked_sum != checksum {
                let msg = format!("incorrect crc32 checksum: {} vs {}", checked_sum, checksum);
                return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
            }
        }

        self.block_offset += HEADER_SIZE + len;
        Ok(record_ty_rs.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;

    use crate::env::{Env, EnvImpl, MemorySequentialFile, MemoryWritableFile, WritableFile};
    use crate::version::VersionPatch;

    use super::*;

    #[test]
    fn sanity() {
        let wf = new_and_write_log("1234567890".as_bytes(), 16);

        let buf = MemoryWritableFile::get_buf(&wf);
        assert_eq!(wf.borrow().get_file_size().unwrap(), buf.len());
    }

    #[test]
    fn read_log() {
        let wf = new_and_write_log("1234567890".as_bytes(), 16);
        let buf = MemoryWritableFile::get_buf(&wf);
        //dbg!(buf.clone());
        let rf = MemorySequentialFile::new_rc(buf);
        let mut log = LogReader::new(rf.clone(), true, 16);
        let rd = log.read().unwrap();
        assert_eq!("1234567890".as_bytes(), rd.as_slice());
    }

    #[test]
    fn block_2_log() {
        let data: [u8; 33] = [0xab; 33];
        let wf = new_and_write_log(&data, 16);
        let buf = MemoryWritableFile::get_buf(&wf);
        let rf = MemorySequentialFile::new_rc(buf);
        let mut log = LogReader::new(rf.clone(), true, 16);
        let rd = log.read().unwrap();
        assert_eq!(data, rd.as_slice());
    }

    #[test]
    fn issue001() -> io::Result<()> {
        let env = EnvImpl::new();
        let file = env.new_sequential_file(&PathBuf::from("tests/issue001-MANIFEST-1"))?;
        let mut rd = LogReader::new(file, true, DEFAULT_BLOCK_SIZE);
        loop {
            let buf = rd.read()?;
            if buf.is_empty() {
                break;
            }
            //dbg!(buf);
            let (_, patch) = VersionPatch::from_unmarshal(&buf)?;
            dbg!(patch.redo_log_number());
        }

        Ok(())
    }

    fn new_and_write_log(data: &[u8], block_size: usize) -> Arc<RefCell<dyn WritableFile>> {
        let wf = MemoryWritableFile::new_rc();
        let mut log = LogWriter::new(wf.clone(), block_size);
        let written_bytes = log.append(data).unwrap();

        let borrowed_wf = wf.borrow();
        assert_eq!(written_bytes, borrowed_wf.get_file_size().unwrap());
        wf.clone()
    }
}