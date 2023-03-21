use std::cell::RefCell;
use std::io;
use std::io::Write;
use std::rc::Rc;
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
    record_ty_crc32_sums: [u32;MAX_RECORD_TYPE+1]
}

impl LogWriter {
    pub fn new(file: Rc<RefCell<dyn WritableFile>>, block_size: usize) -> Self {
        Self {
            file_writer: marshal::FileWriter::new(file),
            block_size,
            block_offset: 0,
            record_ty_crc32_sums: [0;MAX_RECORD_TYPE+1]
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
            let fragment_len = if left < avail {left} else {avail};

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

        self.file_writer.write_fixed_u32(0/*checksum*/)?;
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
            block_offset: 0
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
        let checksum = self.file_reader.read_fixed_u32()?;
        let len = self.file_reader.read_fixed_u16()? as usize;
        let raw_rd_ty = self.file_reader.read_byte()?;
        let record_ty_rs = RecordType::try_from_primitive(raw_rd_ty as u32);
        if let Err(_) = record_ty_rs {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "incorrect record type"));
        }

        //let mut record = Vec::with_capacity(len as usize);
        let start = scratch.len();
        scratch.extend(std::iter::repeat(0).take(len));
        self.file_reader.read(&mut scratch.as_mut_slice()[start..start + len])?;

        if self.verify_checksum {
            // TODO
        }

        self.block_offset += HEADER_SIZE + len;
        Ok(record_ty_rs.unwrap())
    }
}