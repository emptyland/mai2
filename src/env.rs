use std::{env, fs, io};
use std::alloc::System;
use std::any::Any;
use std::cell::RefCell;
use std::cmp::min;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Env {
    fn new_sequential_file(&self, path: &Path) -> io::Result<Rc<RefCell<dyn SequentialFile>>>;
    fn new_writable_file(&self, path: &Path, append: bool) -> io::Result<Arc<RefCell<dyn WritableFile>>>;
    fn new_random_access_file(&self, path: &Path) -> io::Result<Rc<RefCell<dyn RandomAccessFile>>>;

    fn make_dir(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn is_dir(&self, path: &Path) -> bool {
        path.is_dir()
    }

    fn get_children(&self, path: &Path) -> io::Result<Vec<String>> {
        let mut rs = Vec::<String>::new();
        for entry in fs::read_dir(path)? {
            let dir = entry?;
            rs.push(String::from(dir.path().as_path().as_os_str().to_str().unwrap()));
        }
        Ok(rs)
    }

    fn delete_file(&self, path: &Path, recursive: bool) -> io::Result<()> {
        if self.is_dir(path) {
            if recursive {
                fs::remove_dir_all(path)
            } else {
                fs::remove_dir(path)
            }
        } else {
            fs::remove_file(path)
        }
    }

    fn get_absolute_path(&self, path: &Path) -> io::Result<PathBuf> { fs::canonicalize(path) }

    fn get_work_dir(&self) -> io::Result<PathBuf> { env::current_dir() }

    fn file_exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn file_not_exists(&self, path: &Path) -> bool {
        !self.file_exists(path)
    }

    fn get_file_size(&self, path: &Path) -> io::Result<u64> {
        let md = fs::metadata(path)?;
        Ok(md.len())
    }

    fn write_all(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let wf = self.new_writable_file(path, true)?;
        let mut borrowed_wf = wf.borrow_mut();
        borrowed_wf.write_all(data)
    }

    fn read_all(&self, path: &Path) -> io::Result<Vec<u8>> {
        let rf = self.new_sequential_file(path)?;
        let mut borrowed_rf = rf.borrow_mut();
        let mut buf = Vec::new();
        borrowed_rf.read_to_end(&mut buf)?;
        return Ok(buf);
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        fs::read_to_string(path)
    }

    fn current_time_micros(&self) -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    }

    fn current_time_mills(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

pub trait SequentialFile: io::Read {
    fn skip(&mut self, bytes: usize) -> io::Result<u64>;
    fn get_file_size(&self) -> io::Result<usize>;
}

pub trait WritableFile: io::Write {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;

    fn append(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write(buf)
    }

    fn positioned_append(&mut self, position: u64, buf: &[u8]) -> io::Result<usize>;
    fn sync(&mut self) -> io::Result<()>;
    fn get_file_size(&self) -> io::Result<usize>;
    fn truncate(&mut self, size: u64) -> io::Result<()>;
}

pub trait RandomAccessFile {
    fn positioned_read(&mut self, position: u64, buf: &mut [u8]) -> io::Result<usize>;
    fn get_file_size(&self) -> io::Result<usize>;
}

pub struct EnvImpl;

impl EnvImpl {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    pub fn new_chroot(root: &Path) -> Self {
        todo!()
    }
}

impl Env for EnvImpl {
    fn new_sequential_file(&self, path: &Path) -> io::Result<Rc<RefCell<dyn SequentialFile>>> {
        let file_impl = SequentialFileImpl::open(path)?;
        Ok(Rc::new(RefCell::new(file_impl)))
    }

    fn new_writable_file(&self, path: &Path, append: bool) -> io::Result<Arc<RefCell<dyn WritableFile>>> {
        let file_impl = WritableFileImpl::open(path, append)?;
        Ok(Arc::new(RefCell::new(file_impl)))
    }

    fn new_random_access_file(&self, path: &Path) -> io::Result<Rc<RefCell<dyn RandomAccessFile>>> {
        let file_impl = RandomAccessFileImpl::open(path)?;
        Ok(Rc::new(RefCell::new(file_impl)))
    }
}

struct SequentialFileImpl {
    file: File,
}

impl SequentialFileImpl {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = open_read_only_file(path)?;
        Ok(Self { file })
    }
}

impl Read for SequentialFileImpl {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl SequentialFile for SequentialFileImpl {
    fn skip(&mut self, bytes: usize) -> io::Result<u64> {
        self.file.seek(SeekFrom::Current(bytes as i64))
    }

    fn get_file_size(&self) -> io::Result<usize> {
        let md = self.file.metadata()?;
        Ok(md.len() as usize)
    }
}

struct WritableFileImpl {
    file: File,
}

impl WritableFileImpl {
    fn open(path: &Path, append: bool) -> io::Result<Self> {
        let file = File::options()
            .write(true)
            .read(false)
            .create(true)
            .create_new(!append)
            .append(append)
            .open(path)?;
        Ok(Self { file })
    }
}

impl Write for WritableFileImpl {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl WritableFile for WritableFileImpl {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn positioned_append(&mut self, position: u64, buf: &[u8]) -> io::Result<usize> {
        self.file.seek(SeekFrom::Start(position))?;
        self.file.write(buf)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn get_file_size(&self) -> io::Result<usize> {
        let md = self.file.metadata()?;
        Ok(md.len() as usize)
    }

    fn truncate(&mut self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }
}

impl Drop for WritableFileImpl {
    fn drop(&mut self) {}
}

struct RandomAccessFileImpl {
    file: File,
}

impl RandomAccessFileImpl {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = open_read_only_file(path)?;
        Ok(Self { file })
    }
}

fn open_read_only_file(path: &Path) -> io::Result<File> {
    File::options()
        .read(true)
        .write(false)
        .create(false)
        .create_new(false)
        .open(path)
}

impl RandomAccessFile for RandomAccessFileImpl {
    fn positioned_read(&mut self, position: u64, buf: &mut [u8]) -> io::Result<usize> {
        self.file.seek(SeekFrom::Start(position))?;
        self.file.read(buf)
    }

    fn get_file_size(&self) -> io::Result<usize> {
        let md = self.file.metadata()?;
        Ok(md.len() as usize)
    }
}

pub struct MemoryWritableFile {
    buf: Vec<u8>,
}

impl MemoryWritableFile {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn new_rc() -> Arc<RefCell<dyn WritableFile>> {
        Arc::new(RefCell::new(MemoryWritableFile::new()))
    }

    pub fn buf(&self) -> &Vec<u8> {
        &self.buf
    }

    pub fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }

    pub fn get_buf(wf: &Arc<RefCell<dyn WritableFile>>) -> Vec<u8> {
        let borrowed_wf = wf.borrow();
        borrowed_wf.as_any().downcast_ref::<Self>().as_ref().unwrap().buf().clone()
    }
}

impl Write for MemoryWritableFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WritableFile for MemoryWritableFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn positioned_append(&mut self, position: u64, buf: &[u8]) -> io::Result<usize> {
        if position as usize + buf.len() > self.buf.len() {
            let fixup = position as usize + buf.len() - self.buf.len();
            for _ in 0..fixup {
                self.buf.push(0);
            }
        }
        let mut placement = &mut self.buf.as_mut_slice()[position as usize..position as usize + buf.len()];
        placement.write(buf)
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn get_file_size(&self) -> io::Result<usize> {
        Ok(self.buf.len())
    }

    fn truncate(&mut self, size: u64) -> io::Result<()> {
        self.buf.clear();
        Ok(())
    }
}


pub struct MemorySequentialFile {
    buf: Vec<u8>,
    offset: usize,
}

impl MemorySequentialFile {
    pub fn new(buf: Vec<u8>) -> Self {
        Self {
            buf,
            offset: 0,
        }
    }

    pub fn new_rc(buf: Vec<u8>) -> Rc<RefCell<dyn SequentialFile>> {
        Rc::new(RefCell::new(MemorySequentialFile::new(buf)))
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.offset
    }
}

impl Read for MemorySequentialFile {
    fn read(&mut self, mut buf: &mut [u8]) -> io::Result<usize> {
        let read_len = min(self.remaining(), buf.len());
        let placement = self.buf.as_slice();
        buf.write(&placement[self.offset..self.offset + read_len])?;
        self.offset += read_len;
        Ok(read_len)
    }
}

impl SequentialFile for MemorySequentialFile {
    fn skip(&mut self, bytes: usize) -> io::Result<u64> {
        let skipped_len = min(self.remaining(), bytes);
        self.offset += skipped_len;
        Ok(skipped_len as u64)
    }

    fn get_file_size(&self) -> io::Result<usize> {
        Ok(self.buf.len())
    }
}

pub struct JunkFilesCleaner {
    paths: Vec<String>,
}

impl JunkFilesCleaner {
    pub fn new(path: &str) -> Self {
        Self { paths: vec![String::from(path)] }
    }

    pub fn new_all(paths: &[&str]) -> Self {
        Self { paths: paths.iter().map(|x| String::from(*x)).collect() }
    }

    pub fn path_str(&self, i: usize) -> &String {
        &self.paths[i]
    }

    pub fn path(&self, i: usize) -> &Path {
        Path::new(self.path_str(i))
    }
}

impl Drop for JunkFilesCleaner {
    fn drop(&mut self) {
        for p in self.paths.iter() {
            let path = Path::new(p);
            if !path.exists() {
                continue;
            }

            if path.is_dir() {
                fs::remove_dir_all(path).expect("clean fail!")
            } else {
                fs::remove_file(path).expect("clean fail!")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::env;

    use super::*;

    #[test]
    fn sanity() -> io::Result<()> {
        let junk = JunkFilesCleaner::new("tests/a.txt");

        let env = EnvImpl::new();
        let file = env.new_writable_file(junk.path(0), false)?;
        file.borrow_mut().write("hello".as_bytes())?;

        assert!(Path::new(junk.path_str(0)).exists());
        Ok(())
    }

    #[test]
    fn work_dir() -> io::Result<()> {
        let path = env::current_dir()?;
        assert!(path.is_absolute());
        //dbg!(path);
        Ok(())
    }

    #[test]
    fn writing() -> io::Result<()> {
        let files = JunkFilesCleaner::new("tests/b.txt");
        let env = EnvImpl::new();
        {
            let file = env.new_writable_file(files.path(0), false)?;
            file.borrow_mut().write("hello\n".as_bytes())?;
        }
        {
            let file = env.new_sequential_file(files.path(0))?;
            let mut buf = Vec::new();
            file.borrow_mut().read_to_end(&mut buf)?;
            assert_eq!("hello\n", String::from_utf8_lossy(&buf));
        }

        Ok(())
    }

    #[test]
    fn open_file_not_exists() -> io::Result<()> {
        let files = JunkFilesCleaner::new("tests/exists");
        let env = EnvImpl::new();
        {
            let file = env.new_writable_file(files.path(0), false)?;
            file.borrow_mut().write("hello\n".as_bytes())?;
        }
        {
            let rs = env.new_writable_file(files.path(0), false);
            assert!(rs.is_err());
        }

        Ok(())
    }

    #[test]
    fn memory_writable_file() -> io::Result<()> {
        let mut wf = MemoryWritableFile::new();
        wf.write("aaa".as_bytes())?;
        assert_eq!(3, wf.get_file_size()?);
        wf.positioned_append(1, "bcd".as_bytes())?;
        assert_eq!(4, wf.get_file_size()?);
        assert_eq!("abcd".as_bytes(), wf.buf.as_slice());
        Ok(())
    }

    // #[test]
    // fn current_time_micros() {
    //     let env = EnvImpl::new();
    //     let v: HashSet<_> = (0..10000).map(|_| {
    //         env.current_time_micros()
    //     }).collect();
    //     assert_eq!(10000, v.len());
    //
    // }
}