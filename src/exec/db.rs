use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::exec::connection::Connection;
use crate::{Corrupting, log_debug, Status, storage};
use crate::base::Logger;
use crate::storage::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DEFAULT_COLUMN_FAMILY_NAME, Env, from_io_result, Options, ReadOptions, WriteBatch, WriteOptions};
use crate::Result;

pub struct DB {
    this: Weak<DB>,
    env: Arc<dyn Env>,
    abs_db_path: PathBuf,
    logger: Arc<dyn Logger>,
    storage: Arc<dyn storage::DB>,
    default_column_family: Arc<dyn storage::ColumnFamily>,
    //tables: Mutex<HashMap<String, Box>>
    tables_handle: Mutex<HashMap<String, TableHandle>>,

    next_table_id: AtomicU64,
    next_conn_id: AtomicU64,
    connections: Mutex<Vec<Arc<Connection>>>,
    // TODO:
}

type LockingTables<'a> = MutexGuard<'a, HashMap<String, TableHandle>>;

impl DB {
    const META_COL_TABLE_NAMES: &'static [u8] = "__metadata_table_names__".as_bytes();
    const META_COL_TABLE_PREFIX: &'static str = "__metadata_table__.";
    const META_COL_NEXT_TABLE_ID: &'static [u8] = "__metadata_next_table_id__".as_bytes();
    const METADATA_FILE_NAME: &'static str = "__METADATA__";

    pub fn open(dir: String, name: String) -> Result<Arc<Self>> {
        let options = Options::with()
            .create_if_missing(true)
            .error_if_exists(false)
            .dir(dir.clone())
            .build();
        let logger = options.logger.clone();
        let env = options.env.clone();
        let desc = Self::try_load_column_family_descriptors(&options.env, &dir, &name)?;
        let (storage, _) = storage::open_kv_storage(options, name.clone(), &desc)?;

        let abs_db_path = storage.get_absolute_path().to_path_buf();
        let db = Arc::new_cyclic(|weak| {
            let default_column_family = storage.default_column_family();
            Self {
                this: weak.clone(),
                env,
                abs_db_path,
                logger,
                storage,
                default_column_family,
                next_conn_id: AtomicU64::new(0),
                next_table_id: AtomicU64::new(0),
                tables_handle: Mutex::default(),
                connections: Mutex::default(),
            }
        });
        db.prepare()?;
        Ok(db)
    }

    fn try_load_column_family_descriptors(env: &Arc<dyn Env>, dir: &String, name: &String)
        -> Result<Vec<ColumnFamilyDescriptor>> {
        let db_path = if dir.is_empty() {
            PathBuf::from(name)
        } else {
            PathBuf::from(dir).join(Path::new(name))
        };
        if env.file_not_exists(&db_path) {
            return Ok(vec![ColumnFamilyDescriptor{
                name: DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                options: ColumnFamilyOptions::default(),
            }]);
        }
        let abs_db_path = from_io_result(env.get_absolute_path(&db_path))?;
        let mut tables = Self::try_load_tables_name(env, &abs_db_path)?;
        tables.push(DEFAULT_COLUMN_FAMILY_NAME.to_string());
        Ok(tables.iter().map(|x| {
            ColumnFamilyDescriptor {
                name: x.clone(),
                options: ColumnFamilyOptions::default(),
            }
        }).collect())
    }

    fn try_load_tables_name(env: &Arc<dyn Env>, abs_db_path: &Path) -> Result<Vec<String>> {
        let metadata_path = abs_db_path.to_path_buf().join(Path::new(Self::METADATA_FILE_NAME));
        if env.file_not_exists(&metadata_path) {
            return Ok(vec![DEFAULT_COLUMN_FAMILY_NAME.to_string()]);
        }

        let parts = from_io_result(env.read_to_string(&metadata_path))?;
        Ok(parts.split(",").map(|x|{x.to_string()}).collect())
    }

    fn prepare(&self) -> Result<()> {
        let rd_opts = ReadOptions::default();

        let tables_name: Vec<String>;
        let rs = self.storage.get_pinnable(&rd_opts,
                                           &self.default_column_family,
                                           Self::META_COL_TABLE_NAMES);
        match rs {
            Err(status) => {
                return if status == Status::NotFound {
                    Ok(())
                } else {
                    Err(status)
                };
            }
            Ok(pin_val) => {
                //pin_val.to_utf8_string().split(",").collect();
                let parts = pin_val.to_utf8_string();
                log_debug!(self.logger, "tables: {}", parts);
                tables_name = parts.split(",").map(|x| { x.to_string() }).collect()
            }
        }

        let cfs = self.storage.get_all_column_families()?;
        for name in tables_name {
            let col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, &name);
            let yaml = self.storage.get_pinnable(&rd_opts,
                                                 &self.default_column_family,
                                                 col_name.as_bytes())?.to_utf8_string();
            match serde_yaml::from_str::<TableMetadata>(&yaml) {
                Err(e) => {
                    let message = format!("Parse yaml fail: {}", e.to_string());
                    return Err(Status::corrupted(message));
                }
                Ok(table) => {
                    let cf = cfs.iter()
                        .cloned()
                        .find(|x| { x.name() == table.name });
                    if cf.is_none() {
                        let message = format!("Can not find table column family: {}", &table.name);
                        return Err(Status::corrupted(message));
                    }
                    let mut locking = self.tables_handle.lock().unwrap();
                    locking.insert(table.name.clone(),
                                   TableHandle::new(cf.unwrap(), table));
                }
            }
            log_debug!(self.logger, "load table:\n {}", yaml);
        }

        let next_id = self.storage.get_pinnable(&rd_opts,
                                                &self.default_column_family,
                                                Self::META_COL_NEXT_TABLE_ID)?
            .to_utf8_string();
        self.next_table_id.store(u64::from_str_radix(&next_id, 10).unwrap(),
                                 Ordering::Relaxed);
        log_debug!(self.logger, "next table id: {}", &next_id);
        Ok(())
    }

    pub fn connect(&self) -> Arc<Connection> {
        let id = self.next_conn_id.fetch_add(1, Ordering::AcqRel) + 1;
        let conn = Arc::new(Connection::new(id, &self.this));
        let mut locking = self.connections.lock().unwrap();
        locking.push(conn.clone());
        conn
    }

    pub fn next_table_id(&self) -> Result<u64> {
        let id = self.next_table_id.fetch_add(1, Ordering::AcqRel) + 1;
        let mut wr_opts = WriteOptions::default();
        wr_opts.sync = true;
        self.storage.insert(&wr_opts, &self.default_column_family,
                            Self::META_COL_NEXT_TABLE_ID,
                            id.to_string().as_bytes())?;
        Ok(id)
    }

    pub fn create_table(&self, table_metadata: TableMetadata, tables: &mut LockingTables) -> Result<u64> {
        let mut batch = WriteBatch::new();
        //let mut tables = self.tables_handle.lock().unwrap();

        let mut names: Vec<String> = tables.keys().cloned().collect();
        names.push(table_metadata.name.clone());
        self.sync_tables_name(&names)?;
        batch.insert(&self.default_column_family, Self::META_COL_TABLE_NAMES,
                     names.join(",").as_bytes());

        match serde_yaml::to_string(&table_metadata) {
            Err(e) => {
                let message = format!("Yaml serialize fail: {}", e.to_string());
                return Err(Status::corrupted(message));
            }
            Ok(yaml) => {
                let col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, &table_metadata.name);
                batch.insert(&self.default_column_family, col_name.as_bytes(),
                             yaml.as_bytes());
            }
        }
        let id = table_metadata.id;

        let column_family = self.storage.new_column_family(
            table_metadata.name.as_str(),
            ColumnFamilyOptions::default())?;
        // [kkkkk.a] = [value]
        // [kkkkk.b] = [value]

        let mut wr_opts = WriteOptions::default();
        wr_opts.sync = true;
        match self.storage.write(&wr_opts, batch) {
            Err(e) => {
                self.storage.drop_column_family(column_family)?;
                Err(e)
            }
            Ok(()) => {
                tables.insert(table_metadata.name.clone(),
                              TableHandle::new(column_family, table_metadata));
                Ok(id)
            }
        }
    }

    pub fn drop_table(&self, name: &String, tables: &mut LockingTables) -> Result<u64> {
        let rs = tables.get(name);
        if rs.is_none() {
            return Err(Status::corrupted(format!("Table `{}` not found", name)));
        }
        let cf = rs.unwrap().column_family.clone();
        let table = rs.unwrap().metadata.clone();
        let table_id = table.borrow().id;
        drop(rs);

        self.storage.drop_column_family(cf)?;
        // TODO: delete secondary indexes

        let names: Vec<String> = tables.keys().cloned().filter(|x| {x != name}).collect();
        self.sync_tables_name(&names)?;
        let mut batch = WriteBatch::new();
        batch.insert(&self.default_column_family, Self::META_COL_TABLE_NAMES,
                     names.join(",").as_bytes());

        let col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, name);
        batch.delete(&self.default_column_family, col_name.as_bytes());

        let mut wr_opts = WriteOptions::default();
        wr_opts.sync = true;
        self.storage.write(&wr_opts, batch)?;

        tables.remove(name);
        Ok(table_id)
    }

    fn get_table_handle(&self, name: &String) -> Option<(Arc<dyn ColumnFamily>, Arc<RefCell<TableMetadata>>)> {
        let tables = self.tables_handle.lock().unwrap();
        match tables.get(name) {
            Some(handle) => Some((handle.column_family.clone(), handle.metadata.clone())),
            None => None
        }
    }

    pub fn lock_tables(&self) -> LockingTables {
        self.tables_handle.lock().unwrap()
    }

    fn sync_tables_name(&self, names: &[String]) -> Result<()> {
        let metadata_path = self.abs_db_path.join(Path::new(Self::METADATA_FILE_NAME));
        from_io_result(self.env.write_all(&metadata_path, names.join(",").as_bytes()))?;
        Ok(())
    }

    pub fn remove_connection(&self, conn: &Connection) {
        let mut locking = self.connections.lock().unwrap();
        for i in 0..locking.len() {
            if conn.id == locking[i].id {
                locking.remove(i);
                break;
            }
        }
    }
}

pub struct TableHandle {
    column_family: Arc<dyn ColumnFamily>,
    secondary_indexes: Vec<Arc<dyn ColumnFamily>>,
    anonymous_row_key_counter: AtomicU64,
    auto_increment_counter: AtomicU64,
    metadata: Arc<RefCell<TableMetadata>>,
}

impl TableHandle {
    fn new(column_family: Arc<dyn ColumnFamily>, metadata: TableMetadata) -> Self {
        Self {
            column_family,
            secondary_indexes: Vec::default(),
            anonymous_row_key_counter: AtomicU64::new(0),
            auto_increment_counter: AtomicU64::new(0),
            metadata: Arc::new(RefCell::new(metadata))
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub id: u64,
    pub created_at: String,
    pub updated_at: String,
    pub raw_ast: String,
    pub rows: usize,
    pub primary_keys: Vec<u32>,
    pub columns: Vec<ColumnMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub id: u32,
    pub ty: ColumnType,
    pub not_null: bool,
    pub default_value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnType {
    TinyInt(u32),
    SmallInt(u32),
    Int(u32),
    BigInt(u32),
    Float(u32, u32),
    Double(u32, u32),
    Char(u32),
    Varchar(u32),
}

#[cfg(test)]
mod tests {
    use crate::base::Arena;
    use super::*;
    use crate::storage::JunkFilesCleaner;

    #[test]
    fn sanity() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db100");
        let db = DB::open("tests".to_string(), "db100".to_string())?;

        let arena = Arena::new_rc();
        let conn = db.connect();
        conn.execute_str("create table a { a tinyint(1) not null } ", &arena)?;

        let (cf, tb) = db.get_table_handle(&"a".to_string()).unwrap();
        assert_eq!("a", cf.name());
        assert_eq!("a", tb.borrow().name);
        Ok(())
    }

    #[test]
    fn recover() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db101");
        let arena = Arena::new_rc();
        {
            let db = DB::open("tests".to_string(), "db101".to_string())?;
            let sql = " create table t1 {\n\
                a tinyint(1) not null,\n\
                b char(6)\n\
            }\n";
            let conn = db.connect();
            conn.execute_str(sql, &arena)?;
        }

        let db = DB::open("tests".to_string(), "db101".to_string())?;
        let (cf1, t1) = db.get_table_handle(&"t1".to_string()).unwrap();
        assert_eq!("t1", cf1.name());
        assert_eq!("t1", t1.borrow().name);
        assert_eq!(2, t1.borrow().columns.len());
        Ok(())
    }

    #[test]
    fn create_before_drop_table() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db102");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db102".to_string())?;
        let sql = " create table t1 {\n\
                a tinyint(1) not null,\n\
                b char(6)\n\
            };\n\
            drop table if exists t1;\n\
            ";
        let conn = db.connect();
        conn.execute_str(sql, &arena)?;
        assert!(db.get_table_handle(&"t1".to_string()).is_none());
        Ok(())
    }
}