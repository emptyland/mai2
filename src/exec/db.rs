use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::exec::connection::Connection;
use crate::{Corrupting, log_debug, Status, storage};
use crate::base::{Arena, ArenaBox, ArenaStr, Logger};
use crate::exec::evaluator::Value;
use crate::exec::executor::{ColumnSet, SecondaryIndexBundle, Tuple};
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
    tables_handle: Mutex<HashMap<String, TableRef>>,

    next_table_id: AtomicU64,
    next_index_id: AtomicU64,
    next_conn_id: AtomicU64,
    connections: Mutex<Vec<Arc<Connection>>>,
    // TODO:
}

type TableRef = Arc<TableHandle>;
type LockingTables<'a> = MutexGuard<'a, HashMap<String, TableRef>>;

impl DB {
    const META_COL_TABLE_NAMES: &'static [u8] = "__metadata_table_names__".as_bytes();
    const META_COL_TABLE_PREFIX: &'static str = "__metadata_table__.";
    const META_COL_NEXT_TABLE_ID: &'static [u8] = "__metadata_next_table_id__".as_bytes();
    const META_COL_NEXT_INDEX_ID: &'static [u8] = "__metadata_next_index_id__".as_bytes();
    const METADATA_FILE_NAME: &'static str = "__METADATA__";

    const DATA_COL_TABLE_PREFIX: &'static str = "__table__";

    const PRIMARY_KEY_ID: u32 = 0;
    pub const PRIMARY_KEY_ID_BYTES: [u8; 4] = Self::PRIMARY_KEY_ID.to_be_bytes();

    const NULL_BYTE: u8 = 0xff;
    const NULL_BYTES: [u8; 1] = [Self::NULL_BYTE; 1];

    const NOT_NULL_BYTE: u8 = 0;
    const NOT_NULL_BYTES: [u8; 1] = [Self::NOT_NULL_BYTE; 1];

    const CHAR_FILLING_BYTE: u8 = ' ' as u8;
    const CHAR_FILLING_BYTES: [u8;1] = [Self::CHAR_FILLING_BYTE; 1];

    const VARCHAR_SEGMENT_LEN: usize = 9;

    pub const ANONYMOUS_ROW_KEY: &'static str = "#anonymous_row_key#";
    pub const ANONYMOUS_ROW_KEY_KEY: &'static [u8] = Self::ANONYMOUS_ROW_KEY.as_bytes();
    pub const AUTO_INCREMENT_KEY: &'static [u8] = "#auto_increment#".as_bytes();

    const VARCHAR_CMP_LESS_THAN_SPACES: u8 = 1;
    const VARCHAR_CMP_EQUAL_TO_SPACES: u8 = 2;
    const VARCHAR_CMP_GREATER_THAN_SPACES: u8 = 3;

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
                next_index_id: AtomicU64::new(1), // 0 = primary key
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
            return Ok(vec![ColumnFamilyDescriptor {
                name: DEFAULT_COLUMN_FAMILY_NAME.to_string(),
                options: ColumnFamilyOptions::default(),
            }]);
        }
        let abs_db_path = from_io_result(env.get_absolute_path(&db_path))?;
        let tables = Self::try_load_tables_name_to_id(env, &abs_db_path)?;
        let mut cfds: Vec<ColumnFamilyDescriptor> = tables.values()
            .cloned()
            .map(|x| {
                ColumnFamilyDescriptor {
                    name: format!("{}{}", Self::DATA_COL_TABLE_PREFIX, x),
                    options: ColumnFamilyOptions::default(),
                }
            }).collect();
        cfds.push(ColumnFamilyDescriptor {
            name: DEFAULT_COLUMN_FAMILY_NAME.to_string(),
            options: ColumnFamilyOptions::default(),
        });
        Ok(cfds)
    }

    fn try_load_tables_name_to_id(env: &Arc<dyn Env>, abs_db_path: &Path) -> Result<HashMap<String, u64>> {
        let metadata_path = abs_db_path.to_path_buf().join(Path::new(Self::METADATA_FILE_NAME));
        if env.file_not_exists(&metadata_path) {
            return Ok(HashMap::default());
        }

        let yaml = from_io_result(env.read_to_string(&metadata_path))?;
        match serde_yaml::from_str(yaml.as_str()) {
            Ok(name_to_id) => Ok(name_to_id),
            Err(e) => Err(Status::corrupted(e.to_string()))
        }
    }

    fn prepare(&self) -> Result<()> {
        let rd_opts = ReadOptions::default();

        let tables_name_to_id: HashMap<String, u64>;
        match self.storage.get_pinnable(&rd_opts,
                                        &self.default_column_family,
                                        Self::META_COL_TABLE_NAMES) {
            Err(status) => {
                return if status == Status::NotFound {
                    Ok(())
                } else {
                    Err(status)
                };
            }
            Ok(pin_val) => {
                let yaml = pin_val.to_utf8_string();
                log_debug!(self.logger, "tables: {}", yaml);
                tables_name_to_id = serde_yaml::from_str(yaml.as_str()).unwrap();
            }
        }

        let cfs = self.storage.get_all_column_families()?;
        for (_, id) in tables_name_to_id {
            let meta_col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, id);
            let yaml = self.storage.get_pinnable(&rd_opts,
                                                 &self.default_column_family,
                                                 meta_col_name.as_bytes())?.to_utf8_string();
            let data_col_name = format!("{}{}", Self::DATA_COL_TABLE_PREFIX, id);
            match serde_yaml::from_str::<TableMetadata>(&yaml) {
                Err(e) => {
                    let message = format!("Parse yaml fail: {}", e.to_string());
                    return Err(Status::corrupted(message));
                }
                Ok(table) => {
                    let cf = cfs.iter()
                        .cloned()
                        .find(|x| { x.name() == data_col_name });
                    if cf.is_none() {
                        let message = format!("Can not find table column family: {}", &table.name);
                        return Err(Status::corrupted(message));
                    }
                    self.prepare_table(cf.unwrap(), table)?;
                }
            }
            log_debug!(self.logger, "load table:\n {}", yaml);
        }

        let next_id = self.load_number(&self.default_column_family,
                                       Self::META_COL_NEXT_TABLE_ID, 0)?;
        self.next_table_id.store(next_id, Ordering::Relaxed);
        log_debug!(self.logger, "next table id: {}", &next_id);
        let next_id = self.load_number(&self.default_column_family,
                                       Self::META_COL_NEXT_INDEX_ID, 1)?;
        self.next_index_id.store(next_id, Ordering::Relaxed);
        log_debug!(self.logger, "next index id: {}", &next_id);
        Ok(())
    }

    fn prepare_table(&self, cf: Arc<dyn ColumnFamily>, table: TableMetadata) -> Result<()> {
        let mut locking = self.tables_handle.lock().unwrap();
        let table_name = table.name.clone();
        let anonymous_row_key_counter = self.load_number(&cf, Self::ANONYMOUS_ROW_KEY_KEY, 0)?;
        let auto_increment_counter = self.load_number(&cf, Self::AUTO_INCREMENT_KEY, 0)?;

        let table_handle = TableHandle::new(cf,
                                            anonymous_row_key_counter,
                                            auto_increment_counter,
                                            table);
        locking.insert(table_name, Arc::new(table_handle));
        Ok(())
    }

    fn load_number(&self, cf: &Arc<dyn ColumnFamily>, key: &[u8], default_val: u64) -> Result<u64> {
        let rd_opts = ReadOptions::default();
        match self.storage.get_pinnable(&rd_opts, &cf, key) {
            Ok(value) => {
                Ok(u64::from_le_bytes(value.value().try_into().unwrap()))
            }
            Err(status) => if status == Status::NotFound {
                Ok(default_val)
            } else {
                return Err(status);
            }
        }
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
                            &id.to_le_bytes())?;
        Ok(id)
    }

    pub fn next_index_id(&self) -> Result<u64> {
        let id = self.next_index_id.fetch_add(1, Ordering::AcqRel) + 1;
        let mut wr_opts = WriteOptions::default();
        wr_opts.sync = true;
        self.storage.insert(&wr_opts, &self.default_column_family,
                            Self::META_COL_NEXT_INDEX_ID,
                            &id.to_le_bytes())?;
        Ok(id)
    }

    pub fn create_table(&self, table_metadata: TableMetadata, tables: &mut LockingTables) -> Result<u64> {
        let mut batch = WriteBatch::new();

        let mut name_to_id: HashMap<String, u64> = tables.values()
            .cloned()
            .map(|x| { (x.metadata.name.clone(), x.metadata.id) })
            .collect();
        name_to_id.insert(table_metadata.name.clone(), table_metadata.id);
        let yaml = self.sync_tables_name_to_id(&name_to_id)?;
        batch.insert(&self.default_column_family, Self::META_COL_TABLE_NAMES,
                     yaml.as_bytes());

        match serde_yaml::to_string(&table_metadata) {
            Err(e) => {
                let message = format!("Yaml serialize fail: {}", e.to_string());
                return Err(Status::corrupted(message));
            }
            Ok(yaml) => {
                let col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, table_metadata.id);
                batch.insert(&self.default_column_family, col_name.as_bytes(),
                             yaml.as_bytes());
            }
        }
        let id = table_metadata.id;

        let column_family = self.storage.new_column_family(
            format!("{}{}", Self::DATA_COL_TABLE_PREFIX, table_metadata.id).as_str(),
            ColumnFamilyOptions::default())?;

        let mut wr_opts = WriteOptions::default();
        wr_opts.sync = true;
        match self.storage.write(&wr_opts, batch) {
            Err(e) => {
                self.storage.drop_column_family(column_family)?;
                Err(e)
            }
            Ok(()) => {
                let table_name = table_metadata.name.clone();
                let table_handle = TableHandle::new(column_family,
                                                    0,
                                                    0,
                                                    table_metadata);
                tables.insert(table_name, Arc::new(table_handle));
                Ok(id)
            }
        }
    }

    pub fn drop_table(&self, name: &String, tables: &mut LockingTables) -> Result<u64> {
        let rs = tables.get(name);
        if rs.is_none() {
            return Err(Status::corrupted(format!("Table `{}` not found", name)));
        }
        let table_handle = rs.unwrap().clone();
        let cf = table_handle.column_family.clone();
        let table = table_handle.metadata.clone();
        let table_id = table.id;
        drop(rs);
        drop(table_handle);

        self.storage.drop_column_family(cf)?;
        // TODO: delete secondary indexes

        // let names: Vec<String> = tables.keys().cloned().filter(|x| { x != name }).collect();
        // self.sync_tables_name(&names)?;
        let name_to_id: HashMap<String, u64> = tables.values()
            .cloned()
            .filter(|x| { name.ne(&x.metadata.name) })
            .map(|x| { (x.metadata.name.clone(), x.metadata.id) })
            .collect();
        let yaml = self.sync_tables_name_to_id(&name_to_id)?;
        let mut batch = WriteBatch::new();
        batch.insert(&self.default_column_family, Self::META_COL_TABLE_NAMES,
                     yaml.as_bytes());

        let col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, name);
        batch.delete(&self.default_column_family, col_name.as_bytes());

        let mut wr_opts = WriteOptions::default();
        wr_opts.sync = true;
        self.storage.write(&wr_opts, batch)?;

        tables.remove(name);
        Ok(table_id)
    }

    pub fn insert_rows(&self, column_family: &Arc<dyn ColumnFamily>, table_id: u64,
                       tuples: &[Tuple],
                       secondary_indices: &[SecondaryIndexBundle],
                       anonymous_row_key_value: Option<u64>,
                       auto_increment_value: Option<u64>)
                       -> Result<usize> {
        let mut batch = WriteBatch::new();
        let wr_opts = WriteOptions::default();
        assert_eq!(column_family.name(), format!("{}{}", Self::DATA_COL_TABLE_PREFIX, table_id));

        let mut key = Vec::new();
        for tuple in tuples {
            let prefix_key_len = tuple.row_key().len();
            key.write(tuple.row_key()).unwrap();
            for col in &tuple.columns().columns {
                let value = tuple.get(col.order);
                assert!(!value.is_unit());
                if value.is_null() {
                    continue;
                }
                key.write(format!("#{}", col.name).as_bytes()).unwrap();
                let row_key_len = key.len();

                Self::encode_column_value(tuple.get(col.order), &col.ty, &mut key);
                batch.insert(column_family, &key[..row_key_len], &key[row_key_len..]);

                key.truncate(prefix_key_len); // keep row key prefix.
            }
            key.clear();
        }

        for index in secondary_indices {
            for key in &index.index_keys {
                batch.insert(column_family, key.as_slice(), index.row_key());
            }
        }

        if let Some(value) = anonymous_row_key_value {
            batch.insert(column_family, Self::ANONYMOUS_ROW_KEY_KEY, &value.to_le_bytes());
        }
        if let Some(value) = auto_increment_value {
            batch.insert(column_family, Self::AUTO_INCREMENT_KEY, &value.to_le_bytes());
        }

        self.storage.write(&wr_opts, batch)?;
        log_debug!(self.logger, "rows: {} insert ok", tuples.len());
        Ok(tuples.len())
    }

    pub fn encode_anonymous_row_key(counter: u64, row_key: &mut Vec<u8>) {
        row_key.write(&Self::PRIMARY_KEY_ID_BYTES).unwrap(); // primary key id always is 0
        row_key.write(&counter.to_be_bytes()).unwrap();
    }

    pub fn encode_row_key(value: &Value, ty: &ColumnType, buf: &mut Vec<u8>) -> Result<()> {
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                match value {
                    Value::Int(n) => { buf.write(&n.to_be_bytes()).unwrap(); }
                    _ => unreachable!()
                }
            }
            ColumnType::Float(_, _) => match value {
                Value::Float(n) => { buf.write(&n.to_be_bytes()).unwrap(); }
                _ => unreachable!()
            },
            ColumnType::Double(_, _) => match value {
                Value::Float(n) => { buf.write(&n.to_be_bytes()).unwrap(); }
                _ => unreachable!()
            },
            ColumnType::Char(n) => match value {
                Value::Str(s) => {
                    if s.len() > *n as usize {
                        return Err(Status::corrupted("Char type too long"));
                    }
                    Self::encode_char_ty_for_key(s, *n as usize, buf);
                }
                _ => unreachable!()
            },
            ColumnType::Varchar(n) => match value {
                Value::Str(s) => {
                    if s.len() > *n as usize {
                        return Err(Status::corrupted("Varchar type too long"));
                    }
                    Self::encode_varchar_ty_for_key(s, buf);
                }
                _ => unreachable!()
            },
        }
        Ok(())
    }

    pub fn encode_secondary_index<W: Write>(value: &Value, ty: &ColumnType, wr: &mut W) {
        if value.is_null() {
            wr.write(&Self::NULL_BYTES).unwrap();
            return;
        }
        assert!(!value.is_unit());

        wr.write(&Self::NOT_NULL_BYTES).unwrap();
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                match value {
                    Value::Int(n) => wr.write(&n.to_be_bytes()).unwrap(),
                    _ => unreachable!()
                };
            }
            ColumnType::Float(_, _) => {
                match value {
                    Value::Float(n) => wr.write(&(*n as f32).to_be_bytes()).unwrap(),
                    _ => unreachable!()
                };
            },
            ColumnType::Double(_, _) => {
                match value {
                    Value::Float(n) => wr.write(&n.to_be_bytes()).unwrap(),
                    _ => unreachable!()
                };
            },
            ColumnType::Char(n) => {
                match value {
                    Value::Str(s) => Self::encode_char_ty_for_key(s, *n as usize, wr),
                    _ => unreachable!()
                }
            },
            ColumnType::Varchar(n) => {
                match value {
                    Value::Str(s) => Self::encode_varchar_ty_for_key(s, wr),
                    _ => unreachable!()
                }
            },
        }
    }

    fn encode_char_ty_for_key<W: Write>(s: &ArenaStr, n: usize, wr: &mut W) {
        assert!(s.len() <= n);
        wr.write(s.as_bytes()).unwrap();
        for _ in 0..n - s.len() {
            wr.write(&Self::CHAR_FILLING_BYTES).unwrap();
        }
    }

    fn encode_varchar_ty_for_key<W: Write>(s: &ArenaStr, wr: &mut W) {
        let part_len = Self::VARCHAR_SEGMENT_LEN - 1;
        let n_parts = (s.len() + (part_len - 1)) / (part_len);
        for i in 0..n_parts {
            let part = s.bytes_part(i * part_len, (i + 1) * part_len);
            wr.write(part).unwrap();
            if part.len() < part_len {
                Self::encode_varchar_ty_filling(part_len - part.len(), wr);
                break;
            }
            let ch = *part.last().unwrap();
            let successor = s.as_bytes()[(i + 1) * part_len];
            if successor > ch {
                wr.write(&[Self::VARCHAR_CMP_GREATER_THAN_SPACES]).unwrap();
            } else if successor < ch {
                wr.write(&[Self::VARCHAR_CMP_LESS_THAN_SPACES]).unwrap();
            } else {
                wr.write(&[Self::VARCHAR_CMP_EQUAL_TO_SPACES]).unwrap();
            }
        }
    }

    #[inline]
    fn encode_varchar_ty_filling<W: Write>(n: usize, wr: &mut W) {
        for _ in 0..n {
            wr.write(&Self::CHAR_FILLING_BYTES).unwrap();
        }
        wr.write(&[Self::VARCHAR_CMP_EQUAL_TO_SPACES]).unwrap();
    }

    pub fn encode_column_value<W: Write>(value: &Value, ty: &ColumnType, wr: &mut W) {
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                match value {
                    Value::Int(n) => wr.write(&n.to_le_bytes()).unwrap(),
                    _ => unreachable!()
                };
            }
            ColumnType::Float(_, _) => unreachable!(),
            ColumnType::Double(_, _) => unreachable!(),
            ColumnType::Char(_)
            | ColumnType::Varchar(_) => {
                match value {
                    Value::Str(s) => wr.write(s.as_bytes()).unwrap(),
                    _ => unreachable!()
                };
            },
        }
    }

    pub fn decode_column_value(ty: &ColumnType, value: &[u8]) -> Value {
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                Value::Int(i64::from_le_bytes(value.try_into().unwrap()))
            }
            ColumnType::Float(_, _) => unreachable!(),
            ColumnType::Double(_, _) => unreachable!(),
            ColumnType::Char(_) => unreachable!(),
            ColumnType::Varchar(_) => unreachable!(),
        }
    }

    fn get_table_handle(&self, name: &String) -> Option<(Arc<dyn ColumnFamily>, Arc<TableMetadata>)> {
        let tables = self.tables_handle.lock().unwrap();
        match tables.get(name) {
            Some(handle) =>
                Some((handle.column_family.clone(), handle.metadata.clone())),
            None => None
        }
    }

    pub fn lock_tables(&self) -> LockingTables {
        self.tables_handle.lock().unwrap()
    }

    pub fn is_table_exists(&self, table_name: &String) -> bool {
        let locking = self.lock_tables();
        locking.contains_key(table_name)
    }

    fn sync_tables_name_to_id(&self, name_to_id: &HashMap<String, u64>) -> Result<String> {
        let metadata_path = self.abs_db_path.join(Path::new(Self::METADATA_FILE_NAME));
        match serde_yaml::to_string(name_to_id) {
            Ok(yaml) => {
                from_io_result(self.env.write_all(&metadata_path, yaml.as_bytes()))?;
                Ok(yaml)
            }
            Err(e) => Err(Status::corrupted(e.to_string()))
        }
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

    pub fn _test_get_row(&self, table_name: &String,
                         columns_set: &ArenaBox<ColumnSet>,
                         row_key: &[u8],
                         arena: &Rc<RefCell<Arena>>) -> Result<Tuple> {
        let tables = self.tables_handle.lock().unwrap();
        let table = tables.get(table_name).unwrap().clone();
        let mut key = Vec::new();
        key.write(&Self::PRIMARY_KEY_ID_BYTES).unwrap();
        key.write(row_key).unwrap();
        let rd_opts = ReadOptions::default();
        let mut tuple = Tuple::new(&columns_set, &key, arena);
        for col in &columns_set.columns {
            key.write("#".as_bytes()).unwrap();
            key.write(col.name.as_bytes()).unwrap();

            let value = self.storage.get_pinnable(&rd_opts, &table.column_family, &key)?;
            tuple.set(col.order, Self::decode_column_value(&col.ty, value.value()));
            key.truncate(row_key.len() + 4);
        }
        Ok(tuple)
    }
}

pub struct TableHandle {
    pub column_family: Arc<dyn ColumnFamily>,
    pub anonymous_row_key_counter: Arc<Mutex<u64>>,
    pub auto_increment_counter: Arc<Mutex<u64>>,
    columns_by_name: HashMap<String, usize>,
    columns_by_id: HashMap<u32, usize>,
    secondary_indices_by_name: HashMap<String, usize>,
    pub mutex: Arc<Mutex<u64>>,
    pub metadata: Arc<TableMetadata>,
}

impl TableHandle {
    fn new(column_family: Arc<dyn ColumnFamily>,
           anonymous_row_key_counter: u64,
           auto_increment_counter: u64,
           metadata: TableMetadata) -> Self {
        let mut columns_by_name = HashMap::new();
        let mut columns_by_id = HashMap::new();
        for i in 0..metadata.columns.len() {
            let col = &metadata.columns[i];
            columns_by_name.insert(col.name.clone(), i);
            columns_by_id.insert(col.id, i);
        }
        let mut secondary_indices_by_name = HashMap::new();
        for i in 0..metadata.secondary_indices.len() {
            let idx = &metadata.secondary_indices[i];
            secondary_indices_by_name.insert(idx.name.clone(), i);
        }
        Self {
            column_family,
            anonymous_row_key_counter: Arc::new(Mutex::new(anonymous_row_key_counter)),
            auto_increment_counter: Arc::new(Mutex::new(auto_increment_counter)),
            columns_by_name,
            columns_by_id,
            secondary_indices_by_name,
            mutex: Arc::new(Mutex::new(0)),
            metadata: Arc::new(metadata),
        }
    }

    pub fn get_col_by_name(&self, name: &String) -> Option<&ColumnMetadata> {
        match self.columns_by_name.get(name) {
            Some(index) => {
                Some(&self.metadata.columns[*index])
            }
            None => None
        }
    }

    pub fn get_col_by_id(&self, id: u32) -> Option<&ColumnMetadata> {
        match self.columns_by_id.get(&id) {
            Some(index) => {
                Some(&self.metadata.columns[*index])
            }
            None => None
        }
    }

    pub fn get_2rd_idx_by_name(&self, name: &String) -> Option<&SecondaryIndexMetadata> {
        match self.secondary_indices_by_name.get(name) {
            Some(index) => {
                Some(&self.metadata.secondary_indices[*index])
            }
            None => None
        }
    }

    pub fn has_auto_increment_fields(&self) -> bool {
        for col in &self.metadata.columns {
            if col.auto_increment {
                return true;
            }
        }
        return false;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub id: u64,
    pub created_at: String,
    pub updated_at: String,
    pub raw_ast: String,
    pub rows: usize,
    pub primary_keys: Vec<u32>,
    pub auto_increment_keys: Vec<u32>,
    pub columns: Vec<ColumnMetadata>,
    pub secondary_indices: Vec<SecondaryIndexMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    pub order: usize,
    pub id: u32,
    pub ty: ColumnType,
    pub primary_key: bool,
    pub auto_increment: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderBy {
    Desc,
    Asc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecondaryIndexMetadata {
    pub name: String,
    pub id: u64,
    pub key_parts: Vec<u32>,
    pub unique: bool,
    pub order_by: OrderBy,
    // TODO: type etc...
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
        assert_eq!("__table__1", cf.name());
        assert_eq!("a", tb.name);
        Ok(())
    }

    #[test]
    fn encode_varchar_key() {
        let arena = Arena::new_rc();
        let s = ArenaStr::from_arena("", &arena);
        let mut buf = Vec::new();
        DB::encode_varchar_ty_for_key(&s, &mut buf);

        assert_eq!(0, buf.len());

        buf.clear();
        let s = ArenaStr::from_arena("a", &arena);
        DB::encode_varchar_ty_for_key(&s, &mut buf);

        assert_eq!(9, buf.len());
        let raw: [u8;9] = [
            97,
            32,
            32,
            32,
            32,
            32,
            32,
            32,
            2,
        ];
        assert_eq!(&raw, buf.as_slice());

        buf.clear();
        let s = ArenaStr::from_arena("中文", &arena);
        DB::encode_varchar_ty_for_key(&s, &mut buf);

        assert_eq!(9, buf.len());
        let raw: [u8;9] = [
            228,
            184,
            173,
            230,
            150,
            135,
            32,
            32,
            2,
        ];
        assert_eq!(raw, buf.as_slice());

        buf.clear();
        let s = ArenaStr::from_arena("123456789", &arena);
        DB::encode_varchar_ty_for_key(&s, &mut buf);

        assert_eq!(18, buf.len());
        let raw: [u8;18] = [
            49,
            50,
            51,
            52,
            53,
            54,
            55,
            56,
            3,
            57,
            32,
            32,
            32,
            32,
            32,
            32,
            32,
            2,
        ];
        assert_eq!(raw, buf.as_slice());
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
        assert_eq!("__table__1", cf1.name());
        assert_eq!("t1", t1.name);
        assert_eq!(2, t1.columns.len());
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

    #[test]
    fn insert_into_table() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db103");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db103".to_string())?;
        let sql = " create table t1 {\n\
                a int(11) not null,\n\
                b int(11)\n\
            };\n\
            insert into table t1(a,b) values(?,?), (3,4), (5,6);\n\
            ";
        let conn = db.connect();
        let mut stmts = conn.prepare_str(sql, &arena)?;
        conn.execute_prepared_statement(&mut stmts[0], &arena)?;

        let mut stmt = stmts[1].clone();
        assert_eq!(2, stmt.parameters_len());
        stmt.bind_i64(0, 1);
        stmt.bind_i64(1, 2);
        conn.execute_prepared_statement(&mut stmt, &arena)?;

        let mut column_set = ArenaBox::new(ColumnSet::new("t1", &arena), &arena);
        column_set.append("a", "", ColumnType::Int(11));
        column_set.append("b", "", ColumnType::Int(11));

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &1i64.to_be_bytes(), &arena)?;
        assert_eq!(Some(1), tuple.get_i64(0));
        assert_eq!(Some(2), tuple.get_i64(1));
        drop(tuple);

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &2i64.to_be_bytes(), &arena)?;
        assert_eq!(Some(3), tuple.get_i64(0));
        assert_eq!(Some(4), tuple.get_i64(1));
        drop(tuple);
        Ok(())
    }

    #[test]
    fn insert_with_auto_increment() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db104");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db104".to_string())?;
        let sql = " create table t1 {\n\
                a int(11) primary key auto_increment,\n\
                b int(11),\n\
                c int(11)\n\
            };\n\
            insert into table t1(b,c) values(1,2), (3,4), (5,6);\n\
            ";
        let conn = db.connect();
        conn.execute_str(sql, &arena)?;

        let mut column_set = ArenaBox::new(ColumnSet::new("t1", &arena), &arena);
        column_set.append("a", "", ColumnType::Int(11));
        column_set.append("b", "", ColumnType::Int(11));
        column_set.append("c", "", ColumnType::Int(11));

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &1i64.to_be_bytes(), &arena)?;
        assert_eq!(Some(1), tuple.get_i64(0));
        assert_eq!(Some(1), tuple.get_i64(1));
        assert_eq!(Some(2), tuple.get_i64(2));

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &2i64.to_be_bytes(), &arena)?;
        assert_eq!(Some(2), tuple.get_i64(0));
        assert_eq!(Some(3), tuple.get_i64(1));
        assert_eq!(Some(4), tuple.get_i64(2));
        Ok(())
    }

    #[test]
    fn create_table_with_index() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db105");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db105".to_string())?;
        let sql = " create table t1 {\n\
                a int(11) primary key auto_increment,\n\
                b int(11),\n\
                c int(11)\n\
                key idx_bc (b,c)\n\
            };\n\
            ";
        let conn = db.connect();
        conn.execute_str(sql, &arena)?;

        let (_, table) = db.get_table_handle(&"t1".to_string()).unwrap();
        assert_eq!(1, table.secondary_indices.len());
        let index = &table.secondary_indices[0];
        assert_eq!(2, index.id);
        assert_eq!("idx_bc", index.name);
        Ok(())
    }

    #[test]
    fn insert_with_secondary_index() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db106");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db106".to_string())?;
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b int,\n\
                c int,\n\
                d int\n\
                key idx_bc (b,c)\n\
                index idx_d (d)\n\
            };\n\
            insert into table t1(a,b,c,d) values (1,2,3,4), (5,6,7,8);\n\
            ";
        let conn = db.connect();
        conn.execute_str(sql, &arena)?;


        Ok(())
    }

    #[test]
    fn insert_with_char_varchar_index() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db107");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db107".to_string())?;
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9),\n\
                c varchar(255)\n\
                key idx_b (b)\n\
                index idx_c (c)\n\
            };\n\
            insert into table t1(a,b,c) values (1,\"\",\"\"), (5,NULL,\"111111112222\");\n\
            ";
        let conn = db.connect();
        conn.execute_str(sql, &arena)?;

        Ok(())
    }
}