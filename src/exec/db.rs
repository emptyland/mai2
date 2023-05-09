use std::arch::x86_64::_mm_cmpestro;
use std::cell::RefCell;
use std::collections::HashMap;
use std::default::Default;
use std::io::Write;
use std::iter;
use std::mem::size_of;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::sync::atomic::{AtomicU64, Ordering};
use rusty_pool::ThreadPool;

use crate::exec::connection::Connection;
use crate::{Corrupting, log_debug, Status, storage};
use crate::base::{Allocator, Arena, ArenaBox, ArenaStr, Logger};
use crate::exec::evaluator::Value;
use crate::exec::executor::{ColumnSet, SecondaryIndexBundle, Tuple};
use crate::exec::locking::{LockingInstance, LockingManagement};
use crate::storage::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DEFAULT_COLUMN_FAMILY_NAME, Env, from_io_result, Options, ReadOptions, WriteBatch, WriteOptions};
use crate::Result;
use crate::storage::config::MB;

pub struct DB {
    this: Weak<DB>,
    env: Arc<dyn Env>,
    abs_db_path: PathBuf,
    logger: Arc<dyn Logger>,
    storage: Arc<dyn storage::DB>,
    rd_opts: ReadOptions,
    wr_opts: WriteOptions,
    default_column_family: Arc<dyn storage::ColumnFamily>,
    //tables: Mutex<HashMap<String, Box>>
    tables_handle: RwLock<HashMap<String, TableRef>>,

    next_table_id: AtomicU64,
    next_index_id: AtomicU64,
    next_conn_id: AtomicU64,
    connections: Mutex<Vec<Arc<Connection>>>,
    worker_pool: ThreadPool,
    locks: LockingManagement,
    // TODO:
}

type TableRef = Arc<TableHandle>;
type LockingTables<'a> = RwLockReadGuard<'a, HashMap<String, TableRef>>;
type LockingTablesMut<'a> = RwLockWriteGuard<'a, HashMap<String, TableRef>>;


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
    const CHAR_FILLING_BYTES: [u8; 1] = [Self::CHAR_FILLING_BYTE; 1];

    const VARCHAR_SEGMENT_LEN: usize = 9;

    const COL_ID_LEN: usize = size_of::<u32>();

    pub const ANONYMOUS_ROW_KEY_KEY: &'static [u8] = &[
        0xff, 0xff, 0xff, 0xff, // index id tag
        0x80, // split
        0x01, 0x00, 0x00, 0x00 // key id = 1
    ];
    pub const AUTO_INCREMENT_KEY: &'static [u8] = &[
        0xff, 0xff, 0xff, 0xff, // index id tag
        0x80, // split
        0x02, 0x00, 0x00, 0x00 // key id = 2
    ];

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
                rd_opts: ReadOptions::default(),
                wr_opts: WriteOptions::default(),
                default_column_family,
                next_conn_id: AtomicU64::new(0),
                next_table_id: AtomicU64::new(0),
                next_index_id: AtomicU64::new(1), // 0 = primary key
                tables_handle: RwLock::default(),
                connections: Mutex::default(),
                worker_pool: rusty_pool::Builder::new()
                    .core_size(num_cpus::get())
                    .max_size(num_cpus::get() * 2 + 2)
                    .build(),
                locks: LockingManagement::new(),
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
        let table_name = table.name.clone();
        let anonymous_row_key_counter = self.load_number(&cf, Self::ANONYMOUS_ROW_KEY_KEY, 0)?;
        let auto_increment_counter = self.load_number(&cf, Self::AUTO_INCREMENT_KEY, 0)?;

        let table_handle = TableHandle::new(cf,
                                            anonymous_row_key_counter,
                                            auto_increment_counter,
                                            table);

        let mut locking = self.tables_handle.write().unwrap();
        self.locks.install(table_handle.metadata.id);
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

    pub fn create_table(&self, table_metadata: TableMetadata, tables: &mut LockingTablesMut) -> Result<u64> {
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
                self.locks.install(table_handle.metadata.id);
                tables.insert(table_name, Arc::new(table_handle));
                Ok(id)
            }
        }
    }

    pub fn drop_table(&self, name: &String, tables: &mut LockingTablesMut) -> Result<u64> {
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
        self.locks.uninstall(table_id);
        Ok(table_id)
    }

    pub fn write_table_metadata(&self, table_metadata: &TableMetadata) -> Result<()> {
        match serde_yaml::to_string(table_metadata) {
            Err(e) => {
                let message = format!("Yaml serialize fail: {}", e.to_string());
                Err(Status::corrupted(message))
            }
            Ok(yaml) => {
                let col_name = format!("{}{}", Self::META_COL_TABLE_PREFIX, table_metadata.id);
                let mut wr_opts = WriteOptions::default();
                wr_opts.sync = true;
                self.storage.insert(&wr_opts, &self.default_column_family,
                                    col_name.as_bytes(), yaml.as_bytes())
            }
        }
    }

    pub fn build_secondary_index(&self, table: &TableHandle, secondary_index_id: u64) -> Result<u64> {
        let secondary_index = table.get_2rd_idx_by_id(secondary_index_id).unwrap();
        let rd_opts = ReadOptions::default();
        let iter_box = self.storage.new_iterator(&rd_opts, &table.column_family)?;
        let mut iter = iter_box.borrow_mut();
        let col_id_to_idx = {
            let mut tmp = HashMap::new();
            for i in 0..secondary_index.key_parts.len() {
                let col_id = secondary_index.key_parts[i];
                tmp.insert(secondary_index.key_parts[i],
                           (i, table.get_col_by_id(col_id).unwrap()));
            }
            tmp
        };

        let mut arena = Arena::new_rc();

        let mut affected_rows = 0;
        let mut col_vals = Vec::from_iter(iter::repeat(Value::Null)
            .take(secondary_index.key_parts.len()));
        let mut row_key = Vec::default();

        let mut key = Vec::<u8>::default();
        key.write(&(secondary_index_id as u32).to_be_bytes()).unwrap();

        let lock_inst = self.locks.instance(table.metadata.id).unwrap();

        iter.seek(&Self::PRIMARY_KEY_ID_BYTES);
        while iter.valid() {
            if iter.key().len() <= Self::PRIMARY_KEY_ID_BYTES.len() + Self::COL_ID_LEN {
                return Err(Status::corrupted("Incorrect primary key data, too small."));
            }
            let key_id = u32::from_be_bytes((&iter.key()[..4]).try_into().unwrap());
            if key_id != Self::PRIMARY_KEY_ID {
                break; // primary key only
            }

            let col_id_bytes = &iter.key()[iter.key().len() - Self::COL_ID_LEN..];
            let col_id = u32::from_be_bytes(col_id_bytes.try_into().unwrap());
            debug_assert!(table.get_col_by_id(col_id).is_some(), "Column id not exists!");
            //dbg!(&table.get_col_by_id(col_id).unwrap().name);

            if let Some((idx, col)) = col_id_to_idx.get(&col_id) {
                col_vals[*idx] = Self::decode_column_value(&col.ty, iter.value(),
                                                           arena.borrow_mut().deref_mut());
            }

            let rk = &iter.key()[..iter.key().len() - Self::COL_ID_LEN];
            if row_key.is_empty() {
                row_key.extend_from_slice(rk);
                debug_assert_eq!(row_key, rk);
            }

            if row_key != rk {
                key.truncate(4); // still keep index id (4 bytes u32)
                self.build_secondary_index_key(&table.column_family, &col_vals, secondary_index,
                                               &col_id_to_idx, &lock_inst, &row_key, &mut key)?;

                affected_rows += 1;
                col_vals.fill(Value::Null);
                if row_key.len() < rk.len() {
                    row_key.extend_from_slice(rk);
                } else {
                    row_key.copy_from_slice(rk);
                }
                debug_assert_eq!(row_key, rk);

                if arena.borrow().rss_in_bytes > 10 * MB {
                    arena = Arena::new_rc();
                }
            }
            iter.move_next();
        }
        if !row_key.is_empty() {
            key.truncate(4); // still keep index id (4 bytes u32)
            self.build_secondary_index_key(&table.column_family, &col_vals, secondary_index,
                                           &col_id_to_idx, &lock_inst, &row_key, &mut key)?;
            affected_rows += 1;
        }

        if iter.status().is_corruption() {
            Err(iter.status())
        } else {
            Ok(affected_rows)
        }
    }

    fn build_secondary_index_key(&self, column_family: &Arc<dyn ColumnFamily>,
                                 col_vals: &[Value],
                                 secondary_index: &SecondaryIndexMetadata,
                                 col_id_to_idx: &HashMap<u32, (usize, &ColumnMetadata)>,
                                 lock_inst: &Arc<LockingInstance>,
                                 row_key: &[u8],
                                 key: &mut Vec<u8>)
                                 -> Result<()> {
        for i in 0..col_vals.len() {
            let col_id = secondary_index.key_parts[i];
            let (_, col) = col_id_to_idx.get(&col_id).unwrap();
            Self::encode_secondary_index(&col_vals[i], &col.ty, key);
        }

        let _may_locking = if secondary_index.unique {
            Some(lock_inst.exclusive_lock(key))
        } else {
            None
        };
        if secondary_index.unique {
            match self.storage.get_pinnable(&self.rd_opts, &column_family, &key) {
                Ok(_) => {
                    let message = format!("Duplicated secondary key, index {} is unique.",
                                          secondary_index.name);
                    return Err(Status::corrupted(message));
                }
                Err(e) => {
                    if e != Status::NotFound {
                        return Err(e);
                    }
                }
            }
        }

        self.storage.insert(&self.wr_opts, &column_family, key, row_key)?;
        Ok(())
    }

    pub fn remove_secondary_index(&self, table_id: u64,
                                  column_family: &Arc<dyn ColumnFamily>,
                                  secondary_index_id: u64,
                                  unique: bool,
                                  sync: bool) -> Result<u64> {
        let lock_inst = self.locks.instance(table_id).unwrap();
        if sync {
            Self::remove_secondary_index_impl(lock_inst,
                                              self.storage.clone(),
                                              column_family.clone(),
                                              secondary_index_id, unique)
        } else {
            let db = self.storage.clone();
            let cf = column_family.clone();
            self.worker_pool.execute(move || {
                let rs = Self::remove_secondary_index_impl(lock_inst, db, cf,
                                                                       secondary_index_id, unique);
                match rs {
                    Ok(affected_rows) => dbg!(affected_rows),
                    _ => 0
                };
            });
            Ok(0)
        }
    }

    fn remove_secondary_index_impl(lock_inst: Arc<LockingInstance>,
                                   storage: Arc<dyn storage::DB>,
                                   column_family: Arc<dyn ColumnFamily>,
                                   secondary_index_id: u64,
                                   unique: bool) -> Result<u64> {
        let wr_opts = WriteOptions::default();
        let rd_opts = ReadOptions::default();
        let iter_box = storage.new_iterator(&rd_opts, &column_family)?;
        let mut iter = iter_box.borrow_mut();

        let key_prefix = (secondary_index_id as u32).to_be_bytes();
        iter.seek(&key_prefix);

        let mut affected_rows = 0u64;
        while iter.valid() {
            let idx_id = Self::decode_idx_id(iter.key());
            if idx_id != secondary_index_id {
                break;
            }

            let _may_locking = if unique {
                Some(lock_inst.exclusive_lock(iter.key()))
            } else {
                None
            };

            storage.delete(&wr_opts, &column_family, iter.key())?;
            affected_rows += 1;
            iter.move_next();
        }
        if iter.status().is_corruption() {
            Err(iter.status())
        } else {
            Ok(affected_rows)
        }
    }

    pub fn insert_rows(&self, column_family: &Arc<dyn ColumnFamily>,
                       table: &Arc<TableMetadata>,
                       tuples: &[Tuple],
                       secondary_indices: &[SecondaryIndexBundle],
                       anonymous_row_key_value: Option<u64>,
                       auto_increment_value: Option<u64>,
                       check_row_key_unique: bool)
                       -> Result<u64> {
        let mut batch = WriteBatch::new();
        let wr_opts = WriteOptions::default();
        debug_assert_eq!(column_family.name(),
                         format!("{}{}", Self::DATA_COL_TABLE_PREFIX, &table.id));

        let inst = self.locks.instance(table.id).unwrap();
        let mut lock_group = inst.group();
        if check_row_key_unique {
            for tuple in tuples {
                lock_group.add(tuple.row_key());
            }
        }
        for index in secondary_indices {
            for i in 0..table.secondary_indices.len() {
                if table.secondary_indices[i].unique {
                    lock_group.add(index.index_keys[i].as_slice());
                }
            }
        }
        let _locks = lock_group.exclusive_lock_all();

        let rd_opts = ReadOptions::default();
        let iter = if check_row_key_unique {
            Some(self.storage.new_iterator(&rd_opts, column_family)?)
        } else {
            None
        };

        let mut key = Vec::new();
        for tuple in tuples {
            if check_row_key_unique {
                let keep_it = iter.as_ref().cloned().unwrap();
                let mut it = keep_it.borrow_mut();
                it.seek(tuple.row_key());
                if it.valid() && it.key().starts_with(tuple.row_key()) {
                    Err(Status::corrupted("Duplicated primary key, must be unique."))?;
                }
            }

            // key = [row_key(n bytes)|col_id(4 bytes)]
            let prefix_key_len = tuple.row_key().len();
            key.write(tuple.row_key()).unwrap();
            for col in &tuple.columns().columns {
                let value = tuple.get(col.order);
                debug_assert!(!value.is_undefined());
                if value.is_null() {
                    //dbg!(value);
                    continue;
                }
                key.write(&col.id.to_be_bytes()).unwrap();
                let row_key_len = key.len();

                Self::encode_column_value(tuple.get(col.order), &col.ty, &mut key);
                batch.insert(column_family, &key[..row_key_len], &key[row_key_len..]);

                key.truncate(prefix_key_len); // keep row key prefix.
            }
            key.clear();
        }

        for index in secondary_indices {
            for i in 0..table.secondary_indices.len() {
                let key = index.index_keys[i].as_slice();
                if table.secondary_indices[i].unique {
                    if self.storage.get_pinnable(&rd_opts, column_family, key).is_ok() {
                        let message = format!("Duplicated secondary key, index {} is unique.",
                                              table.secondary_indices[i].name);
                        Err(Status::corrupted(message))?;
                    }
                }
                batch.insert(column_family, key, index.row_key());
            }
        }

        if let Some(value) = anonymous_row_key_value {
            batch.insert(column_family, Self::ANONYMOUS_ROW_KEY_KEY, &value.to_le_bytes());
        }
        if let Some(value) = auto_increment_value {
            batch.insert(column_family, Self::AUTO_INCREMENT_KEY, &value.to_le_bytes());
        }

        self.storage.write(&wr_opts, batch)?;
        //log_debug!(self.logger, "rows: {} insert ok", tuples.len());
        Ok(tuples.len() as u64)
    }

    pub fn encode_anonymous_row_key<W: Write>(counter: u64, wr: &mut W) {
        wr.write(&Self::PRIMARY_KEY_ID_BYTES).unwrap(); // primary key id always is 0
        wr.write(&counter.to_be_bytes()).unwrap();
    }

    pub fn encode_idx_id<W: Write>(id: u64, wr: &mut W) {
        wr.write(&(id as u32).to_be_bytes()).unwrap();
    }

    pub fn decode_idx_id(buf: &[u8]) -> u64 {
        u32::from_be_bytes((&buf[..4]).try_into().unwrap()) as u64
    }

    pub fn encode_row_key<W: Write>(value: &Value, ty: &ColumnType, wr: &mut W) -> Result<()> {
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                match value {
                    Value::Int(n) => { wr.write(&n.to_be_bytes()).unwrap(); }
                    _ => unreachable!()
                }
            }
            ColumnType::Float(_, _) => match value {
                Value::Float(n) => { wr.write(&n.to_be_bytes()).unwrap(); }
                _ => unreachable!()
            },
            ColumnType::Double(_, _) => match value {
                Value::Float(n) => { wr.write(&n.to_be_bytes()).unwrap(); }
                _ => unreachable!()
            },
            ColumnType::Char(n) => match value {
                Value::Str(s) => {
                    if s.len() > *n as usize {
                        return Err(Status::corrupted("Char type too long"));
                    }
                    Self::encode_char_ty_for_key(s, *n as usize, wr);
                }
                _ => unreachable!()
            },
            ColumnType::Varchar(n) => match value {
                Value::Str(s) => {
                    if s.len() > *n as usize {
                        return Err(Status::corrupted("Varchar type too long"));
                    }
                    Self::encode_varchar_ty_for_key(s, wr);
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
        assert!(!value.is_undefined());

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
            }
            ColumnType::Double(_, _) => {
                match value {
                    Value::Float(n) => wr.write(&n.to_be_bytes()).unwrap(),
                    _ => unreachable!()
                };
            }
            ColumnType::Char(n) => {
                match value {
                    Value::Str(s) => Self::encode_char_ty_for_key(s, *n as usize, wr),
                    _ => unreachable!()
                }
            }
            ColumnType::Varchar(n) => {
                match value {
                    Value::Str(s) => Self::encode_varchar_ty_for_key(s, wr),
                    _ => unreachable!()
                }
            }
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
            }
        }
    }

    pub fn decode_column_value(ty: &ColumnType, value: &[u8], arena: &mut dyn Allocator) -> Value {
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                Value::Int(i64::from_le_bytes(value.try_into().unwrap()))
            }
            ColumnType::Float(_, _) => unreachable!(),
            ColumnType::Double(_, _) => unreachable!(),
            ColumnType::Char(_)
            | ColumnType::Varchar(_) => {
                Value::Str(ArenaStr::new(std::str::from_utf8(value).unwrap(), arena))
            }
        }
    }

    fn get_table_handle(&self, name: &String) -> Option<(Arc<dyn ColumnFamily>, Arc<TableMetadata>)> {
        let tables = self.tables_handle.read().unwrap();
        match tables.get(name) {
            Some(handle) =>
                Some((handle.column_family.clone(), handle.metadata.clone())),
            None => None
        }
    }

    pub fn lock_tables(&self) -> LockingTables {
        self.tables_handle.read().unwrap()
    }

    pub fn lock_tables_mut(&self) -> LockingTablesMut {
        self.tables_handle.write().unwrap()
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
        let tables = self.tables_handle.read().unwrap();
        let table = tables.get(table_name).unwrap().clone();
        let mut key = Vec::new();
        key.write(&Self::PRIMARY_KEY_ID_BYTES).unwrap();
        key.write(row_key).unwrap();
        let rd_opts = ReadOptions::default();
        let mut tuple = Tuple::from(&columns_set, &key, arena);
        for col in &columns_set.columns {
            let col_meta = table.get_col_by_name(&col.name.to_string()).unwrap();
            key.write(&col_meta.id.to_be_bytes()).unwrap();

            let value = self.storage.get_pinnable(&rd_opts, &table.column_family, &key)?;
            tuple.set(col.order, Self::decode_column_value(&col.ty, value.value(),
                                                           arena.borrow_mut().deref_mut()));
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
    secondary_indices_by_id: HashMap<u64, usize>,
    column_in_indices_by_id: HashMap<u32, usize>,
    // [col_id -> 2rd_idx_index]
    pub mutex: Arc<Mutex<u64>>,
    pub metadata: Arc<TableMetadata>,
}

impl TableHandle {
    fn new(column_family: Arc<dyn ColumnFamily>,
           anonymous_row_key_counter: u64,
           auto_increment_counter: u64,
           metadata: TableMetadata) -> Self {
        Self {
            column_family,
            anonymous_row_key_counter: Arc::new(Mutex::new(anonymous_row_key_counter)),
            auto_increment_counter: Arc::new(Mutex::new(auto_increment_counter)),
            columns_by_name: Default::default(),
            columns_by_id: Default::default(),
            secondary_indices_by_name: Default::default(),
            secondary_indices_by_id: Default::default(),
            column_in_indices_by_id: Default::default(),
            mutex: Arc::new(Mutex::new(0)),
            metadata: Arc::new(metadata),
        }.prepare()
    }

    pub fn update(&self, metadata: TableMetadata) -> Self {
        Self {
            column_family: self.column_family.clone(),
            anonymous_row_key_counter: self.anonymous_row_key_counter.clone(),
            auto_increment_counter: self.auto_increment_counter.clone(),
            columns_by_name: Default::default(),
            columns_by_id: Default::default(),
            secondary_indices_by_name: Default::default(),
            secondary_indices_by_id: Default::default(),
            column_in_indices_by_id: Default::default(),
            mutex: self.mutex.clone(),
            metadata: Arc::new(metadata),
        }.prepare()
    }

    fn prepare(mut self) -> Self {
        for i in 0..self.metadata.columns.len() {
            let col = &self.metadata.columns[i];
            self.columns_by_name.insert(col.name.clone(), i);
            self.columns_by_id.insert(col.id, i);
        }
        for i in 0..self.metadata.secondary_indices.len() {
            let idx = &self.metadata.secondary_indices[i];
            self.secondary_indices_by_name.insert(idx.name.clone(), i);
            self.secondary_indices_by_id.insert(idx.id, i);
            for col_id in &idx.key_parts {
                self.column_in_indices_by_id.insert(*col_id, i);
            }
        }
        self
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

    pub fn get_col_be_part_of_2rd_idx_by_name(&self, name: &String) -> Option<&SecondaryIndexMetadata> {
        match self.get_col_by_name(name) {
            Some(col) => match self.column_in_indices_by_id.get(&col.id) {
                Some(index) => Some(&self.metadata.secondary_indices[*index]),
                None => None
            }
            None => None
        }
    }

    pub fn is_col_be_part_of_primary_key_by_name(&self, name: &String) -> bool {
        match self.get_col_by_name(name) {
            Some(col) => self.metadata.primary_keys.contains(&col.id),
            None => false
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

    pub fn get_2rd_idx_by_id(&self, id: u64) -> Option<&SecondaryIndexMetadata> {
        match self.secondary_indices_by_id.get(&id) {
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
        let raw: [u8; 9] = [
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
        let raw: [u8; 9] = [
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
        let raw: [u8; 18] = [
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
        conn.execute_prepared_statement(&mut stmts[0])?;

        let mut stmt = stmts[1].clone();
        assert_eq!(2, stmt.parameters_len());
        stmt.bind_i64(0, 1);
        stmt.bind_i64(1, 2);
        conn.execute_prepared_statement(&mut stmt)?;

        let mut column_set = ArenaBox::new(ColumnSet::new("t1", &arena), &arena);
        column_set.append_with_name("a", ColumnType::Int(11));
        column_set.append_with_name("b", ColumnType::Int(11));

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
        column_set.append_with_name("a", ColumnType::Int(11));
        column_set.append_with_name("b", ColumnType::Int(11));
        column_set.append_with_name("c", ColumnType::Int(11));

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

    #[test]
    fn insert_duplicated_row_key() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db108");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db108".to_string())?;
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            insert into table t1(a,b) values (1, NULL);\n\
            insert into table t1(a,b) values (1, NULL);\n\
            ";
        let conn = db.connect();
        let rs = conn.execute_str(sql, &arena);
        assert!(rs.is_err());
        assert_eq!(Status::Corruption("Duplicated primary key, must be unique.".to_string()),
                   rs.unwrap_err());
        Ok(())
    }

    #[test]
    fn insert_duplicated_row_key_at_same_statement() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db109");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db109".to_string())?;
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            insert into table t1(a,b) values (1, NULL), (1, NULL);\n\
            ";
        let conn = db.connect();
        let rs = conn.execute_str(sql, &arena);
        assert!(rs.is_err());
        assert_eq!(Status::Corruption("Duplicated primary key, must be unique.".to_string()),
                   rs.unwrap_err());
        Ok(())
    }

    #[test]
    fn large_insert_into_table() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db110");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db110".to_string())?;
        let n = 300000;
        let conn = db.connect();
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            ";
        conn.execute_str(sql, &arena)?;

        let sql = "insert into table t1(a,b) values (?, ?)";
        let mut stmt = conn.prepare_str(sql, &arena)?.first().cloned().unwrap();

        let jiffies = db.env.current_time_mills();
        for i in 0..n {
            stmt.bind_i64(0, i as i64);
            stmt.bind_null(1);
            conn.execute_prepared_statement(&mut stmt)?;
        }
        let cost = (db.env.current_time_mills() - jiffies) as f32 / 1000f32;
        println!("qps: {}", n as f32 / cost);

        Ok(())
    }

    #[test]
    fn create_index_before_inserting() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db111");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db111".to_string())?;
        let n = 1000;
        let conn = db.connect();
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            ";
        conn.execute_str(sql, &arena)?;

        let sql = "insert into table t1(a,b) values (?, ?)";
        let mut stmt = conn.prepare_str(sql, &arena)?.first().cloned().unwrap();
        for i in 0..n {
            stmt.bind_i64(0, i);
            stmt.bind_string(1, format!("{:03}", i), arena.borrow_mut().deref_mut());
            conn.execute_prepared_statement(&mut stmt)?;
        }

        let sql = "create index idx_b on t1(b)";
        let affected_rows = conn.execute_str(sql, &arena)?;
        assert_eq!(1000, affected_rows);

        let (cf, table) = db.get_table_handle(&"t1".to_string()).unwrap();
        assert_eq!(1, table.secondary_indices.len());
        assert_eq!(2, table.secondary_indices[0].id);

        let iter_box = db.storage.new_iterator(&db.rd_opts, &cf)?;
        let mut iter = iter_box.borrow_mut();
        iter.seek(&(table.secondary_indices[0].id as u32).to_be_bytes());
        assert!(iter.valid());
        assert_eq!(Status::Ok, iter.status());
        let mut i = 0u32;
        while iter.valid() {
            //dbg!(iter.key());
            let index_id = u32::from_be_bytes((&iter.key()[..4]).try_into().unwrap());
            if index_id != 2 {
                break;
            }

            let null_byte = iter.key()[4];
            assert_eq!(DB::NOT_NULL_BYTE, null_byte);

            let key = std::str::from_utf8(&iter.key()[5..]).unwrap();
            assert_eq!(9, key.len());
            assert_eq!(i, u32::from_str_radix(key.trim(), 10).unwrap());

            iter.move_next();
            i += 1;
        }
        Ok(())
    }

    #[test]
    fn drop_index_before_inserting() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db112");
        let arena = Arena::new_rc();
        let db = DB::open("tests".to_string(), "db112".to_string())?;
        //let n = 10000;
        let conn = db.connect();
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
                index idx_b(b)\n\
            };\n\
            insert into table t1(b) values (\"aaa\"),(\"bbb\"),(\"ccc\");\n\
            ";
        assert_eq!(3, conn.execute_str(sql, &arena)?);

        let sql = "drop index idx_b on t1";
        assert_eq!(3, conn.execute_str(sql, &arena)?);
        Ok(())
    }
}