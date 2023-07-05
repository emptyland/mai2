use std::cell::RefMut;
use std::collections::{HashMap, HashSet};
use std::default::Default;
use std::io::Write;
use std::iter;
use std::mem::{replace, size_of};
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::sync::atomic::{AtomicU64, Ordering};

use rusty_pool::ThreadPool;

use crate::{arena_vec, ArenaVec, corrupted_err, Corrupting, log_debug, Status, storage, zone_limit_guard};
use crate::base::{Allocator, Arena, ArenaBox, ArenaMut, ArenaStr, Logger};
use crate::exec::connection::{Connection, FeedbackImpl};
use crate::exec::evaluator::{Evaluator, Value};
use crate::exec::executor::{ColumnsAuxResolver, ColumnSet, PreparedStatement, SecondaryIndexBundle, Tuple, UpstreamContext};
use crate::exec::locking::{LockingInstance, LockingManagement};
use crate::exec::physical_plan::PhysicalPlanOps;
use crate::map::ArenaMap;
use crate::Result;
use crate::sql::ast::{Assignment, Expression};
use crate::storage::{ColumnFamily, ColumnFamilyDescriptor, ColumnFamilyOptions, DEFAULT_COLUMN_FAMILY_NAME, Env,
                     from_io_result, Options, ReadOptions, WriteBatch, WriteOptions};
use crate::storage::config::MB;

pub struct DB {
    this: Weak<DB>,
    env: Arc<dyn Env>,
    abs_db_path: PathBuf,
    logger: Arc<dyn Logger>,
    pub storage: Arc<dyn storage::DB>,
    pub rd_opts: ReadOptions,
    wr_opts: WriteOptions,
    snapshot: RwLock<Arc<dyn storage::Snapshot>>,
    default_column_family: Arc<dyn ColumnFamily>,
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

pub type TableRef = Arc<TableHandle>;
pub type LockingTables<'a> = RwLockReadGuard<'a, HashMap<String, TableRef>>;
pub type LockingTablesMut<'a> = RwLockWriteGuard<'a, HashMap<String, TableRef>>;


impl DB {
    const META_COL_TABLE_NAMES: &'static [u8] = "__metadata_table_names__".as_bytes();
    const META_COL_TABLE_PREFIX: &'static str = "__metadata_table__.";
    const META_COL_NEXT_TABLE_ID: &'static [u8] = "__metadata_next_table_id__".as_bytes();
    const META_COL_NEXT_INDEX_ID: &'static [u8] = "__metadata_next_index_id__".as_bytes();
    const METADATA_FILE_NAME: &'static str = "__METADATA__";

    const DATA_COL_TABLE_PREFIX: &'static str = "__table__";

    pub const PRIMARY_KEY_ID: u32 = 0;
    pub const PRIMARY_KEY_ID_BYTES: [u8; 4] = Self::PRIMARY_KEY_ID.to_be_bytes();

    const NULL_BYTE: u8 = 0xff;
    const NULL_BYTES: [u8; 1] = [Self::NULL_BYTE; 1];

    const NOT_NULL_BYTE: u8 = 0;
    const NOT_NULL_BYTES: [u8; 1] = [Self::NOT_NULL_BYTE; 1];

    const CHAR_FILLING_BYTE: u8 = ' ' as u8;
    const CHAR_FILLING_BYTES: [u8; 1] = [Self::CHAR_FILLING_BYTE; 1];

    const VARCHAR_SEGMENT_LEN: usize = 9;

    pub const COL_ID_LEN: usize = size_of::<u32>();
    pub const KEY_ID_LEN: usize = size_of::<u32>();

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

        let snapshot = storage.get_snapshot();
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
                snapshot: RwLock::new(snapshot),
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
        let iter_box = self.storage.new_iterator(&self.rd_opts, &table.column_family)?;
        let mut iter = iter_box.borrow_mut();
        let col_id_to_idx = {
            let mut tmp = HashMap::new();
            for i in 0..secondary_index.key_parts.len() {
                let col_id = secondary_index.key_parts[i];
                tmp.insert(secondary_index.key_parts[i], (i, table.get_col_by_id(col_id).unwrap()));
            }
            tmp
        };

        let mut arena = Arena::new_val();

        let mut affected_rows = 0;
        let mut col_vals = Vec::from_iter(iter::repeat(Value::Null)
            .take(secondary_index.key_parts.len()));
        let mut row_key = Vec::default();

        let mut key = Vec::<u8>::default();
        key.write(&(secondary_index_id as u32).to_be_bytes()).unwrap();

        let lock_inst = self.locks.instance(table.metadata.id).unwrap();

        let secondary_index_iter = if secondary_index.unique {
            Some(self.storage.new_iterator(&self.rd_opts, &table.column_family)?)
        } else {
            None
        };

        iter.seek(&Self::PRIMARY_KEY_ID_BYTES);
        while iter.valid() {
            if iter.key().len() <= Self::PRIMARY_KEY_ID_BYTES.len() + Self::COL_ID_LEN {
                corrupted_err!("Incorrect primary key data, too small.")?
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
                                                           arena.get_mut().deref_mut());
            }

            let rk = &iter.key()[..iter.key().len() - Self::COL_ID_LEN];
            if row_key.is_empty() {
                row_key.extend_from_slice(rk);
                debug_assert_eq!(row_key, rk);
            }

            if row_key != rk {
                key.truncate(DB::KEY_ID_LEN); // still keep index id (4 bytes u32)
                self.build_secondary_index_key(&table.column_family,
                                               secondary_index_iter.as_ref().map(|x| { x.borrow_mut() }),
                                               &col_vals,
                                               secondary_index,
                                               &col_id_to_idx,
                                               &lock_inst,
                                               &row_key,
                                               &mut key)?;

                affected_rows += 1;
                col_vals.fill(Value::Null);
                if row_key.len() < rk.len() {
                    row_key.extend_from_slice(rk);
                } else {
                    row_key.copy_from_slice(rk);
                }
                debug_assert_eq!(row_key, rk);

                if arena.rss_in_bytes > 10 * MB {
                    arena = Arena::new_val();
                }
            }
            iter.move_next();
        }
        if !row_key.is_empty() {
            key.truncate(DB::KEY_ID_LEN); // still keep index id (4 bytes u32)
            self.build_secondary_index_key(&table.column_family,
                                           secondary_index_iter.as_ref().map(|x| { x.borrow_mut() }),
                                           &col_vals,
                                           secondary_index,
                                           &col_id_to_idx,
                                           &lock_inst,
                                           &row_key,
                                           &mut key)?;
            affected_rows += 1;
        }

        if iter.status().is_corruption() {
            Err(iter.status())
        } else {
            Ok(affected_rows)
        }
    }

    fn build_secondary_index_key(&self, column_family: &Arc<dyn ColumnFamily>,
                                 iter: Option<RefMut<dyn storage::Iterator>>,
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
            let mut it = iter.unwrap();
            it.seek(key);
            if !it.valid() && !it.status().is_not_found() {
                Err(it.status().clone())?;
            }
            // Secondary index key is exists!
            if it.valid() && it.key().starts_with(key) {
                corrupted_err!("Duplicated secondary key, index {} is unique.", secondary_index.name)?;
            }
        }

        key.write(row_key).unwrap();
        let pack_info = (row_key.len() as u32).to_le_bytes();
        self.storage.insert(&self.wr_opts, &column_family, key, &pack_info)?;
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
        debug_assert_eq!(column_family.name(), format!("{}{}", Self::DATA_COL_TABLE_PREFIX, &table.id));

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
                    lock_group.add(index.index(i));
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
                    corrupted_err!("Duplicated primary key, must be unique.")?;
                }
            }

            // key = [row_key(n bytes)|col_id(4 bytes)]
            let prefix_key_len = tuple.row_key().len();
            key.write(tuple.row_key()).unwrap();
            for col in &tuple.columns().columns {
                let value = tuple.get(col.order);
                debug_assert!(!value.is_undefined());
                if value.is_null() {
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

        let iter_box = self.storage.new_iterator(&self.rd_opts, column_family)?;
        for index in secondary_indices {
            for i in 0..table.secondary_indices.len() {
                if table.secondary_indices[i].unique {
                    let mut iter = iter_box.borrow_mut();

                    let secondary_index_key = index.index(i);
                    iter.seek(secondary_index_key);
                    if !iter.valid() && !iter.status().is_not_found() {
                        Err(iter.status().clone())?;
                    }
                    if iter.valid() && iter.key().starts_with(secondary_index_key) {
                        corrupted_err!("Duplicated secondary key, index {} is unique.", table.secondary_indices[i].name)?;
                    }
                }
                let pack_info = (index.row_key().len() as u32).to_le_bytes();
                batch.insert(column_family, &index.index_keys[i], &pack_info);
            }
        }

        if let Some(value) = anonymous_row_key_value {
            batch.insert(column_family, Self::ANONYMOUS_ROW_KEY_KEY, &value.to_le_bytes());
        }
        if let Some(value) = auto_increment_value {
            batch.insert(column_family, Self::AUTO_INCREMENT_KEY, &value.to_le_bytes());
        }

        self.storage.write(&wr_opts, batch)?;
        self.update_snapshot();
        Ok(tuples.len() as u64)
    }

    pub fn delete_rows(&self, tables: &[TableRef], mut row_producer: ArenaBox<dyn PhysicalPlanOps>, region: &ArenaMut<Arena>) -> Result<u64> {
        debug_assert!(tables.len() >= 1);
        let mut cols = ColumnsAuxResolver::new(region);

        self.update_snapshot();
        row_producer.prepare()?;
        let mut zone = Arena::new_ref();
        let mut feedback = FeedbackImpl::new(true);
        let mut affected_rows = 0;
        loop {
            zone_limit_guard!(zone, 1);

            let rs = row_producer.next(&mut feedback, &zone);
            if feedback.status.is_not_ok() {
                break Err(feedback.status);
            }
            if rs.is_none() {
                self.update_snapshot();
                break Ok(affected_rows);
            }
            let tuple = rs.unwrap();
            cols.attach(&tuple);

            let row_key = tuple.row_key();
            let arena = zone.get_mut();
            for table in tables {
                self.delete_row_impl(table, &cols, &tuple, &arena)?;
                affected_rows += 1;
            }
        }
    }

    fn delete_row_impl(&self, table: &TableRef, cols: &ColumnsAuxResolver, tuple: &Tuple, arena: &ArenaMut<Arena>) -> Result<()> {
        let inst = self.locks.instance(table.metadata.id).unwrap();
        let mut lock_group = inst.group();

        let mut updates = WriteBatch::new();
        let mut key = ArenaVec::<u8>::new(arena);

        debug_assert_ne!(0, table.metadata.id);
        let row_keys = Self::extract_multi_row_keys_from_tuple(tuple, table.metadata.id);

        for row_key in &row_keys {
            lock_group.add(row_key);

            key.clear();
            key.write(row_key).unwrap();

            let original_len = key.len();
            for col in &table.metadata.columns {
                Self::encode_col_id(col.id, &mut key);
                updates.delete(&table.column_family, &key);
                key.truncate(original_len);
            }
        }

        for index in &table.metadata.secondary_indices {
            key.clear();
            Self::encode_idx_id(index.id, &mut key);
            for col_id in &index.key_parts {
                let col = table.get_col_by_id(*col_id).unwrap();
                let pos = cols.get_column_by_id(table.id(), col.id).unwrap();
                Self::encode_secondary_index(&tuple[pos], &col.ty, &mut key);
            }
            if index.unique {
                lock_group.add(&key);
            }

            let original_len = key.len();
            for row_key in &row_keys {
                key.write(row_key).unwrap();
                updates.delete(&table.column_family, &key);
                key.truncate(original_len);
            }
        }

        let _locking = lock_group.exclusive_lock_all();
        self.storage.write(&self.wr_opts, updates)
    }

    pub fn update_rows(&self, tables: &HashMap<ArenaStr, TableRef>,
                       assignments: &[Assignment],
                       prepared_stmt: Option<ArenaBox<PreparedStatement>>,
                       mut row_producer: ArenaBox<dyn PhysicalPlanOps>,
                       region: &ArenaMut<Arena>) -> Result<u64> {
        debug_assert!(!tables.is_empty());

        let assignments = Self::parse_assignments(tables, assignments);
        let mut cols = ColumnsAuxResolver::new(region);

        self.update_snapshot();
        row_producer.prepare()?;
        let mut zone = Arena::new_ref();
        let mut feedback = FeedbackImpl::new(true);
        let mut affected_rows = 0;
        loop {
            zone_limit_guard!(zone, 1);

            let rs = row_producer.next(&mut feedback, &zone);
            if feedback.status.is_not_ok() {
                self.update_snapshot(); // FIXME: rollback
                break Err(feedback.status);
            }
            if rs.is_none() {
                self.update_snapshot();
                break Ok(affected_rows);
            }

            let origin = rs.unwrap();
            cols.attach(&origin);
            let arena = zone.get_mut();
            let tuple = self.update_tuple_vals(&assignments, prepared_stmt.clone(), &cols,
                                               origin.dup(&arena), &arena)?;
            for (_, t) in tables {
                self.update_row_impl(&assignments, t, &cols, &tuple, &origin, &arena)?;
                affected_rows += 1;
            }
        }
    }

    fn update_tuple_vals(&self,
                         assignments: &[InternalAssignment],
                         prepared_stmt: Option<ArenaBox<PreparedStatement>>,
                         cols: &ColumnsAuxResolver,
                         mut tuple: Tuple,
                         arena: &ArenaMut<Arena>) -> Result<Tuple> {
        let mut ctx = UpstreamContext::new(prepared_stmt, arena);
        ctx.add(tuple.columns());
        ctx.attach(&tuple);
        let env = Arc::new(ctx);

        let mut evaluator = Evaluator::new(arena);

        for item in assignments {
            let mut expr = item.value.clone();
            let rv = evaluator.evaluate(expr.deref_mut(), env.clone())?;
            let pos = cols.get_column_by_id(item.table.id(), item.dest.id).unwrap();
            tuple.set(pos, rv);
        }
        Ok(tuple)
    }

    fn update_row_impl(&self, assignments: &[InternalAssignment], table: &TableRef, cols: &ColumnsAuxResolver,
                       tuple: &Tuple, origin: &Tuple, arena: &ArenaMut<Arena>) -> Result<()> {
        let inst = self.locks.instance(table.metadata.id).unwrap();
        let mut lock_group = inst.group();

        let mut buf = arena_vec!(arena);
        let (row_key, has_row_key_changed) = Self::rebuild_row_key_if_needed(assignments, table, tuple, &mut buf)?;
        lock_group.add(row_key);

        let secondary_keys = Self::rebuild_secondary_index_if_needed(assignments,
                                                                     has_row_key_changed,
                                                                     table, tuple, origin, arena);
        let mut updates = WriteBatch::new();
        if has_row_key_changed {
            self.move_row_to_row_key(&mut updates, row_key, table, cols, tuple, origin, arena)?;
        } else {
            self.update_columns_in_row(&mut updates, row_key, assignments, table, cols, tuple, arena);
        }
        for key in &secondary_keys {
            if key.index.unique {
                lock_group.add(&key.new);
            }
            updates.delete(&table.column_family, &key.old);
            updates.insert(&table.column_family, &key.new, row_key);
        }

        let _locking = lock_group.exclusive_lock_all();
        for key in &secondary_keys {
            if key.index.unique &&
                self.storage.get_pinnable(&self.rd_opts, &table.column_family, &key.new).is_ok() {
                corrupted_err!("Duplicated unique secondary index key: {}, in table: {}", key.index.name, table.name())?;
            }
        }
        if has_row_key_changed &&
            self.storage.get_pinnable(&self.rd_opts, &table.column_family, row_key).is_ok() {
            corrupted_err!("Duplicated primary key, in table: {}", table.name())?;
        }
        self.storage.write(&self.wr_opts, updates)
    }

    fn update_columns_in_row(&self,
                             updates: &mut WriteBatch,
                             row_key: &[u8],
                             assignments: &[InternalAssignment],
                             table: &TableRef,
                             cols: &ColumnsAuxResolver,
                             tuple: &Tuple,
                             arena: &ArenaMut<Arena>) {
        let mut buf = arena_vec!(arena);
        buf.write(row_key).unwrap();
        let row_key_len = buf.len();

        assignments.iter().filter(|x| {
            x.table.id() == table.id()
        }).for_each(|x| {
            Self::encode_col_id(x.dest.id, &mut buf);
            let key_len = buf.len();

            let pos = cols.get_column_by_id(table.id(), x.dest.id).unwrap();
            Self::encode_column_value(&tuple[pos], &x.dest.ty, &mut buf);

            let k = &buf.as_slice()[..key_len];
            let v = &buf.as_slice()[key_len..];
            updates.insert(&table.column_family, k, v);

            buf.truncate(row_key_len);
        })
    }

    fn move_row_to_row_key(&self,
                           updates: &mut WriteBatch,
                           row_key: &[u8],
                           table: &TableRef,
                           cols: &ColumnsAuxResolver,
                           tuple: &Tuple,
                           origin: &Tuple,
                           arena: &ArenaMut<Arena>) -> Result<()> {
        let old_row_keys = Self::extract_multi_row_keys_from_tuple(origin, table.metadata.id);
        let mut old_pk = arena_vec!(arena);
        for pk in old_row_keys {
            old_pk.write(pk).unwrap();
            break;
        }
        let old_pk_len = old_pk.len();
        debug_assert!(old_pk_len > Self::KEY_ID_LEN);

        let mut new_pk = arena_vec!(arena);
        new_pk.write(row_key).unwrap();
        let new_pk_len = new_pk.len();
        debug_assert!(new_pk_len > Self::KEY_ID_LEN);

        let mut new_vl = arena_vec!(arena);
        for col in &table.metadata.columns {
            DB::encode_col_id(col.id, &mut old_pk);
            updates.delete(&table.column_family, &old_pk);
            old_pk.truncate(old_pk_len);

            let pos = cols.get_column_by_id(table.id(), col.id).unwrap();
            Self::encode_column_value(&tuple[pos], &col.ty, &mut new_vl);

            Self::encode_col_id(col.id, &mut new_pk);
            updates.insert(&table.column_family, &new_pk, &new_vl);
            new_pk.truncate(new_pk_len);
            new_vl.clear();
        }
        Ok(())
    }

    fn rebuild_row_key_if_needed<'a>(assignments: &[InternalAssignment], table: &TableRef, tuple: &'a Tuple,
                                     buf: &'a mut ArenaVec<u8>) -> Result<(&'a [u8], bool)> {
        let rs = assignments.iter().find(|x| {
            x.table.metadata.id == table.metadata.id && x.part_of_key == 'p'
        });
        if rs.is_none() {
            let keys = Self::extract_multi_row_keys_from_tuple(tuple, table.id());
            for key in keys {
                return Ok((key, false));
            }
        }

        fn rebuild_row_key(table: &TableRef, tuple: &Tuple, buf: &mut ArenaVec<u8>) -> Result<()> {
            buf.write(&DB::PRIMARY_KEY_ID_BYTES).unwrap();
            for col_id in &table.metadata.primary_keys {
                let col = tuple.columns().find_by_id(*col_id).unwrap();
                DB::encode_row_key(&tuple[col.order], &col.ty, buf)?;
            }
            Ok(())
        }

        let mut tmp = arena_vec!(&buf.owns);
        rebuild_row_key(table, tuple, &mut tmp)?;

        if tuple.columns().original_table_id() == Some(table.metadata.id) {
            buf.write(&tmp).unwrap();
        } else {
            Self::iterate_multi_row_key(tuple.row_key(), |tid, row_key| {
                if tid == table.metadata.id {
                    Self::encode_multi_row_key_impl(tid, &tmp, buf);
                } else {
                    Self::encode_multi_row_key_impl(tid, row_key, buf);
                }
            });
        }
        Ok((buf, true))
    }

    fn rebuild_secondary_index_if_needed<'a>(assignments: &[InternalAssignment<'a>],
                                             force: bool,
                                             table: &'a TableRef,
                                             tuple: &Tuple,
                                             origin: &Tuple,
                                             arena: &ArenaMut<Arena>) -> ArenaVec<InternalSecondaryIndexDesc<'a>> {

        let mut desc = arena_vec!(arena);
        if force {
            for idx in &table.metadata.secondary_indices {
                let mut item = InternalSecondaryIndexDesc {
                    index: idx,
                    old: arena_vec!(arena),
                    new: arena_vec!(arena),
                };
                Self::encode_full_secondary_index(origin, table.id(), idx, &mut item.old);
                Self::encode_full_secondary_index(tuple, table.id(), idx, &mut item.new);
                desc.push(item);
            }
        } else {
            let mut indices = ArenaMap::new(arena);
            for item in assignments {
                if let Some(idx) = item.index {
                    indices.insert(idx.id, idx);
                }
            }
            for (_, idx) in indices.iter() {
                let mut item = InternalSecondaryIndexDesc {
                    index: *idx,
                    old: arena_vec!(arena),
                    new: arena_vec!(arena),
                };
                Self::encode_full_secondary_index(origin, table.id(), *idx, &mut item.old);
                Self::encode_full_secondary_index(tuple, table.id(), *idx, &mut item.new);
                desc.push(item);
            }

        }
        desc
    }

    fn encode_full_secondary_index<W: Write>(row: &Tuple, tid: u64, idx: &SecondaryIndexMetadata, w: &mut W) {
        Self::encode_idx_id(idx.id, w);
        for col_id in &idx.key_parts {
            let pos = if row.columns().original_table_id() == Some(tid) {
                row.columns().index_by_id(*col_id)
            } else {
                row.columns().index_by_original_and_id(tid, *col_id)
            }.unwrap();
            Self::encode_secondary_index(&row[pos], &row.columns()[pos].ty, w)
        }
    }

    fn parse_assignments<'a>(tables: &'a HashMap<ArenaStr, TableRef>, ast: &'a [Assignment])
                             -> Vec<InternalAssignment<'a>> {
        fn build_assignment(t: &TableRef, name: String, expr: ArenaBox<dyn Expression>) -> InternalAssignment {
            let col = t.get_col_by_name(&name).unwrap();
            let index = t.get_col_be_part_of_2rd_idx_by_name(&name);
            let is_pk = t.is_col_be_part_of_primary_key_by_name(&name);
            let part_of_key = if index.is_some() { 'k' } else if is_pk { 'p' } else { ' ' };
            InternalAssignment {
                table: t,
                dest: col,
                index,
                part_of_key,
                value: expr,
            }
        }

        ast.iter().map(|x| {
            if x.lhs.prefix.is_empty() {
                debug_assert_eq!(1, tables.len());
                let name = x.lhs.suffix.to_string();
                let t = tables.values().next().unwrap();
                build_assignment(t, name, x.rhs.clone())
            } else {
                let name = x.lhs.suffix.to_string();
                let t = tables.get(&x.lhs.prefix).unwrap();
                build_assignment(t, name, x.rhs.clone())
            }
        }).collect()
    }

    fn extract_multi_row_keys_from_tuple(tuple: &Tuple, table_id: u64) -> HashSet<&[u8]> {
        let mut row_keys = HashSet::new();
        if tuple.columns().original_table_id() == Some(table_id) {
            row_keys.insert(tuple.row_key());
        } else {
            Self::iterate_multi_row_key(tuple.row_key(), |tid, row_key| {
                if tid == table_id {
                    row_keys.insert(row_key);
                }
            });
        }
        row_keys
    }

    fn iterate_multi_row_key<'a, F>(row_key: &'a [u8], mut callback: F) where F: FnMut(u64, &'a [u8]) {
        let mut pos = 0;
        while pos < row_key.len() {
            let id_part = &row_key[pos..pos + size_of::<u64>()];
            let id = u64::from_be_bytes(id_part.try_into().unwrap());
            pos += size_of::<u64>();

            let len_part = &row_key[pos..pos + size_of::<u32>()];
            pos += size_of::<u32>();
            let len = u32::from_be_bytes(len_part.try_into().unwrap()) as usize;

            let part = &row_key[pos..pos + len];
            pos += len;
            callback(id, part)
        }
    }

    pub fn encode_multi_row_key<W: Write>(tuple: &Tuple, w: &mut W) {
        debug_assert!(!tuple.row_key().is_empty());
        if let Some(tid) = tuple.columns().original_table_id() {
            Self::encode_multi_row_key_impl(tid, tuple.row_key(), w);
        } else {
            w.write(tuple.row_key()).unwrap();
        }
    }

    fn encode_multi_row_key_impl<W: Write>(tid: u64, row_key: &[u8], w: &mut W) {
        w.write(&tid.to_be_bytes()).unwrap();
        w.write(&(row_key.len() as u32).to_be_bytes()).unwrap();
        w.write(row_key).unwrap();
    }

    pub fn is_primary_key(key_id: u64) -> bool { key_id == DB::PRIMARY_KEY_ID as u64 }

    pub fn encode_anonymous_row_key<W: Write>(counter: u64, wr: &mut W) {
        wr.write(&Self::PRIMARY_KEY_ID_BYTES).unwrap(); // primary key id always is 0
        wr.write(&counter.to_be_bytes()).unwrap();
    }

    pub fn encode_col_id<W: Write>(id: u32, wr: &mut W) {
        wr.write(&id.to_be_bytes()).unwrap();
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
                    Value::NegativeInf => (), // ignore
                    Value::PositiveInf => (), // ignore
                    _ => unreachable!()
                }
            }
            ColumnType::Float(_, _) | ColumnType::Double(_, _) => match value {
                Value::Float(n) => { wr.write(&n.to_be_bytes()).unwrap(); }
                Value::NegativeInf => (), // ignore
                Value::PositiveInf => (), // ignore
                _ => unreachable!()
            },
            ColumnType::Char(n) => match value {
                Value::Str(s) => {
                    if s.len() > *n as usize {
                        return Err(Status::corrupted("Char type too long"));
                    }
                    Self::encode_char_ty_for_key(s, *n as usize, wr);
                }
                Value::NegativeInf => (), // ignore
                Value::PositiveInf => (), // ignore
                _ => unreachable!()
            },
            ColumnType::Varchar(n) => match value {
                Value::Str(s) => {
                    if s.len() > *n as usize {
                        return Err(Status::corrupted("Varchar type too long"));
                    }
                    Self::encode_varchar_ty_for_key(s, wr);
                }
                Value::NegativeInf | Value::PositiveInf => (), // ignore
                _ => unreachable!()
            },
        }
        Ok(())
    }

    pub fn decode_row_key_from_secondary_index<'a>(index: &'a [u8], pack_info: &[u8]) -> &'a [u8] {
        let row_key_len = u32::from_le_bytes(pack_info.try_into().unwrap()) as usize;
        debug_assert!(row_key_len < index.len());
        &index[index.len() - row_key_len..]
    }

    pub fn decode_index_from_secondary_index<'a>(index: &'a [u8], pack_info: &[u8]) -> &'a [u8] {
        let row_key_len = u32::from_le_bytes(pack_info.try_into().unwrap()) as usize;
        debug_assert!(row_key_len < index.len());
        &index[..index.len() - row_key_len]
    }

    pub fn encode_key<W: Write>(value: &Value, wr: &mut W) {
        if Self::encode_null_bytes(value, wr) {
            return;
        }
        match value {
            Value::Int(n) => { wr.write(&n.to_be_bytes()).unwrap(); }
            Value::Float(n) => { wr.write(&n.to_be_bytes()).unwrap(); }
            Value::Str(s) => { Self::encode_varchar_ty_for_key(s, wr); }
            _ => unreachable!()
        }
    }

    pub fn decode_tuple(columns: &ArenaBox<ColumnSet>, value: &[u8], arena: &ArenaMut<Arena>) -> Tuple {
        let mut tuple = Tuple::with(columns, arena);
        let mut p = 0usize;
        for i in 0..columns.len() {
            let col = &columns[i];
            let (val, len) = Self::decode_row(&col.ty, &value[p..], arena);
            tuple.set(i, val);
            p += len;
        }
        tuple
    }

    pub fn decode_row(ty: &ColumnType, buf: &[u8], arena: &ArenaMut<Arena>) -> (Value, usize) {
        if buf[0] == Self::NULL_BYTE {
            return (Value::Null, 1);
        }
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                (Value::Int(i64::from_le_bytes((&buf[1..9]).try_into().unwrap())), 9)
            }
            ColumnType::Float(_, _) => {
                let f = f32::from_le_bytes((&buf[1..5]).try_into().unwrap());
                (Value::Float(f as f64), 5)
            }
            ColumnType::Double(_, _) => {
                let f = f64::from_le_bytes((&buf[1..9]).try_into().unwrap());
                (Value::Float(f), 9)
            }
            ColumnType::Char(_)
            | ColumnType::Varchar(_) => {
                let len = u32::from_le_bytes((&buf[1..5]).try_into().unwrap()) as usize;
                let str = std::str::from_utf8(&buf[5..5 + len]).unwrap();
                (Value::Str(ArenaStr::new(str, arena.get_mut())), 5 + len)
            }
        }
    }

    pub fn encode_tuple<W: Write>(tuple: &Tuple, wr: &mut W) {
        for i in 0..tuple.columns().columns.len() {
            Self::encode_row(&tuple[i], &tuple.columns().columns[i].ty, wr)
        }
    }

    pub fn encode_row<W: Write>(value: &Value, ty: &ColumnType, wr: &mut W) {
        if Self::encode_null_bytes(value, wr) {
            return;
        }
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
            ColumnType::Float(_, _) => {
                match value {
                    Value::Float(f) => wr.write(&(*f as f32).to_le_bytes()).unwrap(),
                    _ => unreachable!()
                };
            }
            ColumnType::Double(_, _) => {
                match value {
                    Value::Float(f) => wr.write(&f.to_le_bytes()).unwrap(),
                    _ => unreachable!()
                };
            }
            ColumnType::Char(_)
            | ColumnType::Varchar(_) => {
                match value {
                    Value::Str(s) => {
                        wr.write(&(s.len() as u32).to_le_bytes()).unwrap();
                        wr.write(s.as_bytes()).unwrap();
                    }
                    _ => unreachable!()
                };
            }
        }
    }

    fn encode_null_bytes<W: Write>(value: &Value, wr: &mut W) -> bool {
        if value.is_null() {
            wr.write(&Self::NULL_BYTES).unwrap();
            return true;
        }
        //assert!(value.is_certain());

        wr.write(&Self::NOT_NULL_BYTES).unwrap();
        false
    }

    pub fn encode_secondary_index<W: Write>(value: &Value, ty: &ColumnType, wr: &mut W) {
        if Self::encode_null_bytes(value, wr) {
            return;
        }
        match ty {
            ColumnType::TinyInt(_)
            | ColumnType::SmallInt(_)
            | ColumnType::Int(_)
            | ColumnType::BigInt(_) => {
                match value {
                    Value::Int(n) => wr.write(&n.to_be_bytes()).unwrap(),
                    Value::NegativeInf | Value::PositiveInf => 0, // ignore
                    _ => unreachable!()
                };
            }
            ColumnType::Float(_, _) => {
                match value {
                    Value::Float(n) => wr.write(&(*n as f32).to_be_bytes()).unwrap(),
                    Value::NegativeInf | Value::PositiveInf => 0, // ignore
                    _ => unreachable!()
                };
            }
            ColumnType::Double(_, _) => {
                match value {
                    Value::Float(n) => wr.write(&n.to_be_bytes()).unwrap(),
                    Value::NegativeInf | Value::PositiveInf => 0, // ignore
                    _ => unreachable!()
                };
            }
            ColumnType::Char(n) => {
                match value {
                    Value::Str(s) => Self::encode_char_ty_for_key(s, *n as usize, wr),
                    Value::NegativeInf | Value::PositiveInf => (), // ignore
                    _ => unreachable!()
                }
            }
            ColumnType::Varchar(_) => {
                match value {
                    Value::Str(s) => Self::encode_varchar_ty_for_key(s, wr),
                    Value::NegativeInf | Value::PositiveInf => (), // ignore
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

    pub fn parse_row_key(key: &[u8]) -> (u64, u32) {
        debug_assert!(key.len() > Self::KEY_ID_LEN + Self::COL_ID_LEN);
        let key_id = u32::from_be_bytes((&key[..Self::KEY_ID_LEN]).try_into().unwrap()) as u64;
        let col_id = u32::from_be_bytes((&key[key.len() - Self::COL_ID_LEN..]).try_into().unwrap());
        (key_id, col_id)
    }

    pub fn parse_key_id(key: &[u8]) -> u64 {
        debug_assert!(key.len() >= Self::KEY_ID_LEN);
        u32::from_be_bytes((&key[..Self::KEY_ID_LEN]).try_into().unwrap()) as u64
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

    pub fn update_snapshot(&self) -> Arc<dyn storage::Snapshot> {
        let snapshot = self.storage.get_snapshot();
        let mut slot = self.snapshot.write().unwrap();
        //*slot = snapshot.clone();
        drop(replace(slot.deref_mut(), snapshot.clone()));
        snapshot
    }

    pub fn get_snapshot(&self) -> Arc<dyn storage::Snapshot> {
        let slot = self.snapshot.read().unwrap();
        slot.clone()
    }

    pub fn _test_get_table_ref(&self, name: &str) -> Option<TableRef> {
        let tables = self.lock_tables();
        tables.get(&name.to_string()).cloned()
    }

    pub fn _test_get_row(&self, table_name: &String,
                         columns_set: &ArenaBox<ColumnSet>,
                         row_key: &[u8],
                         arena: &ArenaMut<Arena>) -> Result<Tuple> {
        let tables = self.tables_handle.read().unwrap();
        let table = tables.get(table_name).unwrap().clone();
        let mut key = Vec::new();
        key.write(&Self::PRIMARY_KEY_ID_BYTES).unwrap();
        key.write(row_key).unwrap();
        let rd_opts = ReadOptions::default();
        let mut tuple = Tuple::with_row_key(&columns_set, &key, arena);
        for col in &columns_set.columns {
            let col_meta = table.get_col_by_name(&col.name.to_string()).unwrap();
            key.write(&col_meta.id.to_be_bytes()).unwrap();

            let value = self.storage.get_pinnable(&rd_opts, &table.column_family, &key)?;
            tuple.set(col.order, Self::decode_column_value(&col.ty, value.value(),
                                                           arena.get_mut()));
            key.truncate(row_key.len() + 4);
        }
        Ok(tuple)
    }
}

struct InternalAssignment<'a> {
    table: &'a TableHandle,
    dest: &'a ColumnMetadata,
    index: Option<&'a SecondaryIndexMetadata>,
    /// ' ': not in key
    /// 'p': primary key
    /// 'k': secondary key
    part_of_key: char,
    value: ArenaBox<dyn Expression>,
}

struct InternalSecondaryIndexDesc<'a> {
    index: &'a SecondaryIndexMetadata,
    old: ArenaVec<u8>,
    new: ArenaVec<u8>,
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

    pub fn id(&self) -> u64 { self.metadata.id }

    pub fn name(&self) -> &String { &self.metadata.name }

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
    pub default_value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

impl ColumnType {
    pub fn is_integral(&self) -> bool {
        match self {
            Self::TinyInt(_)
            | Self::SmallInt(_)
            | Self::Int(_)
            | Self::BigInt(_) => true,
            _ => false
        }
    }

    pub fn is_floating(&self) -> bool {
        match self {
            Self::Float(_, _) | Self::Double(_, _) => true,
            _ => false
        }
    }

    pub fn is_number(&self) -> bool {
        self.is_integral() || self.is_floating()
    }

    pub fn is_string(&self) -> bool {
        match self {
            Self::Varchar(_) | Self::Char(_) => true,
            _ => false
        }
    }

    pub fn is_not_compatible_of(&self, other: &Self) -> bool {
        !self.is_compatible_of(other)
    }

    pub fn is_compatible_of(&self, other: &Self) -> bool {
        if self.is_integral() {
            other.is_integral()
        } else if self.is_floating() {
            other.is_floating()
        } else if self.is_string() {
            other.is_string()
        } else {
            false
        }
    }
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
    use crate::ArenaVec;
    use crate::base::Arena;
    use crate::storage::JunkFilesCleaner;
    use crate::suite::testing::SqlSuite;

    use super::*;

    #[test]
    fn sanity() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db100");
        let db = DB::open("tests".to_string(), "db100".to_string())?;

        let arena = Arena::new_ref();
        let conn = db.connect();
        conn.execute_str("create table a { a tinyint(1) not null } ", &arena.get_mut())?;

        let (cf, tb) = db.get_table_handle(&"a".to_string()).unwrap();
        assert_eq!("__table__1", cf.name());
        assert_eq!("a", tb.name);
        Ok(())
    }

    #[test]
    fn encode_varchar_key() {
        let arena = Arena::new_ref();
        let s = ArenaStr::from_arena("", &mut arena.get_mut());
        let mut buf = Vec::new();
        DB::encode_varchar_ty_for_key(&s, &mut buf);

        assert_eq!(0, buf.len());

        buf.clear();
        let s = ArenaStr::from_arena("a", &mut arena.get_mut());
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
        let s = ArenaStr::from_arena("中文", &mut arena.get_mut());
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
        let s = ArenaStr::from_arena("123456789", &mut arena.get_mut());
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
    fn row_encoding() {
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let mut columns = ColumnSet::new("t1", 0, &arena);
        columns.append_with_name("a", ColumnType::Int(11));
        columns.append_with_name("b", ColumnType::Float(0, 0));
        columns.append_with_name("c", ColumnType::Double(0, 0));
        columns.append_with_name("d", ColumnType::Varchar(255));
        columns.append_with_name("e", ColumnType::Char(1));

        let cols = ArenaBox::new(columns, arena.get_mut());
        let mut row1 = Tuple::with(&cols, &arena);
        row1.set(0, Value::Int(100));
        row1.set(1, Value::Float(1.0));
        row1.set(2, Value::Float(2.1));
        row1.set(3, Value::Str(ArenaStr::new("HK man is dog!", arena.get_mut())));
        row1.set(4, Value::Null);

        let mut buf = ArenaVec::new(&arena);
        DB::encode_tuple(&row1, &mut buf);
        assert_eq!(43, buf.len());

        let row2 = DB::decode_tuple(&cols, &buf, &arena);
        assert_eq!(row1.to_string(), row2.to_string());
    }

    #[test]
    fn recover() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db101");
        let arena = Arena::new_ref();
        {
            let db = DB::open("tests".to_string(), "db101".to_string())?;
            let sql = " create table t1 {\n\
                a tinyint(1) not null,\n\
                b char(6)\n\
            }\n";
            let conn = db.connect();
            conn.execute_str(sql, &mut arena.get_mut())?;
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
        let arena = Arena::new_ref();
        let db = DB::open("tests".to_string(), "db102".to_string())?;
        let sql = " create table t1 {\n\
                a tinyint(1) not null,\n\
                b char(6)\n\
            };\n\
            drop table if exists t1;\n\
            ";
        let conn = db.connect();
        conn.execute_str(sql, &mut arena.get_mut())?;
        assert!(db.get_table_handle(&"t1".to_string()).is_none());
        Ok(())
    }

    #[test]
    fn insert_into_table() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db103");
        let arena_ref = Arena::new_ref();
        let mut arena = arena_ref.get_mut();
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

        let mut column_set = ArenaBox::new(ColumnSet::new("t1", 0, &mut arena), arena.deref_mut());
        column_set.append_with_name("a", ColumnType::Int(11));
        column_set.append_with_name("b", ColumnType::Int(11));

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &1i64.to_be_bytes(), &mut arena)?;
        assert_eq!(Some(1), tuple.get_i64(0));
        assert_eq!(Some(2), tuple.get_i64(1));
        drop(tuple);

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &2i64.to_be_bytes(), &mut arena)?;
        assert_eq!(Some(3), tuple.get_i64(0));
        assert_eq!(Some(4), tuple.get_i64(1));
        drop(tuple);
        Ok(())
    }

    #[test]
    fn insert_with_auto_increment() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db104");
        let arena_ref = Arena::new_ref();
        let mut arena = arena_ref.get_mut();
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

        let mut column_set = ArenaBox::new(ColumnSet::new("t1", 0, &mut arena), arena.deref_mut());
        column_set.append_with_name("a", ColumnType::Int(11));
        column_set.append_with_name("b", ColumnType::Int(11));
        column_set.append_with_name("c", ColumnType::Int(11));

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &1i64.to_be_bytes(), &mut arena)?;
        assert_eq!(Some(1), tuple.get_i64(0));
        assert_eq!(Some(1), tuple.get_i64(1));
        assert_eq!(Some(2), tuple.get_i64(2));

        let tuple = db._test_get_row(&"t1".to_string(),
                                     &column_set,
                                     &2i64.to_be_bytes(), &mut arena)?;
        assert_eq!(Some(2), tuple.get_i64(0));
        assert_eq!(Some(3), tuple.get_i64(1));
        assert_eq!(Some(4), tuple.get_i64(2));
        Ok(())
    }

    #[test]
    fn create_table_with_index() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db105");
        let arena_ref = Arena::new_ref();
        let arena = arena_ref.get_mut();
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
        let arena = Arena::new_ref();
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
        conn.execute_str(sql, &arena.get_mut())?;


        Ok(())
    }

    #[test]
    fn insert_with_char_varchar_index() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db107");
        let arena = Arena::new_ref();
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
        conn.execute_str(sql, &arena.get_mut())?;

        Ok(())
    }

    #[test]
    fn insert_duplicated_row_key() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db108");
        let arena = Arena::new_ref();
        let db = DB::open("tests".to_string(), "db108".to_string())?;
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            insert into table t1(a,b) values (1, NULL);\n\
            insert into table t1(a,b) values (1, NULL);\n\
            ";
        let conn = db.connect();
        let rs = conn.execute_str(sql, &arena.get_mut());
        assert!(rs.is_err());
        assert_eq!(Status::Corruption("Duplicated primary key, must be unique.".to_string()),
                   rs.unwrap_err());
        Ok(())
    }

    #[test]
    fn insert_duplicated_row_key_at_same_statement() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db109");
        let arena = Arena::new_ref();
        let db = DB::open("tests".to_string(), "db109".to_string())?;
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            insert into table t1(a,b) values (1, NULL), (1, NULL);\n\
            ";
        let conn = db.connect();
        let rs = conn.execute_str(sql, &arena.get_mut());
        assert!(rs.is_err());
        assert_eq!(Status::Corruption("Duplicated primary key, must be unique.".to_string()),
                   rs.unwrap_err());
        Ok(())
    }

    #[test]
    fn large_insert_into_table() -> Result<()> {
        const N: i32 = 300000;

        let _junk = JunkFilesCleaner::new("tests/db110");
        let arena = Arena::new_ref();
        let db = DB::open("tests".to_string(), "db110".to_string())?;
        let conn = db.connect();
        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
            };\n\
            ";
        conn.execute_str(sql, &arena.get_mut())?;

        let sql = "insert into table t1(a,b) values (?, ?)";
        let mut stmt = conn.prepare_str(sql, &arena.get_mut())?.first().cloned().unwrap();

        let jiffies = db.env.current_time_mills();
        for i in 0..N {
            stmt.bind_i64(0, i as i64);
            stmt.bind_null(1);
            conn.execute_prepared_statement(&mut stmt)?;
        }
        let cost = (db.env.current_time_mills() - jiffies) as f32 / 1000f32;
        println!("qps: {}", N as f32 / cost);

        Ok(())
    }

    #[test]
    fn create_index_before_inserting() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db111");
        let arena_ref = Arena::new_ref();
        let mut arena = arena_ref.get_mut();
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
            stmt.bind_string(1, format!("{:03}", i), arena.deref_mut());
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
            let index = DB::decode_index_from_secondary_index(iter.key(), iter.value());

            let null_byte = index[4];
            assert_eq!(DB::NOT_NULL_BYTE, null_byte);


            let key = std::str::from_utf8(&index[5..]).unwrap();
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
        let arena = Arena::new_ref();
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
        assert_eq!(3, conn.execute_str(sql, &arena.get_mut())?);

        let sql = "drop index idx_b on t1";
        assert_eq!(3, conn.execute_str(sql, &arena.get_mut())?);
        Ok(())
    }

    #[test]
    fn just_select_returning_one() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db113");
        let arena = Arena::new_val();
        let db = DB::open("tests".to_string(), "db113".to_string())?;
        let conn = db.connect();
        let mut rs = conn.execute_query_str("select 1 + 1;", &arena.get_mut())?;
        assert_eq!(1, rs.columns().columns.len());
        assert_eq!("_0", rs.column_name(0));
        assert!(matches!(rs.column_ty(0), ColumnType::BigInt(_)));
        assert!(rs.next());
        let row = rs.current()?;
        assert_eq!(Some(2), row.get_i64(0));
        assert!(!rs.next());

        Ok(())
    }

    #[test]
    fn just_select_calling_version() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db114");
        let arena = Arena::new_val();
        let db = DB::open("tests".to_string(), "db114".to_string())?;
        let conn = db.connect();
        let mut rs = conn.execute_query_str("select version();", &arena.get_mut())?;

        assert_eq!(1, rs.columns().columns.len());
        assert_eq!("_0", rs.column_name(0));
        assert!(matches!(rs.column_ty(0), ColumnType::Varchar(_)));
        assert!(rs.next());
        let row = rs.current()?;
        assert_eq!(Some("mai2-sql:v0.0.1"), row.get_str(0));
        assert!(!rs.next());

        let mut rs = conn.execute_query_str("select length();", &arena.get_mut())?;

        assert_eq!(1, rs.columns().columns.len());
        assert_eq!("_0", rs.column_name(0));
        assert!(matches!(rs.column_ty(0), ColumnType::Int(_)));
        assert!(rs.next());
        let row = rs.current()?;
        assert!(row.get_null(0));
        assert!(!rs.next());
        Ok(())
    }

    #[test]
    fn select_range_rows() -> Result<()> {
        let _junk = JunkFilesCleaner::new("tests/db115");
        let arena = Arena::new_val();
        let db = DB::open("tests".to_string(), "db115".to_string())?;
        let conn = db.connect();

        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
                index idx_b(b)\n\
            };\n\
            insert into table t1(b) values (\"aaa\"),(\"bbb\"),(\"ccc\");\n\
            ";
        assert_eq!(3, conn.execute_str(sql, &arena.get_mut())?);

        let mut rs = conn.execute_query_str("select * from t1 where a > 0", &arena.get_mut())?;
        assert_eq!(2, rs.columns().columns.len());
        assert_eq!("a", rs.column_name(0));
        assert!(matches!(rs.column_ty(0), ColumnType::Int(_)));
        assert_eq!("b", rs.column_name(1));
        assert!(matches!(rs.column_ty(1), ColumnType::Char(_)));

        assert!(rs.next());
        let row = rs.current()?;
        assert_eq!(Some(1), row.get_i64(0));
        assert_eq!(Some("aaa"), row.get_str(1));
        assert_eq!("(1, \"aaa\")", row.to_string());

        assert!(rs.next());
        assert_eq!("(2, \"bbb\")", rs.current()?.to_string());

        assert!(rs.next());
        assert_eq!("(3, \"ccc\")", rs.current()?.to_string());

        assert!(!rs.next());

        let mut rs = conn.execute_query_str("select * from t1 where b >= \"bbb\"", &arena.get_mut())?;
        assert!(rs.next());
        assert_eq!("(2, \"bbb\")", rs.current()?.to_string());

        assert!(rs.next());
        assert_eq!("(3, \"ccc\")", rs.current()?.to_string());

        assert!(!rs.next());
        Ok(())
    }

    #[test]
    fn select_from_prepared_statement() -> Result<()> {
        let junk = JunkFilesCleaner::new("tests/db116");
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let db = DB::open(junk.ensure().path, junk.ensure().name)?;
        let conn = db.connect();

        let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
                index idx_b(b)\n\
            };\n\
            insert into table t1(b) values (\"aaa\"),(\"bbb\"),(\"ccc\");\n\
            ";
        assert_eq!(3, conn.execute_str(sql, &arena)?);


        let mut stmt = conn.prepare_str("select count(*) from t1 where a >= ?", &arena)?[0].clone();
        stmt.bind_i64(0, 1);
        let mut rs = conn.execute_query_prepared_statement(&mut stmt, &arena)?;
        assert!(rs.next());
        assert_eq!("(3)", rs.current()?.to_string());
        assert!(!rs.next());

        stmt.bind_i64(0, 2);
        let mut rs = conn.execute_query_prepared_statement(&mut stmt, &arena)?;
        assert!(rs.next());
        assert_eq!("(2)", rs.current()?.to_string());
        assert!(!rs.next());
        Ok(())
    }

    #[test]
    fn execute_from_file() -> Result<()> {
        let junk = JunkFilesCleaner::new("tests/db117");
        let zone = Arena::new_val();
        let arena = zone.get_mut();
        let db = DB::open(junk.ensure().path, junk.ensure().name)?;
        let conn = db.connect();

        assert_eq!(9, conn.execute_file(Path::new("testdata/t1_with_pk_and_data.sql"), &arena)?);
        assert_eq!(9, conn.execute_file(Path::new("testdata/t2_with_pk_and_data.sql"), &arena)?);

        let mut rs = conn.execute_query_str("select * from t1 union all select * from t2;", &arena)?;
        while rs.next() {
            let row = rs.current()?;
            assert_eq!("xxx", row.get_str(2).unwrap());
        }
        assert_eq!(18, rs.fetched_rows());
        Ok(())
    }

    #[test]
    fn simple_nested_loop_join() -> Result<()> {
        let suite = SqlSuite::new("tests/db118")?;
        suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

        let data = [
            "(1, 100, \"Js\", 1, 101, \"Js\")",
            "(2, 101, \"Jc\", 2, 101, \"Jc\")",
            "(3, 102, \"Jk\", 3, 102, \"Jk\")",
        ];
        let rs = suite.execute_query_str("select * from t3 inner join t4 on(t3.name = t4.name)", &suite.arena)?;
        SqlSuite::assert_rows(&data, rs)?;
        Ok(())
    }

    #[test]
    fn inner_join_with_index_nested_loop_join() -> Result<()> {
        let suite = SqlSuite::new("tests/db119")?;
        suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

        let data = [
            "(1, 100, \"Js\", 1, 101, \"Js\")",
            "(2, 101, \"Jc\", 2, 101, \"Jc\")",
            "(3, 102, \"Jk\", 3, 102, \"Jk\")",
        ];
        let rs = suite.execute_query_str("select * from t3 inner join t4 on(t3.id = t4.id)", &suite.arena)?;
        SqlSuite::assert_rows(&data, rs)?;
        Ok(())
    }

    #[test]
    fn left_outer_join_use_pk() -> Result<()> {
        let suite = SqlSuite::new("tests/db120")?;
        suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

        let data = [
            "(1, 100, \"Js\", 1, 101, \"Js\")",
            "(2, 101, \"Jc\", 2, 101, \"Jc\")",
            "(3, 102, \"Jk\", 3, 102, \"Jk\")",
            "(4, 102, \"Ol\", NULL, NULL, NULL)",
        ];
        let rs = suite.execute_query_str("select * from t3 left join t4 on(t3.id = t4.id)", &suite.arena)?;
        SqlSuite::assert_rows(&data, rs)?;
        Ok(())
    }

    #[test]
    fn inner_join_use_key() -> Result<()> {
        let suite = SqlSuite::new("tests/db121")?;
        suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

        let data = [
            "(2, 101, \"Jc\", 1, 101, \"Js\")",
            "(2, 101, \"Jc\", 2, 101, \"Jc\")",
            "(3, 102, \"Jk\", 3, 102, \"Jk\")",
            "(4, 102, \"Ol\", 3, 102, \"Jk\")",
        ];
        let rs = suite.execute_query_str("select * from t3 inner join t4 on(t3.dd = t4.df)", &suite.arena)?;
        SqlSuite::assert_rows(&data, rs)?;
        Ok(())
    }

    #[test]
    fn right_outer_join_use_pk() -> Result<()> {
        let suite = SqlSuite::new("tests/db122")?;
        suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

        let data = [
            "(1, 100, \"Js\", 1, 101, \"Js\")",
            "(2, 101, \"Jc\", 2, 101, \"Jc\")",
            "(3, 102, \"Jk\", 3, 102, \"Jk\")",
        ];
        let rs = suite.execute_query_str("select * from t3 right join t4 on(t3.id = t4.id)", &suite.arena)?;
        SqlSuite::assert_rows(&data, rs)?;

        let data = [
            "(1, 101, \"Js\", 1, 100, \"Js\")",
            "(2, 101, \"Jc\", 2, 101, \"Jc\")",
            "(3, 102, \"Jk\", 3, 102, \"Jk\")",
            "(NULL, NULL, NULL, 4, 102, \"Ol\")",
        ];
        let rs = suite.execute_query_str("select * from t4 right join t3 on(t3.id = t4.id)", &suite.arena)?;
        SqlSuite::assert_rows(&data, rs)?;
        Ok(())
    }

    #[test]
    fn insert_default_values() -> Result<()> {
        let suite = SqlSuite::new("tests/db123")?;
        suite.execute_file(Path::new("testdata/t1_cols_with_default_value.sql"), &suite.arena)?;

        suite.execute_str("insert into t1 (name) values (\"aaa\"),(\"ccc\"),(\"bbb\")", &suite.arena)?;

        let rs = suite.execute_query_str("select * from t1;", &suite.arena)?;
        SqlSuite::print_rows(rs)?;

        Ok(())
    }
}