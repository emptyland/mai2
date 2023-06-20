#![allow(dead_code)]

//#[cfg(test)]
pub mod testing {
    use std::ops::Deref;
    use std::sync::Arc;

    use crate::{Arena, ArenaBox, ArenaMut, ArenaRef, Result};
    use crate::exec::ColumnSet;
    use crate::exec::connection::{Connection, ResultSet};
    use crate::exec::db::DB;
    use crate::sql::ast::Expression;
    use crate::sql::parse_sql_expr_from_content;
    use crate::storage::JunkFilesCleaner;

    pub struct SqlSuite {
        _junk: JunkFilesCleaner,
        _zone: ArenaRef<Arena>,
        pub arena: ArenaMut<Arena>,
        pub db: Arc<DB>,
        pub conn: Arc<Connection>,
    }

    impl SqlSuite {
        pub fn new(path: &str) -> Result<Self> {
            let junk = JunkFilesCleaner::new(path);
            let zone = Arena::new_ref();
            let arena = zone.get_mut();
            let db = DB::open(junk.ensure().path, junk.ensure().name)?;
            let conn = db.connect();
            Ok(Self {
                _junk: junk,
                _zone: zone,
                arena,
                db,
                conn,
            })
        }

        pub fn get_table_cols_set(&self, name: &str, alias: &str) -> ArenaBox<ColumnSet> {
            let tables = self.db.lock_tables();
            let table = tables.get(&name.to_string()).unwrap();
            let mut cols = if alias.is_empty() {
                ColumnSet::new(name, table.metadata.id, &self.arena)
            } else {
                ColumnSet::new(alias, table.metadata.id, &self.arena)
            };
            for col in &table.metadata.columns {
                cols.append(col.name.as_str(), "", col.id, col.ty.clone());
            }
            ArenaBox::new(cols, self.arena.get_mut())
        }

        pub fn parse_expr(&self, sql: &str) -> Result<ArenaBox<dyn Expression>> {
            parse_sql_expr_from_content(sql, &self.arena)
        }

        pub fn assert_rows(data: &[&str], mut rs: ResultSet) -> Result<()> {
            let mut i = 0;
            while rs.next() {
                assert_eq!(data[i], rs.current()?.to_string());
                i += 1;
            }
            assert_eq!(i, data.len());
            if rs.status.is_ok() {
                Ok(())
            } else {
                Err(rs.status.clone())
            }
        }

        pub fn print_rows(mut rs: ResultSet) -> Result<()> {
            while rs.next() {
                println!("{}", rs.current()?.to_string());
            }
            if rs.status.is_ok() {
                Ok(())
            } else {
                Err(rs.status.clone())
            }
        }
    }

    impl Deref for SqlSuite {
        type Target = Arc<Connection>;

        fn deref(&self) -> &Self::Target {
            &self.conn
        }
    }
}