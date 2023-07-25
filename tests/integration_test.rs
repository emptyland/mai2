use std::path::Path;

use mai2::{Arena, Result};
use mai2::exec::db::DB;
use mai2::storage::JunkFilesCleaner;
use mai2::suite::testing::SqlSuite;

#[test]
fn sql_create_table_and_insert() -> Result<()> {
    let junk = JunkFilesCleaner::new("tests/dbi-001");
    let arena = Arena::new_val();
    let db = DB::open(junk.ensure().path, junk.ensure().name)?;
    let conn = db.connect();

    let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b char(9)\n\
                index idx_b(b)\n\
            };\n\
            insert into table t1(b) values (\"aaa\"),(\"bbb\"),(\"ccc\");\n\
            ";
    assert_eq!(3, conn.execute_str(sql, &arena.get_mut())?);

    Ok(())
}

#[test]
fn sql_range_scanning() -> Result<()> {
    const N: i32 = 1000;

    let junk = JunkFilesCleaner::new("tests/dbi-002");
    let zone = Arena::new_val();
    let db = DB::open(junk.ensure().path, junk.ensure().name)?;
    let conn = db.connect();

    let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b int,\n\
                c varchar(64) \n\
                index idx_b(b)\n\
            };\n\
            ";
    let arena = zone.get_mut();
    assert_eq!(0, conn.execute_str(sql, &arena)?);

    let sql = "insert into table t1(b,c) values(?, ?)";
    let prepared_stmt = &mut conn.prepare_str(sql, &arena)?[0];
    for i in 0..N {
        prepared_stmt.bind_i64(0, i as i64);
        prepared_stmt.bind_string(1, format!("name={i}"), arena.get_mut());
        assert_eq!(1, conn.execute_prepared_statement(prepared_stmt)?);
    }

    let mut rs = conn.execute_query_str("select a, b, c from t1 where a >= 100 and a < 200", &arena)?;
    let mut i = 99;
    while rs.next() {
        let s = format!("({}, {}, \"name={}\")", i + 1, i, i);
        assert_eq!(s, rs.current()?.to_string());
        i += 1;
    }
    assert_eq!(199, i);

    let mut rs = conn.execute_query_str("select b, c from t1 where b >= 100 and b < 200", &arena)?;
    i = 100;
    while rs.next() {
        let s = format!("({}, \"name={}\")", i, i);
        assert_eq!(s, rs.current()?.to_string());
        i += 1;
    }
    assert_eq!(200, i);
    Ok(())
}

#[test]
fn sql_select_col_expression() -> Result<()> {
    let junk = JunkFilesCleaner::new("tests/dbi-003");
    let zone = Arena::new_val();
    let db = DB::open(junk.ensure().path, junk.ensure().name)?;
    let conn = db.connect();

    let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b int,\n\
                c varchar(64) \n\
                index idx_b(b)\n\
            };\n\
            insert into table t1(b,c) values (22, \"hello\");\n\
            ";
    let arena = zone.get_mut();
    assert_eq!(1, conn.execute_str(sql, &arena)?);

    let mut rs = conn.execute_query_str("select a + 1, b - 20 from t1 where a = 1", &arena)?;
    assert_eq!(2, rs.columns().len());
    assert_eq!("_1", rs.column_name(0));
    assert_eq!("_2", rs.column_name(1));

    let ok = rs.next();
    assert!(rs.status.is_ok());
    assert!(ok);

    assert_eq!("(2, 2)", rs.current()?.to_string());
    Ok(())
}

#[test]
fn sql_select_agg_count() -> Result<()> {
    const N: i32 = 1000;

    let junk = JunkFilesCleaner::new("tests/dbi-004");
    let zone = Arena::new_val();
    let db = DB::open(junk.ensure().path, junk.ensure().name)?;
    let conn = db.connect();

    let sql = " create table t1 {\n\
                a int primary key auto_increment,\n\
                b int,\n\
                c varchar(64) \n\
                index idx_b(b)\n\
            };\n";
    let arena = zone.get_mut();
    assert_eq!(0, conn.execute_str(sql, &arena)?);

    let sql = "insert into table t1(b,c) values (?,?)";
    let mut prepared_stmt = conn.prepare_str(sql, &arena)?[0].clone();

    for i in 0..N {
        prepared_stmt.bind_i64(0, (i + 100) as i64);
        prepared_stmt.bind_string(1, format!("timeline-{i}"), arena.get_mut());
        assert_eq!(1, conn.execute_prepared_statement(&mut prepared_stmt)?);
    }

    let mut rs = conn.execute_query_str("select count(a) + 999 from t1 where a > 0", &arena)?;
    assert!(rs.next());
    assert_eq!("(1999)", rs.current()?.to_string()); // 1000 + 999
    Ok(())
}

#[test]
fn sql_select_agg_count_with_group_by() -> Result<()> {
    let junk = JunkFilesCleaner::new("tests/dbi-005");
    let zone = Arena::new_val();
    let db = DB::open(junk.ensure().path, junk.ensure().name)?;
    let conn = db.connect();

    let sql = " create table t1 {\n\
                id int primary key auto_increment,\n\
                record int,\n\
                name varchar(64) \n\
                index idx_name(name)\n\
            };\n";
    let arena = zone.get_mut();
    assert_eq!(0, conn.execute_str(sql, &arena)?);

    let affected_rows = conn.execute_str("\
insert into table t1(record, name) values
(1, \"John\"),
(2, \"John\"),
(3, \"John\"),
(4, \"Tom\"),
(1, \"Tom\"),
(1, \"Jerry\"),
(1, \"Jerry\"),
(1, \"Jerry\"),
(1, \"Jerry\"),
(1, \"ST\");
    ", &arena)?;
    assert_eq!(10, affected_rows);

    let mut rs = conn.execute_query_str("select name, count(1) from t1 where id > 0 group by name", &arena)?;
    // while rs.next() {
    //     let row = rs.current()?;
    //     println!("{}", row.to_string());
    // }
    assert!(rs.next());
    assert_eq!("(\"Jerry\", 4)", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(\"John\", 3)", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(\"ST\", 1)", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(\"Tom\", 2)", rs.current()?.to_string());
    assert!(!rs.next());
    drop(rs);

    let mut rs = conn.execute_query_str("select name, count(1)
    from t1
    group by name
    having count(1) > 2
    ", &arena)?;
    assert!(rs.next());
    assert_eq!("(\"Jerry\", 4)", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(\"John\", 3)", rs.current()?.to_string());
    assert!(!rs.next());
    Ok(())
}

#[test]
fn sql_select_join_small_tables() -> Result<()> {
    let junk = JunkFilesCleaner::new("tests/dbi-006");
    let zone = Arena::new_val();
    let db = DB::open(junk.ensure().path, junk.ensure().name)?;
    let conn = db.connect();

    let arena = zone.get_mut();
    conn.execute_file(Path::new("testdata/t1_t2_small_data_for_join.sql"), &arena)?;

    let mut rs = conn.execute_query_str("select * from t1 a left join t2 b on (a.id = b.id);", &arena)?;
    assert!(rs.next());
    assert_eq!("(1, 100, 1, \"hello\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(2, 200, 2, \"lol\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(3, 300, 3, \"world\")", rs.current()?.to_string());
    assert!(!rs.next());

    let mut rs = conn.execute_query_str("select * from t1 a right join t2 b on (a.id = b.id);", &arena)?;
    assert!(rs.next());
    assert_eq!("(NULL, NULL, 0, \"none\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(1, 100, 1, \"hello\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(2, 200, 2, \"lol\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(3, 300, 3, \"world\")", rs.current()?.to_string());
    assert!(!rs.next());

    let mut rs = conn.execute_query_str("select * from t1 a inner join t2 b on (a.id = b.id);", &arena)?;
    assert!(rs.next());
    assert_eq!("(1, 100, 1, \"hello\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(2, 200, 2, \"lol\")", rs.current()?.to_string());
    assert!(rs.next());
    assert_eq!("(3, 300, 3, \"world\")", rs.current()?.to_string());
    assert!(!rs.next());

    Ok(())
}

#[test]
fn sql_simple_delete_all() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-007")?;
    suite.execute_file(Path::new("testdata/t1_with_pk_and_data.sql"), &suite.arena)?;

    assert_eq!(9, suite.execute_str("delete from t1;", &suite.arena)?);

    let mut rs = suite.execute_query_str("select count(*) from t1;", &suite.arena)?;
    assert!(rs.next());
    assert_eq!(Some(0), rs.current()?.get_i64(0));
    assert!(!rs.next());
    Ok(())
}

#[test]
fn sql_simple_delete_one() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-008")?;
    suite.execute_file(Path::new("testdata/t1_t2_small_data_for_join.sql"), &suite.arena)?;

    assert_eq!(1, suite.execute_str("delete from t1 where id = 2", &suite.arena)?);

    let data = [
        "(1, 100)",
        "(3, 300)",
    ];
    let rs = suite.execute_query_str("select * from t1;", &suite.arena)?;
    SqlSuite::assert_rows(&data, rs)?;

    Ok(())
}

#[test]
fn sql_multi_delete_one() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-009")?;
    suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

    let data = [
        "(2, 101, \"Jc\", 2, 101, \"Jc\")",
    ];
    let rs = suite.execute_query_str("select * from t3 right join t4 on(t3.id = t4.id) where t3.id = 2", &suite.arena)?;
    SqlSuite::assert_rows(&data, rs)?;

    suite.execute_str("delete t3, t4 from t3 right join t4 on (t3.id = t4.id) where t3.id = 2", &suite.arena)?;

    let data = [
        "(1, 100, \"Js\", 1, 101, \"Js\")",
        "(3, 102, \"Jk\", 3, 102, \"Jk\")",
    ];
    let rs = suite.execute_query_str("select * from t3 right join t4 on(t3.id = t4.id)", &suite.arena)?;
    SqlSuite::assert_rows(&data, rs)?;
    Ok(())
}

#[test]
fn sql_update_one_column() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-010")?;
    suite.execute_file(Path::new("testdata/t1_with_pk_and_data.sql"), &suite.arena)?;

    suite.execute_str("update t1 set name = \"bbb\" where id = 3", &suite.arena)?;

    let data = [
        "(1, \"a\", \"xxx\", 200)",
        "(2, \"aa\", \"xxx\", 300)",
        "(3, \"bbb\", \"xxx\", 301)",
        "(4, \"aaa\", \"xxx\", 400)",
        "(5, \"aaaa\", \"xxx\", 500)",
        "(6, \"aaaaa\", \"xxx\", 600)",
        "(7, \"aaaaa\", \"xxx\", 601)",
        "(8, \"aaaaa\", \"xxx\", 602)",
        "(9, \"aaaaaa\", \"xxx\", 700)",
    ];
    let rs = suite.execute_query_str("select * from t1", &suite.arena)?;
    SqlSuite::assert_rows(&data, rs)?;
    Ok(())
}

#[test]
fn sql_update_with_primary_key() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-011")?;
    suite.execute_file(Path::new("testdata/t1_with_pk_and_data.sql"), &suite.arena)?;

    suite.execute_str("update t1 set id = 30, name = \"cc\" where id = 3", &suite.arena)?;

    let data = [
        "(1, \"a\", \"xxx\", 200)",
        "(2, \"aa\", \"xxx\", 300)",
        "(4, \"aaa\", \"xxx\", 400)",
        "(5, \"aaaa\", \"xxx\", 500)",
        "(6, \"aaaaa\", \"xxx\", 600)",
        "(7, \"aaaaa\", \"xxx\", 601)",
        "(8, \"aaaaa\", \"xxx\", 602)",
        "(9, \"aaaaaa\", \"xxx\", 700)",
        "(30, \"cc\", \"xxx\", 301)",
    ];
    let rs = suite.execute_query_str("select * from t1", &suite.arena)?;
    SqlSuite::assert_rows(&data, rs)?;
    Ok(())
}

#[test]
fn sql_multi_update_one() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-012")?;
    suite.execute_file(Path::new("testdata/t3_t4_small_data_for_join.sql"), &suite.arena)?;

    let sql = "update t3 \
    inner join t4 on (t3.id = t4.id) \
    set \
        t3.name = \"cc\"\
        , t4.name = \"dd\" \
    where t4.df = 101";
    assert_eq!(4, suite.execute_str(sql, &suite.arena)?);

    let data = [
        "(1, 100, \"cc\", 1, 101, \"dd\")",
        "(2, 101, \"cc\", 2, 101, \"dd\")",
    ];
    let rs = suite.execute_query_str("select * from t3 inner join t4 on (t3.id = t4.id) where t4.df = 101", &suite.arena)?;
    SqlSuite::assert_rows(&data, rs)?;

    Ok(())
}

#[test]
fn sql_alert_add_col() -> Result<()> {
    let suite = SqlSuite::new("tests/dbi-013")?;
    suite.execute_file(Path::new("testdata/t1_with_pk_and_data.sql"), &suite.arena)?;

    suite.execute_str("alert table t1 add column c varchar(16)", &suite.arena)?;

    let table = suite.db._test_get_table_ref("t1").unwrap();
    {
        let rs = table.get_col_by_name(&"c".to_string());
        assert!(rs.is_some());
        let col = rs.unwrap();
        assert_eq!("c", col.name);
        assert!(!col.not_null);
        assert_eq!(4, col.order);
    }
    drop(table);

    suite.execute_str("alert table t1 add column ddd int after a", &suite.arena)?;
    let table = suite.db._test_get_table_ref("t1").unwrap();
    {
        let rs = table.get_col_by_name(&"ddd".to_string());
        assert!(rs.is_some());
        let col = rs.unwrap();
        assert_eq!("ddd", col.name);
        assert!(!col.not_null);
        assert_eq!(3, col.order);
    }
    drop(table);

    assert_eq!(9, suite.execute_str("alert table t1 add column eff int not null default 0 first", &suite.arena)?);
    let table = suite.db._test_get_table_ref("t1").unwrap();
    {
        let rs = table.get_col_by_name(&"eff".to_string());
        assert!(rs.is_some());
        let col = rs.unwrap();
        assert_eq!("eff", col.name);
        assert!(col.not_null);
        assert_eq!(0, col.order);
    }

    Ok(())
}