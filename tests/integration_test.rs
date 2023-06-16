use std::path::Path;
use mai2::exec::db::DB;
use mai2::storage::JunkFilesCleaner;
use mai2::{Arena, Result, Status};

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