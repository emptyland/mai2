use mai2::exec::db::DB;
use mai2::storage::JunkFilesCleaner;
use mai2::{Arena, Result};

#[test]
fn sql_create_table_and_insert() -> Result<()> {
    let _junk = JunkFilesCleaner::new("tests/dbi-001");
    let arena = Arena::new_val();
    let db = DB::open("tests".to_string(), "dbi-001".to_string())?;
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

    let _junk = JunkFilesCleaner::new("tests/dbi-002");
    let zone = Arena::new_val();
    let db = DB::open("tests".to_string(), "dbi-002".to_string())?;
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