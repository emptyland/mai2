use std::fmt::Write;

const LOCK_NAME: &str = "LOCK";
const CURRENT_NAME: &str = "CURRENT";
const MANIFEST_PREFIX: &str = "MANIFEST-";
const LOG_POSTFIX: &str = ".log";
const SST_TABLE_POSTFIX: &str = ".sst";

pub mod paths {
    use std::path::{Path, PathBuf};
    use super::*;

    pub fn log_file(db_path: &Path, number: u64) -> PathBuf {
        db_path.to_path_buf().join(Path::new(&format!("{}{}", number, LOG_POSTFIX)))
    }

    pub fn table_file(db_path: &Path, cf_name: &String, number: u64) -> PathBuf {
        db_path.to_path_buf()
            .join(cf_name)
            .join(format!("{}{}", number, SST_TABLE_POSTFIX))
    }

    pub fn table_file_by_cf(cf_path: &Path, number: u64) -> PathBuf {
        cf_path.to_path_buf().join(format!("{}{}", number, SST_TABLE_POSTFIX))
    }

    pub fn manifest_file(db_path: &Path, number: u64) -> PathBuf {
        db_path.to_path_buf().join(format!("{}{}", MANIFEST_PREFIX, number))
    }

    pub fn current_file(db_path: &Path) -> PathBuf {
        db_path.to_path_buf().join(CURRENT_NAME)
    }

    pub fn lock_file(db_path: &Path) -> PathBuf {
        db_path.to_path_buf().join(LOCK_NAME)
    }
}

pub fn log_file_name(db_name: &String, number: u64) -> String {
    let mut  name = String::new();
    name.write_fmt(format_args!("{}/{}{}", db_name, number, LOG_POSTFIX)).unwrap();
    name
}

pub fn table_file_name(db_name: &String, cf_name: &String, number: u64) -> String {
    format!("{}/{}/{}{}", db_name, cf_name, number, SST_TABLE_POSTFIX)
}

pub fn table_file_name_by_cf(cf_path: &String, number: u64) -> String {
    format!("{}/{}{}", cf_path, number, SST_TABLE_POSTFIX)
}

pub fn manifest_file_name(db_name: &String, number: u64) -> String {
    format!("{}/{}{}", db_name, MANIFEST_PREFIX, number)
}

pub fn current_file_name(db_name: &String) -> String {
    format!("{}/{}", db_name, CURRENT_NAME)
}

pub fn lock_file_name(db_name: &String) -> String {
    format!("{}/{}", db_name, LOCK_NAME)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use super::*;

    #[test]
    fn sanity() {
        assert_eq!("db/1.log", log_file_name(&String::from("db"), 1));
        assert_eq!("db/2.log", log_file_name(&String::from("db"), 2));
    }

    #[test]
    fn table_name() {
        assert_eq!("db/cf/1.sst", table_file_name(&String::from("db"), &String::from("cf"), 1));
        assert_eq!("db/cf/2.sst", table_file_name(&String::from("db"), &String::from("cf"), 2));
    }

    #[test]
    fn log_file_path() {
        let path = paths::log_file(Path::new("db"), 1);
        assert_eq!(Path::new("db/1.log"), path);
    }

    #[test]
    fn table_file_path() {
        let path = paths::table_file(Path::new("db"), &String::from("cf"), 2);
        assert_eq!(Path::new("db/cf/2.sst"), path);

        let p2 = paths::table_file_by_cf(Path::new("db/cf"), 3);
        assert_eq!(Path::new("db/cf/3.sst"), p2);
    }

    #[test]
    fn manifest_file_path() {
        let path = paths::manifest_file(Path::new("db"), 1);
        assert_eq!(Path::new("db/MANIFEST-1"), path)
    }
}