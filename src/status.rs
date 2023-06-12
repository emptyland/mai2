#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Ok,
    Corruption(String),
    NotFound,
    Eof,
}

#[macro_export]
macro_rules! corrupted_err {
    ($($arg:tt)+) => {
        Err($crate::corrupted!($($arg)+))
    }
}

#[macro_export]
macro_rules! corrupted {
    ($($arg:tt)+) => {
        $crate::Status::corrupted(format!($($arg)+))
    };
}

impl Status {
    pub fn is_ok(&self) -> bool { matches!(self, Self::Ok) }
    pub fn is_corruption(&self) -> bool { matches!(self, Self::Corruption(_)) }
    pub fn is_not_found(&self) -> bool { matches!(self, Self::NotFound) }
    pub fn is_not_ok(&self) -> bool { !self.is_ok() }
}

pub trait Corrupting<T>: Sized {
    fn corrupted(message: T) -> Status;
}

impl Corrupting<String> for Status {
    fn corrupted(message: String) -> Status {
        Status::Corruption(message)
    }
}

impl Corrupting<&str> for Status {
    fn corrupted(message: &str) -> Status {
        Status::Corruption(String::from(message))
    }
}


