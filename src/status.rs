
#[derive(Debug, Clone)]
pub enum Status {
    Ok,
    Corruption(String),
    NotFound,
}

pub trait Corrupting<T> : Sized {
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


