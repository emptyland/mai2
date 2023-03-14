
#[derive(Debug)]
pub enum Status {
    Ok,
    Error(String),
    NotFound,
}
