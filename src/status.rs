
#[derive(Debug)]
pub enum Status {
    Ok,
    Corruption(String),
    NotFound,
}
