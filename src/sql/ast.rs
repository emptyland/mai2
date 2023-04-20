

pub enum Statement {
    CreateTable(CreateTable),
}

pub struct CreateTable {
    pub table_name: String
}