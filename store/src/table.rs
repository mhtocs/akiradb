#![allow(dead_code)]

use crate::schema::Schema;
use std::path::PathBuf;

struct Table {
    blocks: Vec<TableBlock>,
}

struct TableBlock {
    name: String,
    schema: Schema,
    path: PathBuf,
}

impl TableBlock {
    fn scan(&self) {}
    fn read(&self) {}
}
