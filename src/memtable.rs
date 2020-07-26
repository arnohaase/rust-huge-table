use std::sync::Arc;

use crate::table::{TableSchema, DetachedRowData};
use std::collections::BTreeSet;
use crate::sstable::TableConfig;

pub struct MemTable {
    config: Arc<TableConfig>,
    schema: Arc<TableSchema>,
    data: BTreeSet<DetachedRowData>,
    size: u64,
}

impl MemTable {
    pub fn new(config: &Arc<TableConfig>, schema: &Arc<TableSchema>) -> MemTable {
        MemTable {
            config: config.clone(),
            schema: schema.clone(),
            data: BTreeSet::new(),
            size: 0
        }
    }

    pub fn add(&mut self, row: DetachedRowData) {
        match self.data.take(&row) {
            None => self.data.insert(row),
            Some(prev) => {
                self.data.insert(row.merge(&prev.row_data_view()))
            }
        };
    }
}