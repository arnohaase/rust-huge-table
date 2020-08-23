use std::sync::Arc;

use crate::table::{TableSchema, DetachedRowData};
use std::collections::BTreeSet;
use crate::sstable::TableConfig;

pub struct MemTable {
    config: Arc<TableConfig>,
    schema: Arc<TableSchema>,
    data: BTreeSet<DetachedRowData>,
    size: usize,
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
        let to_be_added = match self.data.take(&row) {
            None => row,
            Some(prev) => {
                self.size -= prev.row_data_view().buf.len();
                row.row_data_view().merge(&prev.row_data_view())
            },
        };

        self.size += &to_be_added.row_data_view().buf.len();
        assert!(self.data.insert(to_be_added));
    }
}