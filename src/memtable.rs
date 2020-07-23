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
        let data = BTreeSet::new();


        MemTable {
            config: config.clone(),
            schema: schema.clone(),
            data,
            size: 0
        }
    }
}