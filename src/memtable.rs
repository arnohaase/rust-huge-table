use std::collections::BTreeSet;
use std::sync::Arc;

use crate::config::TableConfig;
use crate::table::{DetachedRowData, TableSchema};

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

    pub fn get(&self, pk_data: &DetachedRowData) -> Option<&DetachedRowData> {
        self.data.get(pk_data)
    }
}


#[cfg(test)]
mod test {
    use crate::memtable::MemTable;
    use crate::table::{ColumnId, ColumnValue};
    use crate::testutils::{SimpleTableTestSetup, test_table_config};
    use crate::time::{HtClock, MergeTimestamp};

    #[test]
    pub fn test_simple() {
        let config = test_table_config();
        let setup = SimpleTableTestSetup::new();

        let mut mem_table = MemTable::new(&config, &setup.schema);
        assert_eq!(0, mem_table.size);

        let row = setup.full_row(1, Option::Some("abc"), Option::Some(123));
        mem_table.add(row);
        assert!(mem_table.size > 0);

        let opt_found = mem_table.get(&setup.pk_row(1));
        let found = opt_found.unwrap();
        let data_view = found.row_data_view();
        let data = data_view.read_col_by_id(ColumnId(1)).unwrap();
        assert_eq!(ColumnValue::Text("abc"), data.value.unwrap());
        assert_eq!(ColumnValue::Int(123), data_view.read_col_by_id(ColumnId(2)).unwrap().value.unwrap());

        // different pk -> not found
        let found = mem_table.get(&setup.pk_row(2));
        assert!(found.is_none());

        // pk row has a different timestamp
        setup.clock.set(MergeTimestamp::from_ticks(0));
        let opt_found = mem_table.get(&setup.pk_row(1));
        let found = opt_found.unwrap();
        let data_view = found.row_data_view();
        assert_eq!(ColumnValue::Text("abc"), data_view.read_col_by_id(ColumnId(1)).unwrap().value.unwrap());
        assert_eq!(ColumnValue::Int(123), data_view.read_col_by_id(ColumnId(2)).unwrap().value.unwrap());

        // merge updates
        setup.clock.set(MergeTimestamp::from_ticks(999999));
        mem_table.add(setup.partial_row(1, Option::Some("xyz")));
        let opt_found = mem_table.get(&setup.pk_row(1));
        let found = opt_found.unwrap();
        let data_view = found.row_data_view();
        assert_eq!(ColumnValue::Text("xyz"), data_view.read_col_by_id(ColumnId(1)).unwrap().value.unwrap());
        assert_eq!(ColumnValue::Int(123), data_view.read_col_by_id(ColumnId(2)).unwrap().value.unwrap());




        // second row
    }

    //TODO expiry
    //TODO with cluster key
    //TODO merging update
}
