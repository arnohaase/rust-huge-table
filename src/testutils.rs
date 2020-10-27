

use std::sync::Arc;
use crate::config::TableConfig;
use std::path::PathBuf;
use crate::table::{TableSchema, ColumnSchema, ColumnId, ColumnType, PrimaryKeySpec, DetachedRowData, ColumnData, ColumnValue, RowData};
use uuid::Uuid;
use crate::time::{ManualClock, MergeTimestamp, HtClock};

const TEST_DIR: &str = "__test__";

pub fn test_table_config() -> Arc<TableConfig> {
    let base_folder = PathBuf::from(TEST_DIR);
    match std::fs::create_dir(&base_folder) {
        Ok(_) => println!("creating folder {:?}", &base_folder),
        Err(_) => {}
    }

    Arc::new(TableConfig {
        base_folder
    })
}


pub struct SimpleTableTestSetup {
    pub schema: Arc<TableSchema>,
    pub clock: ManualClock,
}

impl SimpleTableTestSetup {
    pub fn new() -> SimpleTableTestSetup {
        SimpleTableTestSetup {
            schema: SimpleTableTestSetup::table_schema(),
            clock: ManualClock::new(MergeTimestamp::from_ticks(12345)),
        }
    }

    fn table_schema() -> Arc<TableSchema> {
        Arc::new(TableSchema::new ("test_table", &Uuid::new_v4(), vec!(
            ColumnSchema {
                col_id: ColumnId(0),
                name: "pk".to_string(),
                tpe: ColumnType::BigInt,
                pk_spec: PrimaryKeySpec::PartitionKey
            },
            ColumnSchema {
                col_id: ColumnId(1),
                name: "text".to_string(),
                tpe: ColumnType::Text,
                pk_spec: PrimaryKeySpec::Regular
            },
            ColumnSchema {
                col_id: ColumnId(2),
                name: "int".to_string(),
                tpe: ColumnType::Int,
                pk_spec: PrimaryKeySpec::Regular
            },
        )))
    }

    pub fn full_row(&self, pk: i64, text: Option<&'static str>, int: Option<i64>) -> DetachedRowData {
        DetachedRowData::assemble(&self.schema,
                                  &vec!(
                                      ColumnData::new (ColumnId(0),self.clock.now(),None,Some(ColumnValue::BigInt(pk))),
                                      ColumnData::new (ColumnId(1), self.clock.now(), None, text.map(|t| ColumnValue::Text(t))),
                                      ColumnData::new (ColumnId(2), self.clock.now(), None, int.map(|i| ColumnValue::BigInt(i))),
                                  ),
        )
    }

    pub fn partial_row(&self, pk: i64, text: Option<&'static str>) -> DetachedRowData {
        DetachedRowData::assemble(&self.schema,
                                  &vec!(
                                      ColumnData::new (ColumnId(0),self.clock.now(),None,Some(ColumnValue::BigInt(pk))),
                                      ColumnData::new (ColumnId(1), self.clock.now(), None, text.map(|t| ColumnValue::Text(t))),
                                  ),
        )
    }

    pub fn pk_row(&self, pk: i64) -> DetachedRowData {
        DetachedRowData::assemble(&self.schema,
                                  &vec!(ColumnData::new(ColumnId(0), self.clock.now(), None, Some(ColumnValue::BigInt(pk)))))
    }

    pub fn pk(&self, row: &RowData) -> i64 {
        match row.read_col_by_id(ColumnId(0)).unwrap().value.unwrap() {
            ColumnValue::BigInt(v) => v,
            _ => panic!("no pk value")
        }
    }

    pub fn value<'a>(&self, row: &'a RowData) -> &'a str {
        match row.read_col_by_id(ColumnId(1)).unwrap().value.unwrap() {
            ColumnValue::Text(v) => v,
            _ => panic!("no value")
        }
    }

}