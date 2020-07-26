use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::PathBuf;
use std::slice::from_raw_parts;
use std::sync::Arc;

use memmap::{Mmap, MmapOptions};
use uuid::Uuid;

use crate::prelude::*;
use crate::primitives::*;
use crate::table::*;


pub struct TableConfig {
    base_folder: PathBuf,
}

impl TableConfig {
    pub fn new_file(&self, name_base: &str, extension: &str, writeable: bool) -> std::io::Result<File> {
        let mut path = self.base_folder.clone();
        path.push(format!("{}.{}", name_base, extension));

        OpenOptions::new()
            .create(writeable)
            .write(writeable)
            .read(true)
            .open(&path)
    }
}

struct SsTable {
    schema: Arc<TableSchema>,
    index_mmap: Mmap,
    data_mmap: Mmap,
    name_base: String,
}

impl SsTable {
    pub fn create<'a, RI>(config: &Arc<TableConfig>,
                          schema: &Arc<TableSchema>,
                          rows: RI)
                          -> HtResult<SsTable>
        where RI: Iterator<Item=RowData<'a>> {
        let name_base = format!("{}-{}", schema.name, uuid::Uuid::new_v4().to_string());

        let mut index_file = config.new_file(&name_base, "index", true)?;
        let mut data_file = config.new_file(&name_base, "data", true)?;

        for row in rows {
            let pos = data_file.seek(SeekFrom::Current(0))?;
            index_file.encode_fixed_u64(pos)?;

            row.write_to(&mut data_file)?;
        }

        //TODO marker to handle crash during indexing robustly
        //TODO hash to verify integrity
        //TODO Bloom Filter
        index_file.flush()?;
        data_file.flush()?;

        SsTable::open(config, schema, &name_base)
    }

    pub fn open(config: &Arc<TableConfig>, schema: &Arc<TableSchema>, name_base: &str) -> HtResult<SsTable> {
        let index_file = config.new_file(&name_base, "index", false)?;
        let data_file = config.new_file(&name_base, "data", false)?;
        let index_mmap = unsafe { MmapOptions::new().map(&index_file) }?;
        let data_mmap = unsafe { MmapOptions::new().map(&data_file) }?;

        Ok(SsTable { schema: schema.clone(), index_mmap, data_mmap, name_base: name_base.to_string() })
    }

    pub fn find_by_full_pk(&self, pks: &RowData<'_>) -> HtResult<Option<RowData>> {
        let mut err = None;

        let result = self.index_slice().binary_search_by(|offs| {
            match self.data_at(*offs) {
                _ if err.is_some() => Ordering::Equal,
                Ok(row) => row.compare_by_pk(pks),
                Err(e) => {
                    err = Some(e);
                    Ordering::Equal
                }
            }
        });

        match (result, err) {
            (_, Some(e)) => Err(e),
            (Err(_), _) => Ok(None),
            (Ok(idx), _) => {
                let offs = self.index_slice()[idx];
                Ok(Some(self.data_at(offs)?))
            }
        }
    }

    fn index_slice(&self) -> &[u64] {
        let len = self.index_mmap.len() / size_of::<u64>();
        let ptr = self.index_mmap.as_ptr() as *const u64;
        unsafe { from_raw_parts(ptr, len) }
    }

    fn data_at(&self, offs: u64) -> HtResult<RowData> {
        let mut offs = offs as usize;
        let len = self.data_mmap.decode_varint_usize(&mut offs);
        Ok(RowData::from_view(&self.schema, &self.data_mmap[offs..offs+len]))
    }
}

#[cfg(test)]
mod test {
    use crate::sstable::{TableConfig, SsTable};
    use std::sync::Arc;
    use crate::table::{TableSchema, ColumnSchema, ColumnType, PrimaryKeySpec, ColumnData, DetachedRowData, RowFlags, ColumnFlags, ColumnValue, RowData, ColumnId};
    use uuid::Uuid;
    use std::path::PathBuf;
    use crate::primitives::DecodePrimitives;
    use crate::time::{ManualClock, MergeTimestamp, HtClock};

    const TEST_DIR: &str = "__test__";

    fn table_config() -> Arc<TableConfig> {
        let base_folder = PathBuf::from(TEST_DIR);
        match std::fs::create_dir(&base_folder) {
            Ok(_) => println!("creating folder {:?}", &base_folder),
            Err(_) => {}
        }

        Arc::new(TableConfig {
            base_folder
        })
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
            }
        )))
    }

    #[test]
    pub fn test_simple() {
        let config = table_config();
        let schema = table_schema();

        fn row(pk: i64, text: Option<&'static str>) -> DetachedRowData {
            let table_schema = table_schema();
            let clock = ManualClock::new(MergeTimestamp::from_ticks(123456789));

            DetachedRowData::assemble(&table_schema,
                                      &vec!(
                                          ColumnData::new (ColumnId(0),clock.now(),None,Some(ColumnValue::BigInt(pk))),
                                          ColumnData::new (ColumnId(1), clock.now(), None, text.map(|t| ColumnValue::Text(t))),
                                      ),
            )
        }

        fn pk(row: &RowData) -> i64 {
            match row.read_col_by_id(ColumnId(0)).unwrap().value.unwrap() {
                ColumnValue::BigInt(v) => v,
                _ => panic!("no pk value")
            }
        }
        fn value<'a>(row: &'a RowData) -> &'a str {
            match row.read_col_by_id(ColumnId(1)).unwrap().value.unwrap() {
                ColumnValue::Text(v) => v,
                _ => panic!("no value")
            }
        }

        fn check(ss_table: &SsTable) {
            let found = ss_table.find_by_full_pk(&row(1, None).row_data_view()).unwrap().unwrap();
            assert_eq!(pk(&found), 1);
            assert_eq!(value(&found), "a");

            let found = ss_table.find_by_full_pk(&row(3, None).row_data_view()).unwrap().unwrap();
            assert_eq!(pk(&found), 3);
            assert_eq!(value(&found), "b");

            let found = ss_table.find_by_full_pk(&row(5, None).row_data_view()).unwrap().unwrap();
            assert_eq!(pk(&found), 5);
            assert_eq!(value(&found), "c");

            let found = ss_table.find_by_full_pk(&row(7, None).row_data_view()).unwrap().unwrap();
            assert_eq!(pk(&found), 7);
            assert_eq!(value(&found), "d");

            assert!(ss_table.find_by_full_pk(&row(0, None).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&row(2, None).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&row(4, None).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&row(6, None).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&row(8, None).row_data_view()).unwrap().is_none());
        }

        let rows = vec!(
            row(1, Some("a")),
            row(3, Some("b")),
            row(5, Some("c")),
            row(7, Some("d")),
        );

        let it = rows.iter().map(|r| r.row_data_view());
        let ss_table = SsTable::create(&config, &schema, it).unwrap();
        check(&ss_table);

        let ss_table = SsTable::open(&config, &schema, &ss_table.name_base).unwrap();
        check(&ss_table);
    }
}
