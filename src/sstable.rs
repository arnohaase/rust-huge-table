use std::cmp::Ordering;
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;
use std::slice::from_raw_parts;
use std::sync::Arc;

use memmap::{Mmap, MmapOptions};

use crate::config::TableConfig;
use crate::prelude::*;
use crate::primitives::*;
use crate::table::*;

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
    use crate::sstable::SsTable;
    use crate::testutils::{SimpleTableTestSetup, test_table_config};

    #[test]
    pub fn test_simple() {
        let config = test_table_config();

        let setup = SimpleTableTestSetup::new();

        fn check(setup: &SimpleTableTestSetup, ss_table: &SsTable) {
            let found = ss_table.find_by_full_pk(&setup.pk_row(1).row_data_view()).unwrap().unwrap();
            assert_eq!(setup.pk(&found), 1);
            assert_eq!(setup.value(&found), "a");

            let found = ss_table.find_by_full_pk(&setup.pk_row(3).row_data_view()).unwrap().unwrap();
            assert_eq!(setup.pk(&found), 3);
            assert_eq!(setup.value(&found), "b");

            let found = ss_table.find_by_full_pk(&setup.pk_row(5).row_data_view()).unwrap().unwrap();
            assert_eq!(setup.pk(&found), 5);
            assert_eq!(setup.value(&found), "c");

            let found = ss_table.find_by_full_pk(&setup.pk_row(7).row_data_view()).unwrap().unwrap();
            assert_eq!(setup.pk(&found), 7);
            assert_eq!(setup.value(&found), "d");

            assert!(ss_table.find_by_full_pk(&setup.pk_row(0).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&setup.pk_row(2).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&setup.pk_row(4,).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&setup.pk_row(6).row_data_view()).unwrap().is_none());
            assert!(ss_table.find_by_full_pk(&setup.pk_row(8).row_data_view()).unwrap().is_none());
        }

        let rows = vec!(
            setup.full_row(1, Some("a"), None),
            setup.full_row(3, Some("b"), None),
            setup.full_row(5, Some("c"), None),
            setup.full_row(7, Some("d"), None),
        );

        let it = rows.iter().map(|r| r.row_data_view());
        let ss_table = SsTable::create(&config, &setup.schema, it).unwrap();
        check(&setup, &ss_table);

        let ss_table = SsTable::open(&config, &setup.schema, &ss_table.name_base).unwrap();
        check(&setup, &ss_table);
    }
}
