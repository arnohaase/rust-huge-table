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
}

impl SsTable {
    pub fn create<'a, RI>(config: Arc<TableConfig>,
                          schema: Arc<TableSchema>,
                          rows: &mut RI)
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

    pub fn open(config: Arc<TableConfig>, schema: Arc<TableSchema>, name_base: &str) -> HtResult<SsTable> {
        let index_file = config.new_file(&name_base, "index", false)?;
        let data_file = config.new_file(&name_base, "data", false)?;
        let index_mmap = unsafe { MmapOptions::new().map(&index_file) }?;
        let data_mmap = unsafe { MmapOptions::new().map(&data_file) }?;

        Ok(SsTable { schema, index_mmap, data_mmap })
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
