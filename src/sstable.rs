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
        where RI: Iterator<Item=Row<'a>> {
        let name_base = format!("{}-{}", schema.name, uuid::Uuid::new_v4().to_string());

        let mut index_file = config.new_file(&name_base, "index", true)?;
        let mut data_file = config.new_file(&name_base, "data", true)?;

        for row in rows {
            let pos = data_file.seek(SeekFrom::Current(0))?;
            index_file.encode_fixed_u64(pos)?;

            data_file.encode_varint_u64(row.values.len() as u64)?;
            for (col_id, d) in row.values.iter() {
                SsTable::write_col(&mut data_file, *col_id, d)?;
            }
        }

        SsTable::open(config, schema, &name_base)
    }

    fn write_col<W>(w: &mut W, col_id: u32, data: &ColumnData<'_>) -> std::io::Result<()>
        where W: Write
    {
        w.encode_varint_u32(col_id)?;
        match data.value {
            None => w.encode_varint_u32(0), //TODO other column flags
            Some(v) => {
                w.encode_varint_u32(1)?;
                match v {
                    ColumnValue::Boolean(v) => w.encode_bool(v),
                    ColumnValue::Int(v) => w.encode_varint_i32(v),
                    ColumnValue::BigInt(v) => w.encode_varint_i64(v),
                    ColumnValue::Text(v) => w.encode_utf8(v),
                }
            }
        }
    }

    pub fn open(config: Arc<TableConfig>, schema: Arc<TableSchema>, name_base: &str) -> HtResult<SsTable> {
        let index_file = config.new_file(&name_base, "index", false)?;
        let data_file = config.new_file(&name_base, "data", false)?;
        let index_mmap = unsafe { MmapOptions::new().map(&index_file) }?;
        let data_mmap = unsafe { MmapOptions::new().map(&data_file) }?;

        Ok(SsTable { schema, index_mmap, data_mmap })
    }

    pub fn find_by_full_pk(&self, pks: &Row<'_>) -> HtResult<Option<Row>> {
        let mut err = None;

        let result = self.index_slice().binary_search_by(|offs| {
            match self.data_at(*offs, true) {
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
                Ok(Some(self.data_at(offs, false)?))
            }
        }
    }

    fn index_slice(&self) -> &[u64] {
        let len = self.index_mmap.len() / size_of::<u64>();
        let ptr = self.index_mmap.as_ptr() as *const u64;
        unsafe { from_raw_parts(ptr, len) }
    }

    fn data_at(&self, offs: u64, pk_only: bool) -> HtResult<Row> {
        let mut values = BTreeMap::new();
        let mut offs = offs as usize;

        let buf = &self.data_mmap;
        let mut num_columns = buf.decode_varint_u64(&mut offs);
        if pk_only {
            num_columns = self.schema.pk_columns.len() as u64;
        }

        for _ in 0..num_columns {
            let col_id = buf.decode_varint_u32(&mut offs);
            let flags = buf.decode_varint_u32(&mut offs);

            let data = if flags == 0 { //TODO other column flags
                ColumnData { value: None }
            } else {
                let column_value = match self
                    .schema.column(col_id)?
                    .tpe {
                    ColumnType::Boolean => ColumnValue::Boolean(buf.decode_bool(&mut offs)),
                    ColumnType::Int => ColumnValue::Int(buf.decode_varint_i32(&mut offs)),
                    ColumnType::BigInt => ColumnValue::BigInt(buf.decode_varint_i64(&mut offs)),
                    ColumnType::Text => ColumnValue::Text(buf.decode_utf8(&mut offs)),
                };
                ColumnData { value: Some(column_value) }
            };

            values.insert(col_id, data);
        }
        Ok(Row {
            schema: self.schema.clone(),
            values,
        })
    }
}
