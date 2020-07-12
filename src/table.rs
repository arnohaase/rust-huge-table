use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::PathBuf;
use std::ptr::NonNull;
use std::slice::from_raw_parts;
use std::sync::Arc;

use memmap::{Mmap, MmapOptions};
use uuid::Uuid;

use crate::prelude::*;
use crate::primitives::*;

#[derive(Clone, Debug)]
pub enum ColumnType {
    Boolean,
    Int,
    BigInt,
    Text,
}

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub col_id: u32,
    pub name: String,
    pub tpe: ColumnType,
    pub pk_spec: PrimaryKeySpec,
}

impl ColumnSchema {
    fn is_primary_key(&self) -> bool {
        match self.pk_spec {
            PrimaryKeySpec::PartitionKey => true,
            PrimaryKeySpec::ClusterKey(_) => true,
            PrimaryKeySpec::Regular => false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum PrimaryKeySpec {
    PartitionKey,
    ClusterKey(bool),
    Regular,
}

#[derive(Debug)]
pub struct TableSchema {
    pub name: String,
    pub table_id: Uuid,
    pub columns: Vec<ColumnSchema>,
    pub pk_columns: Vec<ColumnSchema>,
}

impl TableSchema {
    pub fn new(name: &str, table_id: &Uuid, columns: Vec<ColumnSchema>) -> TableSchema {
        let pk_columns = columns
            .iter()
            .filter(|c| c.is_primary_key())
            .map(|c| c.clone())
            .collect();

        TableSchema {
            name: name.to_string(),
            table_id: table_id.clone(),
            columns,
            pk_columns,
        }
    }

    pub fn column(&self, col_id: u32) -> HtResult<&ColumnSchema> {
        match self.columns.iter().find(|c| c.col_id == col_id) {
            Some(c) => Ok(c),
            None => Err(HtError::misc("column not found")),
        }
    }
}


/// A wrapper around (and handle to) a byte buffer containing a row's raw data.
pub struct RowData<'a> {
    pub schema: Arc<TableSchema>,
    pub buf: &'a [u8],
}

impl<'a> RowData<'a> {
    pub fn from_view<'b>(schema: &Arc<TableSchema>, buf: &'b [u8]) -> RowData<'b> {
        RowData {
            schema: schema.clone(),
            buf,
        }
    }

    pub fn schema(&self) -> &TableSchema {
        self.schema.as_ref()
    }

    /// checks that the buffer is well-formed and fits in with the schema
    pub fn validate(&self) -> HtResult<()> {
        //TODO partition key first, then cluster key, then the rest
        //TODO all columns values have the right type
        //TODO full partition key present
        //TODO no surplus bytes at the end
        //TODO valid row flags

        //TODO full cluster key is present (if flag is set) or only leading columns and no regular columns
        //TODO ... and not null

        Ok(())
    }

    pub fn write_to<W>(&self, w: &mut W) -> HtResult<()> where W: Write {
        w.encode_varint_usize(self.buf.len())?;
        w.write_all(self.buf)?;
        Ok(())
    }

    //TODO pub fn col_value(&self, col_id: u32) -> ???

    pub fn flags(&self) -> RowFlags {
        RowFlags {
            flags: self.buf[0]
        }
    }

    /// This is not very efficient and intended for testing and debugging
    pub fn read_col_by_id(&self, col_id: u32) -> Option<ColumnData> {
        let mut offs = self.offs_start_column_data();
        while offs < self.buf.len() {
            let candidate = self.read_col(&mut offs);
            if candidate.col_id == col_id {
                return Some(candidate);
            }
        }
        None
    }

    fn read_col(&self, offs: &mut usize) -> ColumnData {
        let col_id = self.buf.decode_varint_u32(offs);
        let col_flags = ColumnFlags { flags: self.buf[*offs] };
        *offs += 1;

        if col_flags.is_null() {
            return ColumnData {
                col_id,
                flags: col_flags,
                value: None,
            };
        }

        let col_data = match self.schema.column(col_id).unwrap().tpe { //TODO error handling?
            ColumnType::Boolean => ColumnValue::Boolean(self.buf.decode_bool(offs)),
            ColumnType::Int => ColumnValue::Int(self.buf.decode_varint_i32(offs)),
            ColumnType::BigInt => ColumnValue::BigInt(self.buf.decode_varint_i64(offs)),
            ColumnType::Text => ColumnValue::Text(self.buf.decode_utf8(offs)),
        };

        ColumnData {
            col_id,
            flags: col_flags,
            value: Some(col_data),
        }
    }

    fn offs_start_column_data(&self) -> usize {
        1usize
    }

    pub fn compare_by_pk(&self, other: &RowData) -> Ordering {
        assert!(self.flags().has_full_cluster_key());
        assert!(other.flags().has_full_cluster_key());

        let mut offs_self = self.offs_start_column_data();
        let mut offs_other = other.offs_start_column_data();

        for col_meta in &self.schema.columns {
            let desc = match col_meta.pk_spec {
                PrimaryKeySpec::PartitionKey => false,
                PrimaryKeySpec::ClusterKey(asc) => !asc,
                PrimaryKeySpec::Regular => return Ordering::Equal
            };

            let col_self = self.read_col(&mut offs_self);
            let col_other = other.read_col(&mut offs_other);

            assert!(col_meta.col_id == col_self.col_id);
            assert!(col_meta.col_id == col_other.col_id);

            let cmp = match (&col_self.value, &col_other.value) {
                (Some(v1), Some(v2)) => v1.cmp(v2),
                _ => panic!("primary key columns must not be null")
            };

            match cmp {
                Ordering::Equal => {}
                _ if desc => return cmp.reverse(),
                _ => return cmp
            }
        }

        Ordering::Equal
    }
}

pub struct DetachedRowData {
    schema: Arc<TableSchema>,
    buf: Vec<u8>,
}

impl DetachedRowData {
    pub fn assemble(schema: &Arc<TableSchema>,
                    row_flags: RowFlags,
                    columns: &Vec<ColumnData>) -> HtResult<DetachedRowData> {
        let mut buf = Vec::new();
        buf.push(row_flags.flags);

        for col in columns {
            buf.encode_varint_u32(col.col_id)?;
            buf.push(col.flags.flags);

            //TODO verify that 'has_full_cluster_key' really means all cluster key columns are present
            //TODO verify that pk columns go first and are in schema order

            if col.flags.is_null() != col.value.is_none() {
                return Err(HtError::misc("column not null flag mismatch"));
            }

            //TODO verify that pk columns can not be null - absent is ok for incomplete rows, but explicit values of null are not

            match col.value {
                None => {}
                Some(ColumnValue::Boolean(v)) => buf.encode_bool(v)?,
                Some(ColumnValue::Int(v)) => buf.encode_varint_i32(v)?,
                Some(ColumnValue::BigInt(v)) => buf.encode_varint_i64(v)?,
                Some(ColumnValue::Text(v)) => buf.encode_utf8(v)?,
            }
        }

        Ok(DetachedRowData {
            schema: schema.clone(),
            buf,
        })
    }

    pub fn row_data_view(&self) -> RowData {
        RowData::from_view(&self.schema, &self.buf)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RowFlags {
    flags: u8,
}

impl RowFlags {
    const FULL_CLUSTER_KEY: u8 = 1;

    pub fn create(has_full_cluster_key: bool) -> RowFlags {
        let mut flags = 0;

        if has_full_cluster_key {
            flags |= RowFlags::FULL_CLUSTER_KEY;
        }

        RowFlags { flags }
    }

    pub fn has_full_cluster_key(&self) -> bool {
        self.flags & RowFlags::FULL_CLUSTER_KEY != 0
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct ColumnFlags {
    flags: u8,
}

impl ColumnFlags {
    const NULL_VALUE: u8 = 1;

    #[inline]
    pub fn create(is_null: bool) -> ColumnFlags {
        let mut flags = 0;
        if is_null {
            flags |= ColumnFlags::NULL_VALUE;
        }

        ColumnFlags { flags }
    }


    pub fn with_null(&self) -> ColumnFlags {
        ColumnFlags { flags: self.flags | ColumnFlags::NULL_VALUE }
    }

    pub fn is_null(&self) -> bool {
        self.flags & ColumnFlags::NULL_VALUE != 0
    }
}

pub struct ColumnData<'a> {
    pub col_id: u32,
    pub flags: ColumnFlags,
    pub value: Option<ColumnValue<'a>>,
}

#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum ColumnValue<'a> {
    Boolean(bool),
    Int(i32),
    BigInt(i64),
    Text(&'a str),
}


#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use uuid::Uuid;

    use crate::prelude::HtError;
    use crate::primitives::DecodePrimitives;
    use crate::table::{ColumnData, ColumnFlags, ColumnSchema, ColumnType, ColumnValue, DetachedRowData, PrimaryKeySpec, RowFlags, TableSchema};

    fn table_schema() -> TableSchema {
        TableSchema::new(
            "my_table",
            &Uuid::new_v4(),
            vec!(
                ColumnSchema {
                    col_id: 0,
                    name: "part_key".to_string(),
                    tpe: ColumnType::BigInt,
                    pk_spec: PrimaryKeySpec::PartitionKey,
                },
                ColumnSchema {
                    col_id: 33,
                    name: "cl_key_1".to_string(),
                    tpe: ColumnType::Int,
                    pk_spec: PrimaryKeySpec::ClusterKey(false),
                },
                ColumnSchema {
                    col_id: 22,
                    name: "cl_key_2".to_string(),
                    tpe: ColumnType::Text,
                    pk_spec: PrimaryKeySpec::ClusterKey(true),
                },
                ColumnSchema {
                    col_id: 11,
                    name: "regular".to_string(),
                    tpe: ColumnType::Boolean,
                    pk_spec: PrimaryKeySpec::Regular,
                },
            ))
    }

    #[test]
    pub fn test_table_schema() {
        let table_schema = table_schema();

        assert_eq!(&table_schema.pk_columns
            .iter()
            .map(|c| &c.name)
            .collect::<Vec<&String>>(),
                   &vec!("part_key", "cl_key_1", "cl_key_2"));

        assert_eq!(table_schema.column(0).unwrap().name, "part_key");
        assert_eq!(table_schema.column(33).unwrap().name, "cl_key_1");
        assert_eq!(table_schema.column(22).unwrap().name, "cl_key_2");
        assert_eq!(table_schema.column(11).unwrap().name, "regular");

        assert!(table_schema.column(1).is_err());
    }

    fn col1_data(v: i64) -> ColumnData<'static> {
        ColumnData {
            col_id: 0,
            flags: ColumnFlags::create(false),
            value: Some(ColumnValue::BigInt(v)),
        }
    }

    fn col2_data(v: i32) -> ColumnData<'static> {
        ColumnData {
            col_id: 33,
            flags: ColumnFlags::create(false),
            value: Some(ColumnValue::Int(v)),
        }
    }

    fn col3_data<'a>(v: &'a str) -> ColumnData<'a> {
        ColumnData {
            col_id: 22,
            flags: ColumnFlags::create(false),
            value: Some(ColumnValue::Text(v)),
        }
    }

    fn col4_data(v: Option<bool>) -> ColumnData<'static> {
        ColumnData {
            col_id: 11,
            flags: ColumnFlags::create(v.is_none()),
            value: v.map(|b| ColumnValue::Boolean(b)),
        }
    }

    #[test]
    pub fn test_detached_row_data() {
        let table_schema = table_schema();

        let columns = vec!(
            col1_data(12345),
            col2_data(123),
            col3_data("yo"),
            col4_data(Some(true))
        );

        let row = DetachedRowData::assemble(
            &Arc::new(table_schema),
            RowFlags::create(true),
            &columns,
        ).unwrap();


        let row_data = row.row_data_view();

        assert_eq!(row.schema.table_id, row_data.schema.table_id);

        let mut v2 = Vec::new();
        row_data.write_to(&mut v2).unwrap();

        let mut offs = 0;
        assert_eq!(v2.decode_varint_usize(&mut offs), row.buf.len());
        assert_eq!(&row.buf, &&v2[offs..]);
        assert_eq!(RowFlags::create(true), row_data.flags());

        let mut offs = row_data.offs_start_column_data();
        let col = row_data.read_col(&mut offs);
        assert_eq!(col.flags, ColumnFlags::create(false));
        assert_eq!(col.col_id, 0);
        assert_eq!(col.value, Some(ColumnValue::BigInt(12345)));

        let col = row_data.read_col(&mut offs);
        assert_eq!(col.flags, ColumnFlags::create(false));
        assert_eq!(col.col_id, 33);
        assert_eq!(col.value, Some(ColumnValue::Int(123)));

        let col = row_data.read_col(&mut offs);
        assert_eq!(col.flags, ColumnFlags::create(false));
        assert_eq!(col.col_id, 22);
        assert_eq!(col.value, Some(ColumnValue::Text("yo")));

        let col = row_data.read_col(&mut offs);
        assert_eq!(col.flags, ColumnFlags::create(false));
        assert_eq!(col.col_id, 11);
        assert_eq!(col.value, Some(ColumnValue::Boolean(true)));
    }

    #[test]
    pub fn test_row_data_null_value() {
        let table_schema = table_schema();

        let row = DetachedRowData::assemble(&Arc::new(table_schema),
                                            RowFlags::create(false),
                                            &vec!(col4_data(None)))
            .unwrap();

        let row_data = row.row_data_view();

        let mut offs = row_data.offs_start_column_data();
        let col = row_data.read_col(&mut offs);
        assert_eq!(col.value, None);
    }

    #[test]
    pub fn test_compare_by_pk() {
        fn row(v1: i64, v2: i32, v3: &'static str, v4: Option<bool>) -> DetachedRowData {
            let table_schema = Arc::new(table_schema());
            let flags = RowFlags::create(true);
            DetachedRowData::assemble(&table_schema, flags,
                                      &vec!(col1_data(v1), col2_data(v2), col3_data(v3), col4_data(v4)),
            ).unwrap()
        }

        let row0 = row(100, 100, "hi", Some(true));

        let row_greater_1 = row(101, 101, "a", Some(true));
        let row_less_1 = row(99, 99, "z", Some(true));

        let row_greater_2 = row(100, 99, "a", Some(true));
        let row_less_2 = row(100, 101, "z", Some(true));

        let row_greater_3 = row(100, 100, "z", Some(true));
        let row_less_3 = row(100, 100, "a", Some(true));

        let row_regular_different = row(100, 100, "hi", Some(false));
        let row_regular_different2 = row(100, 100, "hi", None);

        let rd0 = row0.row_data_view();

        let rd_greater_1 = row_greater_1.row_data_view();
        let rd_less_1 = row_less_1.row_data_view();

        let rd_greater_2 = row_greater_2.row_data_view();
        let rd_less_2 = row_less_2.row_data_view();

        let rd_greater_3 = row_greater_3.row_data_view();
        let rd_less_3 = row_less_3.row_data_view();

        let rd_regular_different = row_regular_different.row_data_view();
        let rd_regular_different2 = row_regular_different2.row_data_view();

        assert_eq!(rd0.compare_by_pk(&rd_greater_1), Ordering::Less);
        assert_eq!(rd0.compare_by_pk(&rd_less_1), Ordering::Greater);

        assert_eq!(rd0.compare_by_pk(&rd_greater_2), Ordering::Less);
        assert_eq!(rd0.compare_by_pk(&rd_less_2), Ordering::Greater);

        assert_eq!(rd0.compare_by_pk(&rd_greater_3), Ordering::Less);
        assert_eq!(rd0.compare_by_pk(&rd_less_3), Ordering::Greater);

        assert_eq!(rd0.compare_by_pk(&rd_regular_different), Ordering::Equal);
        assert_eq!(rd0.compare_by_pk(&rd_regular_different2), Ordering::Equal);
    }
}
