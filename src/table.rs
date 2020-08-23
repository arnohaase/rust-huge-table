use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::Write;
use std::mem::size_of;
use std::sync::Arc;

use uuid::Uuid;

use crate::prelude::*;
use crate::primitives::*;
use crate::time::{MergeTimestamp, TtlTimestamp};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColumnId( pub u8 );
impl ColumnId {
    pub const MAX: ColumnId = ColumnId(63); //TODO extend this limitation? --> Bitset for columns that are present in a row
}

impl <W> Encode<ColumnId> for W where W: Write {
    fn encode(&mut self, v: ColumnId) -> std::io::Result<()> {
        self.encode_u8(v.0)
    }
}
impl Decode<ColumnId> for &[u8] {
    fn decode(&self, offs: &mut usize) -> ColumnId {
        ColumnId(self.decode_u8(offs))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ColumnType {
    Boolean,
    Int,
    BigInt,
    Text,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnSchema {
    pub col_id: ColumnId,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrimaryKeySpec {
    PartitionKey,
    ClusterKey(bool),
    Regular,
}

#[derive(Debug, Eq, PartialEq)]
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

    pub fn column(&self, col_id: ColumnId) -> HtResult<&ColumnSchema> {
        match self.columns.iter().find(|c| c.col_id == col_id) {
            Some(c) => Ok(c),
            None => Err(HtError::misc("column not found")),
        }
    }
}


//TODO separate tombstone data structures - row, range etc.
//TODO unit tests for merge timestamp, expiry (row and column level)


//TODO u64 as a bitset for 'present columns', col_id as u8


/// A wrapper around (and handle to) a byte buffer containing a row's raw data.
///
/// row format:
///   varint<usize>     number of bytes (on disk only, otherwise encoded in the wide pointer)
///   u8                RowFlags.
///   fixed u64         row timestamp (MergeTimestamp). We treat an empty row (i.e. all non-pk
///                      columns are NULL) as non-existent, so rows need no inherent merge
///                      timestamp, and this timestamp has no inherent meaning. Columns however can
///                      reference this timestamp
///                      (ColumnFlags::COLUMN_TIMESTAMP), saving storage in the frequent case that
///                      several columns in a row share the same timestamp.
///   opt fixed u32     optional (if TTL row flag is set) row TtlTimestamp. We treat empty rows
///                      as non-existent, so there is no inherent concept of 'row TTL', but for
///                      the frequent case that several / all columns in a row share the same TTL,
///                      the row can store a TTL that can then be referenced from columns
///                      (ColumnFlags::ROW_EXPIRY)
///   varint 64         bitset for col_ids of columns present in this row
///
///   columns:
///     u8              column id
///     u8              ColumnFlags
///     opt fixed u64   column timestamp - only present if column flags indicate that this column's
///                      timestamp differs from the row timestamp, otherwise the row's timestamp
///                      is used as this column's timestamp
///     opt fixed u32   column TTL - only present if ColumnFlags::COLUMN_EXPIRY and *not*
///                      ColumnFlags::ROW_EXPIRY
///     opt value       format depends on column type; only if 'is null' column flag is not set
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
        self.buf.decode(&mut 0)
    }

    pub fn timestamp(&self) -> MergeTimestamp {
        self.buf.decode(&mut 1)
    }

    pub fn expiry(&self) -> Option<TtlTimestamp> {
        if self.flags().has_row_expiry() {
            let mut offs = 1 + size_of::<u64>();
            Some(self.buf.decode(&mut offs))
        }
        else {
            None
        }
    }

    /// This is not very efficient and intended for testing and debugging
    pub fn read_col_by_id(&self, col_id: ColumnId) -> Option<ColumnData> {
        let mut offs = self.offs_start_column_data();
        while offs < self.buf.len() {
            let candidate = self.read_col(self.timestamp(), self.expiry(), &mut offs);
            if candidate.col_id == col_id {
                return Some(candidate);
            }
        }
        None
    }

    fn read_col(&self, row_timestamp: MergeTimestamp, row_expiry: Option<TtlTimestamp>, offs: &mut usize) -> ColumnData {
        let col_id = self.buf.decode(offs);
        let col_flags: ColumnFlags = self.buf.decode(offs);

        let timestamp = match col_flags.has_col_timestamp() {
            true => MergeTimestamp::from_ticks(self.buf.decode_fixed_u64(offs)),
            false => row_timestamp,
        };

        use ColumnExpiryKind::*;
        let expiry = match col_flags.expiry() {
            NoExpiry => None,
            ColumnExpiry => Some (self.buf.decode(offs)),
            RowExpiry => row_expiry,
        };

        let mut col_data = None;

        if !col_flags.is_null() {
            col_data = Some(match self.schema.column(col_id).unwrap().tpe { //TODO error handling?
                ColumnType::Boolean => ColumnValue::Boolean(self.buf.decode_bool(offs)),
                ColumnType::Int => ColumnValue::Int(self.buf.decode_varint_i32(offs)),
                ColumnType::BigInt => ColumnValue::BigInt(self.buf.decode_varint_i64(offs)),
                ColumnType::Text => ColumnValue::Text(self.buf.decode_utf8(offs)),
            });
        }
        ColumnData::new (col_id, timestamp, expiry, col_data)
    }

    fn offs_start_column_data(&self) -> usize {
        let row_flags = RowFlags(self.buf[0]);
        let mut offs = 1 + size_of::<MergeTimestamp>();

        if row_flags.has_row_expiry() {
            self.buf.decode_varint_u32(&mut offs);
        }

        offs
    }

    pub fn compare_by_pk(&self, other: &RowData) -> Ordering {
        let mut offs_self = self.offs_start_column_data();
        let mut offs_other = other.offs_start_column_data();

        for col_meta in &self.schema.columns {
            let desc = match col_meta.pk_spec {
                PrimaryKeySpec::PartitionKey => false,
                PrimaryKeySpec::ClusterKey(asc) => !asc,
                PrimaryKeySpec::Regular => return Ordering::Equal
            };

            //TODO special handling for primary key columns: never store TTL or timestamp

            //TODO optimization: "read_col_value" to avoid having to pass in timestamps
            let col_self = self.read_col(self.timestamp(), self.expiry(), &mut offs_self);
            let col_other = other.read_col(other.timestamp(), other.expiry(), &mut offs_other);

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

    pub fn columns(&'a self) -> RowColumnIter<'a> {
        RowColumnIter { row: &self, offs: 0 }
    }

    pub fn merge(&self, other: &RowData) -> DetachedRowData {
        assert_eq!(self.schema, other.schema);

        let self_columns = &mut self.columns();
        let other_columns = &mut other.columns();

        let mut cur_self = self_columns.next();
        let mut cur_other = other_columns.next();

        let mut columns = Vec::new();

        loop {
            match (&cur_self, &cur_other) {
                (Some(s), Some(o)) => {
                    if s.col_id < o.col_id {
                        columns.push(cur_self.unwrap());
                        cur_self = self_columns.next();
                    }
                    else if o.col_id < s.col_id {
                        columns.push(cur_other.unwrap());
                        cur_other = other_columns.next();
                    }
                    else {
                        if s.timestamp > o.timestamp {
                            columns.push(cur_self.unwrap());
                        }
                        else {
                            columns.push(cur_other.unwrap());
                        }
                        cur_self = self_columns.next();
                        cur_other = other_columns.next();
                    }
                },
                (Some(_), None) => {
                    while cur_self.is_some() {
                        columns.push(cur_self.unwrap());
                        cur_self = self_columns.next();
                    }
                    break;
                },
                (None, Some(_)) => {
                    while cur_other.is_some() {
                        columns.push(cur_other.unwrap());
                        cur_other = other_columns.next();
                    }
                    break;
                }
                _ => {
                    break;
                }
            }
        }

        DetachedRowData::assemble(
            &self.schema.clone(),
            &columns
        )
    }
}

pub struct RowColumnIter<'a> {
    row: &'a RowData<'a>,
    offs: usize,
}

impl <'a> RowColumnIter<'a> {
    pub fn new(row: &'a RowData<'a>) -> RowColumnIter<'a> {
        let offs = row.offs_start_column_data();
        RowColumnIter {
            row,
            offs
        }
    }
}

impl <'a> Iterator for RowColumnIter<'a> {
    type Item = ColumnData<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offs >= self.row.buf.len() {
            None
        }
        else {
            Some(self.row.read_col(self.row.timestamp(), self.row.expiry(), &mut self.offs))
        }
    }
}

pub struct DetachedRowData {
    schema: Arc<TableSchema>,
    buf: Vec<u8>,
}

ordered!(DetachedRowData);

impl DetachedRowData {
    fn compare(a: &DetachedRowData, b: &DetachedRowData) -> Ordering {
        a.row_data_view().compare_by_pk(&b.row_data_view())
    }

    fn most_frequent_timestamp(columns: &Vec<ColumnData>) -> MergeTimestamp {
        //TODO how to handle 'no columns'?
        assert!(columns.len() > 0);

        let mut timestamp_counter = HashMap::new();
        columns.iter().for_each(|c| {
            let count: u32 = *timestamp_counter.get(&c.timestamp).unwrap_or(&0);
            timestamp_counter.insert(c.timestamp, count + 1);
        });

        let max = timestamp_counter.iter().max_by_key(|e| e.1);
        *max.unwrap().0
    }

    fn most_frequent_expiry(columns: &Vec<ColumnData>) -> Option<TtlTimestamp> {

        let mut timestamp_counter = HashMap::new();
        columns.iter().for_each(|c| {
            match c.expiry {
                None => {},
                Some(ts) => {
                    let count: u32 = *timestamp_counter.get(&ts).unwrap_or(&0);
                    timestamp_counter.insert(ts, count + 1);
                },
            }
        });

        timestamp_counter.iter()
            .max_by_key(|e| e.1)
            .map(|e|*e.0)
    }

    fn encode_column(buf: &mut Vec<u8>, col: &ColumnData, row_timestamp: MergeTimestamp, row_expiry: Option<TtlTimestamp>) {
        buf.encode(col.col_id).expect("error writing Vec<u8>"); //TODO unchecked variant for Vec<u8>?

        let col_flags = ColumnFlags::new(
            col.value.is_none(),
            col.timestamp != row_timestamp,
            col.expiry.is_some() && col.expiry != row_expiry,
            col.expiry.is_some() && col.expiry == row_expiry,
        );

        buf.encode(col_flags).expect("error writing Vec<u8>");

        if col.timestamp != row_timestamp {
            buf.encode(col.timestamp).expect("error writing Vec<u8>");
        }


        match col.value {
            None => {}
            Some(ColumnValue::Boolean(v)) => buf.encode_bool(v).expect("error writing Vec<u8>"),
            Some(ColumnValue::Int(v)) => buf.encode_varint_i32(v).expect("error writing Vec<u8>"),
            Some(ColumnValue::BigInt(v)) => buf.encode_varint_i64(v).expect("error writing Vec<u8>"),
            Some(ColumnValue::Text(v)) => buf.encode_utf8(v).expect("error writing Vec<u8>"),
        }
    }

    pub fn assemble(schema: &Arc<TableSchema>, columns: &Vec<ColumnData>) -> DetachedRowData {
        let row_timestamp = DetachedRowData::most_frequent_timestamp(columns);
        let row_expiry = DetachedRowData::most_frequent_expiry(columns);

        let row_flags = RowFlags::create(row_expiry.is_some());

        let mut buf = Vec::new();
        buf.encode(row_flags).expect("error writing Vec<u8>");

        let timestamp = DetachedRowData::most_frequent_timestamp(columns);
        buf.encode(timestamp).expect("error writing Vec<u8>");

        match row_expiry {
            Some(ttl) => buf.encode(ttl).expect("error writing Vec<u8>"),
            None => {}
        }

        //TODO verify that pk columns go first and are in schema order
        //TODO verify that pk columns can not be null - absent is ok for incomplete rows, but explicit values of null are not

        for col in columns {
            DetachedRowData::encode_column(&mut buf, col, row_timestamp, row_expiry);
        }

        DetachedRowData {
            schema: schema.clone(),
            buf,
        }
    }

    pub fn row_data_view(&self) -> RowData {
        RowData::from_view(&self.schema, &self.buf)
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct RowFlags (u8);

impl RowFlags {
    const ROW_EXPIRY: u8 = 1;

    pub fn create(has_row_expiry: bool) -> RowFlags {
        let mut flags = 0;

        if has_row_expiry {
            flags |= RowFlags::ROW_EXPIRY;
        }
        RowFlags ( flags )
    }

    pub fn has_row_expiry(&self) -> bool {
        self.0 & RowFlags::ROW_EXPIRY != 0
    }
}

impl <W> Encode<RowFlags> for W where W: Write {
    fn encode(&mut self, v: RowFlags) -> std::io::Result<()> {
        self.encode_u8(v.0)
    }
}
impl Decode<RowFlags> for &[u8] {
    fn decode(&self, offs: &mut usize) -> RowFlags {
        RowFlags(self.decode_u8(offs))
    }
}


#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct ColumnFlags (u8);

impl ColumnFlags {
    /// the column is NULL - no value is present in the column representation
    const NULL_VALUE: u8 = 1;
    /// the column's timestamp is stored with the column - otherwise the 'row timestamp' is used
    const COLUMN_TIMESTAMP: u8 = 2;
    /// the column has an expiry which is stored with the column.
    const COLUMN_EXPIRY: u8 = 4;
    /// the column has an expiry which is the 'row expiry'. This flag is mutually exclusive with
    ///  COLUMN_EXPIRY, and it requires RowFlags::ROW_EXPIRY to be set.
    const ROW_EXPIRY: u8 = 8;

    #[inline]
    fn new(
        is_null: bool,
        has_timestamp: bool,
        has_col_expiry: bool,
        has_row_expiry: bool) -> ColumnFlags
    {
        let mut flags = 0;
        if is_null {
            flags |= ColumnFlags::NULL_VALUE;
        }
        if has_timestamp {
            flags |= ColumnFlags::COLUMN_TIMESTAMP;
        }
        if has_col_expiry {
            flags |= ColumnFlags::COLUMN_EXPIRY
        }
        if has_row_expiry {
            flags |= ColumnFlags::ROW_EXPIRY
        }

        ColumnFlags ( flags )
    }

    pub fn is_null(&self) -> bool {
        self.0 & ColumnFlags::NULL_VALUE != 0
    }
    pub fn has_col_timestamp(&self) -> bool {
        self.0 & ColumnFlags::COLUMN_TIMESTAMP != 0
    }
    pub fn expiry(&self) -> ColumnExpiryKind {
        let row_expiry = self.0 & ColumnFlags::ROW_EXPIRY != 0;
        let col_expiry = self.0 & ColumnFlags::COLUMN_EXPIRY != 0;

        use ColumnExpiryKind::*;

        match (row_expiry, col_expiry) {
            (true, _) => RowExpiry,
            (false, true) => ColumnExpiry,
            (false, false) => NoExpiry,
        }
    }
}

pub enum ColumnExpiryKind {
    NoExpiry,
    RowExpiry,
    ColumnExpiry,
}

impl <W> Encode<ColumnFlags> for W where W: Write {
    fn encode(&mut self, value: ColumnFlags) -> std::io::Result<()> {
        self.encode_u8(value.0)
    }
}
impl Decode<ColumnFlags> for &[u8] {
    fn decode(&self, offs: &mut usize) -> ColumnFlags {
        ColumnFlags(self.decode_u8(offs))
    }
}

/// This is the logical representation of a column's data in a row. It holds a similar but
///  different data structure from a RowData's raw buffer, resolving some storage optimizations
#[derive(Eq, PartialEq)]
pub struct ColumnData<'a> {
    pub col_id: ColumnId,
    pub timestamp: MergeTimestamp,
    pub expiry: Option<TtlTimestamp>,
    pub value: Option<ColumnValue<'a>>,
}
impl<'a> ColumnData<'a> {
    pub fn new(col_id: ColumnId, timestamp: MergeTimestamp, expiry: Option<TtlTimestamp>, value: Option<ColumnValue<'a>>) -> ColumnData<'a> {
        assert!(col_id <= ColumnId::MAX);

        ColumnData { col_id, timestamp, expiry, value }
    }

    pub fn merge<'b>(col1: ColumnData<'b>, col2: ColumnData<'b>) -> ColumnData<'b> {
        assert_eq!(col1.col_id, col2.col_id);

        // this basically asserts that merge timestamps are globally unique
        assert!(col1.timestamp != col2.timestamp || col1 == col2);

        if col1.timestamp > col2.timestamp {
            col1
        }
        else {
            col2
        }
    }
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

    use crate::primitives::DecodePrimitives;
    use crate::table::{ColumnData, ColumnSchema, ColumnType, ColumnValue, DetachedRowData, PrimaryKeySpec, RowFlags, TableSchema, ColumnId};
    use crate::time::{ManualClock, MergeTimestamp, HtClock};

    fn table_schema() -> TableSchema {
        TableSchema::new(
            "my_table",
            &Uuid::new_v4(),
            vec!(
                ColumnSchema {
                    col_id: ColumnId(0),
                    name: "part_key".to_string(),
                    tpe: ColumnType::BigInt,
                    pk_spec: PrimaryKeySpec::PartitionKey,
                },
                ColumnSchema {
                    col_id: ColumnId(33),
                    name: "cl_key_1".to_string(),
                    tpe: ColumnType::Int,
                    pk_spec: PrimaryKeySpec::ClusterKey(false),
                },
                ColumnSchema {
                    col_id: ColumnId(22),
                    name: "cl_key_2".to_string(),
                    tpe: ColumnType::Text,
                    pk_spec: PrimaryKeySpec::ClusterKey(true),
                },
                ColumnSchema {
                    col_id: ColumnId(11),
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

        assert_eq!(table_schema.column(ColumnId(0)).unwrap().name, "part_key");
        assert_eq!(table_schema.column(ColumnId(33)).unwrap().name, "cl_key_1");
        assert_eq!(table_schema.column(ColumnId(22)).unwrap().name, "cl_key_2");
        assert_eq!(table_schema.column(ColumnId(11)).unwrap().name, "regular");

        assert!(table_schema.column(ColumnId(1)).is_err());
    }

    fn col1_data(timestamp: MergeTimestamp, v: i64) -> ColumnData<'static> {
        ColumnData {
            col_id: ColumnId(0),
            timestamp,
            expiry: None,
            value: Some(ColumnValue::BigInt(v)),
        }
    }

    fn col2_data(timestamp: MergeTimestamp, v: i32) -> ColumnData<'static> {
        ColumnData {
            col_id: ColumnId(33),
            timestamp,
            expiry: None,
            value: Some(ColumnValue::Int(v)),
        }
    }

    fn col3_data<'a>(timestamp: MergeTimestamp, v: &'a str) -> ColumnData<'a> {
        ColumnData {
            col_id: ColumnId(22),
            timestamp,
            expiry: None,
            value: Some(ColumnValue::Text(v)),
        }
    }

    fn col4_data(timestamp: MergeTimestamp, v: Option<bool>) -> ColumnData<'static> {
        ColumnData {
            col_id: ColumnId(11),
            timestamp,
            expiry: None,
            value: v.map(|b| ColumnValue::Boolean(b)),
        }
    }

    #[test]
    pub fn test_detached_row_data() {
        let table_schema = table_schema();

        let clock = ManualClock::new(MergeTimestamp::from_ticks(123456789));

        let columns = vec!(
            col1_data(clock.now(), 12345),
            col2_data(clock.now(), 123),
            col3_data(clock.now(), "yo"),
            col4_data(clock.now(), Some(true))
        );

        let row = DetachedRowData::assemble(
            &Arc::new(table_schema),
            &columns,
        );


        let row_data = row.row_data_view();

        assert_eq!(row.schema.table_id, row_data.schema.table_id);

        let mut v2 = Vec::new();
        row_data.write_to(&mut v2).unwrap();

        let mut offs = 0;
        assert_eq!(v2.decode_varint_usize(&mut offs), row.buf.len());
        assert_eq!(&row.buf, &&v2[offs..]);
        assert_eq!(RowFlags::create(false), row_data.flags());

        let mut offs = row_data.offs_start_column_data();
        let col = row_data.read_col(clock.now(), None, &mut offs);
        // assert_eq!(col.flags, ColumnFlags::new(false, false, false, false));
        assert_eq!(col.col_id, ColumnId(0));
        assert_eq!(col.value, Some(ColumnValue::BigInt(12345)));

        let col = row_data.read_col(clock.now(), None,&mut offs);
        // assert_eq!(col.flags, ColumnFlags::new(false, false, false, false));
        assert_eq!(col.col_id, ColumnId(33));
        assert_eq!(col.value, Some(ColumnValue::Int(123)));

        let col = row_data.read_col(clock.now(), None, &mut offs);
        // assert_eq!(col.flags, ColumnFlags::new(false, false, false, false));
        assert_eq!(col.col_id, ColumnId(22));
        assert_eq!(col.value, Some(ColumnValue::Text("yo")));

        let col = row_data.read_col(clock.now(), None, &mut offs);
        // assert_eq!(col.flags, ColumnFlags::new(false, false, false, false));
        assert_eq!(col.col_id, ColumnId(11));
        assert_eq!(col.value, Some(ColumnValue::Boolean(true)));
    }

    #[test]
    pub fn test_row_data_null_value() {
        let table_schema = table_schema();

        let clock = ManualClock::new(MergeTimestamp::from_ticks(123456789));

        let row = DetachedRowData::assemble(&Arc::new(table_schema),
                                            &vec!(col4_data(clock.now(), None)));

        let row_data = row.row_data_view();

        let mut offs = row_data.offs_start_column_data();
        let col = row_data.read_col(clock.now(), None, &mut offs);
        assert_eq!(col.value, None);
    }

    #[test]
    pub fn test_compare_by_pk() {
        fn row(v1: i64, v2: i32, v3: &'static str, v4: Option<bool>) -> DetachedRowData {
            let table_schema = Arc::new(table_schema());
            let clock = ManualClock::new(MergeTimestamp::from_ticks(123456789));

            DetachedRowData::assemble(&table_schema,&vec!(
                col1_data(clock.now(), v1),
                col2_data(clock.now(), v2),
                col3_data(clock.now(), v3),
                col4_data(clock.now(), v4)),
            )
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
