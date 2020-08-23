use crate::table::{ColumnValue, TableSchema, RowData, ColumnType};
use crate::time::MergeTimestamp;
use crate::primitives::*;

use std::sync::Arc;
use std::cmp::Ordering;

pub struct TombStone<'a> {
    pub schema: Arc<TableSchema>,
    timestamp: MergeTimestamp,
    // partition_key: ColumnValue<'a>,
    flags: TombStoneFlags,
    lower_bound: Option<PartialClusterKey<'a>>,
    upper_bound: Option<PartialClusterKey<'a>>,
}

impl <'a> TombStone<'a> {
    pub fn matches(&self, row: &'a RowData) -> bool {
        match &self.lower_bound {
            Some(pck) => {
                match pck.compare_to(row) {
                    Ordering::Greater => return false,
                    Ordering::Equal => if !self.flags.lower_bound_inclusive() { return false },
                    _ => {}
                }
            },
            None => {},
        }

        match &self.upper_bound {
            Some(pck) => {
                match pck.compare_to(row) {
                    Ordering::Less => return false,
                    Ordering::Equal => if !self.flags.upper_bound_inclusive() { return false },
                    _ => {}
                }
            },
            None => {},
        }

        true
    }
}

pub struct TombStoneFlags(u8);

impl TombStoneFlags {
    const HAS_LOWER_BOUND: u8 = 1;
    const LOWER_BOUND_INCLUSIVE: u8 = 2;
    const HAS_UPPER_BOUND: u8 = 4;
    const UPPER_BOUND_INCLUSIVE: u8 = 8;

    pub fn has_lower_bound(&self) -> bool {
        self.0 & TombStoneFlags::HAS_LOWER_BOUND != 0
    }
    pub fn lower_bound_inclusive(&self) -> bool {
        self.0 & TombStoneFlags::LOWER_BOUND_INCLUSIVE != 0
    }
    pub fn has_upper_bound(&self) -> bool {
        self.0 & TombStoneFlags::HAS_UPPER_BOUND != 0
    }
    pub fn upper_bound_inclusive(&self) -> bool {
        self.0 & TombStoneFlags::UPPER_BOUND_INCLUSIVE != 0
    }
}

pub struct PartialClusterKey<'a> {
    schema: Arc<TableSchema>,
    buf: &'a [u8],
}

impl <'a> PartialClusterKey<'a> {
    pub fn compare_to(&self, row: &'a RowData) -> Ordering {
        assert_eq!(*self.schema, *row.schema);

        let mut offs = 0usize;
        let mut iter = row.columns();

        for col_schema in &self.schema.pk_columns {
            if offs >= self.buf.len() {
                break;
            }

            let col = match col_schema.tpe {
                ColumnType::Boolean => ColumnValue::Boolean(self.buf.decode_bool(&mut offs)),
                ColumnType::Int => ColumnValue::Int(self.buf.decode_varint_i32(&mut offs)),
                ColumnType::BigInt => ColumnValue::BigInt(self.buf.decode_varint_i64(&mut offs)),
                ColumnType::Text => ColumnValue::Text(self.buf.decode_utf8(&mut offs)),
            };

            let row_col = iter.next().expect("row has incomplete cluster key")
                .value.expect("cluster key is null in row");

            let cmp = col.cmp(&row_col);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }

        Ordering::Equal
    }
}
