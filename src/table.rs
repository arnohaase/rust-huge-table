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

pub enum ColumnType {
    Boolean,
    Int,
    BigInt,
    Text,
}

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub enum ColumnValue<'a> {
    Boolean(bool),
    Int(i32),
    BigInt(i64),
    Text(&'a str),
}

#[derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
pub struct ColumnData<'a> {
    pub value: Option<ColumnValue<'a>>,
    //TODO timestamp
}

pub struct ColumnSchema {
    pub col_id: u32,
    pub name: String,
    pub tpe: ColumnType,
    pub pk_spec: PrimaryKeySpec,
}

pub enum PrimaryKeySpec {
    PartitionKey,
    ClusterKey(bool),
    // ascending
    Regular,
}

pub struct TableSchema {
    pub name: String,
    pub table_id: Uuid,
    pub columns: Vec<ColumnSchema>,
    pub pk_columns: Vec<ColumnSchema>,
}
impl TableSchema {
    pub fn column(&self, col_id: u32) -> HtResult<&ColumnSchema> {
        match self.columns.iter().find(|c| c.col_id == col_id) {
            Some(c) => Ok(c),
            None => Err(HtError::FileIntegrity("invalid column id".to_string())),
        }
    }
}

pub struct Row<'a> {
    pub schema: Arc<TableSchema>,
    pub values: BTreeMap<u32, ColumnData<'a>>,
}

impl<'a> Row<'a> {
    fn schema(&self) -> Arc<TableSchema> {
        self.schema.clone()
    }

    fn value(&self, column_id: u32) -> Option<&ColumnData<'a>> {
        self.values.get(&column_id)
    }

    pub fn compare_by_pk(&self, other: &Row) -> Ordering {
        for col in &self.schema.pk_columns {
            let self_value = self.values.get(&col.col_id).unwrap(); //TODO handle missing
            let other_value = other.values.get(&col.col_id).unwrap(); //TODO handle missing

            match self_value.cmp(other_value) {
                Ordering::Equal => {}
                ord => return ord,
            }
        }
        Ordering::Equal
    }
}

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
