use crate::consts::*;
use crate::update::Update;
use crate::utils::*;
use bytes::Bytes;
use rocksdb::WriteBatchIterator;
use std::fmt::{Debug, Formatter, Result as FmtResult};

pub struct UpdateBatch {
    pub sn: u64,
    pub updates: Vec<Update>,
}

impl WriteBatchIterator for UpdateBatch {
    fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.updates.push(Update::Put {
            key: Bytes::copy_from_slice(key.as_ref()),
            value: Bytes::copy_from_slice(value.as_ref()),
        })
    }
    fn delete(&mut self, key: Box<[u8]>) {
        let table_id = extract_table_id(&key);
        if table_id == DELETE_RANGE_HINT_TABLE_ID {
            let (from_key, to_key) = extract_delete_range_hint(key);
            self.updates.push(Update::DeleteRange { from_key, to_key })
        } else {
            self.updates.push(Update::Delete { key: Bytes::copy_from_slice(key.as_ref()) })
        }
    }
}

impl Debug for UpdateBatch {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}@{:?}", &self.updates, self.sn)
    }
}

impl UpdateBatch {
    pub fn new() -> Self {
        UpdateBatch { sn: 0, updates: vec![] }
    }
}
