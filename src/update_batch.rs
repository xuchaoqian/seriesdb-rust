use bytes::Bytes;
use prost::Message;
use rocksdb::WriteBatchIterator;

use crate::update::*;

#[derive(Clone, PartialEq, Message)]
pub struct UpdateBatch {
  #[prost(uint64, tag = "1")]
  pub sn: u64,
  #[prost(message, repeated, tag = "2")]
  pub updates: Vec<OptionalUpdate>,
}

impl WriteBatchIterator for UpdateBatch {
  fn put(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
    let put = Put {
      key: Bytes::copy_from_slice(key.as_ref()),
      value: Bytes::copy_from_slice(value.as_ref()),
    };
    self.updates.push(OptionalUpdate { update: Some(Update::Put(put)) })
  }

  fn delete(&mut self, key: Box<[u8]>) {
    let delete = Delete { key: Bytes::copy_from_slice(key.as_ref()) };
    self.updates.push(OptionalUpdate { update: Some(Update::Delete(delete)) })
  }

  fn delete_range(&mut self, begin_key: Box<[u8]>, end_key: Box<[u8]>) {
    let delete_range = DeleteRange {
      begin_key: Bytes::copy_from_slice(begin_key.as_ref()),
      end_key: Bytes::copy_from_slice(end_key.as_ref()),
    };
    self.updates.push(OptionalUpdate { update: Some(Update::DeleteRange(delete_range)) })
  }

  fn merge(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
    let merge = Merge {
      key: Bytes::copy_from_slice(key.as_ref()),
      value: Bytes::copy_from_slice(value.as_ref()),
    };
    self.updates.push(OptionalUpdate { update: Some(Update::Merge(merge)) })
  }
}

impl UpdateBatch {
  pub fn new() -> Self {
    UpdateBatch { sn: 0, updates: vec![] }
  }
}
