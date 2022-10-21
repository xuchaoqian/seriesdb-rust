use crate::update::*;
use bytes::Bytes;
use prost::Message;
use rocksdb::WriteBatchIterator;

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
}

impl UpdateBatch {
  pub fn new() -> Self {
    UpdateBatch { sn: 0, updates: vec![] }
  }
}
