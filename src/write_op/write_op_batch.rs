use bytes::Bytes;
use prost::Message;
use rocksdb::WriteBatchIterator;

use super::write_op::*;

#[derive(Clone, PartialEq, Message)]
pub struct WriteOpBatch {
  #[prost(uint64, tag = "1")]
  pub sn: u64,
  #[prost(message, repeated, tag = "2")]
  pub write_ops: Vec<OptionalWriteOp>,
}

impl WriteBatchIterator for WriteOpBatch {
  fn put(&mut self, inner_key: Box<[u8]>, inner_value: Box<[u8]>) {
    let put_op = PutOp {
      inner_key: Bytes::copy_from_slice(inner_key.as_ref()),
      inner_value: Bytes::copy_from_slice(inner_value.as_ref()),
    };
    self.write_ops.push(OptionalWriteOp { inner: Some(WriteOp::PutOp(put_op)) })
  }

  fn delete(&mut self, inner_key: Box<[u8]>) {
    let delete_op = DeleteOp { inner_key: Bytes::copy_from_slice(inner_key.as_ref()) };
    self.write_ops.push(OptionalWriteOp { inner: Some(WriteOp::DeleteOp(delete_op)) })
  }

  fn delete_range(&mut self, begin_inner_key: Box<[u8]>, end_inner_key: Box<[u8]>) {
    let delete_range_op = DeleteRangeOp {
      begin_inner_key: Bytes::copy_from_slice(begin_inner_key.as_ref()),
      end_inner_key: Bytes::copy_from_slice(end_inner_key.as_ref()),
    };
    self.write_ops.push(OptionalWriteOp { inner: Some(WriteOp::DeleteRangeOp(delete_range_op)) })
  }

  fn merge(&mut self, inner_key: Box<[u8]>, inner_value: Box<[u8]>) {
    let merge_op = MergeOp {
      inner_key: Bytes::copy_from_slice(inner_key.as_ref()),
      inner_value: Bytes::copy_from_slice(inner_value.as_ref()),
    };
    self.write_ops.push(OptionalWriteOp { inner: Some(WriteOp::MergeOp(merge_op)) })
  }
}

impl WriteOpBatch {
  pub fn new() -> Self {
    WriteOpBatch { sn: 0, write_ops: vec![] }
  }
}
