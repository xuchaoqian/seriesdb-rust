use bytes::Bytes;
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct PutOp {
  #[prost(bytes = "bytes", tag = "1")]
  pub inner_key: Bytes,
  #[prost(bytes = "bytes", tag = "2")]
  pub inner_value: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteOp {
  #[prost(bytes = "bytes", tag = "1")]
  pub inner_key: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteRangeOp {
  #[prost(bytes = "bytes", tag = "1")]
  pub begin_inner_key: Bytes,
  #[prost(bytes = "bytes", tag = "2")]
  pub end_inner_key: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct MergeOp {
  #[prost(bytes = "bytes", tag = "1")]
  pub inner_key: Bytes,
  #[prost(bytes = "bytes", tag = "2")]
  pub inner_value: Bytes,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum WriteOp {
  #[prost(message, tag = "1")]
  PutOp(PutOp),
  #[prost(message, tag = "2")]
  DeleteOp(DeleteOp),
  #[prost(message, tag = "3")]
  DeleteRangeOp(DeleteRangeOp),
  #[prost(message, tag = "4")]
  MergeOp(MergeOp),
}

#[derive(Clone, PartialEq, Message)]
pub struct OptionalWriteOp {
  #[prost(oneof = "WriteOp", tags = "1, 2, 3, 4")]
  pub inner: Option<WriteOp>,
}
