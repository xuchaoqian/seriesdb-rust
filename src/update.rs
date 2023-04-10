use bytes::Bytes;
use prost::{Message, Oneof};

#[derive(Clone, PartialEq, Message)]
pub struct Put {
  #[prost(bytes = "bytes", tag = "1")]
  pub key: Bytes,
  #[prost(bytes = "bytes", tag = "2")]
  pub value: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct Delete {
  #[prost(bytes = "bytes", tag = "1")]
  pub key: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteRange {
  #[prost(bytes = "bytes", tag = "1")]
  pub begin_key: Bytes,
  #[prost(bytes = "bytes", tag = "2")]
  pub end_key: Bytes,
}

#[derive(Clone, PartialEq, Message)]
pub struct Merge {
  #[prost(bytes = "bytes", tag = "1")]
  pub key: Bytes,
  #[prost(bytes = "bytes", tag = "2")]
  pub value: Bytes,
}

#[derive(Clone, PartialEq, Oneof)]
pub enum Update {
  #[prost(message, tag = "1")]
  Put(Put),
  #[prost(message, tag = "2")]
  Delete(Delete),
  #[prost(message, tag = "3")]
  DeleteRange(DeleteRange),
  #[prost(message, tag = "4")]
  Merge(Merge),
}

#[derive(Clone, PartialEq, Message)]
pub struct OptionalUpdate {
  #[prost(oneof = "Update", tags = "1, 2, 3, 4")]
  pub update: Option<Update>,
}
