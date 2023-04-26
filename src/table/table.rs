use std::{cmp::Ord, sync::Arc};

use bytes::Bytes;

use super::TableEnhanced;
use crate::coder::Coder;
use crate::cursor::Cursor;
use crate::error::Error;
use crate::types::*;
use crate::write_batch::WriteBatch;

pub trait Table {
  type Cursor<'a>: Cursor<'a>
  where Self: 'a;
  type WriteBatch: WriteBatch;

  fn id(&self) -> TableId;

  fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>;

  fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error>;

  #[inline]
  fn delete_range<K: AsRef<[u8]>>(&self, from_key: K, to_key: K) -> Result<(), Error> {
    let mut batch = self.new_write_batch();
    batch.delete_range(from_key, to_key);
    batch.write()
  }

  fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Bytes>, Error>;

  fn new_write_batch(&self) -> Self::WriteBatch;

  fn new_cursor<'a>(&'a self) -> Self::Cursor<'a>;

  #[inline]
  fn enhance<K: Ord, V, C: Coder<K, V>>(self: Arc<Self>) -> TableEnhanced<Self, K, V, C>
  where Self: Sized {
    TableEnhanced::new(self)
  }
}
