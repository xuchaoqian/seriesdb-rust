use bytes::Bytes;

use crate::error::Error;
use crate::types::*;

pub trait Table {
  type Cursor<'a>
  where Self: 'a;
  type WriteBatch;

  fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>;

  fn new_write_batch(&self) -> Self::WriteBatch;

  fn write(&self, batch: Self::WriteBatch) -> Result<(), Error>;

  fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error>;

  fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Bytes>, Error>;

  fn cursor<'a>(&'a self) -> Self::Cursor<'a>;

  fn id(&self) -> TableId;
}
