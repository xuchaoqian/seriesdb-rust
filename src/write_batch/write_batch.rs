use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::WriteBatchEnhanced;
use crate::coder::Coder;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;

pub trait WriteBatch {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch;

  fn table_id(&self) -> TableId;

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  fn put<K, V>(&mut self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>;

  #[inline]
  fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
    let table_id = self.table_id();
    self.inner_mut().delete(build_inner_key(table_id, key))
  }

  #[inline]
  fn delete_range<K: AsRef<[u8]>>(&mut self, from_key: K, to_key: K) {
    let table_id = self.table_id();
    self
      .inner_mut()
      .delete_range(build_inner_key(table_id, from_key), build_inner_key(table_id, to_key))
  }

  fn write(self) -> Result<(), Error>;

  #[inline]
  fn enhance<K, V, C: Coder<K, V>>(self) -> WriteBatchEnhanced<Self, K, V, C>
  where Self: Sized {
    WriteBatchEnhanced::new(self)
  }
}
