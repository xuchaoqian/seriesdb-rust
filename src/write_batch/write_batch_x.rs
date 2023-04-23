use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::WriteBatchXEnhanced;
use crate::coder::Coder;
use crate::types::*;
use crate::utils::*;

pub trait WriteBatchX {
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch;

  #[inline]
  fn put<K, V>(&mut self, table_id: TableId, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    self.inner_mut().put(build_inner_key(table_id, key), value)
  }

  #[inline]
  fn delete<K: AsRef<[u8]>>(&mut self, table_id: TableId, key: K) {
    self.inner_mut().delete(build_inner_key(table_id, key))
  }

  #[inline]
  fn delete_range<F, T>(&mut self, table_id: TableId, from_key: F, to_key: T)
  where
    F: AsRef<[u8]>,
    T: AsRef<[u8]>, {
    self
      .inner_mut()
      .delete_range(build_inner_key(table_id, from_key), build_inner_key(table_id, to_key))
  }

  #[inline]
  fn enhance<K, V, C: Coder<K, V>>(self) -> WriteBatchXEnhanced<Self, K, V, C>
  where Self: Sized {
    WriteBatchXEnhanced::new(self)
  }
}
