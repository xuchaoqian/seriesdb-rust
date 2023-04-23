use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::WriteBatchEnhanced;
use crate::coder::Coder;
use crate::types::*;
use crate::utils::*;

pub trait WriteBatch {
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch;

  fn table_id(&self) -> TableId;

  #[inline]
  fn put<K, V>(&mut self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    let key = build_inner_key(self.table_id(), key);
    self.inner_mut().put(key, value)
  }

  #[inline]
  fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
    let key = build_inner_key(self.table_id(), key);
    self.inner_mut().delete(key)
  }

  #[inline]
  fn delete_range<F, T>(&mut self, from_key: F, to_key: T)
  where
    F: AsRef<[u8]>,
    T: AsRef<[u8]>, {
    let from_key = build_inner_key(self.table_id(), from_key);
    let to_key = build_inner_key(self.table_id(), to_key);
    self.inner_mut().delete_range(from_key, to_key)
  }

  #[inline]
  fn enhance<K, V, C: Coder<K, V>>(self) -> WriteBatchEnhanced<Self, K, V, C>
  where Self: Sized {
    WriteBatchEnhanced::new(self)
  }
}
