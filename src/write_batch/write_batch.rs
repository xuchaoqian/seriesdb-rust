use rocksdb::WriteBatch as WriteBatchInner;

use crate::types::*;
use crate::utils::*;

pub trait WriteBatch {
  #[doc(hidden)]
  fn inner_write_batch_mut(&mut self) -> &mut WriteBatchInner;
  #[doc(hidden)]
  fn table_id(&self) -> TableId;

  #[inline]
  fn put<K, V>(&mut self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    let key = build_inner_key(self.table_id(), key);
    self.inner_write_batch_mut().put(key, value)
  }

  #[inline]
  fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
    let key = build_inner_key(self.table_id(), key);
    self.inner_write_batch_mut().delete(key)
  }

  #[inline]
  fn delete_range<F, T>(&mut self, from_key: F, to_key: T)
  where
    F: AsRef<[u8]>,
    T: AsRef<[u8]>, {
    let from_key = build_inner_key(self.table_id(), from_key);
    let to_key = build_inner_key(self.table_id(), to_key);
    self.inner_write_batch_mut().delete_range(from_key, to_key)
  }
}
