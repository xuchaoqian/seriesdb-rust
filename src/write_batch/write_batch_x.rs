use rocksdb::WriteBatch as WriteBatchInner;

use crate::types::*;
use crate::utils::*;

pub trait WriteBatchX {
  #[doc(hidden)]
  fn inner_write_batch_mut(&mut self) -> &mut WriteBatchInner;

  #[inline]
  fn put<K, V>(&mut self, table_id: TableId, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    self.inner_write_batch_mut().put(build_inner_key(table_id, key), value)
  }

  #[inline]
  fn delete<K: AsRef<[u8]>>(&mut self, table_id: TableId, key: K) {
    self.inner_write_batch_mut().delete(build_inner_key(table_id, key))
  }

  #[inline]
  fn delete_range<F, T>(&mut self, table_id: TableId, from_key: F, to_key: T)
  where
    F: AsRef<[u8]>,
    T: AsRef<[u8]>, {
    self
      .inner_write_batch_mut()
      .delete_range(build_inner_key(table_id, from_key), build_inner_key(table_id, to_key))
  }
}
