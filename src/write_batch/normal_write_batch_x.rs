use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::write_batch_x::*;
use crate::types::*;
use crate::utils::*;

pub struct NormalWriteBatchX {
  pub(crate) inner: RocksdbWriteBatch,
}

impl WriteBatchX for NormalWriteBatchX {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    &mut self.inner
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn put<K, V>(&mut self, table_id: TableId, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    self.inner_mut().put(build_inner_key(table_id, key), value)
  }
}

impl NormalWriteBatchX {
  #[inline]
  pub(crate) fn new() -> Self {
    NormalWriteBatchX { inner: RocksdbWriteBatch::default() }
  }
}
