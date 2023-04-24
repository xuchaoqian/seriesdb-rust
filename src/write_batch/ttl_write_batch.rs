use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::write_batch::*;
use crate::types::*;
use crate::utils::*;

pub struct TtlWriteBatch {
  pub(crate) inner: RocksdbWriteBatch,
  pub(crate) table_id: TableId,
}

impl WriteBatch for TtlWriteBatch {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    &mut self.inner
  }

  #[inline(always)]
  fn table_id(&self) -> TableId {
    self.table_id
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn put<K, V>(&mut self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    let table_id = self.table_id();
    self
      .inner_mut()
      .put(build_inner_key(table_id, key), build_timestamped_value(u32_to_u8a4(now()), value))
  }
}

impl TtlWriteBatch {
  #[inline]
  pub fn new(table_id: TableId) -> Self {
    TtlWriteBatch { inner: RocksdbWriteBatch::default(), table_id }
  }
}
