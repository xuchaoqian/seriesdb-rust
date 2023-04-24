use rocksdb::WriteBatch as WriteBatchInner;

use super::write_batch::*;
use crate::types::*;
use crate::utils::*;

pub struct NormalWriteBatch {
  pub(crate) inner: WriteBatchInner,
  pub(crate) table_id: TableId,
}

impl WriteBatch for NormalWriteBatch {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut WriteBatchInner {
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
    self.inner_mut().put(build_inner_key(table_id, key), value)
  }
}

impl NormalWriteBatch {
  #[inline]
  pub fn new(table_id: TableId) -> Self {
    NormalWriteBatch { inner: WriteBatchInner::default(), table_id }
  }
}
