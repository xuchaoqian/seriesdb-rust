use rocksdb::WriteBatch as WriteBatchInner;

use super::write_batch::*;
use crate::types::*;

pub struct NormalWriteBatch {
  pub(crate) inner: WriteBatchInner,
  table_id: TableId,
}

impl WriteBatch for NormalWriteBatch {
  #[doc(hidden)]
  #[inline]
  fn inner_write_batch_mut(&mut self) -> &mut WriteBatchInner {
    &mut self.inner
  }
  #[doc(hidden)]
  #[inline]
  fn table_id(&self) -> TableId {
    self.table_id
  }
}

impl NormalWriteBatch {
  #[inline]
  pub(crate) fn new(table_id: TableId) -> Self {
    NormalWriteBatch { inner: WriteBatchInner::default(), table_id }
  }
}
