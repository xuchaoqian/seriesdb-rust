use rocksdb::WriteBatch as WriteBatchInner;

use super::write_batch::*;
use crate::types::*;

pub struct TtlWriteBatch {
  pub(crate) inner: WriteBatchInner,
  table_id: TableId,
}

impl WriteBatch for TtlWriteBatch {
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

impl TtlWriteBatch {
  #[inline]
  pub(crate) fn new(table_id: TableId) -> Self {
    TtlWriteBatch { inner: WriteBatchInner::default(), table_id }
  }
}
