use rocksdb::WriteBatch as WriteBatchInner;

use super::write_batch::*;
use crate::types::*;

pub struct NormalWriteBatch {
  pub(crate) inner: WriteBatchInner,
  table_id: TableId,
}

impl WriteBatch for NormalWriteBatch {
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut WriteBatchInner {
    &mut self.inner
  }

  #[inline(always)]
  fn table_id(&self) -> TableId {
    self.table_id
  }
}

impl NormalWriteBatch {
  #[inline]
  pub fn new(table_id: TableId) -> Self {
    NormalWriteBatch { inner: WriteBatchInner::default(), table_id }
  }
}
