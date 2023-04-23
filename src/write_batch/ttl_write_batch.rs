use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::write_batch::*;
use crate::types::*;

pub struct TtlWriteBatch {
  pub(crate) inner: RocksdbWriteBatch,
  table_id: TableId,
}

impl WriteBatch for TtlWriteBatch {
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    &mut self.inner
  }

  #[inline(always)]
  fn table_id(&self) -> TableId {
    self.table_id
  }
}

impl TtlWriteBatch {
  #[inline]
  pub fn new(table_id: TableId) -> Self {
    TtlWriteBatch { inner: RocksdbWriteBatch::default(), table_id }
  }
}
