use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::write_batch_x::*;

pub struct TtlWriteBatchX {
  pub(crate) inner: RocksdbWriteBatch,
}

impl WriteBatchX for TtlWriteBatchX {
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    &mut self.inner
  }
}

impl TtlWriteBatchX {
  #[inline]
  pub(crate) fn new() -> Self {
    TtlWriteBatchX { inner: RocksdbWriteBatch::default() }
  }
}
