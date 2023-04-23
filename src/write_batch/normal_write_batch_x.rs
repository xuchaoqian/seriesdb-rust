use rocksdb::WriteBatch as RocksdbWriteBatch;

use super::write_batch_x::*;

pub struct NormalWriteBatchX {
  pub(crate) inner: RocksdbWriteBatch,
}

impl WriteBatchX for NormalWriteBatchX {
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    &mut self.inner
  }
}

impl NormalWriteBatchX {
  #[inline]
  pub(crate) fn new() -> Self {
    NormalWriteBatchX { inner: RocksdbWriteBatch::default() }
  }
}
