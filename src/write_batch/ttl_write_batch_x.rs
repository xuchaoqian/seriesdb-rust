use rocksdb::WriteBatch as WriteBatchInner;

use super::write_batch_x::*;

pub struct TtlWriteBatchX {
  pub(crate) inner: WriteBatchInner,
}

impl WriteBatchX for TtlWriteBatchX {
  #[doc(hidden)]
  #[inline]
  fn inner_write_batch_mut(&mut self) -> &mut WriteBatchInner {
    &mut self.inner
  }
}

impl TtlWriteBatchX {
  #[inline]
  pub(crate) fn new() -> Self {
    TtlWriteBatchX { inner: WriteBatchInner::default() }
  }
}
