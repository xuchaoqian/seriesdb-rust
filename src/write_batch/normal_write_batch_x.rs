use rocksdb::WriteBatch as WriteBatchInner;

use super::write_batch_x::*;

pub struct NormalWriteBatchX {
  pub(crate) inner: WriteBatchInner,
}

impl WriteBatchX for NormalWriteBatchX {
  #[doc(hidden)]
  #[inline]
  fn inner_write_batch_mut(&mut self) -> &mut WriteBatchInner {
    &mut self.inner
  }
}

impl NormalWriteBatchX {
  #[inline]
  pub(crate) fn new() -> Self {
    NormalWriteBatchX { inner: WriteBatchInner::default() }
  }
}
