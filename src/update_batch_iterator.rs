use crate::update_batch::UpdateBatch;
use rocksdb::DBWALIterator;

pub struct UpdateBatchIterator {
  inner: DBWALIterator,
}

impl Iterator for UpdateBatchIterator {
  type Item = UpdateBatch;
  fn next(&mut self) -> Option<Self::Item> {
    if let Some(result) = self.inner.next() {
      let (sn, b) = result.unwrap();
      let mut ub = UpdateBatch::new();
      ub.sn = sn;
      b.iterate(&mut ub);
      Some(ub)
    } else {
      None
    }
  }
}

impl UpdateBatchIterator {
  pub fn new(inner: DBWALIterator) -> Self {
    UpdateBatchIterator { inner }
  }
}
