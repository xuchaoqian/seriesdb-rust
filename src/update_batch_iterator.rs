use crate::update_batch::UpdateBatch;
use crate::Error;
use rocksdb::DBWALIterator;

pub struct UpdateBatchIterator {
  inner: DBWALIterator,
}

impl Iterator for UpdateBatchIterator {
  type Item = Result<UpdateBatch, Error>;

  fn next(&mut self) -> Option<Self::Item> {
    match self.inner.next() {
      Some(result) => {
        match result {
          Ok((sn, batch_inner)) => {
            let mut batch = UpdateBatch::new();
            batch.sn = sn;
            batch_inner.iterate(&mut batch);
            return Some(Ok(batch));
          }
          Err(err) => {
            return Some(Err(err));
          }
        };
      }
      None => return None,
    };
  }
}

impl UpdateBatchIterator {
  pub fn new(inner: DBWALIterator) -> Self {
    UpdateBatchIterator { inner }
  }
}
