use rocksdb::DBWALIterator;

use super::write_op_batch::WriteOpBatch;
use crate::error::Error;

pub struct WriteOpBatchIterator {
  inner: DBWALIterator,
}

impl Iterator for WriteOpBatchIterator {
  type Item = Result<WriteOpBatch, Error>;

  fn next(&mut self) -> Option<Self::Item> {
    match self.inner.next() {
      Some(result) => {
        match result {
          Ok((sn, batch_inner)) => {
            let mut batch = WriteOpBatch::new();
            batch.sn = sn;
            batch_inner.iterate(&mut batch);
            return Some(Ok(batch));
          }
          Err(err) => {
            return Some(Err(Error::RocksdbError(err)));
          }
        };
      }
      None => return None,
    };
  }
}

impl WriteOpBatchIterator {
  #[inline]
  pub fn new(inner: DBWALIterator) -> Self {
    WriteOpBatchIterator { inner }
  }
}
