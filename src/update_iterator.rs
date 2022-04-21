use crate::update_batch::UpdateBatch;
use rocksdb::DBWALIterator;

pub struct UpdateIterator {
    inner: DBWALIterator,
}

impl Iterator for UpdateIterator {
    type Item = UpdateBatch;
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.next();
        if result.is_none() {
            None
        } else {
            let (sn, b) = result.unwrap();
            let mut ub = UpdateBatch::new();
            ub.sn = sn;
            b.iterate(&mut ub);
            Some(ub)
        }
    }
}

impl UpdateIterator {
    pub fn new(inner: DBWALIterator) -> Self {
        UpdateIterator { inner }
    }
}
