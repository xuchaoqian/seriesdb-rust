use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use rocksdb::ReadOptions;
use rocksdb::DB as RocksdbDb;

use crate::entry_cursor::EntryCursor;
use crate::types::*;
use crate::utils::*;
use crate::write_batch::WriteBatch;
use crate::Error;

#[derive(Clone)]
pub struct Table {
  pub(crate) db_inner: Arc<RocksdbDb>,
  pub(crate) id: TableId,
  pub(crate) anchor: Bytes,
}

impl fmt::Debug for Table {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "id: {:?}, anchor: {:?}", self.id, self.anchor)
  }
}

impl Table {
  #[inline]
  pub(crate) fn new(db_inner: Arc<RocksdbDb>, id: TableId, anchor: Bytes) -> Table {
    Table { db_inner, id, anchor }
  }

  #[inline]
  pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    self.db_inner.put(build_inner_key(self.id, key), value)
  }

  #[inline]
  pub fn write_batch(&self) -> WriteBatch {
    WriteBatch::new(self.id)
  }

  #[inline]
  pub fn write(&self, batch: WriteBatch) -> Result<(), Error> {
    self.db_inner.write(batch.inner)
  }

  #[inline]
  pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
    self.db_inner.delete(build_inner_key(self.id, key))
  }

  #[inline]
  pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
    self.db_inner.get(build_inner_key(self.id, key))
  }

  #[inline]
  pub fn cursor(&self) -> EntryCursor {
    let mut opts = ReadOptions::default();
    opts.set_prefix_same_as_start(true);
    EntryCursor::new(self.db_inner.raw_iterator_opt(opts), self.id, &self.anchor)
  }

  #[inline]
  pub fn id(&self) -> TableId {
    self.id
  }
}

#[cfg(test)]
mod tests {
  use crate::setup;

  #[test]
  fn test_put() {
    setup!("test_put"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.new_table(name).unwrap();
    let result = table.put(b"k111", b"v111");
    assert!(result.is_ok());
  }

  #[allow(unused_must_use)]
  #[test]
  fn test_get() {
    setup!("test_get"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.new_table(name).unwrap();
    table.put(b"k111", b"v111");
    let result = table.get(b"k111");
    assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
  }

  #[allow(unused_must_use)]
  #[test]
  fn test_delete() {
    setup!("test_delete"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.new_table(name).unwrap();
    table.put(b"k111", b"v111");
    table.get(b"k111");
    let result = table.delete(b"k111");
    assert!(result.is_ok());
    let result = table.get(b"k111");
    assert!(result.unwrap().is_none());
  }
}
