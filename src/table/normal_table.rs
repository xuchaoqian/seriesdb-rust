use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use rocksdb::ReadOptions;
use rocksdb::DB as RocksdbDb;

use super::table::Table;
use crate::cursor::*;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;
use crate::write_batch::*;

#[derive(Clone)]
pub struct NormalTable {
  pub(crate) inner_db: Arc<RocksdbDb>,
  pub(crate) id: TableId,
  pub(crate) anchor: Bytes,
}

impl fmt::Debug for NormalTable {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "id: {:?}, anchor: {:?}", self.id, self.anchor)
  }
}

impl Table for NormalTable {
  type Cursor<'a> = NormalCursor<'a>;
  type WriteBatch = NormalWriteBatch;

  #[inline(always)]
  fn id(&self) -> TableId {
    self.id
  }

  #[inline]
  fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    Ok(self.inner_db.put(build_inner_key(self.id, key), value)?)
  }

  #[inline]
  fn new_write_batch(&self) -> Self::WriteBatch {
    NormalWriteBatch::new(self.id)
  }

  #[inline]
  fn write(&self, batch: Self::WriteBatch) -> Result<(), Error> {
    Ok(self.inner_db.write(batch.inner)?)
  }

  #[inline]
  fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
    Ok(self.inner_db.delete(build_inner_key(self.id, key))?)
  }

  #[inline]
  fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Bytes>, Error> {
    Ok(self.inner_db.get(build_inner_key(self.id, key))?.map(|value| Bytes::from(value)))
  }

  #[inline]
  fn new_cursor<'a>(&'a self) -> Self::Cursor<'a> {
    let mut opts = ReadOptions::default();
    opts.set_prefix_same_as_start(true);
    NormalCursor::new(self.inner_db.raw_iterator_opt(opts), self.id, &self.anchor)
  }
}

impl NormalTable {
  #[inline]
  pub(crate) fn new(inner_db: Arc<RocksdbDb>, id: TableId, anchor: Bytes) -> Self {
    NormalTable { inner_db, id, anchor }
  }
}

#[cfg(test)]
mod tests {
  use crate::db::*;
  use crate::setup;
  use crate::table::*;

  #[test]
  fn test_put() {
    setup!("normal_table.test_put"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    let result = table.put(b"k111", b"v111");
    assert!(result.is_ok());
  }

  #[allow(unused_must_use)]
  #[test]
  fn test_get() {
    setup!("normal_table.test_get"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    table.put(b"k111", b"v111");
    let result = table.get(b"k111");
    assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
  }

  #[allow(unused_must_use)]
  #[test]
  fn test_delete() {
    setup!("normal_table.test_delete"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    table.put(b"k111", b"v111");
    table.get(b"k111");
    let result = table.delete(b"k111");
    assert!(result.is_ok());
    let result = table.get(b"k111");
    assert!(result.unwrap().is_none());
  }
}
