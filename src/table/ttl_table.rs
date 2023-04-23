use std::fmt;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use rocksdb::ReadOptions;
use rocksdb::DB as RocksdbDb;

use super::table::Table;
use crate::consts::*;
use crate::cursor::*;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;
use crate::write_batch::*;

#[derive(Clone)]
pub struct TtlTable {
  pub(crate) inner_db: Arc<RocksdbDb>,
  pub(crate) id: TableId,
  pub(crate) anchor: Bytes,
}

impl fmt::Debug for TtlTable {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "id: {:?}, anchor: {:?}", self.id, self.anchor)
  }
}

impl Table for TtlTable {
  type Cursor<'a> = TtlCursor<'a>;
  type WriteBatch = TtlWriteBatch;

  #[inline(always)]
  fn id(&self) -> TableId {
    self.id
  }

  #[inline]
  fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    Ok(
      self
        .inner_db
        .put(build_inner_key(self.id, key), build_timestamped_value(u32_to_u8a4(now()), value))?,
    )
  }

  #[inline]
  fn new_write_batch(&self) -> Self::WriteBatch {
    TtlWriteBatch::new(self.id)
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
    if let Some(timestamped_value) = self.inner_db.get(build_inner_key(self.id, key))? {
      let mut value = Bytes::from(timestamped_value);
      value.advance(TIMESTAMP_LEN);
      return Ok(Some(value));
    } else {
      return Ok(None);
    }
  }

  #[inline]
  fn cursor<'a>(&'a self) -> Self::Cursor<'a> {
    let mut opts = ReadOptions::default();
    opts.set_prefix_same_as_start(true);
    TtlCursor::new(self.inner_db.raw_iterator_opt(opts), self.id, &self.anchor)
  }
}

impl TtlTable {
  #[inline]
  pub(crate) fn new(inner_db: Arc<RocksdbDb>, id: TableId, anchor: Bytes) -> Self {
    TtlTable { inner_db, id, anchor }
  }
}

#[cfg(test)]
mod tests {
  use crate::db::*;
  use crate::setup;
  use crate::table::*;

  #[test]
  fn test_put() {
    setup!("ttl_table.test_put"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    let result = table.put(b"k111", b"v111");
    assert!(result.is_ok());
  }

  #[allow(unused_must_use)]
  #[test]
  fn test_get() {
    setup!("ttl_table.test_get"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    table.put(b"k111", b"v111");
    let result = table.get(b"k111");
    assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
  }

  #[allow(unused_must_use)]
  #[test]
  fn test_delete() {
    setup!("ttl_table.test_delete"; db);
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