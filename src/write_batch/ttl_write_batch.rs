use std::sync::Arc;

use rocksdb::WriteBatch as RocksdbWriteBatch;
use rocksdb::DB as RocksdbDb;

use super::write_batch::*;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;

pub struct TtlWriteBatch {
  pub(crate) inner_db: Arc<RocksdbDb>,
  pub(crate) inner: Option<RocksdbWriteBatch>,
  pub(crate) table_id: TableId,
}

impl WriteBatch for TtlWriteBatch {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    self.inner.as_mut().unwrap()
  }

  #[inline(always)]
  fn table_id(&self) -> TableId {
    self.table_id
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn put<K, V>(&mut self, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    let table_id = self.table_id();
    self
      .inner_mut()
      .put(build_inner_key(table_id, key), build_timestamped_value(u32_to_u8a4(now()), value))
  }

  #[inline]
  fn write(mut self) -> Result<(), Error> {
    Ok(self.inner_db.write(self.inner.take().unwrap())?)
  }
}

impl TtlWriteBatch {
  #[inline]
  pub fn new(inner_db: Arc<RocksdbDb>, table_id: TableId) -> Self {
    TtlWriteBatch { inner_db, inner: Some(RocksdbWriteBatch::default()), table_id }
  }
}

#[cfg(test)]
mod tests {

  use crate::db::*;
  use crate::setup_with_ttl;
  use crate::table::*;
  use crate::write_batch::WriteBatch;

  #[test]
  fn test_write_batch() {
    setup_with_ttl!("ttl_write_batch.test_write_batch"; 3; db);

    let name = "huobi.btc.usdt.1m";
    let table = db.open_table(name).unwrap();

    let mut wb = table.new_write_batch();
    wb.put(b"k1", b"v1");
    wb.put(b"k2", b"v2");
    wb.put(b"k3", b"v3");
    wb.put(b"k4", b"v4");
    wb.put(b"k5", b"v5");
    wb.delete(b"k2");
    wb.delete_range(b"k3", b"k5");
    assert!(wb.write().is_ok());

    assert_eq!(table.get(b"k1").unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(table.get(b"k5").unwrap().unwrap().as_ref(), b"v5");

    assert!(table.get(b"k2").unwrap().is_none());
    assert!(table.get(b"k3").unwrap().is_none());
    assert!(table.get(b"k4").unwrap().is_none());
  }
}
