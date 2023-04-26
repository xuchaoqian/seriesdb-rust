use std::sync::Arc;

use rocksdb::WriteBatch as RocksdbWriteBatch;
use rocksdb::DB as RocksdbDb;

use super::write_batch_x::*;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;

pub struct TtlWriteBatchX {
  pub(crate) inner_db: Arc<RocksdbDb>,
  pub(crate) inner: Option<RocksdbWriteBatch>,
}

impl WriteBatchX for TtlWriteBatchX {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters s
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner_mut(&mut self) -> &mut RocksdbWriteBatch {
    self.inner.as_mut().unwrap()
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn put<K, V>(&mut self, table_id: TableId, key: K, value: V)
  where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>, {
    self
      .inner_mut()
      .put(build_inner_key(table_id, key), build_timestamped_value(u32_to_u8a4(now()), value))
  }

  #[inline]
  fn write(mut self) -> Result<(), Error> {
    Ok(self.inner_db.write(self.inner.take().unwrap())?)
  }
}

impl TtlWriteBatchX {
  #[inline]
  pub(crate) fn new(inner_db: Arc<RocksdbDb>) -> Self {
    TtlWriteBatchX { inner_db, inner: Some(RocksdbWriteBatch::default()) }
  }
}

#[cfg(test)]
mod tests {

  use crate::db::*;
  use crate::setup_with_ttl;
  use crate::table::*;
  use crate::write_batch::*;

  #[test]
  fn test_write_batch() {
    setup_with_ttl!("ttl_write_batch_x.test_write_batch"; 3; db);

    let name1m = "huobi.btc.usdt.1m";
    let table1m = db.open_table(name1m).unwrap();
    let name3m = "huobi.btc.usdt.3m";
    let table3m = db.open_table(name3m).unwrap();

    let mut wb = db.new_write_batch_x();
    wb.put(table1m.id(), b"k1", b"v1");
    wb.put(table1m.id(), b"k2", b"v2");
    wb.put(table1m.id(), b"k3", b"v3");
    wb.put(table3m.id(), b"k4", b"v4");
    wb.put(table3m.id(), b"k5", b"v5");

    wb.delete(table1m.id(), b"k2");
    wb.delete_range(table3m.id(), b"k3", b"k5");
    assert!(wb.write().is_ok());

    assert_eq!(table1m.get(b"k1").unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(table1m.get(b"k3").unwrap().unwrap().as_ref(), b"v3");

    assert!(table3m.get(b"k3").unwrap().is_none());
    assert!(table3m.get(b"k4").unwrap().is_none());
    assert_eq!(table3m.get(b"k5").unwrap().unwrap().as_ref(), b"v5");
  }
}
