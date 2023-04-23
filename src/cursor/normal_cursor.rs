use bytes::Bytes;
use rocksdb::DBRawIterator;

use crate::cursor::*;
use crate::types::*;

pub struct NormalCursor<'a> {
  pub(crate) inner: DBRawIterator<'a>,
  pub(crate) table_id: TableId,
  pub(crate) anchor: &'a Bytes,
}

impl<'a> Cursor<'a> for NormalCursor<'a> {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner(&self) -> &DBRawIterator<'a> {
    &self.inner
  }

  #[inline(always)]
  fn inner_mut(&mut self) -> &mut DBRawIterator<'a> {
    &mut self.inner
  }

  #[inline(always)]
  fn table_id(&self) -> TableId {
    self.table_id
  }

  #[inline(always)]
  fn anchor(&self) -> &'a Bytes {
    &self.anchor
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn value(&self) -> Option<&[u8]> {
    self.inner.value()
  }
}

impl<'a> NormalCursor<'a> {
  pub fn new(inner: DBRawIterator<'a>, table_id: TableId, anchor: &'a Bytes) -> Self {
    NormalCursor { inner, table_id, anchor }
  }
}

#[cfg(test)]
mod tests {
  use crate::cursor::*;
  use crate::db::*;
  use crate::setup;
  use crate::table::*;

  #[test]
  fn test_seek() {
    setup!("normal_cursor.test_seek"; db);
    let name = "huobi.btc.usdt.1m";
    let table = db.open_table(name).unwrap();
    let k1 = b"k1";
    let v1 = b"v1";
    let k2 = b"k2";
    let v2 = b"v2";
    let k3 = b"k3";
    let v3 = b"v3";
    assert!(table.put(k1, v1).is_ok());
    assert!(table.put(k2, v2).is_ok());
    assert!(table.put(k3, v3).is_ok());
    let mut iter = table.cursor();
    iter.seek_to_first();
    assert!(iter.is_valid());
    assert_eq!(k1, iter.key().unwrap());
  }
}
