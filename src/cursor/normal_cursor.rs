use bytes::Bytes;
use rocksdb::DBRawIterator;

use crate::cursor::*;
use crate::types::*;
use crate::utils::*;

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
  fn key(&self) -> Option<&[u8]> {
    if let Some(v) = self.inner.key() {
      Some(extract_key(v))
    } else {
      None
    }
  }

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
  use bytes::Bytes;

  use crate::cursor::*;
  use crate::db::*;
  use crate::setup;
  use crate::table::*;

  #[test]
  fn test_seek() {
    setup!("normal_cursor.test_seek"; db);

    let name = "huobi.btc.usdt.1m";
    let table = db.open_table(name).unwrap();
    let name3m = "huobi.btc.usdt.3m";
    let table3m = db.open_table(name3m).unwrap();
    let name5m = "huobi.btc.usdt.5m";
    let table5m = db.open_table(name5m).unwrap();

    let k1 = b"k1";
    let v1 = b"v1";
    let k2 = b"k2";
    let v2 = b"v2";
    let k3 = b"k3";
    let k4 = b"k4";
    let v4 = b"v4";

    assert!(table.put(k1, v1).is_ok());
    assert!(table.put(k2, v2).is_ok());
    assert!(table.put(k4, v4).is_ok());

    let mut cursor = table.new_cursor();
    cursor.seek_to_first();
    assert!(cursor.is_valid());
    assert_eq!(k1, cursor.key().unwrap());

    cursor.seek_for_prev(k3);
    assert!(cursor.is_valid());
    assert_eq!(k2, cursor.key().unwrap());

    cursor.seek(k3);
    assert!(cursor.is_valid());
    assert_eq!(k4, cursor.key().unwrap());

    assert!(table5m.put(k1, v1).is_ok());
    assert_eq!(table5m.get(k1).unwrap().unwrap(), Bytes::from(v1.as_ref()));

    let mut cursor3m = table3m.new_cursor();
    cursor3m.seek_to_first();
    assert!(!cursor3m.is_valid());
  }
}
