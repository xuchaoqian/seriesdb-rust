use bytes::Bytes;
use rocksdb::DBRawIterator;

use crate::cursor::*;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;

pub struct NormalCursor<'a> {
  inner: DBRawIterator<'a>,
  table_id: TableId,
  anchor: &'a Bytes,
}

impl<'a> Cursor for NormalCursor<'a> {
  #[inline]
  fn is_valid(&self) -> bool {
    self.inner.valid()
  }

  #[inline]
  fn status(&self) -> Result<(), Error> {
    Ok(self.inner.status()?)
  }

  #[inline]
  fn seek_to_first(&mut self) {
    self.inner.seek(self.table_id)
  }

  #[inline]
  fn seek_to_last(&mut self) {
    self.inner.seek_for_prev(self.anchor);
  }

  #[inline]
  fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
    self.inner.seek(build_inner_key(self.table_id, key));
  }

  #[inline]
  fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
    self.inner.seek_for_prev(build_inner_key(self.table_id, key));
  }

  #[inline]
  fn next(&mut self) {
    self.inner.next()
  }

  #[inline]
  fn prev(&mut self) {
    self.inner.prev()
  }

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
  pub(crate) fn new(inner: DBRawIterator<'a>, table_id: TableId, anchor: &'a Bytes) -> Self {
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
