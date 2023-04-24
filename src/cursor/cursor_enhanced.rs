use std::marker::PhantomData;

use crate::coder::*;
use crate::cursor::*;
use crate::error::Error;

pub struct CursorEnhanced<'a, C: Cursor<'a>, K, V, C2: Coder<K, V>> {
  pub(crate) raw: C,
  phantom: PhantomData<&'a (K, V, C2)>,
}

impl<'a, C: Cursor<'a>, K, V, C2: Coder<K, V>> CursorEnhanced<'a, C, K, V, C2> {
  #[inline]
  pub fn new(raw: C) -> Self {
    Self { raw, phantom: PhantomData }
  }

  #[inline]
  pub fn is_valid(&self) -> bool {
    self.raw.is_valid()
  }

  #[inline]
  pub fn status(&self) -> Result<(), Error> {
    Ok(self.raw.status()?)
  }

  #[inline]
  pub fn seek_to_first(&mut self) {
    self.raw.seek_to_first()
  }

  #[inline]
  pub fn seek_to_last(&mut self) {
    self.raw.seek_to_last();
  }

  #[inline]
  pub fn seek(&mut self, key: K) {
    self.raw.seek(C2::encode_key(key));
  }

  #[inline]
  pub fn seek_for_prev(&mut self, key: K) {
    self.raw.seek_for_prev(C2::encode_key(key));
  }

  #[inline]
  pub fn next(&mut self) {
    self.raw.next()
  }

  #[inline]
  pub fn prev(&mut self) {
    self.raw.prev()
  }

  #[inline]
  pub fn key(&'a self) -> Option<K> {
    self.raw.key().map(|key| C2::decode_key(key))
  }

  #[inline]
  pub fn value(&self) -> Option<V> {
    self.raw.value().map(|value| C2::decode_value(value))
  }
}

#[cfg(test)]
mod tests {
  use byteorder::{BigEndian, ByteOrder};
  use bytes::{BufMut, Bytes, BytesMut};

  use crate::coder::Coder as SeriesdbCoder;
  use crate::db::*;
  use crate::setup;
  use crate::table::*;
  use crate::types::*;

  type Key = u32;

  type Value = u32;

  struct Coder;
  impl SeriesdbCoder<Key, Value> for Coder {
    type EncodedKey = U8a4;
    type EncodedValue = Bytes;

    #[inline(always)]
    fn encode_key(key: Key) -> Self::EncodedKey {
      let mut buf = [0; 4];
      BigEndian::write_u32(&mut buf, key);
      buf
    }

    #[inline(always)]
    fn decode_key(key: &[u8]) -> Key {
      BigEndian::read_u32(key)
    }

    #[inline(always)]
    fn encode_value(value: Value) -> Self::EncodedValue {
      let mut buf = BytesMut::with_capacity(4);
      buf.put_u32(value);
      buf.freeze()
    }

    #[inline(always)]
    fn decode_value(value: &[u8]) -> Value {
      BigEndian::read_u32(value)
    }
  }

  #[test]
  fn test_seek() {
    setup!("cursor_enhanced.test_seek"; db);
    let name = "huobi.btc.usdt.1m";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();
    let k1 = 1;
    let v1 = 1;
    let k2 = 2;
    let v2 = 2;
    let k3 = 3;
    let v3 = 3;
    assert!(table.put(k1, v1).is_ok());
    assert!(table.put(k2, v2).is_ok());
    assert!(table.put(k3, v3).is_ok());

    let mut cursor = table.new_cursor();

    cursor.seek_to_first();
    assert!(cursor.is_valid());
    assert_eq!(k1, cursor.key().unwrap());
    assert_eq!(v1, cursor.value().unwrap());

    cursor.seek_to_last();
    assert!(cursor.is_valid());
    assert_eq!(k3, cursor.key().unwrap());
    assert_eq!(v3, cursor.value().unwrap());

    cursor.prev();
    assert!(cursor.is_valid());
    assert_eq!(k2, cursor.key().unwrap());
    assert_eq!(v2, cursor.value().unwrap());

    cursor.next();
    assert!(cursor.is_valid());
    assert_eq!(k3, cursor.key().unwrap());
    assert_eq!(v3, cursor.value().unwrap());

    cursor.seek_for_prev(1);
    assert!(cursor.is_valid());
    assert_eq!(k1, cursor.key().unwrap());
    assert_eq!(v1, cursor.value().unwrap());

    cursor.seek_for_prev(0);
    assert!(!cursor.is_valid());

    cursor.seek_for_prev(4);
    assert_eq!(k3, cursor.key().unwrap());
    assert_eq!(v3, cursor.value().unwrap());

    cursor.seek(2);
    assert_eq!(k2, cursor.key().unwrap());
    assert_eq!(v2, cursor.value().unwrap());
  }
}
