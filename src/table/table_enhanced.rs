use std::{marker::PhantomData, sync::Arc};

use super::table::Table;
use crate::coder::*;
use crate::cursor::*;
use crate::error::Error;
use crate::types::*;
use crate::write_batch::*;

#[derive(Debug)]
pub struct TableEnhanced<T: Table, K, V, C: Coder<K, V>> {
  pub(crate) raw: Arc<T>,
  phantom: PhantomData<(K, V, C)>,
}

impl<T: Table, K, V, C: Coder<K, V>> TableEnhanced<T, K, V, C> {
  #[inline]
  pub fn new(raw: Arc<T>) -> Self {
    Self { raw, phantom: PhantomData }
  }

  #[inline(always)]
  pub fn raw(&self) -> &Arc<T> {
    &self.raw
  }

  #[inline(always)]
  pub fn id(&self) -> TableId {
    self.raw.id()
  }

  #[inline]
  pub fn put(&self, key: &K, value: &V) -> Result<(), Error> {
    self.raw.put(C::encode_key(key), C::encode_value(value))
  }

  #[inline]
  pub fn delete(&self, key: &K) -> Result<(), Error> {
    self.raw.delete(C::encode_key(key))
  }

  #[inline]
  pub fn delete_range(&self, from_key: &K, to_key: &K) -> Result<(), Error> {
    self.raw.delete_range(C::encode_key(from_key), C::encode_key(to_key))
  }

  #[inline]
  pub fn get(&self, key: &K) -> Result<Option<V>, Error> {
    Ok(self.raw.get(C::encode_key(key))?.map(|value| C::decode_value(value.as_ref())))
  }

  #[inline]
  pub fn get_reverse_nth(&self, n: u32) -> Option<V> {
    let mut value = None;
    let mut count = 0;
    let mut cursor = self.new_cursor();
    cursor.seek_to_last();
    while cursor.is_valid() {
      if count == n {
        value = Some(cursor.value().unwrap());
        break;
      }
      cursor.prev();
      count += 1;
    }
    value
  }

  #[inline]
  pub fn get_since(&self, key: &K, limit: u32) -> Vec<V> {
    let mut values = Vec::new();
    let mut count = 0;
    let mut cursor = self.new_cursor();
    cursor.seek(key);
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      values.push(cursor.value().unwrap());
      cursor.next();
      count += 1;
    }
    values
  }

  #[inline]
  pub fn get_until(&self, key: &K, limit: u32) -> Vec<V> {
    let mut reversed_values = Vec::new();
    let mut count = 0;
    let mut cursor = self.new_cursor();
    cursor.seek_for_prev(key);
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      reversed_values.push(cursor.value().unwrap());
      cursor.prev();
      count += 1;
    }
    reversed_values.reverse();
    reversed_values
  }

  #[inline]
  pub fn get_until_last(&self, limit: u32) -> Vec<V> {
    let mut reversed_values = Vec::new();
    let mut count = 0;
    let mut cursor = self.new_cursor();
    cursor.seek_to_last();
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      reversed_values.push(cursor.value().unwrap());
      cursor.prev();
      count += 1;
    }
    reversed_values.reverse();
    reversed_values
  }

  #[inline]
  pub fn get_between(&self, begin_key: &K, end_key: &K, limit: u32) -> Vec<V> {
    let mut values = Vec::new();
    let mut count = 0;
    let mut cursor = self.new_cursor().raw;
    cursor.seek(C::encode_key(begin_key));
    let end_key = C::encode_key(end_key);
    let end_key = end_key.as_ref();
    while cursor.is_valid() {
      if count >= limit {
        break;
      }
      let curr_key = cursor.key().unwrap();
      if curr_key > end_key {
        break;
      }
      values.push(C::decode_value(cursor.value().unwrap()));
      cursor.next();
      count += 1;
    }
    values
  }

  #[inline]
  pub fn get_first_key(&self) -> Option<K> {
    let mut cursor = self.new_cursor();
    cursor.seek_to_first();
    if cursor.is_valid() {
      Some(cursor.key().unwrap())
    } else {
      None
    }
  }

  #[inline]
  pub fn get_last_key(&self) -> Option<K> {
    let mut cursor = self.new_cursor();
    cursor.seek_to_last();
    if cursor.is_valid() {
      Some(cursor.key().unwrap())
    } else {
      None
    }
  }

  #[inline]
  pub fn get_boundary_keys(&self) -> Option<(K, K)> {
    let mut cursor = self.new_cursor();
    cursor.seek_to_first();
    if cursor.is_valid() {
      let first_ts = cursor.key().unwrap();
      cursor.seek_to_last();
      let last_ts = cursor.key().unwrap();
      Some((first_ts, last_ts))
    } else {
      None
    }
  }

  #[inline]
  pub fn new_write_batch(&self) -> WriteBatchEnhanced<T::WriteBatch, K, V, C> {
    self.raw.new_write_batch().enhance()
  }

  #[inline]
  pub fn new_cursor<'a>(&'a self) -> CursorEnhanced<T::Cursor<'a>, K, V, C> {
    self.raw.new_cursor().enhance()
  }
}

#[cfg(test)]
mod tests {

  use std::vec;

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
    fn encode_key(key: &Key) -> Self::EncodedKey {
      let mut buf = [0; 4];
      BigEndian::write_u32(&mut buf, *key);
      buf
    }

    #[inline(always)]
    fn decode_key(key: &[u8]) -> Key {
      BigEndian::read_u32(key)
    }

    #[inline(always)]
    fn encode_value(value: &Value) -> Self::EncodedValue {
      let mut buf = BytesMut::with_capacity(4);
      buf.put_u32(*value);
      buf.freeze()
    }

    #[inline(always)]
    fn decode_value(value: &[u8]) -> Value {
      BigEndian::read_u32(value)
    }
  }

  #[test]
  fn test_basic() {
    setup!("table_enhanced.test_basic"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();

    assert!(table.put(&1, &1).is_ok());
    assert_eq!(table.get(&1).unwrap().unwrap(), 1);

    assert!(table.delete(&1).is_ok());
    assert!(table.get(&1).unwrap().is_none());

    assert!(table.put(&2, &2).is_ok());
    assert!(table.put(&3, &3).is_ok());
    assert_eq!(table.get(&2).unwrap().unwrap(), 2);
    assert_eq!(table.get(&3).unwrap().unwrap(), 3);

    assert!(table.delete_range(&2, &3).is_ok());
    assert!(table.get(&2).unwrap().is_none());
    assert_eq!(table.get(&3).unwrap().unwrap(), 3);
  }

  #[test]
  fn test_get_reverse_nth() {
    setup!("table_enhanced.test_get_reverse_nth"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();
    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_reverse_nth(0).unwrap(), 3);
    assert_eq!(table.get_reverse_nth(1).unwrap(), 2);
    assert_eq!(table.get_reverse_nth(2).unwrap(), 1);
    assert!(table.get_reverse_nth(3).is_none());
  }

  #[test]
  fn test_get_since() {
    setup!("table_enhanced.get_since"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();
    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_since(&0, 3), vec![1, 2, 3]);
    assert_eq!(table.get_since(&1, 2), vec![1, 2]);
    assert_eq!(table.get_since(&1, 1), vec![1]);
    assert_eq!(table.get_since(&4, 3), vec![]);
  }

  #[test]
  fn test_get_until() {
    setup!("table_enhanced.get_until"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();
    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_until(&0, 3), vec![]);
    assert_eq!(table.get_until(&1, 2), vec![1]);
    assert_eq!(table.get_until(&3, 2), vec![2, 3]);
    assert_eq!(table.get_until(&4, 3), vec![1, 2, 3]);
  }

  #[test]
  fn test_get_until_last() {
    setup!("table_enhanced.get_until_last"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();
    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_until_last(3), vec![1, 2, 3]);
    assert_eq!(table.get_until_last(2), vec![2, 3]);
    assert_eq!(table.get_until_last(1), vec![3]);
    assert_eq!(table.get_until_last(0), vec![]);
  }

  #[test]
  fn test_get_between() {
    setup!("table_enhanced.get_between"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();
    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_between(&1, &3, 3), vec![1, 2, 3]);
    assert_eq!(table.get_between(&1, &3, 0), vec![]);
    assert_eq!(table.get_between(&1, &3, 1), vec![1]);
    assert_eq!(table.get_between(&0, &2, 3), vec![1, 2]);
  }

  #[test]
  fn test_get_first_key() {
    setup!("table_enhanced.get_first_key"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();

    assert!(table.get_first_key().is_none());

    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_first_key().unwrap(), 1);
  }

  #[test]
  fn test_get_last_key() {
    setup!("table_enhanced.get_last_key"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();

    assert!(table.get_last_key().is_none());

    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_last_key().unwrap(), 3);
  }

  #[test]
  fn test_get_boundary_keys() {
    setup!("table_enhanced.get_boundary_keys"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();

    assert!(table.get_boundary_keys().is_none());

    table.put(&1, &1).unwrap();
    table.put(&2, &2).unwrap();
    table.put(&3, &3).unwrap();

    assert_eq!(table.get_boundary_keys().unwrap(), (1, 3));
  }
}
