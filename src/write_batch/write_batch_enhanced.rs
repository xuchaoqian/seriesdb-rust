use std::marker::PhantomData;

use crate::coder::Coder;
use crate::error::Error;
use crate::write_batch::*;

pub struct WriteBatchEnhanced<WB: WriteBatch, K, V, C: Coder<K, V>> {
  pub(crate) raw: WB,
  phantom: PhantomData<(K, V, C)>,
}

impl<WB: WriteBatch, K, V, C: Coder<K, V>> WriteBatchEnhanced<WB, K, V, C> {
  #[inline]
  pub fn new(raw: WB) -> Self {
    Self { raw, phantom: PhantomData }
  }

  #[inline]
  pub fn put(&mut self, key: &K, value: &V) {
    self.raw.put(C::encode_key(key), C::encode_value(value))
  }

  #[inline]
  pub fn delete(&mut self, key: &K) {
    self.raw.delete(C::encode_key(key))
  }

  #[inline]
  pub fn delete_range(&mut self, from_key: &K, to_key: &K) {
    self.raw.delete_range(C::encode_key(from_key), C::encode_key(to_key))
  }

  #[inline]
  pub fn write(self) -> Result<(), Error> {
    self.raw.write()
  }
}

#[cfg(test)]
mod tests {
  use byteorder::{BigEndian, ByteOrder};
  use bytes::{BufMut, Bytes, BytesMut};

  use crate::coder::Coder as SeriesdbCoder;
  use crate::db::*;
  use crate::table::*;
  use crate::types::*;
  use crate::{setup, setup_with_ttl};

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
  fn test_write_batch_for_normal_table() {
    setup!("write_batch_enhanced.test_write_batch_for_normal_table"; db);

    let name = "huobi.btc.usdt.1m";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();

    let mut wb = table.new_write_batch();
    wb.put(&1, &1);
    wb.put(&2, &2);
    wb.put(&3, &3);
    wb.put(&4, &4);
    wb.put(&5, &5);
    wb.delete(&2);
    wb.delete_range(&3, &5);
    assert!(wb.write().is_ok());

    assert_eq!(table.get(&1).unwrap().unwrap(), 1);
    assert_eq!(table.get(&5).unwrap().unwrap(), 5);

    assert!(table.get(&2).unwrap().is_none());
    assert!(table.get(&3).unwrap().is_none());
    assert!(table.get(&4).unwrap().is_none());
  }

  #[test]
  fn test_write_batch_for_ttl_table() {
    setup_with_ttl!("write_batch_enhanced.test_write_batch_for_ttl_table"; 3; db);

    let name = "huobi.btc.usdt.1m";
    let table = db.open_table(name).unwrap().enhance::<Key, Value, Coder>();

    let mut wb = table.new_write_batch();
    wb.put(&1, &1);
    wb.put(&2, &2);
    wb.put(&3, &3);
    wb.put(&4, &4);
    wb.put(&5, &5);
    wb.delete(&2);
    wb.delete_range(&3, &5);
    assert!(wb.write().is_ok());

    assert_eq!(table.get(&1).unwrap().unwrap(), 1);
    assert_eq!(table.get(&5).unwrap().unwrap(), 5);

    assert!(table.get(&2).unwrap().is_none());
    assert!(table.get(&3).unwrap().is_none());
    assert!(table.get(&4).unwrap().is_none());
  }
}
