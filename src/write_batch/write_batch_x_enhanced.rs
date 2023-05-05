use std::{borrow::Borrow, marker::PhantomData};

use crate::coder::Coder;
use crate::error::Error;
use crate::types::*;
use crate::write_batch::*;

pub struct WriteBatchXEnhanced<WB: WriteBatchX, K, V, C: Coder<K, V>> {
  pub(crate) raw: WB,
  phantom: PhantomData<(K, V, C)>,
}

impl<WB: WriteBatchX, K, V, C: Coder<K, V>> WriteBatchXEnhanced<WB, K, V, C> {
  #[inline]
  pub fn new(raw: WB) -> Self {
    Self { raw, phantom: PhantomData }
  }

  #[inline]
  pub fn put<BK: Borrow<K>, BV: Borrow<V>>(&mut self, table_id: TableId, key: BK, value: BV) {
    self.raw.put(table_id, C::encode_key(key), C::encode_value(value));
  }

  #[inline]
  pub fn delete<BK: Borrow<K>>(&mut self, table_id: TableId, key: BK) {
    self.raw.delete(table_id, C::encode_key(key))
  }

  #[inline]
  pub fn delete_range<BK: Borrow<K>>(&mut self, table_id: TableId, from_key: BK, to_key: BK) {
    self.raw.delete_range(table_id, C::encode_key(from_key), C::encode_key(to_key))
  }

  #[inline]
  pub fn write(self) -> Result<(), Error> {
    self.raw.write()
  }
}

#[cfg(test)]
mod tests {
  use std::borrow::Borrow;

  use byteorder::{BigEndian, ByteOrder};
  use bytes::{BufMut, Bytes, BytesMut};

  use crate::db::*;
  use crate::table::*;
  use crate::types::*;
  use crate::{coder::Coder as SeriesdbCoder, write_batch::WriteBatchX};
  use crate::{setup, setup_with_ttl};

  type Key = u32;

  type Value = u32;

  struct Coder;
  impl SeriesdbCoder<Key, Value> for Coder {
    type EncodedKey = U8a4;
    type EncodedValue = Bytes;

    #[inline(always)]
    fn encode_key<BK: Borrow<Key>>(key: BK) -> Self::EncodedKey {
      let mut buf = [0; 4];
      BigEndian::write_u32(&mut buf, *key.borrow());
      buf
    }

    #[inline(always)]
    fn decode_key(key: &[u8]) -> Key {
      BigEndian::read_u32(key)
    }

    #[inline(always)]
    fn encode_value<BV: Borrow<Value>>(value: BV) -> Self::EncodedValue {
      let mut buf = BytesMut::with_capacity(4);
      buf.put_u32(*value.borrow());
      buf.freeze()
    }

    #[inline(always)]
    fn decode_value(value: &[u8]) -> Value {
      BigEndian::read_u32(value)
    }
  }

  #[test]
  fn test_write_batch_for_normal_table() {
    setup!("write_batch_x_enhanced.test_write_batch_for_normal_table"; db);

    let name1m = "huobi.btc.usdt.1m";
    let table1m = db.open_table(name1m).unwrap().enhance::<Key, Value, Coder>();
    let name3m = "huobi.btc.usdt.3m";
    let table3m = db.open_table(name3m).unwrap().enhance::<Key, Value, Coder>();

    let mut wb = db.new_write_batch_x().enhance::<Key, Value, Coder>();
    wb.put(table1m.id(), 1, 1);
    wb.put(table1m.id(), 2, 2);
    wb.put(table1m.id(), 3, 3);
    wb.put(table3m.id(), 4, 4);
    wb.put(table3m.id(), 5, 5);

    wb.delete(table1m.id(), 2);
    wb.delete_range(table3m.id(), 3, 5);
    assert!(wb.write().is_ok());

    assert_eq!(table1m.get(1).unwrap().unwrap(), 1);
    assert_eq!(table1m.get(3).unwrap().unwrap(), 3);

    assert!(table3m.get(3).unwrap().is_none());
    assert!(table3m.get(4).unwrap().is_none());
    assert_eq!(table3m.get(5).unwrap().unwrap(), 5);
  }

  #[test]
  fn test_write_batch_for_ttl_table() {
    setup_with_ttl!("write_batch_x_enhanced.test_write_batch_for_ttl_table"; 3; db);

    let name1m = "huobi.btc.usdt.1m";
    let table1m = db.open_table(name1m).unwrap().enhance::<Key, Value, Coder>();
    let name3m = "huobi.btc.usdt.3m";
    let table3m = db.open_table(name3m).unwrap().enhance::<Key, Value, Coder>();

    let mut wb = db.new_write_batch_x().enhance::<Key, Value, Coder>();
    wb.put(table1m.id(), 1, 1);
    wb.put(table1m.id(), 2, 2);
    wb.put(table1m.id(), 3, 3);
    wb.put(table3m.id(), 4, 4);
    wb.put(table3m.id(), 5, 5);
    wb.delete(table1m.id(), 2);
    wb.delete_range(table3m.id(), 3, 5);
    assert!(wb.write().is_ok());

    assert_eq!(table1m.get(1).unwrap().unwrap(), 1);
    assert_eq!(table1m.get(3).unwrap().unwrap(), 3);

    assert!(table3m.get(3).unwrap().is_none());
    assert!(table3m.get(4).unwrap().is_none());
    assert_eq!(table3m.get(5).unwrap().unwrap(), 5);
  }
}
