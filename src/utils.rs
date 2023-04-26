use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use chrono::prelude::*;

use crate::consts::*;
use crate::types::*;

////////////////////////////////////////////////////////////////////////////////
/// conversion utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn u32_to_u8a4(u32: u32) -> U8a4 {
  let mut buf = [0; 4];
  BigEndian::write_u32(&mut buf, u32);
  buf
}

#[inline]
pub fn u8a4_to_u32(u8a4: U8a4) -> u32 {
  BigEndian::read_u32(&u8a4)
}

#[inline]
pub fn u8s_to_u8a4(u8s: &[u8]) -> U8a4 {
  u32_to_u8a4(u8s_to_u32(u8s))
}

#[inline]
pub fn u8s_to_u32(u8s: &[u8]) -> u32 {
  BigEndian::read_u32(u8s)
}

////////////////////////////////////////////////////////////////////////////////
/// key and value utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn build_info_table_inner_key(item_id: ItemId) -> Bytes {
  build_inner_key(INFO_TABLE_ID, item_id)
}

#[inline]
pub fn build_name_to_id_table_inner_key<N: AsRef<[u8]>>(table_name: N) -> Bytes {
  build_inner_key(NAME_TO_ID_TABLE_ID, table_name)
}

#[inline]
pub fn build_id_to_name_table_inner_key(table_id: TableId) -> Bytes {
  build_inner_key(ID_TO_NAME_TABLE_ID, table_id)
}

#[inline]
pub fn build_id_to_name_table_anchor() -> Bytes {
  build_inner_key(ID_TO_NAME_TABLE_ID, set_every_bit_to_one(TABLE_ID_LEN + 1))
}

#[inline]
pub fn build_userland_table_anchor(table_id: TableId, key_len: usize) -> Bytes {
  build_inner_key(table_id, set_every_bit_to_one((key_len + 1).into()))
}

#[inline]
pub fn build_inner_key<K: AsRef<[u8]>>(table_id: TableId, key: K) -> Bytes {
  let key = key.as_ref();
  let mut buf = BytesMut::with_capacity(table_id.len() + key.len());
  buf.extend_from_slice(table_id.as_ref());
  buf.extend_from_slice(key);
  buf.freeze()
}

#[inline]
pub fn build_timestamped_value<V: AsRef<[u8]>>(timestamp: Timestamp, value: V) -> Bytes {
  let value = value.as_ref();
  let mut buf = BytesMut::with_capacity(timestamp.len() + value.len());
  buf.extend_from_slice(timestamp.as_ref());
  buf.extend_from_slice(value);
  buf.freeze()
}

#[inline]
pub fn extract_table_id(buf: &[u8]) -> &[u8] {
  &buf[..TABLE_ID_LEN]
}

#[inline]
pub fn extract_key<'a>(buf: &'a [u8]) -> &'a [u8] {
  &buf[TABLE_ID_LEN..]
}

#[inline]
pub fn extract_timestamp(buf: &[u8]) -> &[u8] {
  &buf[..TIMESTAMP_LEN]
}

#[inline]
pub fn extract_value(buf: &[u8]) -> &[u8] {
  &buf[TIMESTAMP_LEN..]
}

#[inline]
fn set_every_bit_to_one(key_len: usize) -> Bytes {
  Bytes::from(vec![255; key_len.into()])
}

////////////////////////////////////////////////////////////////////////////////
/// other utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn now() -> u32 {
  Utc::now().timestamp() as u32
}

////////////////////////////////////////////////////////////////////////////////
/// test utils
////////////////////////////////////////////////////////////////////////////////

#[macro_use]
#[cfg(test)]
pub(crate) mod test_utils {
  use std::sync::Arc;

  use crate::db::*;

  pub struct TestContext<D: Db> {
    db: Option<Arc<D>>,
    path: String,
  }

  impl<D: Db> Drop for TestContext<D> {
    fn drop(&mut self) {
      let db = std::mem::replace(&mut self.db, None);
      drop(db.unwrap());
      let path = self.path.clone();
      let result = D::destroy(path);
      assert!(result.is_ok())
    }
  }

  impl<D: Db> TestContext<D> {
    pub fn db(&self) -> Arc<D> {
      Arc::clone(self.db.as_ref().unwrap())
    }
  }

  impl TestContext<NormalDb> {
    pub fn new(db_name: &str) -> Self {
      let mut path = String::from("./data/");
      path.push_str(db_name);
      let result = NormalDb::open(path.clone(), &mut crate::options::Options::new());
      assert!(result.is_ok());
      TestContext { db: Some(Arc::new(result.unwrap())), path: path }
    }
  }

  impl TestContext<TtlDb> {
    pub fn with_ttl(db_name: &str, ttl: u32) -> Self {
      let mut path = String::from("./data/");
      path.push_str(db_name);
      let result = TtlDb::open(path.clone(), ttl, &mut crate::options::Options::new());
      assert!(result.is_ok());
      TestContext { db: Some(Arc::new(result.unwrap())), path: path }
    }
  }

  #[macro_export]
  macro_rules! setup {
    ( $($param:expr),*; $($member:ident),* ) => {
      let ctx = crate::utils::test_utils::TestContext::new(
        $(
          $param
        )*
      );
      $(
          let $member = ctx.$member();
      )*
    };
  }

  #[macro_export]
  macro_rules! setup_with_ttl {
    ( $($param:expr),*; $ttl:expr; $($member:ident),* ) => {
      let ctx = crate::utils::test_utils::TestContext::with_ttl(
        $(
          $param
        )*,
        $ttl
      );
      $(
          let $member = ctx.$member();
      )*
    };
  }
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_build_info_table_inner_key() {
    assert_eq!(build_info_table_inner_key([0, 0]), vec![0, 0, 0, 0, 0, 0]);
  }

  #[test]
  fn test_build_name_to_id_table_inner_key() {
    assert_eq!(
      build_name_to_id_table_inner_key("huobi.btc.usdt.1m"),
      vec![
        0, 0, 0, 1, 104, 117, 111, 98, 105, 46, 98, 116, 99, 46, 117, 115, 100, 116, 46, 49, 109
      ]
    );
  }

  #[test]
  fn test_build_id_to_name_table_inner_key() {
    assert_eq!(build_id_to_name_table_inner_key([0, 0, 4, 0]), vec![0, 0, 0, 2, 0, 0, 4, 0]);
  }

  #[test]
  fn test_build_userland_table_anchor() {
    assert_eq!(
      build_userland_table_anchor([0, 0, 4, 0], 4),
      vec![0, 0, 4, 0, 255, 255, 255, 255, 255]
    );
  }

  #[test]
  fn test_build_inner_key() {
    let inner_key = build_inner_key([0, 0, 4, 0], [0, 0, 0, 0]);
    assert_eq!(inner_key, vec![0, 0, 4, 0, 0, 0, 0, 0]);
  }

  #[test]
  fn test_extract_table_id() {
    let inner_key = [0, 0, 4, 0, 0, 0, 0, 0];
    let table_id = extract_table_id(inner_key.as_ref());
    assert_eq!(table_id, [0, 0, 4, 0]);
  }

  #[test]
  fn test_extract_key() {
    let inner_key = [0, 0, 4, 0, 0, 0, 0, 128, 0, 254];
    let table_id = extract_key(&inner_key);
    assert_eq!(table_id, [0, 0, 0, 128, 0, 254]);
  }
}
