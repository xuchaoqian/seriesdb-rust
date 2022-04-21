use crate::consts::*;
#[cfg(test)]
use crate::db::Db;
use crate::types::*;
use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};

////////////////////////////////////////////////////////////////////////////////
/// conversion utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn u32_to_table_id(u32: u32) -> TableId {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, u32);
    buf
}

#[inline]
pub fn table_id_to_u32(table_id: TableId) -> u32 {
    BigEndian::read_u32(&table_id)
}

#[inline]
pub fn u8s_to_table_id(u8s: &[u8]) -> TableId {
    u32_to_table_id(u8s_to_u32(u8s))
}

#[inline]
pub fn u8s_to_u32(u8s: &[u8]) -> u32 {
    BigEndian::read_u32(u8s)
}

////////////////////////////////////////////////////////////////////////////////
/// key utils
////////////////////////////////////////////////////////////////////////////////
#[inline]
pub fn build_info_table_inner_key(item_id: ItemId) -> Bytes {
    build_inner_key(INFO_TABLE_ID, item_id)
}

#[inline]
pub fn build_name_to_id_table_inner_key<N: AsRef<[u8]>>(name: N) -> Bytes {
    build_inner_key(NAME_TO_ID_TABLE_ID, name)
}

#[inline]
pub fn build_id_to_name_table_inner_key(table_id: TableId) -> Bytes {
    build_inner_key(ID_TO_NAME_TABLE_ID, table_id)
}

#[inline]
pub fn build_delete_range_hint_table_inner_key<F, T>(from_key: F, to_key: T) -> Bytes
where
    F: AsRef<[u8]>,
    T: AsRef<[u8]>, {
    let key = rmp_serde::to_vec(&(from_key.as_ref().to_vec(), to_key.as_ref().to_vec())).unwrap();
    build_inner_key(DELETE_RANGE_HINT_TABLE_ID, key)
}

#[inline]
pub fn extract_delete_range_hint<K: AsRef<[u8]>>(inner_key: K) -> (Bytes, Bytes) {
    let key = extract_key(inner_key.as_ref());
    let (from_key, to_key): (Vec<u8>, Vec<u8>) = rmp_serde::from_slice(key).unwrap();
    (Bytes::from(from_key), Bytes::from(to_key))
}

#[inline]
#[inline]
pub fn build_userland_table_anchor(table_id: TableId, key_len: u8) -> Bytes {
    build_inner_key(table_id, set_every_bit_to_one((key_len + 1).into()))
}

#[inline]
pub fn build_inner_key<K: AsRef<[u8]>>(table_id: TableId, key: K) -> Bytes {
    let table_id = table_id.as_ref();
    let key = key.as_ref();
    let mut buf = BytesMut::with_capacity(table_id.len() + key.len());
    buf.extend_from_slice(table_id);
    buf.extend_from_slice(key);
    buf.freeze()
}

#[inline]
pub fn extract_table_id<B: AsRef<[u8]>>(buf: B) -> TableId {
    let mut array: TableId = [0; TABLE_ID_LEN];
    array.copy_from_slice(&buf.as_ref()[..TABLE_ID_LEN]);
    array
}

#[inline]
pub fn extract_key(buf: &[u8]) -> &[u8] {
    &buf[TABLE_ID_LEN..]
}

#[inline]
fn set_every_bit_to_one(key_len: u8) -> Bytes {
    Bytes::from(vec![255; key_len.into()])
}

////////////////////////////////////////////////////////////////////////////////
/// unit test utils
////////////////////////////////////////////////////////////////////////////////
#[cfg(test)]
pub(in crate) fn run_test<T>(db_name: &str, test: T) -> ()
where T: FnOnce(Db) -> () + std::panic::UnwindSafe {
    let mut path = String::from("./data/");
    path.push_str(db_name);
    let db = setup(&path);
    let result = std::panic::catch_unwind(|| {
        test(db);
    });
    teardown(&path);
    assert!(result.is_ok())
}

#[cfg(test)]
fn setup(path: &str) -> Db {
    let result = Db::new(path, &crate::options::Options::new());
    assert!(result.is_ok());
    result.unwrap()
}

#[cfg(test)]
fn teardown(path: &str) {
    assert!(Db::destroy(path).is_ok())
}

////////////////////////////////////////////////////////////////////////////////
/// test cases
////////////////////////////////////////////////////////////////////////////////
#[test]
fn test_build_info_table_inner_key() {
    assert_eq!(build_info_table_inner_key([0, 0]), vec![0, 0, 0, 0, 0, 0]);
}

#[test]
fn test_build_name_to_id_table_inner_key() {
    assert_eq!(
        build_name_to_id_table_inner_key("huobi.btc.usdt.1m"),
        vec![
            0, 0, 0, 1, 104, 117, 111, 98, 105, 46, 98, 116, 99, 46, 117, 115, 100, 116, 46, 49,
            109
        ]
    );
}

#[test]
fn test_build_id_to_name_table_inner_key() {
    assert_eq!(build_id_to_name_table_inner_key([0, 0, 4, 0]), vec![0, 0, 0, 2, 0, 0, 4, 0]);
}

#[test]
fn test_build_delete_range_hint_table_inner_key() {
    assert_eq!(
        build_delete_range_hint_table_inner_key([0, 0, 4, 0], [0, 0, 4, 1]).as_ref(),
        b"\0\0\0\x03\x92\x94\0\0\x04\0\x94\0\0\x04\x01"
    );
}

#[test]
fn test_extract_delete_range_hint() {
    let inner_key = b"\0\0\0\x03\x92\x94\0\0\x04\0\x94\0\0\x04\x01";
    let (from_key, to_key) = extract_delete_range_hint(inner_key);
    assert_eq!(from_key.as_ref(), [0, 0, 4, 0]);
    assert_eq!(to_key.as_ref(), [0, 0, 4, 1]);
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
    let table_id = extract_table_id(inner_key);
    assert_eq!(table_id, [0, 0, 4, 0]);
}

#[test]
fn test_extract_key() {
    let inner_key = [0, 0, 4, 0, 0, 0, 0, 128, 0, 254];
    let table_id = extract_key(&inner_key);
    assert_eq!(table_id, [0, 0, 0, 128, 0, 254]);
}
