use crate::types::*;
use crate::utils::*;
use bytes::Bytes;
use rocksdb::DBRawIterator;

pub struct EntryCursor<'a> {
    inner: DBRawIterator<'a>,
    table_id: TableId,
    anchor: &'a Bytes,
}

impl<'a> EntryCursor<'a> {
    pub(in crate) fn new(inner: DBRawIterator<'a>, table_id: TableId, anchor: &'a Bytes) -> Self {
        EntryCursor { inner, table_id, anchor }
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        if self.inner.valid() {
            return true;
        } else {
            return false;
        }
    }

    #[inline]
    pub fn seek_to_first(&mut self) {
        self.inner.seek(self.table_id)
    }

    #[inline]
    pub fn seek_to_last(&mut self) {
        self.inner.seek_for_prev(self.anchor);
    }

    #[inline]
    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        self.inner.seek(build_inner_key(self.table_id, key));
    }

    #[inline]
    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        self.inner.seek_for_prev(build_inner_key(self.table_id, key));
    }

    #[inline]
    pub fn next(&mut self) {
        self.inner.next()
    }

    #[inline]
    pub fn prev(&mut self) {
        self.inner.prev()
    }

    #[inline]
    pub fn key(&self) -> Option<&[u8]> {
        if let Some(v) = self.inner.key() {
            Some(extract_key(v))
        } else {
            None
        }
    }

    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        self.inner.value()
    }
}

#[test]
fn test_seek() {
    run_test("test_seek", |db| {
        let name = "huobi.btc.usdt.1m";
        let table = db.new_table(name).unwrap();
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
    });
}
