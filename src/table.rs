use crate::batch::Batch;
use crate::db::Db;
use crate::entry_cursor::EntryCursor;
use crate::types::*;
use crate::utils::*;
use crate::Error;
use bytes::Bytes;
use rocksdb::ReadOptions;
use std::fmt;

#[derive(Clone)]
pub struct Table<'a> {
    pub(in crate) db: &'a Db,
    pub(in crate) id: TableId,
    pub(in crate) anchor: Bytes,
}

impl<'a> fmt::Debug for Table<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "id: {:?}, anchor: {:?}", self.id, self.anchor)
    }
}

impl<'a> Table<'a> {
    #[inline]
    pub(in crate) fn new(db: &Db, id: TableId, anchor: Bytes) -> Table {
        Table { db, id, anchor }
    }

    #[inline]
    pub fn put<K, V>(&self, key: K, value: V) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.db.inner.put(build_inner_key(self.id, key), value)
    }

    #[inline]
    pub fn batch(&self) -> Batch {
        Batch::new(self.id)
    }

    #[inline]
    pub fn write(&self, b: Batch) -> Result<(), Error> {
        self.db.inner.write(b.inner)
    }

    #[inline]
    pub fn delete<K: AsRef<[u8]>>(&self, key: K) -> Result<(), Error> {
        self.db.inner.delete(build_inner_key(self.id, key))
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error> {
        self.db.inner.get(build_inner_key(self.id, key))
    }

    #[inline]
    pub fn cursor(&self) -> EntryCursor {
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        EntryCursor::new(self.db.inner.raw_iterator_opt(opts), self.id, &self.anchor)
    }

    #[inline]
    pub fn db(&self) -> &Db {
        self.db
    }

    #[inline]
    pub fn id(&self) -> TableId {
        self.id
    }
}

#[test]
fn test_put() {
    run_test("test_put", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        let result = table.put(b"k111", b"v111");
        assert!(result.is_ok());
    })
}

#[allow(unused_must_use)]
#[test]
fn test_get() {
    run_test("test_get", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111");
        let result = table.get(b"k111");
        assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
    })
}

#[allow(unused_must_use)]
#[test]
fn test_delete() {
    run_test("test_delete", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111");
        table.get(b"k111");
        let result = table.delete(b"k111");
        assert!(result.is_ok());
        let result = table.get(b"k111");
        assert!(result.unwrap().is_none());
    })
}
