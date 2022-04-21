use crate::batch_x::BatchX;
use crate::consts::*;
use crate::options::Options;
use crate::table::Table;
use crate::types::*;
use crate::update_iterator::UpdateIterator;
use crate::utils::*;
use crate::Error;
use rocksdb::DB;
use rocksdb::{ReadOptions, WriteBatch};
use std::path::Path;

pub struct Db {
    pub(in crate) inner: DB,
}

impl Db {
    #[inline]
    pub fn new<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Db, Error> {
        Ok(Db { inner: DB::open(&opts.inner, path)? })
    }

    #[inline]
    pub fn destroy<P: AsRef<Path>>(path: P) -> Result<(), Error> {
        DB::destroy(&Options::new().inner, path)
    }

    #[inline]
    pub fn new_table(&self, name: &str) -> Result<Table, Error> {
        if let Some(id) = self.get_table_id_by_name(name)? {
            Ok(Table {
                db: &self,
                id,
                anchor: build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN),
            })
        } else {
            Ok(self.create_table(name)?)
        }
    }
    pub fn destroy_table(&self, name: &str) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        if let Some(id) = self.get_table_id_by_name(name)? {
            batch.delete(&build_name_to_id_table_inner_key(name));
            batch.delete(&build_id_to_name_table_inner_key(id));
            let anchor = build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN);
            batch.delete(&build_delete_range_hint_table_inner_key(&id, &anchor));
            batch.delete_range(id.as_ref(), anchor.as_ref());
        }
        self.inner.write(batch)
    }

    pub fn truncate_table(&self, name: &str) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        if let Some(id) = self.get_table_id_by_name(name)? {
            let anchor = build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN);
            batch.delete(&build_delete_range_hint_table_inner_key(&id, &anchor));
            batch.delete_range(id.as_ref(), anchor.as_ref());
        }
        self.inner.write(batch)
    }

    pub fn rename_table(&self, old_name: &str, new_name: &str) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        if let Some(id) = self.get_table_id_by_name(old_name)? {
            let id_to_name_table_inner_key = build_id_to_name_table_inner_key(id);
            batch.delete(&build_name_to_id_table_inner_key(old_name));
            batch.delete(&id_to_name_table_inner_key);
            batch.put(build_name_to_id_table_inner_key(new_name), id);
            batch.put(id_to_name_table_inner_key, new_name);
        }
        self.inner.write(batch)
    }

    pub fn get_tables(&self) -> Vec<(String, u32)> {
        let mut result: Vec<(String, u32)> = Vec::new();
        let mut opts = ReadOptions::default();
        opts.set_prefix_same_as_start(true);
        let mut iter = self.inner.raw_iterator_opt(opts);
        iter.seek(ID_TO_NAME_TABLE_ID);
        while iter.valid() {
            let key = iter.key().unwrap();
            let value = iter.value().unwrap();
            let id = u8s_to_u32(extract_key(key));
            let name = std::str::from_utf8(value).unwrap().to_string();
            result.push((name, id));
            iter.next();
        }
        result
    }

    #[inline]
    pub fn get_table_id_by_name(&self, name: &str) -> Result<Option<TableId>, Error> {
        let name_to_id_table_inner_key = build_name_to_id_table_inner_key(name);
        if let Some(id) = self.inner.get(name_to_id_table_inner_key)? {
            Ok(Some(u8s_to_table_id(id.as_ref())))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn get_table_name_by_id(&self, id: TableId) -> Result<Option<String>, Error> {
        let id_to_name_table_inner_key = build_id_to_name_table_inner_key(id);
        if let Some(name) = self.inner.get(id_to_name_table_inner_key)? {
            Ok(Some(std::str::from_utf8(name.as_ref()).unwrap().to_string()))
        } else {
            Ok(None)
        }
    }

    #[inline]
    pub fn get_latest_sn(&self) -> u64 {
        self.inner.latest_sequence_number()
    }

    #[inline]
    pub fn get_updates_since(&self, sn: u64) -> Result<UpdateIterator, Error> {
        let iter = self.inner.get_updates_since(sn)?;
        Ok(UpdateIterator::new(iter))
    }

    #[inline]
    pub fn batch_x() -> BatchX {
        BatchX::new()
    }

    #[inline]
    pub fn write(&self, b: BatchX) -> Result<(), Error> {
        self.inner.write(b.inner)
    }

    fn create_table(&self, name: &str) -> Result<Table, Error> {
        let name_to_id_table_inner_key = build_name_to_id_table_inner_key(name);
        let id = self.generate_next_table_id()?;
        let id_to_name_table_inner_key = build_id_to_name_table_inner_key(id);
        self.register_table(name_to_id_table_inner_key, id, id_to_name_table_inner_key, name)?;
        let anchor = build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN);
        Ok(Table::new(&self, id, anchor))
    }

    fn generate_next_table_id(&self) -> Result<TableId, Error> {
        let seed_key = build_info_table_inner_key(SEED_ITEM_ID);
        if let Some(seed_value) = self.inner.get(&seed_key)? {
            let seed_value = u8s_to_u32(seed_value.as_ref());
            if u32_to_table_id(seed_value) >= MAX_USERLAND_TABLE_ID {
                panic!("Exceeded limit: {:?}", MAX_USERLAND_TABLE_ID)
            }
            let seed_value = seed_value + 1;
            let next_id = u32_to_table_id(seed_value);
            self.inner.put(&seed_key, next_id)?;
            Ok(next_id)
        } else {
            // Double write to fixed raw get_updates_since bug.
            self.inner.put(&seed_key, MIN_USERLAND_TABLE_ID)?;
            self.inner.put(&seed_key, MIN_USERLAND_TABLE_ID)?;
            Ok(MIN_USERLAND_TABLE_ID)
        }
    }

    #[inline]
    fn register_table<K: AsRef<[u8]>>(
        &self, name_to_id_table_inner_key: K, id: TableId, id_to_name_table_inner_key: K,
        name: &str,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();
        batch.put(name_to_id_table_inner_key, id);
        batch.put(id_to_name_table_inner_key, name);
        self.inner.write(batch)
    }
}

#[test]
fn test_new_table() {
    run_test("test_new_table", |db| assert!(db.new_table("huobi.btc.usdt.1min").is_ok()));
}

#[test]
fn test_destroy_table() {
    run_test("test_destroy_table", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111").unwrap();
        let result = table.get(b"k111");
        assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
        db.destroy_table(name).unwrap();
        let result = table.get(b"k111");
        assert!(result.unwrap().is_none());
    });
}

#[test]
fn test_truncate_table() {
    run_test("test_truncate_table", |db| {
        let name = "huobi.btc.usdt.1min";
        let table = db.new_table(name).unwrap();
        table.put(b"k111", b"v111").unwrap();
        let result = table.get(b"k111");
        assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
        db.truncate_table(name).unwrap();
        let result = table.get(b"k111");
        assert!(result.unwrap().is_none());
    });
}

#[test]
fn test_rename_table() {
    run_test("test_rename_table", |db| {
        let old_name = "huobi.btc.usdt.1min";
        let new_name = "huobi.btc.usdt.5min";
        let table = db.new_table(old_name).unwrap();
        assert!(db.rename_table(old_name, new_name).is_ok());

        let old_name_to_id_table_inner_key = build_name_to_id_table_inner_key(&old_name);
        let id = table.db.inner.get(old_name_to_id_table_inner_key);
        assert!(id.unwrap().is_none());

        let new_name_to_id_table_inner_key = build_name_to_id_table_inner_key(&new_name);
        let id = table.db.inner.get(new_name_to_id_table_inner_key);
        assert_eq!(id.unwrap().unwrap().as_ref(), table.id);

        let id_to_name_table_inner_key = build_id_to_name_table_inner_key(table.id);
        let name = table.db.inner.get(id_to_name_table_inner_key);
        assert_eq!(std::str::from_utf8(&name.unwrap().unwrap()).unwrap(), new_name);
    });
}

#[test]
fn test_get_tables() {
    run_test("test_get_tables", |db| {
        let name0 = "huobi.btc.usdt.1min".to_owned();
        let name1 = "huobi.btc.usdt.3min".to_owned();
        let name2 = "huobi.btc.usdt.5min".to_owned();
        let table0 = db.new_table(&name0).unwrap();
        let table1 = db.new_table(&name1).unwrap();
        let table2 = db.new_table(&name2).unwrap();
        let id0 = table_id_to_u32(table0.id);
        let id1 = table_id_to_u32(table1.id);
        let id2 = table_id_to_u32(table2.id);
        let result = db.get_tables();
        assert_eq!(result, vec![(name0, id0), (name1, id1), (name2, id2)]);
    });
}

#[test]
fn test_get_table_id_by_name() {
    run_test("test_get_table_id_by_name", |db| {
        let table = db.create_table("huobi.btc.usdt.1m").unwrap();
        assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
        assert_eq!(
            db.get_table_id_by_name("huobi.btc.usdt.1m").unwrap().unwrap(),
            MIN_USERLAND_TABLE_ID
        );
    })
}

#[test]
fn test_get_table_name_by_id() {
    run_test("test_get_table_name_by_id", |db| {
        let table = db.create_table("huobi.btc.usdt.1m").unwrap();
        assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
        assert_eq!(
            db.get_table_name_by_id(MIN_USERLAND_TABLE_ID).unwrap().unwrap(),
            "huobi.btc.usdt.1m"
        );
    })
}

#[test]
fn test_get_latest_sn() {
    run_test("test_get_latest_sn", |db| {
        let table = db.create_table("huobi.btc.usdt.1m").unwrap();
        assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
        let sn1 = db.get_latest_sn();
        let result = table.put(b"k111", b"v111");
        assert!(result.is_ok());
        let result = table.delete(b"k111");
        assert!(result.is_ok());
        let sn2 = db.get_latest_sn();
        assert_eq!(sn1 + 2, sn2);
        let mut batch = table.batch();
        batch.put(b"k111", b"v111");
        batch.delete(b"k111");
        batch.delete_range(b"k111", b"k112");
        table.write(batch).unwrap();
        let sn3 = db.get_latest_sn();
        assert_eq!(sn2 + 4, sn3);
    })
}

#[test]
fn test_get_updates_since() {
    run_test("test_get_updates_since", |db| {
        let table = db.create_table("huobi.btc.usdt.1m").unwrap();
        assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
        let table = db.create_table("huobi.btc.usdt.3m").unwrap();
        db.destroy_table("huobi.btc.usdt.3m").unwrap();
        let sn1 = db.get_latest_sn();
        let result = table.put(b"k111", b"v111");
        assert!(result.is_ok());
        let result = table.delete(b"k111");
        assert!(result.is_ok());
        let sn2 = db.get_latest_sn();
        assert_eq!(sn1 + 2, sn2);
        let mut batch = table.batch();
        batch.put(b"k112", b"v112");
        batch.delete(b"k111");
        batch.delete_range(b"k111", b"k112");
        table.write(batch).unwrap();
        let sn3 = db.get_latest_sn();
        assert_eq!(sn2 + 4, sn3);
        let iter = db.get_updates_since(0).unwrap();
        let mut result = vec![];
        for ub in iter {
            result.push(ub);
        }
        assert_eq!(format!("{:?}", result), "[[Put {key:b\"\\0\\0\\0\\0\\0\\0\", value:b\"\\0\\0\\x04\\0\"}]@2, [Put {key:b\"\\0\\0\\0\\x01huobi.btc.usdt.1m\", value:b\"\\0\\0\\x04\\0\"}, Put {key:b\"\\0\\0\\0\\x02\\0\\0\\x04\\0\", value:b\"huobi.btc.usdt.1m\"}]@3, [Put {key:b\"\\0\\0\\0\\0\\0\\0\", value:b\"\\0\\0\\x04\\x01\"}]@5, [Put {key:b\"\\0\\0\\0\\x01huobi.btc.usdt.3m\", value:b\"\\0\\0\\x04\\x01\"}, Put {key:b\"\\0\\0\\0\\x02\\0\\0\\x04\\x01\", value:b\"huobi.btc.usdt.3m\"}]@6, [Delete {key:b\"\\0\\0\\0\\x01huobi.btc.usdt.3m\"}, Delete {key:b\"\\0\\0\\0\\x02\\0\\0\\x04\\x01\"}, DeleteRange {from_key:b\"\\0\\0\\x04\\x01\", to_key:b\"\\0\\0\\x04\\x01\\xff\\xff\\xff\\xff\\xff\"}]@8, [Put {key:b\"\\0\\0\\x04\\x01k111\", value:b\"v111\"}]@12, [Delete {key:b\"\\0\\0\\x04\\x01k111\"}]@13, [Put {key:b\"\\0\\0\\x04\\x01k112\", value:b\"v112\"}, Delete {key:b\"\\0\\0\\x04\\x01k111\"}, DeleteRange {from_key:b\"k111\", to_key:b\"k112\"}]@14]");
    })
}

#[test]
fn test_create_table() {
    run_test("test_create_table", |db| {
        let table = db.create_table("huobi.btc.usdt.1m").unwrap();
        assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
        let table = db.create_table("huobi.btc.usdt.5m").unwrap();
        assert_eq!(table.id, [0, 0, 4, 1]);
    })
}

#[test]
fn test_generate_next_table_id() {
    run_test("test_generate_next_table_id", |db| {
        let id = db.generate_next_table_id().unwrap();
        assert_eq!(id, MIN_USERLAND_TABLE_ID);
    })
}

#[test]
fn test_register_table() {
    run_test("test_register_table", |db| {
        let name = "huobi.btc.usdt.1m";
        let name_clone = name.clone();
        let table = db.new_table(name).unwrap();
        let name_to_id_table_inner_key = build_name_to_id_table_inner_key(&name_clone);
        let id_to_name_table_inner_key = build_id_to_name_table_inner_key(MIN_USERLAND_TABLE_ID);
        let result = db.register_table(
            &name_to_id_table_inner_key,
            MIN_USERLAND_TABLE_ID,
            &id_to_name_table_inner_key,
            &name_clone,
        );
        assert!(result.is_ok());

        let id = table.db.inner.get(name_to_id_table_inner_key);
        assert_eq!(id.unwrap().unwrap().as_ref(), [0, 0, 4, 0]);

        let name = table.db.inner.get(id_to_name_table_inner_key);
        assert_eq!(std::str::from_utf8(&name.unwrap().unwrap()).unwrap(), "huobi.btc.usdt.1m");
    })
}
