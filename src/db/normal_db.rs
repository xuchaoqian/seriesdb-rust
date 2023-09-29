use std::{
  path::Path,
  sync::{atomic::AtomicU32, Arc},
};

use concurrent_initializer::ConcurrentInitializer;
use quick_cache::{sync::Cache, Weighter};
use rocksdb::DB as RocksdbDb;

use super::db::Db;
use crate::error::Error;
use crate::options::Options;
use crate::table::*;
use crate::types::*;
use crate::write_batch::*;

#[derive(Clone)]
pub struct NormalTableWeighter;

impl Weighter<String, Arc<NormalTable>> for NormalTableWeighter {
  fn weight(&self, _key: &String, _val: &Arc<NormalTable>) -> u32 {
    16 as u32
  }
}

pub struct NormalDb {
  pub(crate) inner: Arc<RocksdbDb>,
  pub(crate) cache: Cache<String, Arc<NormalTable>, NormalTableWeighter>,
  pub(crate) last_table_id: AtomicU32,
  pub(crate) initializer: ConcurrentInitializer<String, TableId>,
  pub(crate) opts: Options,
}

impl Db for NormalDb {
  type Table = NormalTable;
  type TableWeighter = NormalTableWeighter;
  type WriteBatchX = NormalWriteBatchX;

  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline(always)]
  fn inner(&self) -> &Arc<RocksdbDb> {
    &self.inner
  }

  #[inline(always)]
  fn cache(&self) -> &Cache<String, Arc<Self::Table>, Self::TableWeighter> {
    &self.cache
  }

  #[inline(always)]
  fn last_table_id(&self) -> &AtomicU32 {
    &self.last_table_id
  }

  #[inline(always)]
  fn initializer(&self) -> &ConcurrentInitializer<String, TableId> {
    &self.initializer
  }

  #[inline]
  fn opts(&self) -> &Options {
    &self.opts
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn new_table(&self, id: TableId) -> Self::Table {
    NormalTable::new(self.inner.clone(), id)
  }

  #[inline]
  fn new_write_batch_x(&self) -> Self::WriteBatchX {
    NormalWriteBatchX::new(self.inner.clone())
  }
}

impl NormalDb {
  pub fn open<P: AsRef<Path>>(path: P, opts: &Options) -> Result<Self, Error> {
    let opts = opts.clone();
    let inner_db = Arc::new(RocksdbDb::open(&opts.inner, path)?);
    Self::try_put_placeholder_to_fix_wal_bug(inner_db.clone())?;
    Self::ensure_ttl_enabled_consistent(inner_db.clone(), false)?;
    Ok(NormalDb {
      inner: inner_db.clone(),
      cache: Cache::with_weighter(
        opts.cache_capacity,
        opts.cache_capacity as u64 * 20,
        NormalTableWeighter,
      ),
      last_table_id: AtomicU32::new(Self::get_last_table_id(inner_db)?),
      initializer: ConcurrentInitializer::new(),
      opts,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::consts::*;
  use crate::setup;
  use crate::utils::*;

  #[test]
  fn test_new_table() {
    setup!("normal_db.test_new_table"; db);
    assert!(db.open_table("huobi.btc.usdt.1min").is_ok());
  }

  #[test]
  fn test_destroy_table() {
    setup!("normal_db.test_destroy_table"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    table.put(b"k111", b"v111").unwrap();
    let result = table.get(b"k111");
    assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
    db.destroy_table(name).unwrap();
    let result = table.get(b"k111");
    assert!(result.unwrap().is_none());
  }

  #[test]
  fn test_truncate_table() {
    setup!("normal_db.test_truncate_table"; db);
    let name = "huobi.btc.usdt.1min";
    let table = db.open_table(name).unwrap();
    table.put(b"k111", b"v111").unwrap();
    let result = table.get(b"k111");
    assert_eq!(std::str::from_utf8(&result.unwrap().unwrap()).unwrap(), "v111");
    db.truncate_table(name).unwrap();
    let result = table.get(b"k111");
    assert!(result.unwrap().is_none());
  }

  #[test]
  fn test_rename_table() {
    setup!("normal_db.test_rename_table"; db);

    let old_name = "huobi.btc.usdt.1min";
    let new_name = "huobi.btc.usdt.5min";
    let table = db.open_table(old_name).unwrap();
    assert!(db.rename_table(old_name, new_name).is_ok());

    let old_name_to_id_table_inner_key = build_name_to_id_table_inner_key(&old_name);
    let id = table.inner_db.get(old_name_to_id_table_inner_key);
    assert!(id.unwrap().is_none());

    let new_name_to_id_table_inner_key = build_name_to_id_table_inner_key(&new_name);
    let id = table.inner_db.get(new_name_to_id_table_inner_key);
    assert_eq!(id.unwrap().unwrap().as_ref(), table.id);

    let id_to_name_table_inner_key = build_id_to_name_table_inner_key(table.id);
    let name = table.inner_db.get(id_to_name_table_inner_key);
    assert_eq!(std::str::from_utf8(&name.unwrap().unwrap()).unwrap(), new_name);
  }

  #[test]
  fn test_get_tables() {
    setup!("normal_db.test_get_tables"; db);
    let name0 = "huobi.btc.usdt.1min".to_owned();
    let name1 = "huobi.btc.usdt.3min".to_owned();
    let name2 = "huobi.btc.usdt.5min".to_owned();
    let table0 = db.open_table(&name0).unwrap();
    let table1 = db.open_table(&name1).unwrap();
    let table2 = db.open_table(&name2).unwrap();
    let id0 = u8a4_to_u32(table0.id);
    let id1 = u8a4_to_u32(table1.id);
    let id2 = u8a4_to_u32(table2.id);
    let result = db.get_table_infos();
    assert_eq!(result, vec![(name0, id0), (name1, id1), (name2, id2)]);
  }

  #[test]
  fn test_get_table_id_by_name() {
    setup!("normal_db.test_get_table_id_by_name"; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    assert_eq!(
      db.get_table_id_by_name("huobi.btc.usdt.1m").unwrap().unwrap(),
      MIN_USERLAND_TABLE_ID
    );
  }

  #[test]
  fn test_get_table_name_by_id() {
    setup!("normal_db.test_get_table_name_by_id"; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    assert_eq!(
      db.get_table_name_by_id(MIN_USERLAND_TABLE_ID).unwrap().unwrap(),
      "huobi.btc.usdt.1m"
    );
  }

  #[test]
  fn test_get_latest_sn() {
    setup!("normal_db.test_get_latest_sn"; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    let sn1 = db.get_latest_sn();
    let result = table.put(b"k111", b"v111");
    assert!(result.is_ok());
    let result = table.delete(b"k111");
    assert!(result.is_ok());
    let sn2 = db.get_latest_sn();
    assert_eq!(sn1 + 2, sn2);
    let mut batch = table.new_write_batch();
    batch.put(b"k111", b"v111");
    batch.delete(b"k111");
    batch.delete_range(b"k111", b"k112");
    batch.write().unwrap();
    let sn3 = db.get_latest_sn();
    assert_eq!(sn2 + 3, sn3);
  }

  #[test]
  fn test_get_write_op_batches_since() {
    setup!("normal_db.test_get_write_op_batches_since"; db);

    let sn0 = db.get_latest_sn();
    assert_eq!(sn0, 2);

    let table = db.create_table("huobi.btc.usdt.1m").unwrap(); // 2 records
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    let table = db.create_table("huobi.btc.usdt.3m").unwrap(); // 2 records
    db.destroy_table("huobi.btc.usdt.3m").unwrap(); // 3 records

    let sn1 = db.get_latest_sn();
    assert_eq!(sn1, 9);
    assert_eq!(sn0 + 7, sn1);

    let result = table.put(b"k111", b"v111"); // 1 record
    assert!(result.is_ok());
    let result = table.delete(b"k111"); // 1 record
    assert!(result.is_ok());

    let sn2 = db.get_latest_sn();
    assert_eq!(sn2, 11);
    assert_eq!(sn1 + 2, sn2);

    let mut batch = table.new_write_batch();
    batch.put(b"k112", b"v112"); // 1 record
    batch.delete(b"k111"); // 1 record
    batch.delete_range(b"k111", b"k112"); // 1 record
    batch.write().unwrap();

    let sn3 = db.get_latest_sn();
    assert_eq!(sn3, 14);
    assert_eq!(sn2 + 3, sn3);

    let iter = db.get_write_op_batches_since(0).unwrap();
    let mut result = vec![];
    for ub in iter {
      result.push(ub.unwrap());
    }
    assert_eq!(format!("{:?}", result), "[WriteOpBatch { sn: 2, write_ops: [OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\0\\0\\x01\\0\\x01\", inner_value: b\"\\0\" })) }] }, WriteOpBatch { sn: 3, write_ops: [OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\0\\x01\\x01huobi.btc.usdt.1m\", inner_value: b\"\\0\\0\\x04\\0\" })) }, OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\0\\x02\\x01\\0\\0\\x04\\0\", inner_value: b\"huobi.btc.usdt.1m\" })) }] }, WriteOpBatch { sn: 5, write_ops: [OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\0\\x01\\x01huobi.btc.usdt.3m\", inner_value: b\"\\0\\0\\x04\\x01\" })) }, OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\0\\x02\\x01\\0\\0\\x04\\x01\", inner_value: b\"huobi.btc.usdt.3m\" })) }] }, WriteOpBatch { sn: 7, write_ops: [OptionalWriteOp { inner: Some(DeleteOp(DeleteOp { inner_key: b\"\\0\\0\\0\\x01\\x01huobi.btc.usdt.3m\" })) }, OptionalWriteOp { inner: Some(DeleteOp(DeleteOp { inner_key: b\"\\0\\0\\0\\x02\\x01\\0\\0\\x04\\x01\" })) }, OptionalWriteOp { inner: Some(DeleteRangeOp(DeleteRangeOp { begin_inner_key: b\"\\0\\0\\x04\\x01\\0\", end_inner_key: b\"\\0\\0\\x04\\x01\\x02\" })) }] }, WriteOpBatch { sn: 10, write_ops: [OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\x04\\x01\\x01k111\", inner_value: b\"v111\" })) }] }, WriteOpBatch { sn: 11, write_ops: [OptionalWriteOp { inner: Some(DeleteOp(DeleteOp { inner_key: b\"\\0\\0\\x04\\x01\\x01k111\" })) }] }, WriteOpBatch { sn: 12, write_ops: [OptionalWriteOp { inner: Some(PutOp(PutOp { inner_key: b\"\\0\\0\\x04\\x01\\x01k112\", inner_value: b\"v112\" })) }, OptionalWriteOp { inner: Some(DeleteOp(DeleteOp { inner_key: b\"\\0\\0\\x04\\x01\\x01k111\" })) }, OptionalWriteOp { inner: Some(DeleteRangeOp(DeleteRangeOp { begin_inner_key: b\"\\0\\0\\x04\\x01\\x01k111\", end_inner_key: b\"\\0\\0\\x04\\x01\\x01k112\" })) }] }]");
  }

  #[test]
  fn test_create_table() {
    setup!("normal_db.test_create_table"; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    let table = db.create_table("huobi.btc.usdt.5m").unwrap();
    assert_eq!(table.id, [0, 0, 4, 1]);
  }

  #[test]
  fn test_generate_next_table_id() {
    setup!("normal_db.test_generate_next_table_id"; db);
    let id = db.generate_next_table_id().unwrap();
    assert_eq!(id, MIN_USERLAND_TABLE_ID);
  }

  #[test]
  fn test_register_table() {
    setup!("normal_db.test_register_table"; db);

    let name = "huobi.btc.usdt.1m";
    let name_clone = name.clone();
    let table = db.open_table(name).unwrap();
    let name_to_id_table_inner_key = build_name_to_id_table_inner_key(&name_clone);
    let id_to_name_table_inner_key = build_id_to_name_table_inner_key(MIN_USERLAND_TABLE_ID);
    let result = db.register_table(
      &name_to_id_table_inner_key,
      MIN_USERLAND_TABLE_ID,
      &id_to_name_table_inner_key,
      &name_clone,
    );
    assert!(result.is_ok());

    let id = table.inner_db.get(name_to_id_table_inner_key);
    assert_eq!(id.unwrap().unwrap().as_ref(), [0, 0, 4, 0]);

    let name = table.inner_db.get(id_to_name_table_inner_key);
    assert_eq!(std::str::from_utf8(&name.unwrap().unwrap()).unwrap(), "huobi.btc.usdt.1m");
  }

  #[test]
  fn test_compact_filter() {
    use std::{thread, time};

    setup!("normal_db.test_compact_filter"; db);

    // Spawn n threads.
    let threads: Vec<_> = (0..1_u8)
      .map(|_thread_id| {
        let db = db.clone();

        thread::spawn(move || {
          let table = db.create_table("test_compact_filter").unwrap();

          let _ = table.put(b"k1", b"a");
          let _ = table.put(b"_k", b"b");
          let _ = table.put(b"%k", b"c");

          let begin_key = build_inner_key(table.id(), b"k1");
          let placehoder_key = build_info_table_inner_key(PLACEHOLDER_ITEM_ID);

          db.inner.compact_range(Some(begin_key.clone()), None::<&[u8]>);

          assert_eq!(&*table.get(b"k1").unwrap().unwrap(), b"a");
          assert_eq!(db.inner.get(&placehoder_key).unwrap(), Some(vec![0, 0]));

          thread::sleep(time::Duration::from_secs(2));

          assert_eq!(&*table.get(b"k1").unwrap().unwrap(), b"a");
          assert_eq!(db.inner.get(&placehoder_key).unwrap(), Some(vec![0, 0]));

          db.inner.compact_range(None::<&[u8]>, None::<&[u8]>);

          assert_eq!(&*table.get(b"k1").unwrap().unwrap(), b"a");
          assert_eq!(db.inner.get(&placehoder_key).unwrap(), Some(vec![0, 0]));
        })
      })
      .collect();

    // Wait all threads to complete.
    threads.into_iter().for_each(|t| t.join().expect("Thread failed"));
  }
}
