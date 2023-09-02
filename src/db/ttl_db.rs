use std::{
  path::Path,
  sync::{atomic::AtomicU32, Arc},
};

use concurrent_initializer::ConcurrentInitializer;
use quick_cache::{sync::Cache, Weighter};
use rocksdb::DB as RocksdbDb;

use super::db::Db;
use crate::compact_filter::CompactionFilterFactoryImpl;
use crate::error::Error;
use crate::options::Options;
use crate::table::*;
use crate::types::*;
use crate::write_batch::*;

#[derive(Clone)]
pub struct TtlTableWeighter;

impl Weighter<String, Arc<TtlTable>> for TtlTableWeighter {
  fn weight(&self, _key: &String, val: &Arc<TtlTable>) -> u32 {
    12 + val.anchor.len() as u32
  }
}

pub struct TtlDb {
  pub(crate) inner: Arc<RocksdbDb>,
  pub(crate) cache: Cache<String, Arc<TtlTable>, TtlTableWeighter>,
  pub(crate) last_table_id: AtomicU32,
  pub(crate) initializer: ConcurrentInitializer<String, TableId>,
}

impl Db for TtlDb {
  type Table = TtlTable;
  type TableWeighter = TtlTableWeighter;
  type WriteBatchX = TtlWriteBatchX;

  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn inner(&self) -> &Arc<RocksdbDb> {
    &self.inner
  }

  #[inline]
  fn cache(&self) -> &Cache<String, Arc<Self::Table>, Self::TableWeighter> {
    &self.cache
  }

  #[inline]
  fn last_table_id(&self) -> &AtomicU32 {
    &self.last_table_id
  }

  #[inline]
  fn initializer(&self) -> &ConcurrentInitializer<String, TableId> {
    &self.initializer
  }

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn new_table(&self, id: TableId, anchor: bytes::Bytes) -> Self::Table {
    TtlTable::new(self.inner.clone(), id, anchor)
  }

  #[inline]
  fn new_write_batch_x(&self) -> Self::WriteBatchX {
    TtlWriteBatchX::new(self.inner.clone())
  }
}

impl TtlDb {
  pub fn open<P: AsRef<Path>>(path: P, ttl: u32, opts: &mut Options) -> Result<Self, Error> {
    opts.set_compaction_filter_factory(CompactionFilterFactoryImpl::new(ttl));
    let inner_db = Arc::new(RocksdbDb::open(&opts.inner, path)?);
    Self::try_put_placeholder_to_fix_wal_bug(inner_db.clone())?;
    Self::ensure_ttl_enabled_consistent(inner_db.clone(), true)?;
    Ok(TtlDb {
      inner: inner_db.clone(),
      cache: Cache::with_weighter(
        opts.cache_capacity,
        opts.cache_capacity as u64 * 20,
        TtlTableWeighter,
      ),
      last_table_id: AtomicU32::new(Self::get_last_table_id(inner_db)?),
      initializer: ConcurrentInitializer::new(),
    })
  }
}

#[cfg(test)]
mod tests {

  use super::*;
  use crate::consts::*;
  use crate::setup_with_ttl;
  use crate::utils::*;

  #[test]
  fn test_new_table() {
    setup_with_ttl!("ttl_db.test_new_table"; 3; db);
    assert!(db.open_table("huobi.btc.usdt.1min").is_ok());
  }

  #[test]
  fn test_destroy_table() {
    setup_with_ttl!("ttl_db.test_destroy_table"; 3; db);
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
    setup_with_ttl!("ttl_db.test_truncate_table"; 3; db);
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
    setup_with_ttl!("ttl_db.test_rename_table"; 3; db);

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
    setup_with_ttl!("ttl_db.test_get_tables"; 3; db);
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
    setup_with_ttl!("ttl_db.test_get_table_id_by_name"; 3; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    assert_eq!(
      db.get_table_id_by_name("huobi.btc.usdt.1m").unwrap().unwrap(),
      MIN_USERLAND_TABLE_ID
    );
  }

  #[test]
  fn test_get_table_name_by_id() {
    setup_with_ttl!("ttl_db.test_get_table_name_by_id"; 3; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    assert_eq!(
      db.get_table_name_by_id(MIN_USERLAND_TABLE_ID).unwrap().unwrap(),
      "huobi.btc.usdt.1m"
    );
  }

  #[test]
  fn test_get_latest_sn() {
    setup_with_ttl!("ttl_db.test_get_latest_sn"; 3; db);
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
  fn test_create_table() {
    setup_with_ttl!("ttl_db.test_create_table"; 3; db);
    let table = db.create_table("huobi.btc.usdt.1m").unwrap();
    assert_eq!(table.id, MIN_USERLAND_TABLE_ID);
    let table = db.create_table("huobi.btc.usdt.5m").unwrap();
    assert_eq!(table.id, [0, 0, 4, 1]);
  }

  #[test]
  fn test_generate_next_table_id() {
    setup_with_ttl!("ttl_db.test_generate_next_table_id"; 3; db);
    let id = db.generate_next_table_id().unwrap();
    assert_eq!(id, MIN_USERLAND_TABLE_ID);
  }

  #[test]
  fn test_register_table() {
    setup_with_ttl!("ttl_db.test_register_table"; 3; db);

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

    setup_with_ttl!("ttl_db.test_compact_filter"; 1; db);

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

          assert!(table.get(b"k1").unwrap().is_none());
          assert_eq!(db.inner.get(&placehoder_key).unwrap(), Some(vec![0, 0]));
        })
      })
      .collect();

    // Wait all threads to complete.
    threads.into_iter().for_each(|t| t.join().expect("Thread failed"));
  }
}
