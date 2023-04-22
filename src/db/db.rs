use std::{
  path::Path,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
};

use bytes::Bytes;
use concurrent_initializer::{ConcurrentInitializer, InitResult};
use quick_cache::{sync::Cache, Weighter};
use rocksdb::{ReadOptions, WriteBatch, DB as RocksdbDb};

use crate::consts::*;
use crate::cursor::*;
use crate::error::Error;
use crate::options::Options;
use crate::table::*;
use crate::types::*;
use crate::utils::*;
use crate::write_batch::*;
use crate::write_op::*;

pub trait Db {
  type Table: Table;
  type TableWeighter: Weighter<String, (), Arc<Self::Table>> + Clone;
  type WriteBatchX: WriteBatchX;

  #[doc(hidden)]
  fn inner_db(&self) -> &Arc<RocksdbDb>;
  #[doc(hidden)]
  fn cache(&self) -> &Cache<String, Arc<Self::Table>, Self::TableWeighter>;
  #[doc(hidden)]
  fn last_table_id(&self) -> &AtomicU32;
  #[doc(hidden)]
  fn initializer(&self) -> &ConcurrentInitializer<String, TableId>;

  #[inline]
  fn destroy<P: AsRef<Path>>(path: P) -> Result<(), Error> {
    Ok(RocksdbDb::destroy(&Options::new().inner, path)?)
  }

  #[inline]
  fn open_table(&self, name: &str) -> Result<Arc<Self::Table>, Error> {
    if let Some(table) = self.cache().get(name) {
      return Ok(table);
    } else {
      let table = Arc::new(if let Some(id) = self.get_table_id_by_name(name)? {
        self.new_table(id, build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN))
      } else {
        self.create_table(name)?
      });
      self.cache().insert(name.to_string(), table.clone());
      return Ok(table);
    }
  }

  fn destroy_table(&self, name: &str) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    if let Some(id) = self.get_table_id_by_name(name)? {
      batch.delete(&build_name_to_id_table_inner_key(name));
      batch.delete(&build_id_to_name_table_inner_key(id));
      let anchor = build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN);
      batch.delete_range(id.as_ref(), anchor.as_ref());
    }
    let result = self.inner_db().write(batch);
    self.cache().remove(name);
    result.map_err(|err| Error::RocksdbError(err))
  }

  fn truncate_table(&self, name: &str) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    if let Some(id) = self.get_table_id_by_name(name)? {
      let anchor = build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN);
      batch.delete_range(id.as_ref(), anchor.as_ref());
    }
    Ok(self.inner_db().write(batch)?)
  }

  fn rename_table(&self, old_name: &str, new_name: &str) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    if let Some(id) = self.get_table_id_by_name(old_name)? {
      let id_to_name_table_inner_key = build_id_to_name_table_inner_key(id);
      batch.delete(&build_name_to_id_table_inner_key(old_name));
      batch.delete(&id_to_name_table_inner_key);
      batch.put(build_name_to_id_table_inner_key(new_name), id);
      batch.put(id_to_name_table_inner_key, new_name);
    }
    let result = self.inner_db().write(batch);
    self.cache().remove(old_name);
    result.map_err(|err| Error::RocksdbError(err))
  }

  fn get_table_infos(&self) -> Vec<(String, u32)> {
    let mut result: Vec<(String, u32)> = Vec::new();
    let mut opts = ReadOptions::default();
    opts.set_prefix_same_as_start(true);
    let mut iter = self.inner_db().raw_iterator_opt(opts);
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
  fn get_table_id_by_name(&self, name: &str) -> Result<Option<TableId>, Error> {
    let name_to_id_table_inner_key = build_name_to_id_table_inner_key(name);
    if let Some(id) = self.inner_db().get(name_to_id_table_inner_key)? {
      Ok(Some(u8s_to_u8a4(id.as_ref())))
    } else {
      Ok(None)
    }
  }

  #[inline]
  fn get_table_name_by_id(&self, id: TableId) -> Result<Option<String>, Error> {
    let id_to_name_table_inner_key = build_id_to_name_table_inner_key(id);
    if let Some(name) = self.inner_db().get(id_to_name_table_inner_key)? {
      Ok(Some(std::str::from_utf8(name.as_ref()).unwrap().to_string()))
    } else {
      Ok(None)
    }
  }

  #[inline]
  fn get_latest_sn(&self) -> u64 {
    self.inner_db().latest_sequence_number()
  }

  #[inline]
  fn get_write_op_batches_since(&self, sn: u64) -> Result<WriteOpBatchIterator, Error> {
    let iter = self.inner_db().get_updates_since(sn)?;
    Ok(WriteOpBatchIterator::new(iter))
  }

  #[inline]
  fn replay(&self, write_op_batches: Vec<WriteOpBatch>) -> Result<u64, Error> {
    let mut sn = 0;
    for write_op_batch in write_op_batches {
      sn = write_op_batch.sn;
      let mut batch = WriteBatch::default();
      for optional_write_op in write_op_batch.write_ops {
        if let Some(write_op) = optional_write_op.inner {
          match write_op {
            WriteOp::PutOp(put_op) => batch.put(put_op.inner_key, put_op.inner_value),
            WriteOp::DeleteOp(delete_op) => batch.delete(delete_op.inner_key),
            WriteOp::DeleteRangeOp(delete_range_op) => {
              batch.delete_range(delete_range_op.begin_inner_key, delete_range_op.end_inner_key)
            }
            WriteOp::MergeOp(merge_op) => batch.merge(merge_op.inner_key, merge_op.inner_value),
          }
        }
      }
      self.inner_db().write(batch)?;
    }
    Ok(sn)
  }

  fn new_table(&self, id: TableId, anchor: Bytes) -> Self::Table;

  fn new_write_batch_x() -> Self::WriteBatchX;

  fn write(&self, batch: Self::WriteBatchX) -> Result<(), Error>;

  // Use this fix wal bug
  #[doc(hidden)]
  #[inline]
  fn try_put_placeholder(inner_db: Arc<RocksdbDb>) -> Result<(), Error> {
    let placeholder_item_inner_key = build_info_table_inner_key(PLACEHOLDER_ITEM_ID);
    if inner_db.get(&placeholder_item_inner_key)?.is_none() {
      Ok(inner_db.put(placeholder_item_inner_key, PLACEHOLDER_ITEM_ID)?)
    } else {
      Ok(())
    }
  }

  #[doc(hidden)]
  #[inline]
  fn ensure_ttl_enabled_consistent(inner_db: Arc<RocksdbDb>, wanted: bool) -> Result<(), Error> {
    let ttl_item_inner_key = build_info_table_inner_key(TTL_ITEM_ID);
    if let Some(current) = inner_db.get(&ttl_item_inner_key)? {
      let current = if current == vec![1] { true } else { false };
      if wanted != current {
        Err(Error::InconsistentTtlEnabled { current, wanted })
      } else {
        Ok(())
      }
    } else {
      Ok(inner_db.put(ttl_item_inner_key, if wanted { [1] } else { [0] })?)
    }
  }

  #[doc(hidden)]
  #[inline]
  fn get_last_table_id(inner_db: Arc<RocksdbDb>) -> Result<u32, Error> {
    let anchor = build_id_to_name_table_anchor();
    let id_to_name_table = NormalTable::new(inner_db.clone(), ID_TO_NAME_TABLE_ID, anchor);
    let mut cusor = id_to_name_table.cursor();
    cusor.seek_to_last();
    if cusor.is_valid() {
      Ok(u8s_to_u32(cusor.key().unwrap()))
    } else {
      match cusor.status() {
        Ok(_) => Ok(u8a4_to_u32(MIN_USERLAND_TABLE_ID) - 1),
        Err(e) => Err(e),
      }
    }
  }

  #[doc(hidden)]
  fn create_table(&self, name: &str) -> Result<Self::Table, Error> {
    let result = self.initializer().try_get_or_init(
      &Arc::new(name.to_owned()),
      || match self.get_table_id_by_name(name) {
        Ok(ok) => {
          if let Some(id) = ok {
            return Ok(Some(id));
          } else {
            return Ok(None);
          }
        }
        Err(err) => {
          return Err(err);
        }
      },
      || {
        let id = self.generate_next_table_id()?;
        let name_to_id_table_inner_key = build_name_to_id_table_inner_key(name);
        let id_to_name_table_inner_key = build_id_to_name_table_inner_key(id);
        self.register_table(name_to_id_table_inner_key, id, id_to_name_table_inner_key, name)?;
        Ok(id)
      },
    );
    match result {
      InitResult::Initialized(id) => {
        Ok(self.new_table(id, build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN)))
      }
      InitResult::ReadExisting(id) => {
        Ok(self.new_table(id, build_userland_table_anchor(id, MAX_USERLAND_KEY_LEN)))
      }
      InitResult::InitErr(err) => Err(Error::ErrorPtr(err)),
    }
  }

  #[doc(hidden)]
  fn generate_next_table_id(&self) -> Result<TableId, Error> {
    let current_id = self.last_table_id().fetch_add(1, Ordering::SeqCst) + 1;
    let current_id2 = u32_to_u8a4(current_id);
    if current_id2 >= MAX_USERLAND_TABLE_ID {
      Err(Error::ExceededLimitError {
        current: current_id,
        max: u8a4_to_u32(MAX_USERLAND_TABLE_ID),
      })
    } else {
      Ok(current_id2)
    }
  }

  #[doc(hidden)]
  #[inline]
  fn register_table<K: AsRef<[u8]>>(
    &self, name_to_id_table_inner_key: K, id: TableId, id_to_name_table_inner_key: K, name: &str,
  ) -> Result<(), Error> {
    let mut batch = WriteBatch::default();
    batch.put(name_to_id_table_inner_key, id);
    batch.put(id_to_name_table_inner_key, name);
    Ok(self.inner_db().write(batch)?)
  }
}
