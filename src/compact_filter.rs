use std::{
  cmp::Ordering as CmpOrdering,
  ffi::CString,
  path::{Path, PathBuf},
  sync::{
    atomic::{AtomicU32, Ordering as AtomicOrdering},
    Arc,
  },
};

use ahash::AHashMap;
use bytes::Bytes;
use rocksdb::{
  compaction_filter::CompactionFilter,
  compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
  CompactionDecision, DB as RocksdbDb,
};

use crate::consts::*;
use crate::cursor::*;
use crate::options::Options;
use crate::table::*;
use crate::types::*;
use crate::utils::*;

static ID_SEED: AtomicU32 = AtomicU32::new(1);

pub struct CompactionFilterFactoryImpl {
  name: CString,
  path: PathBuf,
  ttl: u32,
  opts: Options,
}

impl CompactionFilterFactory for CompactionFilterFactoryImpl {
  type Filter = CompactionFilterImpl;

  #[inline]
  fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
    match RocksdbDb::open_for_read_only(&self.opts.inner, self.path.clone(), false) {
      Ok(rocksdb) => {
        CompactionFilterImpl::new(self.ttl, Some(MaxKeyComparator::new(Arc::new(rocksdb))))
      }
      Err(err) => {
        log::error!("Creating a pesudo filter instance, since it's failed to open db for read only: err: {:?}", err);
        CompactionFilterImpl::new(self.ttl, None)
      }
    }
  }

  #[inline]
  fn name(&self) -> &std::ffi::CStr {
    self.name.as_c_str()
  }
}

impl CompactionFilterFactoryImpl {
  #[inline]
  pub fn new<P: AsRef<Path>>(path: P, ttl: u32, opts: Options) -> Self {
    let name = "seriesdb_compaction_filter_factory";
    log::info!("Creating a compaction filter factory: name: {:?}", name);
    CompactionFilterFactoryImpl {
      name: CString::new(name).unwrap(),
      path: path.as_ref().to_path_buf(),
      ttl,
      opts,
    }
  }
}

pub struct CompactionFilterImpl {
  name: CString,
  ttl: u32,
  max_key_comparator: Option<MaxKeyComparator>,
}

impl CompactionFilter for CompactionFilterImpl {
  #[inline]
  fn filter(&mut self, _level: u32, inner_key: &[u8], inner_value: &[u8]) -> CompactionDecision {
    if let Some(max_key_comparator) = self.max_key_comparator.as_mut() {
      if inner_key.len() < 4 || inner_value.len() < 4 {
        return CompactionDecision::Keep;
      }

      let table_id = u8s_to_u8a4(extract_table_id(inner_key));
      if table_id < MIN_USERLAND_TABLE_ID {
        return CompactionDecision::Keep;
      }

      let ordering = max_key_comparator.compare(table_id, extract_key(inner_key));
      if ordering == CmpOrdering::Greater {
        let timestamp = u8s_to_u32(extract_timestamp(inner_value));
        if timestamp + self.ttl < now() {
          return CompactionDecision::Remove;
        }
      }
    }

    return CompactionDecision::Keep;
  }

  #[inline]
  fn name(&self) -> &std::ffi::CStr {
    self.name.as_c_str()
  }
}

impl CompactionFilterImpl {
  #[inline]
  pub fn new(ttl: u32, max_key_comparator: Option<MaxKeyComparator>) -> Self {
    let name =
      format!("seriesdb_compaction_filter<{:?}>", ID_SEED.fetch_add(1, AtomicOrdering::Relaxed));
    log::info!("Creating a compaction filter: name: {:?}", name);
    CompactionFilterImpl { name: CString::new(name).unwrap(), ttl, max_key_comparator }
  }
}

pub struct MaxKeyComparator {
  rocksdb: Arc<RocksdbDb>,
  cache: AHashMap<U8a4, Bytes>,
}

impl MaxKeyComparator {
  #[inline]
  fn new(rocksdb: Arc<RocksdbDb>) -> Self {
    MaxKeyComparator { rocksdb, cache: AHashMap::new() }
  }

  #[inline]
  fn compare(&mut self, table_id: U8a4, key: &[u8]) -> CmpOrdering {
    if let Some(cached_max_key) = self.cache.get(&table_id) {
      if cached_max_key.as_ref().cmp(key) != CmpOrdering::Greater {
        if let Some(stored_max_key) = self.get_max_key(table_id) {
          let ordering = stored_max_key.as_ref().cmp(key);
          self.cache.insert(table_id, cached_max_key.clone());
          ordering
        } else {
          CmpOrdering::Equal
        }
      } else {
        CmpOrdering::Greater
      }
    } else {
      if let Some(stored_max_key) = self.get_max_key(table_id) {
        let ordering = stored_max_key.as_ref().cmp(key);
        self.cache.insert(table_id, stored_max_key);
        ordering
      } else {
        CmpOrdering::Equal
      }
    }
  }

  #[inline]
  fn get_max_key(&self, table_id: U8a4) -> Option<Bytes> {
    let table = NormalTable::new(self.rocksdb.clone(), table_id);
    let mut cursor = table.new_cursor();
    cursor.seek_to_last();
    if cursor.is_valid() {
      Some(cursor.key().unwrap().to_vec().into())
    } else {
      None
    }
  }
}
