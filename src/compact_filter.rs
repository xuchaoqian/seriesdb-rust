use std::ffi::CString;

use rocksdb::{
  compaction_filter::CompactionFilter,
  compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
  CompactionDecision,
};

use crate::consts::*;
use crate::utils::*;

pub struct CompactionFilterImpl {
  name: CString,
  ttl: u32,
}

impl CompactionFilter for CompactionFilterImpl {
  fn filter(&mut self, _level: u32, inner_key: &[u8], inner_value: &[u8]) -> CompactionDecision {
    if inner_key.len() < 4 || inner_value.len() < 4 {
      return CompactionDecision::Keep;
    }

    let table_id = u8s_to_u8a4(extract_table_id(inner_key));
    if table_id < MIN_USERLAND_TABLE_ID {
      return CompactionDecision::Keep;
    }

    let timestamp = u8s_to_u32(extract_timestamp(inner_value));
    if timestamp + self.ttl < now() {
      return CompactionDecision::Remove;
    }

    return CompactionDecision::Keep;
  }

  fn name(&self) -> &std::ffi::CStr {
    self.name.as_c_str()
  }
}

impl CompactionFilterImpl {
  pub fn new(ttl: u32) -> Self {
    CompactionFilterImpl { name: CString::new("seriesdb_compaction_filter").unwrap(), ttl }
  }
}

pub struct CompactionFilterFactoryImpl {
  name: CString,
  ttl: u32,
}

impl CompactionFilterFactory for CompactionFilterFactoryImpl {
  type Filter = CompactionFilterImpl;

  fn create(&mut self, _context: CompactionFilterContext) -> Self::Filter {
    CompactionFilterImpl::new(self.ttl)
  }

  fn name(&self) -> &std::ffi::CStr {
    self.name.as_c_str()
  }
}

impl CompactionFilterFactoryImpl {
  pub fn new(ttl: u32) -> Self {
    CompactionFilterFactoryImpl {
      name: CString::new("seriesdb_compaction_filter_factory").unwrap(),
      ttl,
    }
  }
}
