use rocksdb::{DBCompactionStyle, Options as RocksdbOptions, SliceTransform};

use crate::consts::*;
use crate::utils::*;

pub struct Options {
  pub(crate) inner: RocksdbOptions,
  pub(crate) cache_capacity: usize,
}

impl Options {
  pub fn new() -> Self {
    Options { inner: Self::build_default_rocksdb_options(), cache_capacity: 10240 }
  }

  pub fn set_table_cache_num_shard_bits(&mut self, num: i32) {
    self.inner.set_table_cache_num_shard_bits(num);
  }

  pub fn set_write_buffer_size(&mut self, size: usize) {
    self.inner.set_write_buffer_size(size);
  }

  pub fn set_max_write_buffer_number(&mut self, num: i32) {
    self.inner.set_max_write_buffer_number(num);
  }
  pub fn set_min_write_buffer_number_to_merge(&mut self, num: i32) {
    self.inner.set_min_write_buffer_number_to_merge(num);
  }

  pub fn set_max_bytes_for_level_base(&mut self, bytes: u64) {
    self.inner.set_max_bytes_for_level_base(bytes);
  }

  pub fn set_max_bytes_for_level_multiplier(&mut self, bytes: f64) {
    self.inner.set_max_bytes_for_level_multiplier(bytes);
  }

  pub fn set_target_file_size_base(&mut self, size: u64) {
    self.inner.set_target_file_size_base(size);
  }

  pub fn set_target_file_size_multiplier(&mut self, size: i32) {
    self.inner.set_target_file_size_multiplier(size);
  }

  pub fn set_level_zero_file_num_compaction_trigger(&mut self, num: i32) {
    self.inner.set_level_zero_file_num_compaction_trigger(num);
  }

  pub fn set_max_background_jobs(&mut self, num: i32) {
    self.inner.set_max_background_jobs(num);
  }

  pub fn set_wal_ttl_seconds(&mut self, num: u64) {
    self.inner.set_wal_ttl_seconds(num);
  }

  pub fn set_wal_size_limit_mb(&mut self, num: u64) {
    self.inner.set_wal_size_limit_mb(num);
  }

  pub fn set_wal_dir(&mut self, path: &str) {
    self.inner.set_wal_dir(path);
  }

  pub fn set_cache_capacity(&mut self, num: usize) {
    self.cache_capacity = num;
  }

  fn build_default_rocksdb_options() -> RocksdbOptions {
    let mut opts = RocksdbOptions::default();
    opts.create_if_missing(true);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(TABLE_ID_LEN));
    opts.set_max_open_files(-1);
    opts.set_use_fsync(false);
    opts.set_table_cache_num_shard_bits(4);
    opts.set_write_buffer_size(134217728);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(2);
    opts.set_max_bytes_for_level_base(1073741824);
    opts.set_max_bytes_for_level_multiplier(8.0);
    opts.set_target_file_size_base(134217728);
    opts.set_target_file_size_multiplier(8);
    opts.set_disable_auto_compactions(false);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_level_zero_file_num_compaction_trigger(4);
    opts.set_max_background_jobs(4);
    opts.set_merge_operator_associative("merge_table_id", merge_operator);
    opts
  }
}

pub(crate) fn merge_operator(
  inner_key: &[u8], existing_val: Option<&[u8]>, operands: &rocksdb::MergeOperands,
) -> Option<Vec<u8>> {
  let table_id = extract_table_id(inner_key);
  if table_id == NAME_TO_ID_TABLE_ID {
    if operands.is_empty() {
      return None;
    }
    if let Some(_table_id) = existing_val {
      return None;
    } else {
      return Some(operands.iter().next().unwrap().to_vec());
    }
  } else if table_id == ID_TO_NAME_TABLE_ID {
    if operands.is_empty() {
      return None;
    }
    if let Some(_name) = existing_val {
      return None;
    } else {
      return Some(operands.iter().next().unwrap().to_vec());
    }
  } else {
    return None;
  }
}
