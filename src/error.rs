use std::sync::Arc;

use thiserror::Error as ThisError;

use crate::types::RocksdbError;

#[derive(ThisError, Debug)]
pub enum Error {
  #[error("Table id exceeded limit: {current} (expected < {max})")]
  ExceededLimitError { current: u32, max: u32 },

  #[error("Inconsistent ttl enabled: current: {current}, wanted: {wanted}")]
  InconsistentTtlEnabled { current: bool, wanted: bool },

  #[error(transparent)]
  ErrorPtr(#[from] Arc<Error>),

  #[error(transparent)]
  RocksdbError(#[from] RocksdbError),
}
