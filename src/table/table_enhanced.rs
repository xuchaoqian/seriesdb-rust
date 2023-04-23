use std::marker::PhantomData;

use bytes::Bytes;

use super::table::Table;
use crate::coder::*;
use crate::cursor::*;
use crate::error::Error;
use crate::types::*;
use crate::write_batch::*;

pub struct TableEnhanced<T: Table, K, V, C: Coder<K, V>> {
  pub(crate) raw: T,
  phantom: PhantomData<(K, V, C)>,
}

impl<T: Table, K, V, C: Coder<K, V>> TableEnhanced<T, K, V, C> {
  #[inline]
  pub fn new(raw: T) -> Self {
    Self { raw, phantom: PhantomData }
  }

  #[inline(always)]
  pub fn id(&self) -> TableId {
    self.raw.id()
  }

  #[inline]
  pub fn put(&self, key: K, value: V) -> Result<(), Error> {
    Ok(self.raw.put(C::encode_key(key), C::encode_value(value))?)
  }

  #[inline]
  pub fn new_write_batch(&self) -> WriteBatchEnhanced<T::WriteBatch, K, V, C> {
    self.raw.new_write_batch().enhance()
  }

  #[inline]
  pub fn write(&self, batch: WriteBatchEnhanced<T::WriteBatch, K, V, C>) -> Result<(), Error> {
    Ok(self.raw.write(batch.raw)?)
  }

  #[inline]
  pub fn delete(&self, key: K) -> Result<(), Error> {
    Ok(self.raw.delete(C::encode_key(key))?)
  }

  #[inline]
  pub fn get(&self, key: K) -> Result<Option<Bytes>, Error> {
    Ok(self.raw.get(C::encode_key(key))?)
  }

  #[inline]
  pub fn cursor<'a>(&'a self) -> CursorEnhanced<T::Cursor<'a>, K, V, C> {
    self.raw.cursor().enhance()
  }
}
