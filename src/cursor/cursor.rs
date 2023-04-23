use bytes::Bytes;
use rocksdb::DBRawIterator;

use crate::coder::Coder;
use crate::cursor::*;
use crate::error::Error;
use crate::types::*;
use crate::utils::*;

pub trait Cursor<'a> {
  ////////////////////////////////////////////////////////////////////////////////
  /// Getters
  ////////////////////////////////////////////////////////////////////////////////
  fn inner(&self) -> &DBRawIterator<'a>;

  fn inner_mut(&mut self) -> &mut DBRawIterator<'a>;

  fn table_id(&self) -> TableId;

  fn anchor(&self) -> &'a Bytes;

  ////////////////////////////////////////////////////////////////////////////////
  /// APIs
  ////////////////////////////////////////////////////////////////////////////////
  #[inline]
  fn is_valid(&self) -> bool {
    self.inner().valid()
  }

  #[inline]
  fn status(&self) -> Result<(), Error> {
    Ok(self.inner().status()?)
  }

  #[inline]
  fn seek_to_first(&mut self) {
    let table_id = self.table_id();
    self.inner_mut().seek(table_id)
  }

  #[inline]
  fn seek_to_last(&mut self) {
    let anchor = self.anchor();
    self.inner_mut().seek_for_prev(anchor);
  }

  #[inline]
  fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
    let table_id = self.table_id();
    self.inner_mut().seek(build_inner_key(table_id, key));
  }

  #[inline]
  fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
    let table_id = self.table_id();
    self.inner_mut().seek_for_prev(build_inner_key(table_id, key));
  }

  #[inline]
  fn next(&mut self) {
    self.inner_mut().next()
  }

  #[inline]
  fn prev(&mut self) {
    self.inner_mut().prev()
  }

  #[inline]
  fn key(&'a self) -> Option<&[u8]> {
    if let Some(v) = self.inner().key() {
      Some(extract_key(v))
    } else {
      None
    }
  }

  fn value(&self) -> Option<&[u8]>;

  #[inline]
  fn enhance<K, V, C: Coder<K, V>>(self) -> CursorEnhanced<'a, Self, K, V, C>
  where Self: Sized {
    CursorEnhanced::new(self)
  }
}
