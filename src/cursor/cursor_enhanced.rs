use std::marker::PhantomData;

use crate::coder::*;
use crate::cursor::*;
use crate::error::Error;

pub struct CursorEnhanced<'a, C: Cursor<'a>, K, V, C2: Coder<K, V>> {
  pub(crate) raw: C,
  phantom: PhantomData<&'a (K, V, C2)>,
}

impl<'a, C: Cursor<'a>, K, V, C2: Coder<K, V>> CursorEnhanced<'a, C, K, V, C2> {
  #[inline]
  pub fn new(raw: C) -> Self {
    Self { raw, phantom: PhantomData }
  }

  #[inline]
  pub fn is_valid(&self) -> bool {
    self.raw.is_valid()
  }

  #[inline]
  pub fn status(&self) -> Result<(), Error> {
    Ok(self.raw.status()?)
  }

  #[inline]
  pub fn seek_to_first(&mut self) {
    self.raw.seek_to_first()
  }

  #[inline]
  pub fn seek_to_last(&mut self) {
    self.raw.seek_to_last();
  }

  #[inline]
  pub fn seek(&mut self, key: K) {
    self.raw.seek(C2::encode_key(key));
  }

  #[inline]
  pub fn seek_for_prev(&mut self, key: K) {
    self.raw.seek_for_prev(C2::encode_key(key));
  }

  #[inline]
  pub fn next(&mut self) {
    self.raw.next()
  }

  #[inline]
  pub fn prev(&mut self) {
    self.raw.prev()
  }

  #[inline]
  pub fn key(&'a self) -> Option<&[u8]> {
    self.raw.key()
  }

  #[inline]
  pub fn value(&self) -> Option<&[u8]> {
    self.raw.value()
  }
}
