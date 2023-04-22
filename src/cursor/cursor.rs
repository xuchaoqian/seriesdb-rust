use crate::error::Error;

pub trait Cursor {
  fn is_valid(&self) -> bool;

  fn status(&self) -> Result<(), Error>;

  fn seek_to_first(&mut self);

  fn seek_to_last(&mut self);

  fn seek<K: AsRef<[u8]>>(&mut self, key: K);

  fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K);

  fn next(&mut self);

  fn prev(&mut self);

  fn key(&self) -> Option<&[u8]>;

  fn value(&self) -> Option<&[u8]>;
}
