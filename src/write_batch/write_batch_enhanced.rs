use std::marker::PhantomData;

use crate::coder::Coder;
use crate::write_batch::*;

pub struct WriteBatchEnhanced<WB: WriteBatch, K, V, C: Coder<K, V>> {
  pub(crate) raw: WB,
  phantom: PhantomData<(K, V, C)>,
}

impl<WB: WriteBatch, K, V, C: Coder<K, V>> WriteBatchEnhanced<WB, K, V, C> {
  pub fn new(raw: WB) -> Self {
    Self { raw, phantom: PhantomData }
  }
}
