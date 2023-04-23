use std::marker::PhantomData;

use crate::coder::Coder;
use crate::write_batch::*;

pub struct WriteBatchXEnhanced<WB: WriteBatchX, K, V, C: Coder<K, V>> {
  pub raw: WB,
  phantom: PhantomData<(K, V, C)>,
}

impl<WB: WriteBatchX, K, V, C: Coder<K, V>> WriteBatchXEnhanced<WB, K, V, C> {
  pub fn new(raw: WB) -> Self {
    Self { raw, phantom: PhantomData }
  }
}
