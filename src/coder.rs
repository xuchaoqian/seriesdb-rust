use bytes::Bytes;

pub trait Coder<K, V> {
  fn encode_key(key: K) -> Bytes;
  fn decode_key(key: Bytes) -> K;
  fn encode_value(value: V) -> Bytes;
  fn decode_value(value: Bytes) -> V;
}
