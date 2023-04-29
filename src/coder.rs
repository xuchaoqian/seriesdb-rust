pub trait Coder<K, V> {
  type EncodedKey: AsRef<[u8]>;
  type EncodedValue: AsRef<[u8]>;

  fn encode_key(key: &K) -> Self::EncodedKey;
  fn decode_key(key: &[u8]) -> K;
  fn encode_value(value: &V) -> Self::EncodedValue;
  fn decode_value(value: &[u8]) -> V;
}
