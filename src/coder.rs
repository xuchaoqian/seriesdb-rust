pub trait Coder {
  fn encode_key<From, To: AsRef<[u8]>>(key: From) -> To;
  fn decode_key<From: AsRef<[u8]>, To>(key: From) -> To;
  fn encode_value<From, To: AsRef<[u8]>>(value: From) -> To;
  fn decode_value<From: AsRef<[u8]>, To>(value: From) -> To;
}
