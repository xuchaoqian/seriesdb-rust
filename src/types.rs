pub type RocksdbError = rocksdb::Error;

pub type U8a4 = [u8; 4];

pub type TableId = U8a4;

pub type Timestamp = U8a4;

pub(crate) type ItemId = [u8; 2];
