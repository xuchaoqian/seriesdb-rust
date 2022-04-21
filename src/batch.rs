use crate::types::*;
use crate::utils::*;
use rocksdb::WriteBatch;

pub struct Batch {
    pub(in crate) inner: WriteBatch,
    table_id: TableId,
}

impl Batch {
    #[inline]
    pub(in crate) fn new(table_id: TableId) -> Batch {
        Batch { inner: WriteBatch::default(), table_id }
    }

    #[inline]
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>, {
        self.inner.put(build_inner_key(self.table_id, key), value)
    }

    #[inline]
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        self.inner.delete(build_inner_key(self.table_id, key))
    }

    #[inline]
    pub fn delete_range<F, T>(&mut self, from_key: F, to_key: T)
    where
        F: AsRef<[u8]>,
        T: AsRef<[u8]>, {
        self.inner.delete(build_delete_range_hint_table_inner_key(&from_key, &to_key));
        self.inner.delete_range(
            build_inner_key(self.table_id, from_key),
            build_inner_key(self.table_id, to_key),
        )
    }
}
