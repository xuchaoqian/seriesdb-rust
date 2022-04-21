use bytes::Bytes;
use std::fmt::{Debug, Formatter, Result as FmtResult};

pub enum Update {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
    DeleteRange { from_key: Bytes, to_key: Bytes },
}

impl Debug for Update {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            Update::Put { key, value } => write!(f, "Put {{key:{:?}, value:{:?}}}", key, value),
            Update::Delete { key } => write!(f, "Delete {{key:{:?}}}", key),
            Update::DeleteRange { from_key, to_key } => {
                write!(f, "DeleteRange {{from_key:{:?}, to_key:{:?}}}", from_key, to_key)
            }
        }
    }
}
