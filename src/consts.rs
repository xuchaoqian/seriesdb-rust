use crate::types::*;

pub(crate) const TABLE_ID_LEN: usize = 4;

pub(crate) const TIMESTAMP_LEN: usize = 4;

// 1024 as BigEndian
pub(crate) const MIN_USERLAND_TABLE_ID: TableId = [0, 0, 4, 0];

// 4294967294 as BigEndian
pub(crate) const MAX_USERLAND_TABLE_ID: TableId = [255, 255, 255, 254];

// 0 as BigEndian
pub(crate) const INFO_TABLE_ID: TableId = [0, 0, 0, 0];

// 1 as BigEndian
pub(crate) const NAME_TO_ID_TABLE_ID: TableId = [0, 0, 0, 1];

// 2 as BigEndian
pub(crate) const ID_TO_NAME_TABLE_ID: TableId = [0, 0, 0, 2];

// 3 as BigEndian
pub(crate) const ID_TO_MAX_KEY_TABLE_ID: TableId = [0, 0, 0, 3];

// 0 as BigEndian. Use this to fix wal bug.
pub(crate) const PLACEHOLDER_ITEM_ID: ItemId = [0, 0];

// 1 as BigEndian. Use this to ensure consistent open.
pub(crate) const TTL_ITEM_ID: ItemId = [0, 1];
