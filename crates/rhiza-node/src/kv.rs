use rhiza_core::{LogHash, LogIndex};
use rhiza_kv::KvRequestRecord;

pub use rhiza_kv::{KvCommandResultV1, KvCommandV1};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvReadResponse {
    pub value: Option<Vec<u8>>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KvMutationOutcome {
    applied_index: LogIndex,
    hash: LogHash,
    result: KvCommandResultV1,
}

impl KvMutationOutcome {
    pub(crate) fn from_record(record: KvRequestRecord) -> Self {
        Self {
            applied_index: record.original_log_index(),
            hash: record.original_log_hash(),
            result: record.result().clone(),
        }
    }

    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    pub const fn hash(&self) -> LogHash {
        self.hash
    }

    pub const fn result(&self) -> &KvCommandResultV1 {
        &self.result
    }
}
