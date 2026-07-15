use rhiza_core::{LogHash, LogIndex};
use rhiza_graph::RequestRecord;

pub use rhiza_graph::{GraphCommandResultV1, GraphCommandV1, GraphValueV1};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphReadResponse {
    pub value: Option<GraphValueV1>,
    pub applied_index: LogIndex,
    pub hash: LogHash,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphMutationOutcome {
    applied_index: LogIndex,
    hash: LogHash,
    result: GraphCommandResultV1,
}

impl GraphMutationOutcome {
    pub(crate) fn from_record(record: RequestRecord) -> Self {
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

    pub const fn result(&self) -> &GraphCommandResultV1 {
        &self.result
    }
}
