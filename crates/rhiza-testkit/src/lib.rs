use rhiza_core::{LogHash, LogIndex};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppliedState {
    node_id: String,
    applied_index: LogIndex,
    applied_hash: LogHash,
}

impl AppliedState {
    pub fn new(node_id: impl Into<String>, applied_index: LogIndex, applied_hash: LogHash) -> Self {
        Self {
            node_id: node_id.into(),
            applied_index,
            applied_hash,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    pub const fn applied_hash(&self) -> LogHash {
        self.applied_hash
    }
}

pub fn all_applied_hashes_match(states: impl IntoIterator<Item = AppliedState>) -> bool {
    let mut states = states.into_iter();
    let Some(first) = states.next() else {
        return true;
    };

    states.all(|state| {
        state.applied_index == first.applied_index && state.applied_hash == first.applied_hash
    })
}
