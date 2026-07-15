use rhiza_core::LogHash;
use rhiza_testkit::{all_applied_hashes_match, AppliedState};

#[test]
fn all_applied_hashes_match_requires_same_index_and_hash() {
    let state = AppliedState::new("node-1", 7, LogHash::from_bytes([1; 32]));

    assert!(all_applied_hashes_match([state.clone(), state]));
    assert!(!all_applied_hashes_match([
        AppliedState::new("node-1", 7, LogHash::from_bytes([1; 32])),
        AppliedState::new("node-2", 8, LogHash::from_bytes([1; 32])),
    ]));
    assert!(!all_applied_hashes_match([
        AppliedState::new("node-1", 7, LogHash::from_bytes([1; 32])),
        AppliedState::new("node-2", 7, LogHash::from_bytes([2; 32])),
    ]));
}
