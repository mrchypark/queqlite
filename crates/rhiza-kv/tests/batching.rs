use rhiza_core::{EntryType, LogEntry, LogHash};
use rhiza_kv::{
    encode_replicated_kv_batch, encode_replicated_kv_command, Error, KvCommandResultV1,
    KvCommandV1, RedbStateMachine, MAX_KV_BATCH_MEMBERS,
};

#[test]
fn ordered_batch_atomically_applies_members_and_distinct_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let first = KvCommandV1::put("first", b"key".to_vec(), b"one".to_vec()).unwrap();
    let second = KvCommandV1::put("second", b"key".to_vec(), b"two".to_vec()).unwrap();
    let first_payload = encode_replicated_kv_command(&first).unwrap();
    let second_payload = encode_replicated_kv_command(&second).unwrap();
    let entry = entry(
        1,
        LogHash::ZERO,
        encode_replicated_kv_batch(&[first, second]).unwrap(),
    );

    let outcome = state.apply_entry(&entry).unwrap();

    assert_eq!(outcome.applied_index(), 1);
    assert_eq!(outcome.applied_hash(), entry.hash);
    assert_eq!(outcome.result(), None);
    assert_eq!(state.get(b"key").unwrap(), Some(b"two".to_vec()));
    let first = state
        .check_request("first", &first_payload)
        .unwrap()
        .unwrap();
    let second = state
        .check_request("second", &second_payload)
        .unwrap()
        .unwrap();
    assert_eq!(first.original_log_index(), 1);
    assert_eq!(second.original_log_index(), 1);
    assert_eq!(first.original_log_hash(), entry.hash);
    assert_eq!(second.original_log_hash(), entry.hash);
    assert_eq!(first.result(), &KvCommandResultV1::Put { replaced: false });
    assert_eq!(second.result(), &KvCommandResultV1::Put { replaced: true });
}

#[test]
fn request_conflict_rolls_back_every_member_and_the_applied_tip() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let original = KvCommandV1::put("existing", b"stable".to_vec(), b"one".to_vec()).unwrap();
    let original_entry = entry(
        1,
        LogHash::ZERO,
        encode_replicated_kv_command(&original).unwrap(),
    );
    state.apply_entry(&original_entry).unwrap();
    let new = KvCommandV1::put("new", b"new".to_vec(), b"value".to_vec()).unwrap();
    let new_payload = encode_replicated_kv_command(&new).unwrap();
    let conflict = KvCommandV1::put("existing", b"stable".to_vec(), b"different".to_vec()).unwrap();
    let conflicting_entry = entry(
        2,
        original_entry.hash,
        encode_replicated_kv_batch(&[new, conflict]).unwrap(),
    );

    assert!(matches!(
        state.apply_entry(&conflicting_entry),
        Err(Error::RequestConflict { request_id }) if request_id == "existing"
    ));

    assert_eq!(state.applied_index().unwrap(), 1);
    assert_eq!(state.applied_hash().unwrap(), original_entry.hash);
    assert_eq!(state.get(b"new").unwrap(), None);
    assert_eq!(state.get(b"stable").unwrap(), Some(b"one".to_vec()));
    assert_eq!(state.check_request("new", &new_payload).unwrap(), None);
}

#[test]
fn malformed_or_duplicate_members_are_rejected_before_mutating_state() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let command = KvCommandV1::put("same", b"key".to_vec(), b"value".to_vec()).unwrap();
    assert!(matches!(
        encode_replicated_kv_batch(&[command.clone(), command.clone()]),
        Err(Error::InvalidCommand(_))
    ));
    assert!(matches!(
        encode_replicated_kv_batch(&[]),
        Err(Error::InvalidCommand(_))
    ));
    let oversized = (0..=MAX_KV_BATCH_MEMBERS)
        .map(|index| {
            KvCommandV1::put(
                format!("request-{index}"),
                format!("key-{index}").into_bytes(),
                b"value".to_vec(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        encode_replicated_kv_batch(&oversized),
        Err(Error::InvalidCommand(_))
    ));
    let mut malformed = encode_replicated_kv_batch(&[
        command,
        KvCommandV1::delete("delete", b"key".to_vec()).unwrap(),
    ])
    .unwrap();
    malformed.pop();
    let malformed_entry = entry(1, LogHash::ZERO, malformed);

    assert!(state.apply_entry(&malformed_entry).is_err());
    assert_eq!(state.applied_index().unwrap(), 0);
    assert_eq!(state.applied_hash().unwrap(), LogHash::ZERO);
    assert_eq!(state.get(b"key").unwrap(), None);
}

fn state(root: &std::path::Path) -> RedbStateMachine {
    RedbStateMachine::open(root.join("state.redb"), "cluster-1", "node-1", 7, 3).unwrap()
}

fn entry(index: u64, prev_hash: LogHash, payload: Vec<u8>) -> LogEntry {
    let hash = LogEntry::calculate_hash(
        "cluster-1",
        index,
        7,
        3,
        EntryType::Command,
        prev_hash,
        &payload,
    );
    LogEntry {
        cluster_id: "cluster-1".into(),
        epoch: 7,
        config_id: 3,
        index,
        entry_type: EntryType::Command,
        payload,
        prev_hash,
        hash,
    }
}
