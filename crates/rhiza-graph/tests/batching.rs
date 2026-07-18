use rhiza_core::{EntryType, LogEntry, LogHash};
use rhiza_graph::{
    encode_replicated_graph_batch, encode_replicated_graph_command, Error, GraphCommandResultV1,
    GraphCommandV1, GraphValueV1, LadybugStateMachine, MAX_GRAPH_BATCH_MEMBERS,
};

#[test]
fn ordered_client_batch_is_materialized_as_sequential_lgfx_effects_with_distinct_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let first =
        GraphCommandV1::put_document("first", "document", GraphValueV1::String("one".into()))
            .unwrap();
    let second =
        GraphCommandV1::put_document("second", "document", GraphValueV1::String("two".into()))
            .unwrap();
    let first_payload = encode_replicated_graph_command(&first).unwrap();
    let second_payload = encode_replicated_graph_command(&second).unwrap();
    let first_effect = state
        .prepare_graph_effect(&first_payload, 0, LogHash::ZERO)
        .unwrap();
    let first_entry = entry(1, LogHash::ZERO, first_effect);
    let first_outcome = state.apply_entry(&first_entry).unwrap();
    let second_effect = state
        .prepare_graph_effect(&second_payload, 1, first_entry.hash)
        .unwrap();
    let second_entry = entry(2, first_entry.hash, second_effect);
    let second_outcome = state.apply_entry(&second_entry).unwrap();

    assert_eq!(first_outcome.applied_index(), 1);
    assert_eq!(second_outcome.applied_index(), 2);
    assert_eq!(
        state.get_document("document").unwrap(),
        Some(GraphValueV1::String("two".into()))
    );
    let first = state
        .check_request("first", &first_payload)
        .unwrap()
        .unwrap();
    let second = state
        .check_request("second", &second_payload)
        .unwrap()
        .unwrap();
    assert_eq!(first.original_log_index(), 1);
    assert_eq!(second.original_log_index(), 2);
    assert_eq!(first.original_log_hash(), first_entry.hash);
    assert_eq!(second.original_log_hash(), second_entry.hash);
    assert_eq!(
        first.result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );
    assert_eq!(
        second.result(),
        &GraphCommandResultV1::PutDocument { created: false }
    );
}

#[test]
fn request_conflict_is_rejected_during_sequential_preparation_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let original =
        GraphCommandV1::put_document("existing", "stable", GraphValueV1::String("one".into()))
            .unwrap();
    let original_payload = encode_replicated_graph_command(&original).unwrap();
    let original_effect = state
        .prepare_graph_effect(&original_payload, 0, LogHash::ZERO)
        .unwrap();
    let original_entry = entry(1, LogHash::ZERO, original_effect);
    state.apply_entry(&original_entry).unwrap();
    let new = GraphCommandV1::put_document("new", "new", GraphValueV1::U64(7)).unwrap();
    let new_payload = encode_replicated_graph_command(&new).unwrap();
    let conflict = GraphCommandV1::put_document(
        "existing",
        "stable",
        GraphValueV1::String("different".into()),
    )
    .unwrap();
    let conflict_payload = encode_replicated_graph_command(&conflict).unwrap();

    assert!(matches!(
        state.prepare_graph_effect(&conflict_payload, 1, original_entry.hash),
        Err(Error::RequestConflict { request_id, .. }) if request_id == "existing"
    ));

    assert_eq!(state.applied_index().unwrap(), 1);
    assert_eq!(state.applied_hash().unwrap(), original_entry.hash);
    assert_eq!(state.get_document("new").unwrap(), None);
    assert_eq!(
        state.get_document("stable").unwrap(),
        Some(GraphValueV1::String("one".into()))
    );
    assert_eq!(state.check_request("new", &new_payload).unwrap(), None);
}

#[test]
fn malformed_duplicate_and_oversized_batches_are_rejected_without_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let state = state(dir.path());
    let command = GraphCommandV1::delete_document("same", "document").unwrap();
    assert!(matches!(
        encode_replicated_graph_batch(&[command.clone(), command.clone()]),
        Err(Error::InvalidCommand(_))
    ));
    assert!(matches!(
        encode_replicated_graph_batch(&[]),
        Err(Error::InvalidCommand(_))
    ));
    let oversized = (0..=MAX_GRAPH_BATCH_MEMBERS)
        .map(|index| {
            GraphCommandV1::delete_document(format!("request-{index}"), format!("document-{index}"))
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        encode_replicated_graph_batch(&oversized),
        Err(Error::InvalidCommand(_))
    ));
    let mut malformed = encode_replicated_graph_batch(&[
        GraphCommandV1::put_document("new", "new", GraphValueV1::U64(9)).unwrap(),
        GraphCommandV1::delete_document("delete", "document").unwrap(),
    ])
    .unwrap();
    let member_start = malformed
        .windows(6)
        .rposition(|window| window == b"RHGC\0\x01")
        .unwrap();
    let operation_tag = member_start + 6 + 4 + "delete".len();
    malformed[operation_tag] = u8::MAX;
    let malformed_entry = entry(1, LogHash::ZERO, malformed);

    assert!(state.apply_entry(&malformed_entry).is_err());
    assert_eq!(state.applied_index().unwrap(), 0);
    assert_eq!(state.applied_hash().unwrap(), LogHash::ZERO);
    assert_eq!(state.get_document("document").unwrap(), None);
    assert_eq!(state.get_document("new").unwrap(), None);
}

fn state(root: &std::path::Path) -> LadybugStateMachine {
    LadybugStateMachine::open(root.join("graph.lbug"), "cluster-1", "node-1", 7, 3).unwrap()
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
