use std::{
    fs,
    path::{Path, PathBuf},
};

use rhiza_core::LogAnchor;
use rhiza_core::{ConfigChange, ConfigurationState, EntryType, LogEntry, LogHash};
use rhiza_graph::{
    apply_lgfx_to_exact_base, decode_snapshot, encode_replicated_graph_command, encode_snapshot,
    graph_materializer_fingerprint, restore_snapshot_file, ControlIdentity, ControlStore, Error,
    GraphCommandResultV1, GraphCommandV1, GraphValueV1, LadybugFileEffectV1, LadybugStateMachine,
    PendingApply,
};

fn command(request: &str, id: &str, value: u64) -> (GraphCommandV1, Vec<u8>) {
    let command = GraphCommandV1::put_document(request, id, GraphValueV1::U64(value)).unwrap();
    let payload = encode_replicated_graph_command(&command).unwrap();
    (command, payload)
}

fn lgfx_entry(index: u64, prev_hash: LogHash, payload: Vec<u8>) -> LogEntry {
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

fn control_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".control");
    value.into()
}

#[test]
fn prepare_is_nonmutating_and_lgfx_apply_atomically_publishes_data_tip_and_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.lbug");
    let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    let (_, request) = command("request-1", "document-1", 42);
    let before = fs::read(&path).unwrap();

    let effect = state
        .prepare_graph_effect(&request, 0, LogHash::ZERO)
        .unwrap();

    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(state.applied_index().unwrap(), 0);
    assert_eq!(state.get_document("document-1").unwrap(), None);

    let entry = lgfx_entry(1, LogHash::ZERO, effect);
    let outcome = state.apply_entry(&entry).unwrap();
    assert_eq!(outcome.applied_index(), 1);
    assert_eq!(outcome.applied_hash(), entry.hash);
    assert_eq!(
        outcome.result(),
        Some(&GraphCommandResultV1::PutDocument { created: true })
    );
    assert_eq!(
        state.get_document_with_tip("document-1").unwrap(),
        (Some(GraphValueV1::U64(42)), 1, entry.hash)
    );
    assert_eq!(
        state
            .check_request("request-1", &request)
            .unwrap()
            .unwrap()
            .original_log_hash(),
        entry.hash
    );

    assert_eq!(state.apply_entry(&entry).unwrap(), outcome);
}

#[test]
fn apply_rejects_semantic_or_stale_commands_without_mutating_the_canonical_pair() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.lbug");
    let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    let (_, request) = command("request-1", "document-1", 1);
    let semantic = lgfx_entry(1, LogHash::ZERO, request.clone());
    assert!(matches!(
        state.apply_entry(&semantic),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(state.applied_index().unwrap(), 0);

    let effect = state
        .prepare_graph_effect(&request, 0, LogHash::ZERO)
        .unwrap();
    let first = lgfx_entry(1, LogHash::ZERO, effect);
    state.apply_entry(&first).unwrap();
    let (_, second_request) = command("request-2", "document-2", 2);
    assert!(state
        .prepare_graph_effect(&second_request, 0, LogHash::ZERO)
        .is_err());
    assert_eq!(state.applied_hash().unwrap(), first.hash);
}

#[test]
fn open_requires_a_complete_database_control_pair_and_rejects_rhgs_v1() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.lbug");
    let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    let snapshot = state.create_snapshot(0).unwrap();
    let mut encoded = encode_snapshot(&snapshot).unwrap();
    encoded[4..6].copy_from_slice(&1u16.to_be_bytes());
    assert!(matches!(
        decode_snapshot(&encoded),
        Err(Error::InvalidSnapshot(_))
    ));
    drop(state);

    fs::remove_file(control_path(&path)).unwrap();
    assert!(matches!(
        LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3),
        Err(Error::IdentityMismatch(_))
    ));
}

#[test]
fn rhgs_v2_restores_clean_bytes_replicated_tip_and_receipt_to_another_node() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.lbug");
    let target_path = dir.path().join("target.lbug");
    let source = LadybugStateMachine::open(&source_path, "cluster-1", "node-1", 7, 3).unwrap();
    let (_, request) = command("request-1", "document-1", 7);
    let effect = source
        .prepare_graph_effect(&request, 0, LogHash::ZERO)
        .unwrap();
    let entry = lgfx_entry(1, LogHash::ZERO, effect);
    source.apply_entry(&entry).unwrap();
    let snapshot = source.create_snapshot(1).unwrap();

    restore_snapshot_file(&target_path, &snapshot, "node-2").unwrap();
    let target = LadybugStateMachine::open(&target_path, "cluster-1", "node-2", 7, 3).unwrap();
    assert_eq!(
        fs::read(&source_path).unwrap(),
        fs::read(&target_path).unwrap()
    );
    assert_eq!(
        target.get_document_with_tip("document-1").unwrap(),
        (Some(GraphValueV1::U64(7)), 1, entry.hash)
    );
    assert_eq!(
        target
            .check_request("request-1", &request)
            .unwrap()
            .unwrap()
            .original_log_hash(),
        entry.hash
    );
}

#[test]
fn restart_reapplies_the_same_pending_effect_from_either_base_or_installed_target() {
    for target_installed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("graph.lbug");
        let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        let (_, request) = command("request-1", "document-1", 9);
        let encoded = state
            .prepare_graph_effect(&request, 0, LogHash::ZERO)
            .unwrap();
        let effect = LadybugFileEffectV1::decode(&encoded).unwrap();
        let entry = lgfx_entry(1, LogHash::ZERO, encoded);
        drop(state);

        let control = ControlStore::open_existing(control_path(&path)).unwrap();
        let pending = PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, entry.hash),
            effect.base_db_digest,
            effect.target_db_digest,
            effect.target_file_bytes,
        );
        control.begin_pending(&pending).unwrap();
        drop(control);

        if target_installed {
            let target = dir.path().join("installed.lbug");
            apply_lgfx_to_exact_base(&path, &target, &effect).unwrap();
            fs::rename(target, &path).unwrap();
        }

        let reopened = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
        assert!(reopened.get_document("document-1").is_err());
        let outcome = reopened.apply_entry(&entry).unwrap();
        assert_eq!(outcome.applied_hash(), entry.hash);
        assert_eq!(
            reopened.get_document_with_tip("document-1").unwrap(),
            (Some(GraphValueV1::U64(9)), 1, entry.hash)
        );
    }
}

#[test]
fn noop_and_configuration_entries_change_only_replicated_control_and_survive_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.lbug");
    let restored_path = dir.path().join("restored.lbug");
    let config_digest = LogHash::digest(&[b"configuration-3"]);
    let state = LadybugStateMachine::open_with_configuration(
        &source_path,
        "cluster-1",
        "node-1",
        7,
        ConfigurationState::active(3, config_digest),
    )
    .unwrap();
    let original_bytes = fs::read(&source_path).unwrap();

    let noop_hash =
        LogEntry::calculate_hash("cluster-1", 1, 7, 3, EntryType::Noop, LogHash::ZERO, &[]);
    let noop = LogEntry {
        cluster_id: "cluster-1".into(),
        epoch: 7,
        config_id: 3,
        index: 1,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash: LogHash::ZERO,
        hash: noop_hash,
    };
    state.apply_entry(&noop).unwrap();
    assert_eq!(fs::read(&source_path).unwrap(), original_bytes);

    let stop = ConfigChange::stop(3, config_digest).to_stored_command();
    let stop_hash = LogEntry::calculate_hash(
        "cluster-1",
        2,
        7,
        3,
        stop.entry_type,
        noop.hash,
        &stop.payload,
    );
    let stop_entry = LogEntry {
        cluster_id: "cluster-1".into(),
        epoch: 7,
        config_id: 3,
        index: 2,
        entry_type: stop.entry_type,
        payload: stop.payload,
        prev_hash: noop.hash,
        hash: stop_hash,
    };
    state.apply_entry(&stop_entry).unwrap();
    assert_eq!(fs::read(&source_path).unwrap(), original_bytes);
    assert!(matches!(
        state.configuration_state_value().unwrap(),
        ConfigurationState::Stopped { config_id: 3, .. }
    ));

    let snapshot = state.create_snapshot(2).unwrap();
    restore_snapshot_file(&restored_path, &snapshot, "node-2").unwrap();
    let restored = LadybugStateMachine::open(&restored_path, "cluster-1", "node-2", 7, 3).unwrap();
    assert_eq!(
        restored.materialized_tip().unwrap(),
        LogAnchor::new(2, stop_hash)
    );
    assert!(matches!(
        restored.configuration_state_value().unwrap(),
        ConfigurationState::Stopped { config_id: 3, .. }
    ));
}

#[test]
fn open_rejects_legacy_internal_metadata_and_corrupt_control_without_migration() {
    let dir = tempfile::tempdir().unwrap();
    let legacy_path = dir.path().join("legacy.lbug");
    let database = lbug::Database::new(&legacy_path, lbug::SystemConfig::default()).unwrap();
    let connection = lbug::Connection::new(&database).unwrap();
    connection
        .query("CREATE NODE TABLE __RhizaMeta(key STRING PRIMARY KEY, value STRING)")
        .unwrap();
    connection.query("CHECKPOINT").unwrap();
    drop(connection);
    drop(database);
    let legacy_digest = LogHash::digest(&[&fs::read(&legacy_path).unwrap()]);
    ControlStore::create(
        control_path(&legacy_path),
        &ControlIdentity::new(
            "cluster-1",
            "node-1",
            7,
            ConfigurationState::active(3, LogHash::ZERO),
            1,
            graph_materializer_fingerprint(),
            legacy_digest,
        ),
    )
    .unwrap();
    assert!(matches!(
        LadybugStateMachine::open(&legacy_path, "cluster-1", "node-1", 7, 3),
        Err(Error::IdentityMismatch(message)) if message.contains("legacy")
    ));

    let fresh_path = dir.path().join("fresh.lbug");
    let fresh = LadybugStateMachine::open(&fresh_path, "cluster-1", "node-1", 7, 3).unwrap();
    drop(fresh);
    let control = control_path(&fresh_path);
    let mut bytes = fs::read(&control).unwrap();
    bytes[0] ^= 0xff;
    fs::write(&control, bytes).unwrap();
    assert!(LadybugStateMachine::open(&fresh_path, "cluster-1", "node-1", 7, 3).is_err());
}
