use std::{
    fs,
    path::{Path, PathBuf},
};

use rhiza_core::LogAnchor;
use rhiza_core::{ConfigChange, ConfigurationState, EntryType, LogEntry, LogHash};
#[cfg(feature = "vfs")]
use rhiza_graph::diff_closed_ladybug_files;
use rhiza_graph::{
    apply_lgfx_to_exact_base, decode_snapshot, encode_replicated_graph_command, encode_snapshot,
    graph_materializer_fingerprint, lgfx_chunks_digest, restore_snapshot_file, ControlIdentity,
    ControlStore, Error, GraphCommandResultV1, GraphCommandV1, GraphValueV1, LadybugFileChunkV1,
    LadybugFileEffectV1, LadybugStateMachine, PendingApply, LGFX_CHUNK_BYTES,
};

const DOCUMENT_SCHEMA: &str = r#"CREATE NODE TABLE RhizaDocument(
    id STRING PRIMARY KEY,
    kind UINT8,
    bool_value BOOL,
    i64_value INT64,
    u64_value UINT64,
    f64_value DOUBLE,
    string_value STRING,
    bytes_value BLOB
)"#;

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

fn custom_genesis(path: &Path, node_id: &str, mutation: &str) -> LadybugStateMachine {
    let database = lbug::Database::new(path, lbug::SystemConfig::default()).unwrap();
    let connection = lbug::Connection::new(&database).unwrap();
    connection.query(DOCUMENT_SCHEMA).unwrap();
    connection.query(mutation).unwrap();
    connection.query("CHECKPOINT").unwrap();
    drop(connection);
    drop(database);
    let digest = LogHash::digest(&[&fs::read(path).unwrap()]);
    ControlStore::create(
        control_path(path),
        &ControlIdentity::new(
            "cluster-1",
            node_id,
            7,
            ConfigurationState::active(3, LogHash::ZERO),
            1,
            graph_materializer_fingerprint(),
            digest,
        ),
    )
    .unwrap();
    LadybugStateMachine::open(path, "cluster-1", node_id, 7, 3).unwrap()
}

fn full_chunks(path: &Path) -> Vec<LadybugFileChunkV1> {
    fs::read(path)
        .unwrap()
        .chunks_exact(LGFX_CHUNK_BYTES)
        .enumerate()
        .map(|(chunk_index, after_image)| LadybugFileChunkV1 {
            chunk_index: chunk_index as u64,
            after_image: after_image.to_vec(),
        })
        .collect()
}

fn assert_pair_unchanged(
    state: &LadybugStateMachine,
    path: &Path,
    entry: &LogEntry,
    before_db: &[u8],
    before_control: &[u8],
) {
    assert!(state.apply_entry(entry).is_err());
    assert_eq!(fs::read(path).unwrap(), before_db);
    assert_eq!(fs::read(control_path(path)).unwrap(), before_control);
}

#[cfg(feature = "vfs")]
fn assert_sparse_effect_matches_full_closed_file_diff(
    directory: &Path,
    base: &Path,
    encoded: &[u8],
) {
    let effect = LadybugFileEffectV1::decode(encoded).unwrap();
    let target = directory.join(format!("target-{}.lbug", effect.base_log_index + 1));
    apply_lgfx_to_exact_base(base, &target, &effect).unwrap();
    assert_eq!(
        effect.chunks,
        diff_closed_ladybug_files(base, &target).unwrap()
    );
    fs::remove_file(target).unwrap();
}

#[cfg(feature = "vfs")]
#[test]
fn dirty_chunk_effects_match_full_diff_for_insert_update_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.lbug");
    let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    let (_, bootstrap) = command("bootstrap", "seed", 1);
    let bootstrap = state
        .prepare_graph_effect(&bootstrap, 0, LogHash::ZERO)
        .unwrap();
    let bootstrap_entry = lgfx_entry(1, LogHash::ZERO, bootstrap);
    state.apply_entry(&bootstrap_entry).unwrap();

    let commands = [
        GraphCommandV1::put_document("insert", "document", GraphValueV1::U64(7)).unwrap(),
        GraphCommandV1::put_document("update", "document", GraphValueV1::U64(9)).unwrap(),
        GraphCommandV1::delete_document("delete", "document").unwrap(),
    ];
    let mut previous = bootstrap_entry.hash;
    for (index, command) in (2_u64..).zip(commands) {
        let request = encode_replicated_graph_command(&command).unwrap();
        let encoded = state
            .prepare_graph_effect(&request, index - 1, previous)
            .unwrap();
        assert_sparse_effect_matches_full_closed_file_diff(dir.path(), &path, &encoded);
        let entry = lgfx_entry(index, previous, encoded);
        state.apply_entry(&entry).unwrap();
        previous = entry.hash;
    }
}

#[test]
fn first_effect_bootstraps_independent_fresh_ladybug_genesis_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let proposer_path = dir.path().join("proposer.lbug");
    let follower_path = dir.path().join("follower.lbug");
    let exact_path = dir.path().join("exact-follower.lbug");
    let proposer = LadybugStateMachine::open(&proposer_path, "cluster-1", "node-1", 7, 3).unwrap();
    let follower = LadybugStateMachine::open(&follower_path, "cluster-1", "node-2", 7, 3).unwrap();
    fs::copy(&proposer_path, &exact_path).unwrap();
    let exact_digest = proposer.canonical_db_digest().unwrap();
    ControlStore::create(
        control_path(&exact_path),
        &ControlIdentity::new(
            "cluster-1",
            "node-3",
            7,
            ConfigurationState::active(3, LogHash::ZERO),
            1,
            graph_materializer_fingerprint(),
            exact_digest,
        ),
    )
    .unwrap();
    let exact = LadybugStateMachine::open(&exact_path, "cluster-1", "node-3", 7, 3).unwrap();
    assert_eq!(exact.canonical_db_digest().unwrap(), exact_digest);
    assert_eq!(
        proposer.reconciliation_base_state().unwrap(),
        (
            proposer.materialized_tip().unwrap(),
            proposer.configuration_state_value().unwrap(),
        )
    );
    assert_ne!(exact_digest, follower.canonical_db_digest().unwrap());

    let (_, request) = command("request-1", "document-1", 42);
    let encoded = proposer
        .prepare_graph_effect(&request, 0, LogHash::ZERO)
        .unwrap();
    let effect = LadybugFileEffectV1::decode(&encoded).unwrap();
    assert_eq!(
        effect.chunks.len() as u64,
        effect.target_file_bytes / rhiza_graph::LGFX_CHUNK_BYTES as u64
    );

    let entry = lgfx_entry(1, LogHash::ZERO, encoded);
    proposer.apply_entry(&entry).unwrap();
    follower.apply_entry(&entry).unwrap();
    exact.apply_entry(&entry).unwrap();

    assert_eq!(
        fs::read(&proposer_path).unwrap(),
        fs::read(&follower_path).unwrap()
    );
    assert_eq!(
        fs::read(&proposer_path).unwrap(),
        fs::read(&exact_path).unwrap()
    );
    assert_eq!(
        follower.get_document_with_tip("document-1").unwrap(),
        (Some(GraphValueV1::U64(42)), 1, entry.hash)
    );

    let (_, second_request) = command("request-2", "document-2", 7);
    let second_encoded = proposer
        .prepare_graph_effect(&second_request, 1, entry.hash)
        .unwrap();
    let second_effect = LadybugFileEffectV1::decode(&second_encoded).unwrap();
    assert!(
        (second_effect.chunks.len() as u64)
            < second_effect.target_file_bytes / rhiza_graph::LGFX_CHUNK_BYTES as u64
    );
    let second_entry = lgfx_entry(2, entry.hash, second_encoded);
    proposer.apply_entry(&second_entry).unwrap();
    follower.apply_entry(&second_entry).unwrap();
    assert_eq!(
        fs::read(&proposer_path).unwrap(),
        fs::read(&follower_path).unwrap()
    );
}

#[test]
fn divergent_bootstrap_rejections_leave_database_and_control_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let proposer_path = dir.path().join("proposer.lbug");
    let proposer = LadybugStateMachine::open(&proposer_path, "cluster-1", "node-1", 7, 3).unwrap();
    let (_, request) = command("request-1", "document-1", 42);
    let encoded = proposer
        .prepare_graph_effect(&request, 0, LogHash::ZERO)
        .unwrap();
    let effect = LadybugFileEffectV1::decode(&encoded).unwrap();
    let entry = lgfx_entry(1, LogHash::ZERO, encoded);

    for (name, mutation) in [
        (
            "extra-schema",
            "CREATE NODE TABLE Unexpected(id STRING PRIMARY KEY)",
        ),
        (
            "nonempty",
            "CREATE (:RhizaDocument {id: 'existing', kind: 1})",
        ),
    ] {
        let path = dir.path().join(format!("{name}.lbug"));
        let state = custom_genesis(&path, name, mutation);
        let before_db = fs::read(&path).unwrap();
        let before_control = fs::read(control_path(&path)).unwrap();
        let (_, polluted_request) = command("polluted-request", "new-document", 1);
        assert!(state
            .prepare_graph_effect(&polluted_request, 0, LogHash::ZERO)
            .is_err());
        assert_eq!(fs::read(&path).unwrap(), before_db);
        assert_eq!(fs::read(control_path(&path)).unwrap(), before_control);
        assert_pair_unchanged(&state, &path, &entry, &before_db, &before_control);
    }

    let missing_path = dir.path().join("missing-chunk.lbug");
    let missing =
        LadybugStateMachine::open(&missing_path, "cluster-1", "node-missing", 7, 3).unwrap();
    let mut incomplete = effect.clone();
    incomplete.chunks.remove(0);
    incomplete.chunks_digest = lgfx_chunks_digest(&incomplete.chunks);
    let incomplete_entry = lgfx_entry(1, LogHash::ZERO, incomplete.encode().unwrap());
    let proposer_before_db = fs::read(&proposer_path).unwrap();
    let proposer_before_control = fs::read(control_path(&proposer_path)).unwrap();
    assert_pair_unchanged(
        &proposer,
        &proposer_path,
        &incomplete_entry,
        &proposer_before_db,
        &proposer_before_control,
    );
    let before_db = fs::read(&missing_path).unwrap();
    let before_control = fs::read(control_path(&missing_path)).unwrap();
    assert_pair_unchanged(
        &missing,
        &missing_path,
        &incomplete_entry,
        &before_db,
        &before_control,
    );

    let receipt_path = dir.path().join("receipt.lbug");
    let receipt =
        LadybugStateMachine::open(&receipt_path, "cluster-1", "node-receipt", 7, 3).unwrap();
    proposer.apply_entry(&entry).unwrap();
    receipt.apply_entry(&entry).unwrap();
    let (_, second_request) = command("request-2", "document-2", 7);
    let mut second = LadybugFileEffectV1::decode(
        &proposer
            .prepare_graph_effect(&second_request, 1, entry.hash)
            .unwrap(),
    )
    .unwrap();
    let second_target = dir.path().join("second-target.lbug");
    apply_lgfx_to_exact_base(&proposer_path, &second_target, &second).unwrap();
    let divergent_path = dir.path().join("divergent.lbug");
    let divergent =
        LadybugStateMachine::open(&divergent_path, "cluster-1", "node-divergent", 7, 3).unwrap();
    second.base_db_digest = divergent.canonical_db_digest().unwrap();
    second.base_file_bytes = fs::metadata(&divergent_path).unwrap().len();
    second.chunks = full_chunks(&second_target);
    second.chunks_digest = lgfx_chunks_digest(&second.chunks);
    let second_entry = lgfx_entry(2, entry.hash, second.encode().unwrap());
    let before_db = fs::read(&receipt_path).unwrap();
    let before_control = fs::read(control_path(&receipt_path)).unwrap();
    assert_pair_unchanged(
        &receipt,
        &receipt_path,
        &second_entry,
        &before_db,
        &before_control,
    );
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
fn nondefault_recovery_generation_survives_reopen_and_snapshot_restore() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.lbug");
    let target_path = dir.path().join("target.lbug");
    let configuration = ConfigurationState::active(3, LogHash::ZERO);

    let source = LadybugStateMachine::open_with_configuration(
        &source_path,
        "cluster-1",
        "node-1",
        7,
        configuration.clone(),
        42,
    )
    .unwrap();
    assert_eq!(
        ControlStore::open_existing(control_path(&source_path))
            .unwrap()
            .recovery_generation()
            .unwrap(),
        42
    );
    let snapshot = source.create_snapshot(0).unwrap();
    drop(source);

    assert!(matches!(
        LadybugStateMachine::open_with_configuration(
            &source_path,
            "cluster-1",
            "node-1",
            7,
            configuration.clone(),
            41,
        ),
        Err(Error::IdentityMismatch(reason)) if reason == "recovery_generation"
    ));
    LadybugStateMachine::open_with_configuration(
        &source_path,
        "cluster-1",
        "node-1",
        7,
        configuration.clone(),
        42,
    )
    .unwrap();

    restore_snapshot_file(&target_path, &snapshot, "node-2").unwrap();
    LadybugStateMachine::open_with_configuration(
        &target_path,
        "cluster-1",
        "node-2",
        7,
        configuration,
        42,
    )
    .unwrap();
    assert_eq!(
        ControlStore::open_existing(control_path(&target_path))
            .unwrap()
            .recovery_generation()
            .unwrap(),
        42
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
        assert!(reopened.materialized_tip().is_err());
        assert_eq!(
            reopened.reconciliation_base_state().unwrap(),
            (
                LogAnchor::new(0, LogHash::ZERO),
                ConfigurationState::active(3, LogHash::ZERO),
            )
        );
        let outcome = reopened.apply_entry(&entry).unwrap();
        assert_eq!(outcome.applied_hash(), entry.hash);
        assert_eq!(
            reopened.get_document_with_tip("document-1").unwrap(),
            (Some(GraphValueV1::U64(9)), 1, entry.hash)
        );
    }
}

#[test]
fn restart_reapplies_bootstrap_pending_from_local_genesis_or_installed_target() {
    for target_installed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let proposer_path = dir.path().join("proposer.lbug");
        let follower_path = dir.path().join("follower.lbug");
        let proposer =
            LadybugStateMachine::open(&proposer_path, "cluster-1", "node-1", 7, 3).unwrap();
        let follower =
            LadybugStateMachine::open(&follower_path, "cluster-1", "node-2", 7, 3).unwrap();
        let follower_digest = follower.canonical_db_digest().unwrap();
        let (_, request) = command("request-1", "document-1", 9);
        let encoded = proposer
            .prepare_graph_effect(&request, 0, LogHash::ZERO)
            .unwrap();
        let effect = LadybugFileEffectV1::decode(&encoded).unwrap();
        let entry = lgfx_entry(1, LogHash::ZERO, encoded);
        drop(proposer);
        drop(follower);

        let pending = PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, entry.hash),
            follower_digest,
            effect.target_db_digest,
            effect.target_file_bytes,
        );
        let control = ControlStore::open_existing(control_path(&follower_path)).unwrap();
        control.begin_pending(&pending).unwrap();
        drop(control);

        if target_installed {
            let target = dir.path().join("installed.lbug");
            apply_lgfx_to_exact_base(&proposer_path, &target, &effect).unwrap();
            fs::rename(target, &follower_path).unwrap();
        }

        let reopened =
            LadybugStateMachine::open(&follower_path, "cluster-1", "node-2", 7, 3).unwrap();
        assert!(reopened.materialized_tip().is_err());
        assert_eq!(
            reopened.reconciliation_base_state().unwrap(),
            (
                LogAnchor::new(0, LogHash::ZERO),
                ConfigurationState::active(3, LogHash::ZERO),
            )
        );
        reopened.apply_entry(&entry).unwrap();
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
        1,
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
