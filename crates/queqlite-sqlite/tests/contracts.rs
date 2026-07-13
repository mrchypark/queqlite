use std::fs;

use queqlite_core::{
    ConfigChange, ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash, Snapshot,
    SnapshotManifest,
};
use queqlite_sqlite::{
    encode_sql_command, encode_sql_command_v1, encode_write_batch, required_meta_keys,
    restore_recovery_snapshot_file, restore_snapshot_file, sql_executor_fingerprint, Error,
    MetaKey, RequestOutcome, SqlCommand, SqlEffectPreparation, SqlStatement, SqlValue,
    SqliteStateMachine, CREATE_META_TABLE_SQL, MAX_RETURNING_ROWS,
};
use rusqlite::{params, Connection};

fn entry(
    cluster_id: &str,
    epoch: u64,
    config_id: u64,
    index: u64,
    entry_type: EntryType,
    prev_hash: LogHash,
    payload: &[u8],
) -> LogEntry {
    LogEntry {
        cluster_id: cluster_id.into(),
        epoch,
        config_id,
        index,
        entry_type,
        payload: payload.to_vec(),
        prev_hash,
        hash: LogEntry::calculate_hash(
            cluster_id, index, epoch, config_id, entry_type, prev_hash, payload,
        ),
    }
}

fn v1_entry(index: u64, entry_type: EntryType, prev_hash: LogHash, payload: &[u8]) -> LogEntry {
    entry("cluster-a", 1, 1, index, entry_type, prev_hash, payload)
}

#[test]
fn entry_hashes_are_bound_to_the_cluster() {
    let first = entry(
        "cluster-a",
        1,
        1,
        1,
        EntryType::Command,
        LogHash::ZERO,
        b"put\talpha\tone",
    );
    let second = entry(
        "cluster-b",
        1,
        1,
        1,
        EntryType::Command,
        LogHash::ZERO,
        b"put\talpha\tone",
    );

    assert_ne!(first.hash, second.hash);
    assert_eq!(first.hash, first.recompute_hash());
    assert_eq!(second.hash, second.recompute_hash());
}

#[test]
fn required_meta_keys_match_the_design_contract() {
    let keys: Vec<&str> = required_meta_keys()
        .iter()
        .map(|key| key.as_str())
        .collect();

    assert_eq!(
        keys,
        vec![
            "cluster_id",
            "node_id",
            "epoch",
            "config_id",
            "configuration_state",
            "applied_index",
            "applied_hash",
            "schema_version",
            "snapshot_id",
            "created_at",
        ]
    );
}

#[test]
fn meta_table_sql_creates_the_reserved_table() {
    assert!(CREATE_META_TABLE_SQL.contains("__queqlite_meta"));
    assert!(CREATE_META_TABLE_SQL.contains("key TEXT PRIMARY KEY"));
    assert!(CREATE_META_TABLE_SQL.contains("value BLOB NOT NULL"));
    assert!(CREATE_META_TABLE_SQL.contains("__queqlite_migrations"));
}

#[test]
fn meta_key_names_are_stable() {
    assert_eq!(MetaKey::AppliedIndex.as_str(), "applied_index");
    assert_eq!(MetaKey::AppliedHash.as_str(), "applied_hash");
}

#[test]
fn reopen_preserves_progress_and_rejects_identity_mismatches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, b"put\talpha\tone");

    {
        let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
        db.apply_entry(&first).unwrap();
    }

    let reopened = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    assert_eq!(reopened.applied_index_value().unwrap(), 1);
    assert_eq!(reopened.applied_hash_value().unwrap(), first.hash);
    assert_eq!(reopened.get_value("alpha").unwrap().as_deref(), Some("one"));

    assert!(SqliteStateMachine::open(&path, "cluster-b", "node-1", 1, 1).is_err());
    assert!(SqliteStateMachine::open(&path, "cluster-a", "node-2", 1, 1).is_err());
    assert!(SqliteStateMachine::open(&path, "cluster-a", "node-1", 2, 1).is_err());
    assert!(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 2).is_err());

    assert_eq!(reopened.applied_index_value().unwrap(), 1);
    assert_eq!(reopened.applied_hash_value().unwrap(), first.hash);
}

#[test]
fn apply_accepts_only_the_next_valid_entry_or_the_exact_current_entry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, b"put\talpha\tone");

    db.apply_entry(&first).unwrap();
    assert_eq!(db.apply_entry(&first).unwrap().applied_index(), 1);

    let same_index_conflict = v1_entry(1, EntryType::Command, LogHash::ZERO, b"put\talpha\ttwo");
    let old_index = v1_entry(0, EntryType::Noop, LogHash::ZERO, b"");
    let gap = v1_entry(3, EntryType::Noop, first.hash, b"");
    let wrong_prev = v1_entry(2, EntryType::Noop, LogHash::digest(&[b"wrong"]), b"");
    let mut wrong_hash = v1_entry(2, EntryType::Noop, first.hash, b"");
    wrong_hash.hash = LogHash::ZERO;
    let wrong_cluster = entry("cluster-b", 1, 1, 2, EntryType::Noop, first.hash, b"");
    let wrong_epoch = entry("cluster-a", 2, 1, 2, EntryType::Noop, first.hash, b"");
    let wrong_config = entry("cluster-a", 1, 2, 2, EntryType::Noop, first.hash, b"");

    for invalid in [
        same_index_conflict,
        old_index,
        gap,
        wrong_prev,
        wrong_hash,
        wrong_cluster,
        wrong_epoch,
        wrong_config,
    ] {
        assert!(db.apply_entry(&invalid).is_err());
        assert_eq!(db.applied_index_value().unwrap(), 1);
        assert_eq!(db.applied_hash_value().unwrap(), first.hash);
    }
}

#[test]
fn request_outcome_stays_stable_across_committed_duplicate_replays() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let request_payload = b"put\trequest-1\talpha\tone";
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, request_payload);
    let second = v1_entry(2, EntryType::Command, first.hash, b"put\talpha\ttwo");
    let repeated = v1_entry(3, EntryType::Command, second.hash, request_payload);
    let conflict_payload = b"put\trequest-1\talpha\tconflict";
    let conflict = v1_entry(4, EntryType::Command, repeated.hash, conflict_payload);
    let expected = RequestOutcome::new(1, first.hash);

    assert_eq!(
        db.check_request("request-1", request_payload).unwrap(),
        None
    );

    db.apply_entry(&first).unwrap();
    assert_eq!(
        db.check_request("request-1", request_payload).unwrap(),
        Some(expected)
    );

    let error = db.check_request("request-1", conflict_payload).unwrap_err();
    let Error::RequestConflict(conflict_error) = error else {
        panic!("expected typed request conflict");
    };
    assert_eq!(conflict_error.request_id(), "request-1");
    assert_eq!(conflict_error.original_outcome(), expected);

    db.apply_entry(&second).unwrap();
    db.apply_entry(&repeated).unwrap();
    assert!(matches!(
        db.apply_entry(&conflict),
        Err(Error::RequestConflict(_))
    ));

    assert_eq!(db.get_value("alpha").unwrap().as_deref(), Some("two"));
    assert_eq!(db.applied_index_value().unwrap(), 3);
    assert_eq!(db.applied_hash_value().unwrap(), repeated.hash);
    assert_eq!(
        db.check_request("request-1", request_payload).unwrap(),
        Some(expected)
    );
    assert!(matches!(
        db.check_request("request-1", conflict_payload),
        Err(Error::RequestConflict(_))
    ));

    let inspection = Connection::open(&path).unwrap();
    let (request_index, request_hash, command_digest): (i64, Vec<u8>, Vec<u8>) = inspection
        .query_row(
            "SELECT original_log_index, original_log_hash, command_digest
             FROM __queqlite_requests
             WHERE request_id = ?1",
            params!["request-1"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(request_index, 1);
    assert_eq!(request_hash, first.hash.as_bytes());
    assert_eq!(
        command_digest,
        LogHash::digest(&[request_payload]).as_bytes()
    );
    drop(inspection);
    drop(db);

    let reopened = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    assert_eq!(
        reopened
            .check_request("request-1", request_payload)
            .unwrap(),
        Some(expected)
    );
}

#[test]
fn write_batch_applies_distinct_requests_atomically_and_replays_each_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let first = b"put\trequest-a\talpha\tone".to_vec();
    let second = b"put\trequest-b\tbeta\ttwo".to_vec();
    let payload = encode_write_batch(&[first.clone(), second.clone()]).unwrap();
    let committed = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);

    db.apply_entry(&committed).unwrap();

    let expected = RequestOutcome::new(1, committed.hash);
    assert_eq!(
        db.check_request("request-a", &first).unwrap(),
        Some(expected)
    );
    assert_eq!(
        db.check_request("request-b", &second).unwrap(),
        Some(expected)
    );
    assert_eq!(db.get_value("alpha").unwrap().as_deref(), Some("one"));
    assert_eq!(db.get_value("beta").unwrap().as_deref(), Some("two"));

    db.apply_entry(&committed).unwrap();
    assert_eq!(
        db.check_request("request-a", &first).unwrap(),
        Some(expected)
    );
    assert_eq!(
        db.check_request("request-b", &second).unwrap(),
        Some(expected)
    );
}

#[test]
fn write_batch_keeps_existing_duplicate_outcome_and_commits_new_request() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let duplicate = b"put\trequest-a\talpha\tone".to_vec();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, &duplicate);
    db.apply_entry(&first).unwrap();

    let fresh = b"put\trequest-b\tbeta\ttwo".to_vec();
    let payload = encode_write_batch(&[duplicate.clone(), fresh.clone()]).unwrap();
    let batch = v1_entry(2, EntryType::Command, first.hash, &payload);
    db.apply_entry(&batch).unwrap();

    assert_eq!(
        db.check_request("request-a", &duplicate).unwrap(),
        Some(RequestOutcome::new(1, first.hash))
    );
    assert_eq!(
        db.check_request("request-b", &fresh).unwrap(),
        Some(RequestOutcome::new(2, batch.hash))
    );
}

#[test]
fn invalid_write_batch_member_rolls_back_values_requests_and_applied_tip() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let valid = b"put\trequest-a\talpha\tone".to_vec();
    let invalid = b"not-a-command".to_vec();
    let payload = encode_write_batch(&[valid.clone(), invalid]).unwrap();
    let batch = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);

    assert!(matches!(
        db.apply_entry(&batch),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(db.get_value("alpha").unwrap(), None);
    assert_eq!(db.check_request("request-a", &valid).unwrap(), None);
    assert_eq!(db.applied_index_value().unwrap(), 0);
    assert_eq!(db.applied_hash_value().unwrap(), LogHash::ZERO);
}

#[test]
fn sqlite_writer_connection_uses_wal_and_normal_synchronous() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();

    let (journal_mode, synchronous) = db.connection_pragmas().unwrap();
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    assert_eq!(synchronous, 1);
}

#[test]
fn request_outcome_rolls_back_when_applied_metadata_cannot_advance() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let payload = b"put\trequest-1\talpha\tone";
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, payload);

    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_applied_index_update
             BEFORE UPDATE ON __queqlite_meta
             WHEN NEW.key = 'applied_index'
             BEGIN
                 SELECT RAISE(ABORT, 'forced applied metadata failure');
             END;",
        )
        .unwrap();

    assert!(matches!(db.apply_entry(&first), Err(Error::Sqlite(_))));
    assert_eq!(db.get_value("alpha").unwrap(), None);
    assert_eq!(db.check_request("request-1", payload).unwrap(), None);
    assert_eq!(db.applied_index_value().unwrap(), 0);
    assert_eq!(db.applied_hash_value().unwrap(), LogHash::ZERO);
}

#[test]
fn request_ids_are_scoped_to_the_current_database_identity() {
    let dir = tempfile::tempdir().unwrap();
    let first_path = dir.path().join("cluster-a.sqlite");
    let second_path = dir.path().join("cluster-b.sqlite");
    let first_db = SqliteStateMachine::open(&first_path, "cluster-a", "node-1", 1, 1).unwrap();
    let second_db = SqliteStateMachine::open(&second_path, "cluster-b", "node-2", 1, 2).unwrap();
    let first_payload = b"put\tshared-request\talpha\tone";
    let second_payload = b"put\tshared-request\talpha\ttwo";
    let first = entry(
        "cluster-a",
        1,
        1,
        1,
        EntryType::Command,
        LogHash::ZERO,
        first_payload,
    );
    let second = entry(
        "cluster-b",
        1,
        2,
        1,
        EntryType::Command,
        LogHash::ZERO,
        second_payload,
    );

    first_db.apply_entry(&first).unwrap();
    second_db.apply_entry(&second).unwrap();

    assert_eq!(first_db.get_value("alpha").unwrap().as_deref(), Some("one"));
    assert_eq!(
        second_db.get_value("alpha").unwrap().as_deref(),
        Some("two")
    );
    assert_eq!(
        first_db
            .check_request("shared-request", first_payload)
            .unwrap(),
        Some(RequestOutcome::new(1, first.hash))
    );
    assert_eq!(
        second_db
            .check_request("shared-request", second_payload)
            .unwrap(),
        Some(RequestOutcome::new(1, second.hash))
    );
}

#[test]
fn open_rejects_the_incompatible_prototype_request_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");

    drop(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap());
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "DROP TABLE __queqlite_requests;
             CREATE TABLE __queqlite_requests (
                 request_id TEXT PRIMARY KEY,
                 log_index INTEGER NOT NULL,
                 command_digest BLOB NOT NULL CHECK(length(command_digest) = 32)
             );",
        )
        .unwrap();

    match SqliteStateMachine::open_existing(&path) {
        Err(Error::Sqlite(message)) => assert_eq!(
            message,
            "incompatible __queqlite_requests schema; recreate this prototype database"
        ),
        Err(error) => panic!("expected SQLite schema error, got {error}"),
        Ok(_) => panic!("incompatible request schema was accepted"),
    }
}

#[test]
fn open_rejects_incompatible_request_column_definitions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");

    drop(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap());
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "DROP TABLE __queqlite_requests;
             CREATE TABLE __queqlite_requests (
                 request_id BLOB,
                 original_log_index TEXT,
                 original_log_hash TEXT,
                 command_digest TEXT
             );",
        )
        .unwrap();

    assert!(matches!(
        SqliteStateMachine::open_existing(&path),
        Err(Error::Sqlite(message))
            if message
                == "incompatible __queqlite_requests schema; recreate this prototype database"
    ));
}

#[test]
fn open_migrates_the_v1_request_table_without_losing_decided_requests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let payload = b"put\trequest-1\talpha\tone";
    let original_hash = LogHash::digest(&[b"original"]);

    drop(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap());
    Connection::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "DROP TABLE __queqlite_requests;
             CREATE TABLE __queqlite_requests (
                 request_id TEXT PRIMARY KEY,
                 original_log_index INTEGER NOT NULL,
                 original_log_hash BLOB NOT NULL CHECK(length(original_log_hash) = 32),
                 command_digest BLOB NOT NULL CHECK(length(command_digest) = 32)
             );
             INSERT INTO __queqlite_requests VALUES (
                 'request-1', 7, x'{}', x'{}'
             );
             UPDATE __queqlite_meta SET value = x'31' WHERE key = 'schema_version';",
            original_hash.to_hex(),
            LogHash::digest(&[payload]).to_hex(),
        ))
        .unwrap();

    let migrated = SqliteStateMachine::open_existing(&path).unwrap();
    assert_eq!(
        migrated.check_request("request-1", payload).unwrap(),
        Some(RequestOutcome::new(7, original_hash))
    );

    let inspection = Connection::open(&path).unwrap();
    let columns = inspection
        .prepare("PRAGMA table_info(__queqlite_requests)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        columns,
        [
            "request_id",
            "original_log_index",
            "original_log_hash",
            "command_digest",
            "result_blob",
        ]
    );
    let schema_version: String = inspection
        .query_row(
            "SELECT CAST(value AS TEXT) FROM __queqlite_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(schema_version, "3");
}

#[test]
fn noop_only_advances_progress_and_unsupported_payloads_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = v1_entry(1, EntryType::Command, LogHash::ZERO, b"put\talpha\tone");
    let noop = v1_entry(2, EntryType::Noop, command.hash, b"");

    db.apply_entry(&command).unwrap();
    let progress = db.apply_entry(&noop).unwrap();

    assert_eq!(progress.applied_index(), 2);
    assert_eq!(progress.applied_hash(), noop.hash);
    assert_eq!(db.get_value("alpha").unwrap().as_deref(), Some("one"));

    let nonempty_noop = v1_entry(3, EntryType::Noop, noop.hash, b"payload");
    let config_change = v1_entry(3, EntryType::ConfigChange, noop.hash, b"");
    let raw_sql = v1_entry(
        3,
        EntryType::Command,
        noop.hash,
        b"DELETE FROM __queqlite_kv",
    );
    for invalid in [nonempty_noop, config_change, raw_sql] {
        assert!(db.apply_entry(&invalid).is_err());
        assert_eq!(db.applied_index_value().unwrap(), 2);
    }
}

#[test]
fn online_snapshot_is_valid_and_contains_matching_tip_and_snapshot_id() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("live.sqlite");
    let snapshot_path = dir.path().join("snapshot.sqlite");
    let db = SqliteStateMachine::open(&live, "cluster-a", "node-1", 1, 1).unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, b"put\talpha\tbravo");
    db.apply_entry(&first).unwrap();

    let snapshot = db.create_snapshot(1).unwrap();
    fs::write(&snapshot_path, snapshot.db_bytes()).unwrap();
    let snapshot_db = Connection::open(&snapshot_path).unwrap();
    let integrity: String = snapshot_db
        .query_row("PRAGMA integrity_check;", [], |row| row.get(0))
        .unwrap();
    let meta = |key: &str| -> Vec<u8> {
        snapshot_db
            .query_row(
                "SELECT value FROM __queqlite_meta WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .unwrap()
    };

    assert_eq!(integrity, "ok");
    assert_eq!(snapshot.manifest().snapshot_index(), 1);
    assert_eq!(snapshot.manifest().applied_hash(), first.hash);
    assert_eq!(snapshot.manifest().created_by(), "node-1");
    assert_eq!(
        snapshot.manifest().executor_fingerprint(),
        Some(sql_executor_fingerprint().unwrap())
    );
    assert_eq!(meta("node_id"), b"node-1");
    assert_eq!(meta("applied_index"), b"1");
    assert_eq!(meta("applied_hash"), first.hash.to_hex().as_bytes());
    assert_eq!(
        meta("snapshot_id"),
        snapshot.manifest().snapshot_id().as_bytes()
    );
}

#[test]
fn qsql_v2_rejects_a_mismatched_executor_fingerprint_before_apply() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "fingerprint-mismatch".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(value TEXT)".into(),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&payload[b"QSQL\0\x02".len()..]).unwrap();
    envelope["executor_fingerprint"] = serde_json::json!(vec![255_u8; 32]);
    let mut tampered = b"QSQL\0\x02".to_vec();
    tampered.extend_from_slice(&serde_json::to_vec(&envelope).unwrap());

    assert!(db
        .apply_entry(&v1_entry(1, EntryType::Command, LogHash::ZERO, &tampered))
        .is_err());
    assert_eq!(db.applied_index_value().unwrap(), 0);
}

#[test]
fn recovery_snapshot_binds_bytes_identity_generation_and_applied_tip() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("live.sqlite"), "cluster-a", "node-1", 7, 9)
        .unwrap();
    let applied = entry(
        "cluster-a",
        7,
        9,
        1,
        EntryType::Command,
        LogHash::ZERO,
        b"put\talpha\tbravo",
    );
    db.apply_entry(&applied).unwrap();

    let recovery = db.create_recovery_snapshot(11).unwrap();
    let anchor = recovery.anchor();

    assert_eq!(recovery.db_bytes(), recovery.snapshot().db_bytes());
    assert_eq!(anchor.cluster_id(), "cluster-a");
    assert_eq!(anchor.epoch(), 7);
    assert_eq!(anchor.config_id(), 9);
    assert_eq!(anchor.recovery_generation(), 11);
    assert_eq!(anchor.compacted().index(), 1);
    assert_eq!(anchor.compacted().hash(), applied.hash);
    assert_eq!(
        anchor.snapshot().snapshot_id(),
        recovery.snapshot().manifest().snapshot_id()
    );
    assert_eq!(
        anchor.snapshot().digest(),
        LogHash::digest(&[recovery.db_bytes()])
    );
    assert_eq!(
        anchor.snapshot().size_bytes(),
        recovery.db_bytes().len() as u64
    );
}

#[test]
fn recovery_snapshot_restore_rebinds_only_the_node_identity() {
    let dir = tempfile::tempdir().unwrap();
    let source = SqliteStateMachine::open(
        dir.path().join("source.sqlite"),
        "cluster-a",
        "node-1",
        7,
        9,
    )
    .unwrap();
    let applied = entry(
        "cluster-a",
        7,
        9,
        1,
        EntryType::Command,
        LogHash::ZERO,
        b"put\talpha\tbravo",
    );
    source.apply_entry(&applied).unwrap();
    let recovery = source.create_recovery_snapshot(11).unwrap();
    let target = dir.path().join("target.sqlite");

    restore_recovery_snapshot_file(&target, recovery.db_bytes(), recovery.anchor(), "node-2")
        .unwrap();

    let restored = SqliteStateMachine::open(&target, "cluster-a", "node-2", 7, 9).unwrap();
    assert_eq!(restored.applied_index_value().unwrap(), 1);
    assert_eq!(restored.applied_hash_value().unwrap(), applied.hash);
    assert_eq!(
        restored.get_value("alpha").unwrap().as_deref(),
        Some("bravo")
    );
}

#[test]
fn restore_rejects_corruption_and_manifest_mismatches_without_replacing_target() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.sqlite");
    let target = dir.path().join("target.sqlite");
    let source_db = SqliteStateMachine::open(&source, "cluster-a", "node-1", 1, 1).unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, b"put\talpha\tbravo");
    source_db.apply_entry(&first).unwrap();
    let valid = source_db.create_snapshot(1).unwrap();

    restore_snapshot_file(&target, &valid, "node-2").unwrap();
    let restored = SqliteStateMachine::open(&target, "cluster-a", "node-2", 1, 1).unwrap();
    assert_eq!(restored.applied_index_value().unwrap(), 1);
    assert_eq!(restored.applied_hash_value().unwrap(), first.hash);
    assert_eq!(
        restored.get_value("alpha").unwrap().as_deref(),
        Some("bravo")
    );
    drop(restored);
    assert!(SqliteStateMachine::open(&target, "cluster-a", "node-1", 1, 1).is_err());

    let assert_target_unchanged = || {
        let restored = SqliteStateMachine::open(&target, "cluster-a", "node-2", 1, 1).unwrap();
        assert_eq!(restored.applied_index_value().unwrap(), 1);
        assert_eq!(restored.applied_hash_value().unwrap(), first.hash);
        assert_eq!(
            restored.get_value("alpha").unwrap().as_deref(),
            Some("bravo")
        );
    };

    let corrupted = Snapshot::new(valid.manifest().clone(), vec![0; 256]);
    assert!(restore_snapshot_file(&target, &corrupted, "node-2").is_err());
    assert_target_unchanged();

    let invalid_manifests = [
        SnapshotManifest::new("cluster-b", 1, 1, 1, first.hash, 1, "node-1"),
        SnapshotManifest::new("cluster-a", 1, 2, 1, first.hash, 1, "node-1"),
        SnapshotManifest::new("cluster-a", 2, 1, 1, first.hash, 1, "node-1"),
        SnapshotManifest::new("cluster-a", 1, 1, 1, first.hash, 99, "node-1"),
        SnapshotManifest::new(
            "cluster-a",
            1,
            1,
            1,
            LogHash::digest(&[b"wrong"]),
            1,
            "node-1",
        ),
        SnapshotManifest::new("cluster-a", 1, 1, 2, first.hash, 1, "node-1"),
        SnapshotManifest::new("cluster-a", 1, 1, 1, first.hash, 1, "node-9"),
    ];
    for manifest in invalid_manifests {
        let invalid = Snapshot::new(manifest, valid.db_bytes().to_vec());
        assert!(restore_snapshot_file(&target, &invalid, "node-2").is_err());
        assert_target_unchanged();
    }
}

#[test]
fn config_changes_update_durable_state_without_running_sql() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let db =
        SqliteStateMachine::open_with_configuration(&path, "cluster-a", "node-1", 1, old.clone())
            .unwrap();
    let stop = config_entry(1, 1, LogHash::ZERO, ConfigChange::stop(1, old.digest()));
    db.apply_entry(&stop).unwrap();
    assert_eq!(
        db.configuration_state_value().unwrap(),
        ConfigurationState::stopped(1, old.digest(), LogAnchor::new(1, stop.hash))
    );
    drop(db);

    let db = SqliteStateMachine::open_existing(&path).unwrap();
    let activation = config_entry(
        2,
        2,
        stop.hash,
        ConfigChange::activation_barrier(2, LogHash::from_bytes([2; 32]), 1, stop.hash),
    );
    db.apply_entry(&activation).unwrap();
    assert_eq!(
        db.configuration_state_value().unwrap(),
        ConfigurationState::active(2, LogHash::from_bytes([2; 32]))
    );
    drop(db);

    let reopened = SqliteStateMachine::open_with_configuration(
        &path,
        "cluster-a",
        "node-1",
        1,
        ConfigurationState::active(2, LogHash::from_bytes([2; 32])),
    )
    .unwrap();
    assert_eq!(reopened.applied_hash_value().unwrap(), activation.hash);
}

#[test]
fn bound_stop_binding_survives_reopen_and_recovery_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let db = SqliteStateMachine::open_with_configuration(
        &source_path,
        "cluster-a",
        "node-1",
        1,
        old.clone(),
    )
    .unwrap();
    let stop = config_entry(
        1,
        1,
        LogHash::ZERO,
        ConfigChange::bound_stop(
            "cluster-a",
            1,
            old.digest(),
            2,
            vec!["node-1".into(), "node-2".into(), "node-3".into()],
        )
        .unwrap(),
    );
    let expected = old.validate_entry(&stop).unwrap();
    db.apply_entry(&stop).unwrap();
    assert_eq!(db.configuration_state_value().unwrap(), expected);

    let snapshot = db.create_snapshot(1).unwrap();
    let recovery = db.create_recovery_snapshot(1).unwrap();
    assert_eq!(snapshot.manifest().configuration_state(), &expected);
    assert_eq!(recovery.anchor().configuration_state(), &expected);
    drop(db);

    let reopened = SqliteStateMachine::open_existing(&source_path).unwrap();
    assert_eq!(reopened.configuration_state_value().unwrap(), expected);
    drop(reopened);

    let snapshot_target = dir.path().join("snapshot.sqlite");
    restore_snapshot_file(&snapshot_target, &snapshot, "node-2").unwrap();
    assert_eq!(
        SqliteStateMachine::open_existing(snapshot_target)
            .unwrap()
            .configuration_state_value()
            .unwrap(),
        expected
    );

    let recovery_target = dir.path().join("recovery.sqlite");
    restore_recovery_snapshot_file(
        &recovery_target,
        recovery.snapshot().db_bytes(),
        recovery.anchor(),
        "node-3",
    )
    .unwrap();
    assert_eq!(
        SqliteStateMachine::open_existing(recovery_target)
            .unwrap()
            .configuration_state_value()
            .unwrap(),
        expected
    );
}

#[test]
fn legacy_stopped_configuration_state_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    drop(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap());

    let mut legacy_stopped = vec![2];
    legacy_stopped.extend_from_slice(&1_u64.to_be_bytes());
    legacy_stopped.extend_from_slice(LogHash::ZERO.as_bytes());
    legacy_stopped.extend_from_slice(&1_u64.to_be_bytes());
    legacy_stopped.extend_from_slice(LogHash::ZERO.as_bytes());
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE __queqlite_meta SET value = ?1 WHERE key = 'configuration_state'",
        params![legacy_stopped],
    )
    .unwrap();
    conn.execute(
        "UPDATE __queqlite_meta SET value = x'32' WHERE key = 'schema_version'",
        [],
    )
    .unwrap();
    drop(conn);

    let error = match SqliteStateMachine::open_existing(&path) {
        Ok(_) => panic!("ambiguous legacy stopped state was accepted"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("ambiguous legacy stopped"));
}

#[test]
fn rejected_activation_leaves_configuration_and_tip_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let db = SqliteStateMachine::open_with_configuration(
        dir.path().join("state.sqlite"),
        "cluster-a",
        "node-1",
        1,
        old.clone(),
    )
    .unwrap();
    let stop = config_entry(1, 1, LogHash::ZERO, ConfigChange::stop(1, old.digest()));
    db.apply_entry(&stop).unwrap();
    let stopped = db.configuration_state_value().unwrap();
    let invalid = config_entry(
        2,
        2,
        stop.hash,
        ConfigChange::activation_barrier(2, LogHash::from_bytes([2; 32]), 1, LogHash::ZERO),
    );

    assert!(db.apply_entry(&invalid).is_err());
    assert_eq!(db.configuration_state_value().unwrap(), stopped);
    assert_eq!(db.applied_index_value().unwrap(), 1);
    assert_eq!(db.applied_hash_value().unwrap(), stop.hash);
}

#[test]
fn snapshots_at_stop_and_activation_restore_configuration_state() {
    let dir = tempfile::tempdir().unwrap();
    let old = ConfigurationState::active(1, LogHash::from_bytes([1; 32]));
    let source_path = dir.path().join("source.sqlite");
    let db = SqliteStateMachine::open_with_configuration(
        &source_path,
        "cluster-a",
        "node-1",
        1,
        old.clone(),
    )
    .unwrap();
    let stop = config_entry(1, 1, LogHash::ZERO, ConfigChange::stop(1, old.digest()));
    db.apply_entry(&stop).unwrap();
    let stop_snapshot = db.create_snapshot(1).unwrap();
    assert_eq!(
        stop_snapshot.manifest().configuration_state(),
        &ConfigurationState::stopped(1, old.digest(), LogAnchor::new(1, stop.hash))
    );

    let activation = config_entry(
        2,
        2,
        stop.hash,
        ConfigChange::activation_barrier(2, LogHash::from_bytes([2; 32]), 1, stop.hash),
    );
    db.apply_entry(&activation).unwrap();
    let activation_snapshot = db.create_snapshot(2).unwrap();

    for (name, snapshot, expected) in [
        (
            "stop.sqlite",
            &stop_snapshot,
            ConfigurationState::stopped(1, old.digest(), LogAnchor::new(1, stop.hash)),
        ),
        (
            "activation.sqlite",
            &activation_snapshot,
            ConfigurationState::active(2, LogHash::from_bytes([2; 32])),
        ),
    ] {
        let target = dir.path().join(name);
        restore_snapshot_file(&target, snapshot, "node-2").unwrap();
        let restored = SqliteStateMachine::open_existing(target).unwrap();
        assert_eq!(restored.configuration_state_value().unwrap(), expected);
    }
}

#[test]
fn scalar_config_database_migrates_to_active_zero_digest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    drop(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 7).unwrap());
    Connection::open(&path)
        .unwrap()
        .execute(
            "DELETE FROM __queqlite_meta WHERE key = 'configuration_state'",
            [],
        )
        .unwrap();

    let migrated = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 7).unwrap();
    assert_eq!(
        migrated.configuration_state_value().unwrap(),
        ConfigurationState::active(7, LogHash::ZERO)
    );
}

#[test]
fn legacy_active_configuration_state_migrates_without_changing_its_digest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    drop(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 7).unwrap());
    let digest = LogHash::from_bytes([7; 32]);
    let mut legacy_active = vec![1];
    legacy_active.extend_from_slice(&7_u64.to_be_bytes());
    legacy_active.extend_from_slice(digest.as_bytes());
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE __queqlite_meta SET value = ?1 WHERE key = 'configuration_state'",
        params![legacy_active],
    )
    .unwrap();
    conn.execute(
        "UPDATE __queqlite_meta SET value = x'32' WHERE key = 'schema_version'",
        [],
    )
    .unwrap();
    drop(conn);

    let migrated = SqliteStateMachine::open_existing(&path).unwrap();
    assert_eq!(
        migrated.configuration_state_value().unwrap(),
        ConfigurationState::active(7, digest)
    );
    drop(migrated);
    let version: String = Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT CAST(value AS TEXT) FROM __queqlite_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "3");
}

#[test]
fn sql_batch_applies_ddl_and_typed_dml_atomically_and_queries_typed_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "sql-1".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB)"
                    .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO users(id, name, score, data) VALUES (?1, ?2, ?3, ?4)".into(),
                parameters: vec![
                    SqlValue::Integer(7),
                    SqlValue::Text("Ada".into()),
                    SqlValue::Real(9.5),
                    SqlValue::Blob(vec![0, 1, 255]),
                ],
            },
        ],
    };
    let payload = encode_sql_command(&command).unwrap();
    let applied = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);

    db.apply_entry(&applied).unwrap();
    let result = db
        .query_sql(
            &SqlStatement {
                sql: "SELECT id, name, score, data, NULL AS absent FROM users WHERE id = ?1".into(),
                parameters: vec![SqlValue::Integer(7)],
            },
            10,
            1024,
        )
        .unwrap();

    assert_eq!(result.columns, ["id", "name", "score", "data", "absent"]);
    assert_eq!(
        result.rows,
        [vec![
            SqlValue::Integer(7),
            SqlValue::Text("Ada".into()),
            SqlValue::Real(9.5),
            SqlValue::Blob(vec![0, 1, 255]),
            SqlValue::Null,
        ]]
    );
}

#[test]
fn qsql_v2_persists_statement_results_and_returns_them_on_exact_retries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "result-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "result-1".into(),
        statements: vec![
            SqlStatement {
                sql: "INSERT INTO items(id, name) VALUES (1, 'one'), (2, 'two') RETURNING id, name"
                    .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "UPDATE items SET name = upper(name) WHERE id = 2".into(),
                parameters: vec![],
            },
        ],
    };
    let payload = encode_sql_command(&command).unwrap();
    assert!(payload.starts_with(b"QSQL\0\x02"));
    let effect_payload = prepared_effect(&db, &command, &payload, 1, setup.hash);
    assert!(effect_payload.starts_with(b"QEFX\0\x01"));
    let first = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);

    let first_outcome = db.apply_entry_with_result(&first).unwrap();
    assert_eq!(first_outcome.progress().applied_index(), 2);
    let first_result = first_outcome.sql_result().unwrap();
    assert_eq!(first_result.statement_results.len(), 2);
    assert_eq!(first_result.statement_results[0].rows_affected, 2);
    assert_eq!(
        first_result.statement_results[0]
            .returning
            .as_ref()
            .unwrap(),
        &queqlite_sqlite::SqlQueryResult {
            columns: vec!["id".into(), "name".into()],
            rows: vec![
                vec![SqlValue::Integer(1), SqlValue::Text("one".into())],
                vec![SqlValue::Integer(2), SqlValue::Text("two".into())],
            ],
        }
    );
    assert_eq!(first_result.statement_results[1].rows_affected, 1);
    assert_eq!(
        db.apply_entry_with_result(&first).unwrap().sql_result(),
        Some(first_result)
    );

    let result_blob: Vec<u8> = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT result_blob FROM __queqlite_requests WHERE request_id = 'result-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(result_blob.starts_with(b"QRES\0\x01"));

    let repeated = v1_entry(3, EntryType::Command, first.hash, &payload);
    let retry_outcome = db.apply_entry_with_result(&repeated).unwrap();
    assert_eq!(retry_outcome.progress().applied_index(), 3);
    assert_eq!(retry_outcome.sql_result(), Some(first_result));
    assert_eq!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT id, name FROM items ORDER BY id".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap()
        .rows,
        [
            vec![SqlValue::Integer(1), SqlValue::Text("one".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("TWO".into())],
        ]
    );

    drop(db);
    let reopened = SqliteStateMachine::open_existing(&path).unwrap();
    let repeated_after_reopen = v1_entry(4, EntryType::Command, repeated.hash, &payload);
    assert_eq!(
        reopened
            .apply_entry_with_result(&repeated_after_reopen)
            .unwrap()
            .sql_result(),
        Some(first_result)
    );
}

#[test]
fn direct_qsql_v2_returning_is_rejected_without_execution() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "direct-returning-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "direct-returning".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(value) VALUES ('rejected') RETURNING id".into(),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();

    assert!(matches!(
        db.apply_entry_with_result(&v1_entry(2, EntryType::Command, setup.hash, &payload)),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(db.applied_index_value().unwrap(), 1);
    assert_eq!(
        db.check_sql_request("direct-returning", &payload).unwrap(),
        None
    );
    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "SELECT id FROM items".into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn qsql_v2_decided_duplicate_fails_closed_when_the_digest_differs() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let first_payload = encode_sql_command(&SqlCommand {
        request_id: "same-id".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(value TEXT)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, &first_payload);
    db.apply_entry_with_result(&first).unwrap();

    let conflicting_payload = encode_sql_command(&SqlCommand {
        request_id: "same-id".into(),
        statements: vec![SqlStatement {
            sql: "DROP TABLE items".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let conflict = v1_entry(2, EntryType::Command, first.hash, &conflicting_payload);
    assert!(matches!(
        db.apply_entry_with_result(&conflict),
        Err(Error::RequestConflict(_))
    ));
    assert_eq!(db.applied_index_value().unwrap(), 1);
    assert_eq!(db.applied_hash_value().unwrap(), first.hash);
    assert_eq!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'items'"
                    .into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows,
        [vec![SqlValue::Text("items".into())]]
    );
}

#[test]
fn qsql_v2_trigger_environment_rejects_returning_effects_but_admits_statement_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "trigger-setup".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TABLE audit(item_id INTEGER NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TRIGGER items_audit AFTER INSERT ON items BEGIN INSERT INTO audit VALUES (new.id); END".into(),
                parameters: vec![],
            },
        ],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let returning = SqlCommand {
        request_id: "trigger-returning".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(value) VALUES (1) RETURNING id".into(),
            parameters: vec![],
        }],
    };
    let returning_payload = encode_sql_command(&returning).unwrap();
    assert!(matches!(
        db.prepare_sql_effect(&returning, &returning_payload, 1, setup.hash),
        Err(Error::InvalidCommand(_))
    ));

    let replay = SqlCommand {
        request_id: "trigger-replay".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(value) VALUES (2)".into(),
            parameters: vec![],
        }],
    };
    let replay_payload = encode_sql_command(&replay).unwrap();
    assert_eq!(
        db.prepare_sql_effect(&replay, &replay_payload, 1, setup.hash)
            .unwrap(),
        SqlEffectPreparation::StatementReplay
    );
    let replay_entry = v1_entry(2, EntryType::Command, setup.hash, &replay_payload);
    db.apply_entry(&replay_entry).unwrap();
    assert_eq!(db.applied_index_value().unwrap(), 2);
    assert_eq!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT item_id FROM audit".into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows,
        [vec![SqlValue::Integer(1)]]
    );
}

#[test]
fn qsql_v2_retry_fails_closed_when_the_stored_result_is_not_canonical() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let payload = encode_sql_command(&SqlCommand {
        request_id: "corrupt-result".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(value INTEGER)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);
    db.apply_entry_with_result(&first).unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE __queqlite_requests SET result_blob = x'00' WHERE request_id = 'corrupt-result'",
            [],
        )
        .unwrap();

    let retry = v1_entry(2, EntryType::Command, first.hash, &payload);
    assert!(matches!(
        db.apply_entry_with_result(&retry),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(db.applied_index_value().unwrap(), 1);
    assert_eq!(db.applied_hash_value().unwrap(), first.hash);
}

#[test]
fn qsql_v1_keeps_its_no_returning_no_result_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let returning = encode_sql_command_v1(&SqlCommand {
        request_id: "v1-returning".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE rejected(value INTEGER)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO rejected(value) VALUES (1) RETURNING value".into(),
                parameters: vec![],
            },
        ],
    })
    .unwrap();
    assert!(returning.starts_with(b"QSQL\0\x01"));
    assert!(db
        .apply_entry_with_result(&v1_entry(1, EntryType::Command, LogHash::ZERO, &returning,))
        .is_err());

    let accepted = encode_sql_command_v1(&SqlCommand {
        request_id: "v1-execute".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE accepted(value INTEGER)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, &accepted);
    assert_eq!(
        db.apply_entry_with_result(&first).unwrap().sql_result(),
        None
    );
    let retry = v1_entry(2, EntryType::Command, first.hash, &accepted);
    assert_eq!(
        db.apply_entry_with_result(&retry).unwrap().sql_result(),
        None
    );
}

#[test]
fn qsql_v2_returning_limit_rolls_back_the_whole_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "too-many-rows-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "too-many-rows".into(),
        statements: vec![SqlStatement {
            sql: format!(
                "WITH RECURSIVE seq(value) AS (VALUES(1) UNION ALL SELECT value + 1 FROM seq WHERE value < {}) INSERT INTO items(value) SELECT value FROM seq RETURNING value",
                MAX_RETURNING_ROWS + 1
            ),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();
    assert!(matches!(
        db.prepare_sql_effect(&command, &payload, 1, setup.hash),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(db.applied_index_value().unwrap(), 1);
    assert_eq!(db.check_request("too-many-rows", &payload).unwrap(), None);
    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "SELECT value FROM items".into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn qsql_v2_retry_result_property_holds_for_typed_values() {
    let cases = [
        SqlValue::Null,
        SqlValue::Integer(i64::MIN),
        SqlValue::Integer(0),
        SqlValue::Integer(i64::MAX),
        SqlValue::Real(-0.0),
        SqlValue::Real(1.25),
        SqlValue::Text(String::new()),
        SqlValue::Text("typed value".into()),
        SqlValue::Blob(vec![]),
        SqlValue::Blob(vec![0, 1, 127, 255]),
    ];

    for (case_index, value) in cases.into_iter().enumerate() {
        let dir = tempfile::tempdir().unwrap();
        let db =
            SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
                .unwrap();
        let setup_payload = encode_sql_command(&SqlCommand {
            request_id: format!("property-setup-{case_index}"),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE values_table(id INTEGER PRIMARY KEY, value)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
        let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
        db.apply_entry(&setup).unwrap();
        let command = SqlCommand {
            request_id: format!("property-{case_index}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO values_table(value) VALUES (?1) RETURNING value".into(),
                parameters: vec![value.clone()],
            }],
        };
        let payload = encode_sql_command(&command).unwrap();
        let effect_payload = prepared_effect(&db, &command, &payload, 1, setup.hash);
        let first = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);
        let original = db.apply_entry_with_result(&first).unwrap();
        let retry = v1_entry(3, EntryType::Command, first.hash, &payload);
        let replayed = db.apply_entry_with_result(&retry).unwrap();

        assert_eq!(original.sql_result(), replayed.sql_result());
        assert_eq!(
            replayed.sql_result().unwrap().statement_results[0]
                .returning
                .as_ref()
                .unwrap()
                .rows,
            [vec![value]]
        );
    }
}

fn prepared_effect(
    db: &SqliteStateMachine,
    command: &SqlCommand,
    request_payload: &[u8],
    base_index: u64,
    base_hash: LogHash,
) -> Vec<u8> {
    match db
        .prepare_sql_effect(command, request_payload, base_index, base_hash)
        .unwrap()
    {
        SqlEffectPreparation::Effect(payload) => payload,
        SqlEffectPreparation::StatementReplay => panic!("expected a session changeset effect"),
    }
}

#[test]
fn qsql_v2_effect_captures_implicit_max_rowid_identically() {
    let dir = tempfile::tempdir().unwrap();
    let source = SqliteStateMachine::open(
        dir.path().join("source.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let replica = SqliteStateMachine::open(
        dir.path().join("replica.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "rowid-setup".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: format!("INSERT INTO items(id, value) VALUES ({}, 'seed')", i64::MAX),
                parameters: vec![],
            },
        ],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    source.apply_entry(&setup).unwrap();
    replica.apply_entry(&setup).unwrap();

    let command = SqlCommand {
        request_id: "implicit-rowid".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(value) VALUES ('effect') RETURNING id, value".into(),
            parameters: vec![],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&source, &command, &request_payload, 1, setup.hash);
    assert_eq!(
        source
            .query_sql(
                &SqlStatement {
                    sql: "SELECT count(*) FROM items".into(),
                    parameters: vec![],
                },
                1,
                128,
            )
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(1)]],
        "effect generation must roll back"
    );

    let effect = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);
    let source_result = source
        .apply_entry_with_result(&effect)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let replica_result = replica
        .apply_entry_with_result(&effect)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();

    assert_eq!(source_result, replica_result);
    let generated_rowid = source_result.statement_results[0]
        .returning
        .as_ref()
        .unwrap()
        .rows[0][0]
        .clone();
    assert_ne!(generated_rowid, SqlValue::Integer(i64::MAX));
    for db in [&source, &replica] {
        assert_eq!(
            db.query_sql(
                &SqlStatement {
                    sql: "SELECT id FROM items WHERE value = 'effect'".into(),
                    parameters: vec![],
                },
                10,
                1024,
            )
            .unwrap()
            .rows,
            [vec![generated_rowid.clone()]]
        );
    }
}

#[test]
fn qsql_v2_effect_rejects_a_stale_materialized_base() {
    let dir = tempfile::tempdir().unwrap();
    let source = SqliteStateMachine::open(
        dir.path().join("source.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let replica = SqliteStateMachine::open(
        dir.path().join("replica.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "stale-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    source.apply_entry(&setup).unwrap();
    replica.apply_entry(&setup).unwrap();

    let command = SqlCommand {
        request_id: "stale-effect".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, value) VALUES (1, 'one') RETURNING id".into(),
            parameters: vec![],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&source, &command, &request_payload, 1, setup.hash);
    let winner = v1_entry(2, EntryType::Noop, setup.hash, b"");
    replica.apply_entry(&winner).unwrap();
    let stale = v1_entry(3, EntryType::Command, winner.hash, &effect_payload);

    assert!(matches!(
        replica.apply_entry_with_result(&stale),
        Err(Error::InvalidEntry(_))
    ));
    assert_eq!(replica.applied_index_value().unwrap(), 2);
    assert!(replica
        .query_sql(
            &SqlStatement {
                sql: "SELECT id FROM items".into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn qsql_v2_effect_rejects_a_mismatched_executor_fingerprint() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "effect-fingerprint-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "effect-fingerprint".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, value) VALUES (1, 'x') RETURNING id".into(),
            parameters: vec![],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&db, &command, &request_payload, 1, setup.hash);
    let mut envelope: serde_json::Value =
        serde_json::from_slice(&effect_payload[b"QEFX\0\x01".len()..]).unwrap();
    envelope["executor_fingerprint"] = serde_json::json!(vec![255_u8; 32]);
    let mut tampered = b"QEFX\0\x01".to_vec();
    tampered.extend_from_slice(&serde_json::to_vec(&envelope).unwrap());

    assert!(matches!(
        db.apply_entry_with_result(&v1_entry(2, EntryType::Command, setup.hash, &tampered,)),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(db.applied_index_value().unwrap(), 1);
    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "SELECT id FROM items".into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn qsql_v2_effect_aborts_the_whole_apply_on_changeset_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let source = SqliteStateMachine::open(
        dir.path().join("source.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let replica_path = dir.path().join("replica.sqlite");
    let replica = SqliteStateMachine::open(&replica_path, "cluster-a", "node-2", 1, 1).unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "conflict-setup".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO items(id, value) VALUES (1, 'base')".into(),
                parameters: vec![],
            },
        ],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    source.apply_entry(&setup).unwrap();
    replica.apply_entry(&setup).unwrap();

    let command = SqlCommand {
        request_id: "conflicting-effect".into(),
        statements: vec![SqlStatement {
            sql: "UPDATE items SET value = 'effect' WHERE id = 1 RETURNING value".into(),
            parameters: vec![],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&source, &command, &request_payload, 1, setup.hash);
    Connection::open(&replica_path)
        .unwrap()
        .execute("UPDATE items SET value = 'diverged' WHERE id = 1", [])
        .unwrap();
    let effect = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);

    assert!(matches!(
        replica.apply_entry_with_result(&effect),
        Err(Error::Sqlite(_))
    ));
    assert_eq!(replica.applied_index_value().unwrap(), 1);
    assert_eq!(
        replica
            .query_sql(
                &SqlStatement {
                    sql: "SELECT value FROM items WHERE id = 1".into(),
                    parameters: vec![],
                },
                1,
                128,
            )
            .unwrap()
            .rows,
        [vec![SqlValue::Text("diverged".into())]]
    );
    assert_eq!(
        replica
            .check_sql_request("conflicting-effect", &request_payload)
            .unwrap(),
        None
    );
}

#[test]
fn qsql_v2_effect_exact_retry_after_later_writes_returns_original_result() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "retry-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "effect-retry".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, value) VALUES (1, 'first') RETURNING id, value".into(),
            parameters: vec![],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&db, &command, &request_payload, 1, setup.hash);
    let effect = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);
    let original = db
        .apply_entry_with_result(&effect)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let later = v1_entry(3, EntryType::Command, effect.hash, b"put\tlater\tvalue");
    db.apply_entry(&later).unwrap();
    let retry = v1_entry(4, EntryType::Command, later.hash, &effect_payload);
    let retried = db.apply_entry_with_result(&retry).unwrap();

    assert_eq!(retried.progress().applied_index(), 4);
    assert_eq!(retried.sql_result(), Some(&original));
    assert_eq!(
        db.check_sql_request("effect-retry", &request_payload)
            .unwrap()
            .unwrap()
            .0,
        RequestOutcome::new(2, effect.hash)
    );
    assert_eq!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT id, value FROM items".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap()
        .rows,
        [vec![SqlValue::Integer(1), SqlValue::Text("first".into())]]
    );
}

#[test]
fn qsql_v2_effect_supports_composite_primary_keys_identically_on_source_and_replica() {
    let dir = tempfile::tempdir().unwrap();
    let source = SqliteStateMachine::open(
        dir.path().join("source.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let replica = SqliteStateMachine::open(
        dir.path().join("replica.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "composite-key-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(tenant_id INTEGER NOT NULL, item_id INTEGER NOT NULL, value TEXT NOT NULL, PRIMARY KEY (tenant_id, item_id))".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    source.apply_entry(&setup).unwrap();
    replica.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "composite-key-effect".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(tenant_id, item_id, value) VALUES (7, 9, 'effect') RETURNING tenant_id, item_id, value".into(),
            parameters: vec![],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&source, &command, &request_payload, 1, setup.hash);
    let effect = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);

    let source_result = source
        .apply_entry_with_result(&effect)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let replica_result = replica
        .apply_entry_with_result(&effect)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();

    assert_eq!(source_result, replica_result);
    for db in [&source, &replica] {
        assert_eq!(
            db.query_sql(
                &SqlStatement {
                    sql: "SELECT tenant_id, item_id, value FROM items".into(),
                    parameters: vec![],
                },
                1,
                128,
            )
            .unwrap()
            .rows,
            [vec![
                SqlValue::Integer(7),
                SqlValue::Integer(9),
                SqlValue::Text("effect".into()),
            ]]
        );
    }
}

#[test]
fn qsql_v2_effect_result_survives_snapshot_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&db_path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "snapshot-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let command = SqlCommand {
        request_id: "snapshot-effect".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, value) VALUES (?1, ?2) RETURNING id, value".into(),
            parameters: vec![SqlValue::Integer(9), SqlValue::Blob(vec![0, 1, 255])],
        }],
    };
    let request_payload = encode_sql_command(&command).unwrap();
    let effect_payload = prepared_effect(&db, &command, &request_payload, 1, setup.hash);
    let effect = v1_entry(2, EntryType::Command, setup.hash, &effect_payload);
    let original = db
        .apply_entry_with_result(&effect)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let snapshot = db.create_snapshot(2).unwrap();
    drop(db);

    let restored_path = dir.path().join("restored.sqlite");
    restore_snapshot_file(&restored_path, &snapshot, "node-2").unwrap();
    let restored = SqliteStateMachine::open_existing(restored_path).unwrap();
    let stored = restored
        .check_sql_request("snapshot-effect", &request_payload)
        .unwrap()
        .unwrap();

    assert_eq!(stored.0, RequestOutcome::new(2, effect.hash));
    assert_eq!(stored.1.as_ref(), Some(&original));
    assert_eq!(
        restored
            .query_sql(
                &SqlStatement {
                    sql: "SELECT id, value FROM items".into(),
                    parameters: vec![],
                },
                1,
                1024,
            )
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(9), SqlValue::Blob(vec![0, 1, 255])]]
    );
}

#[test]
fn qsql_v2_foreign_key_environment_rejects_returning_effects_but_admits_statement_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "foreign-key-setup".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE parents(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id))".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO parents(id) VALUES (1)".into(),
                parameters: vec![],
            },
        ],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();

    let returning = SqlCommand {
        request_id: "foreign-key-returning".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO parents(id) VALUES (2) RETURNING id".into(),
            parameters: vec![],
        }],
    };
    let returning_payload = encode_sql_command(&returning).unwrap();
    assert!(matches!(
        db.prepare_sql_effect(&returning, &returning_payload, 1, setup.hash),
        Err(Error::InvalidCommand(_))
    ));

    let replay = SqlCommand {
        request_id: "foreign-key-replay".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO children(id, parent_id) VALUES (1, 1)".into(),
            parameters: vec![],
        }],
    };
    let replay_payload = encode_sql_command(&replay).unwrap();
    assert_eq!(
        db.prepare_sql_effect(&replay, &replay_payload, 1, setup.hash)
            .unwrap(),
        SqlEffectPreparation::StatementReplay
    );
    db.apply_entry(&v1_entry(
        2,
        EntryType::Command,
        setup.hash,
        &replay_payload,
    ))
    .unwrap();
    assert_eq!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT parent_id FROM children".into(),
                parameters: vec![],
            },
            1,
            128,
        )
        .unwrap()
        .rows,
        [vec![SqlValue::Integer(1)]]
    );
}

#[test]
fn qsql_v2_returning_effect_gates_known_session_limitations() {
    let cases: &[(&str, &[&str], &str)] = &[
        (
            "nullable-pk",
            &["CREATE TABLE items(code TEXT PRIMARY KEY, value TEXT NOT NULL)"],
            "INSERT INTO items(code, value) VALUES ('a', 'x') RETURNING code",
        ),
        (
            "autoincrement",
            &["CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)"],
            "INSERT INTO items(value) VALUES ('x') RETURNING id",
        ),
        (
            "generated-column",
            &[
                "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL, normalized TEXT GENERATED ALWAYS AS (upper(value)) STORED)",
            ],
            "INSERT INTO items(id, value) VALUES (1, 'x') RETURNING normalized",
        ),
        (
            "trigger",
            &[
                "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                "CREATE TABLE audit(id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL)",
                "CREATE TRIGGER audit_item AFTER INSERT ON items BEGIN INSERT INTO audit(id, item_id) VALUES (NEW.id, NEW.id); END",
            ],
            "INSERT INTO items(id, value) VALUES (1, 'x') RETURNING id",
        ),
        (
            "foreign-key",
            &[
                "CREATE TABLE parents(id INTEGER PRIMARY KEY)",
                "CREATE TABLE children(id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES parents(id))",
            ],
            "INSERT INTO parents(id) VALUES (1) RETURNING id",
        ),
    ];

    for (name, schema, sql) in cases {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteStateMachine::open(
            dir.path().join(format!("{name}.sqlite")),
            "cluster-a",
            "node-1",
            1,
            1,
        )
        .unwrap();
        let setup_payload = encode_sql_command(&SqlCommand {
            request_id: format!("{name}-setup"),
            statements: schema
                .iter()
                .map(|sql| SqlStatement {
                    sql: (*sql).into(),
                    parameters: vec![],
                })
                .collect(),
        })
        .unwrap();
        let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
        db.apply_entry(&setup).unwrap();
        let command = SqlCommand {
            request_id: format!("{name}-effect"),
            statements: vec![SqlStatement {
                sql: (*sql).into(),
                parameters: vec![],
            }],
        };
        let request_payload = encode_sql_command(&command).unwrap();

        assert!(matches!(
            db.prepare_sql_effect(&command, &request_payload, 1, setup.hash),
            Err(Error::InvalidCommand(_))
        ));
    }
}

#[test]
fn qsql_v2_returning_effect_rejects_an_empty_changeset() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let setup_payload = encode_sql_command(&SqlCommand {
        request_id: "empty-effect-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    let setup = v1_entry(1, EntryType::Command, LogHash::ZERO, &setup_payload);
    db.apply_entry(&setup).unwrap();
    let returning = SqlCommand {
        request_id: "empty-returning-effect".into(),
        statements: vec![SqlStatement {
            sql: "UPDATE items SET value = 'x' WHERE id = 99 RETURNING id".into(),
            parameters: vec![],
        }],
    };
    let returning_payload = encode_sql_command(&returning).unwrap();
    assert!(matches!(
        db.prepare_sql_effect(&returning, &returning_payload, 1, setup.hash),
        Err(Error::InvalidCommand(_))
    ));

    let replay = SqlCommand {
        request_id: "empty-replay".into(),
        statements: vec![SqlStatement {
            sql: "UPDATE items SET value = 'x' WHERE id = 99".into(),
            parameters: vec![],
        }],
    };
    let replay_payload = encode_sql_command(&replay).unwrap();
    assert_eq!(
        db.prepare_sql_effect(&replay, &replay_payload, 1, setup.hash)
            .unwrap(),
        SqlEffectPreparation::StatementReplay
    );
}

#[test]
fn sql_request_replay_is_idempotent_and_conflicting_payload_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "sql-1".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(value TEXT)".into(),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();
    let first = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);
    let repeated = v1_entry(2, EntryType::Command, first.hash, &payload);
    db.apply_entry(&first).unwrap();
    db.apply_entry(&repeated).unwrap();

    assert_eq!(
        db.check_request("sql-1", &payload).unwrap(),
        Some(RequestOutcome::new(1, first.hash))
    );
    let conflict = encode_sql_command(&SqlCommand {
        request_id: "sql-1".into(),
        statements: vec![SqlStatement {
            sql: "DROP TABLE items".into(),
            parameters: vec![],
        }],
    })
    .unwrap();
    assert!(matches!(
        db.check_request("sql-1", &conflict),
        Err(Error::RequestConflict(_))
    ));
}

#[test]
fn failed_sql_batch_rolls_back_user_changes_request_record_and_applied_tip() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "sql-fail".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE unique_items(value TEXT UNIQUE)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO unique_items(value) VALUES ('x'), ('x')".into(),
                parameters: vec![],
            },
        ],
    };
    let payload = encode_sql_command(&command).unwrap();
    let entry = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);

    assert!(db.apply_entry(&entry).is_err());
    assert_eq!(db.applied_index_value().unwrap(), 0);
    assert_eq!(db.check_request("sql-fail", &payload).unwrap(), None);
    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "SELECT name FROM sqlite_master WHERE name = 'unique_items'".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn sql_boundary_rejects_nondeterminism_internal_state_and_wrong_statement_mode() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    for (index, statements) in [
        vec![
            "CREATE TABLE random_values(value INTEGER DEFAULT (random()))",
            "INSERT INTO random_values DEFAULT VALUES",
        ],
        vec![
            "CREATE TABLE version_values(value TEXT)",
            "INSERT INTO version_values(value) VALUES (sqlite_version())",
        ],
        vec!["DELETE FROM __queqlite_meta"],
        vec!["PRAGMA journal_mode = OFF"],
        vec!["ATTACH DATABASE ':memory:' AS other"],
        vec!["CREATE TEMP TABLE transient(value TEXT)"],
        vec!["BEGIN IMMEDIATE"],
        vec![
            "CREATE TABLE ordinary(value TEXT)",
            "ALTER TABLE ordinary RENAME TO __queqlite_hidden",
        ],
        vec![
            "CREATE TABLE parents(id INTEGER PRIMARY KEY)",
            "CREATE TABLE children(parent_id INTEGER REFERENCES parents(id))",
            "INSERT INTO children(parent_id) VALUES (99)",
        ],
    ]
    .into_iter()
    .enumerate()
    {
        let command = SqlCommand {
            request_id: format!("rejected-{index}"),
            statements: statements
                .iter()
                .map(|sql| SqlStatement {
                    sql: (*sql).into(),
                    parameters: vec![],
                })
                .collect(),
        };
        let payload = encode_sql_command(&command).unwrap();
        let entry = v1_entry(1, EntryType::Command, LogHash::ZERO, &payload);
        assert!(
            db.apply_entry(&entry).is_err(),
            "accepted {}",
            statements.join("; ")
        );
    }

    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "CREATE TABLE via_query(value TEXT)".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .is_err());
    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "SELECT random()".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .is_err());
}

#[test]
fn sql_query_enforces_row_byte_and_utf8_result_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    for (sql, max_rows, max_bytes) in [
        (
            "WITH rows(value) AS (VALUES (1), (2)) SELECT value FROM rows",
            1,
            1024,
        ),
        ("SELECT zeroblob(32)", 10, 16),
        ("SELECT CAST(x'80' AS TEXT)", 10, 1024),
    ] {
        assert!(
            db.query_sql(
                &SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                },
                max_rows,
                max_bytes,
            )
            .is_err(),
            "accepted unbounded or lossy query: {sql}"
        );
    }
}

#[test]
fn deterministic_sql_supports_indexes_triggers_generated_columns_upsert_ctes_and_joins() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "features".into(),
        statements: [
            "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, normalized TEXT GENERATED ALWAYS AS (lower(name)) STORED)",
            "CREATE INDEX users_normalized ON users(normalized)",
            "CREATE TABLE audit(user_id INTEGER, action TEXT, FOREIGN KEY(user_id) REFERENCES users(id))",
            "CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN INSERT INTO audit(user_id, action) VALUES (new.id, 'insert'); END",
            "INSERT INTO users(id, name) VALUES (1, 'ADA') ON CONFLICT(id) DO UPDATE SET name = excluded.name",
            "INSERT INTO users(id, name) VALUES (1, 'Ada') ON CONFLICT(id) DO UPDATE SET name = excluded.name",
        ]
        .into_iter()
        .map(|sql| SqlStatement {
            sql: sql.into(),
            parameters: vec![],
        })
        .collect(),
    };
    let payload = encode_sql_command(&command).unwrap();
    db.apply_entry(&v1_entry(1, EntryType::Command, LogHash::ZERO, &payload))
        .unwrap();

    let result = db
        .query_sql(
            &SqlStatement {
                sql: "WITH selected AS (SELECT id, normalized FROM users WHERE id = 1) SELECT selected.normalized, audit.action FROM selected JOIN audit ON audit.user_id = selected.id ORDER BY audit.rowid".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(
        result.rows,
        [vec![
            SqlValue::Text("ada".into()),
            SqlValue::Text("insert".into())
        ]]
    );
}

fn config_entry(index: u64, config_id: u64, prev_hash: LogHash, change: ConfigChange) -> LogEntry {
    let command = change.to_stored_command();
    entry(
        "cluster-a",
        1,
        config_id,
        index,
        command.entry_type,
        prev_hash,
        &command.payload,
    )
}
