use rhiza_core::{ConfigurationState, EntryType, LogAnchor, LogEntry, LogHash};
use rhiza_sql::{
    decode_qwal_v2, encode_put_request, encode_sql_command, restore_recovery_snapshot_file,
    restore_snapshot_file, ControlStore, Error, PendingApply, SqlBatchMember, SqlCommand,
    SqlStatement, SqlValue, SqliteStateMachine, MAX_SQL_EFFECT_BYTES, QWAL_V2_MAGIC,
};
use rusqlite::Connection;

fn entry(index: u64, prev_hash: LogHash, payload: &[u8]) -> LogEntry {
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Command,
        payload: payload.to_vec(),
        prev_hash,
        hash: LogEntry::calculate_hash(
            "cluster-a",
            index,
            1,
            1,
            EntryType::Command,
            prev_hash,
            payload,
        ),
    }
}

fn noop(index: u64, prev_hash: LogHash) -> LogEntry {
    LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Noop,
        payload: Vec::new(),
        prev_hash,
        hash: LogEntry::calculate_hash("cluster-a", index, 1, 1, EntryType::Noop, prev_hash, &[]),
    }
}

fn prepared_qwal(
    db: &SqliteStateMachine,
    command: &SqlCommand,
    base_index: u64,
    base_hash: LogHash,
) -> (Vec<u8>, Vec<u8>) {
    let request = encode_sql_command(command).unwrap();
    let preparation = db
        .prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: &request,
            }],
            base_index,
            base_hash,
        )
        .unwrap();
    preparation.results.into_iter().next().unwrap().unwrap();
    let effect = preparation.effect.unwrap();
    assert!(effect.starts_with(QWAL_V2_MAGIC));
    (request, effect)
}

fn query(db: &SqliteStateMachine, sql: &str) -> Vec<Vec<SqlValue>> {
    db.query_sql(
        &SqlStatement {
            sql: sql.into(),
            parameters: vec![],
        },
        100,
        64 * 1024,
    )
    .unwrap()
    .rows
}

#[test]
fn batch_preparation_commits_successes_once_and_isolates_failed_members() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let commands = [
        SqlCommand {
            request_id: "batch-create".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_items(value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        },
        SqlCommand {
            request_id: "batch-fail".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO absent_table VALUES ('must-rollback')".into(),
                parameters: vec![],
            }],
        },
        SqlCommand {
            request_id: "batch-insert".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_items VALUES ('kept') RETURNING value".into(),
                parameters: vec![],
            }],
        },
    ];
    let payloads = commands
        .iter()
        .map(|command| encode_sql_command(command).unwrap())
        .collect::<Vec<_>>();
    let members = commands
        .iter()
        .zip(&payloads)
        .map(|(command, request_payload)| SqlBatchMember {
            command,
            request_payload,
        })
        .collect::<Vec<_>>();

    let preparation = db
        .prepare_sql_batch_effect(&members, 0, LogHash::ZERO)
        .unwrap();
    assert_eq!(preparation.results.len(), 3);
    assert!(preparation.results[0].is_ok());
    assert!(preparation.results[1].is_err());
    assert!(preparation.results[2].is_ok());
    let payload = preparation.effect.unwrap();
    let effect = decode_qwal_v2(&payload).unwrap();
    assert_eq!(
        effect
            .receipts
            .iter()
            .map(|receipt| receipt.request_id.as_str())
            .collect::<Vec<_>>(),
        ["batch-create", "batch-insert"]
    );

    let entry = entry(1, LogHash::ZERO, &payload);
    db.apply_entry(&entry).unwrap();
    assert_eq!(
        query(&db, "SELECT value FROM batch_items"),
        [vec![SqlValue::Text("kept".into())]]
    );
    for (command, request) in [(&commands[0], &payloads[0]), (&commands[2], &payloads[2])] {
        let (outcome, result) = db
            .check_sql_request(&command.request_id, request)
            .unwrap()
            .unwrap();
        assert_eq!(outcome.original_log_index(), 1);
        assert_eq!(outcome.original_log_hash(), entry.hash);
        assert!(result.is_some());
    }
    assert!(db
        .check_sql_request(&commands[1].request_id, &payloads[1])
        .unwrap()
        .is_none());
}

#[test]
fn one_thousand_twenty_four_successes_share_one_entry_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup = SqlCommand {
        request_id: "batch-1024-setup".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE batch_1024(value INTEGER NOT NULL)".into(),
            parameters: vec![],
        }],
    };
    let (_, setup_effect) = prepared_qwal(&db, &setup, 0, LogHash::ZERO);
    let setup_entry = entry(1, LogHash::ZERO, &setup_effect);
    db.apply_entry(&setup_entry).unwrap();
    let commands = (0usize..1024)
        .map(|index| SqlCommand {
            request_id: format!("batch-1024-{index:04}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_1024(value) VALUES (?1)".into(),
                parameters: vec![SqlValue::Integer(index as i64)],
            }],
        })
        .collect::<Vec<_>>();
    let requests = commands
        .iter()
        .map(|command| encode_sql_command(command).unwrap())
        .collect::<Vec<_>>();
    let members = commands
        .iter()
        .zip(&requests)
        .map(|(command, request_payload)| SqlBatchMember {
            command,
            request_payload,
        })
        .collect::<Vec<_>>();

    let preparation = db
        .prepare_sql_batch_effect(&members, 1, setup_entry.hash)
        .unwrap();
    assert_eq!(preparation.results.len(), 1024);
    assert!(preparation.results.iter().all(Result::is_ok));
    let payload = preparation.effect.unwrap();
    assert!(payload.len() <= MAX_SQL_EFFECT_BYTES);
    assert_eq!(decode_qwal_v2(&payload).unwrap().receipts.len(), 1024);
    let decided = entry(2, setup_entry.hash, &payload);
    db.apply_entry(&decided).unwrap();
    drop(db);

    let reopened = SqliteStateMachine::open_existing(&path).unwrap();
    for index in [0, 1023] {
        let (outcome, _) = reopened
            .check_sql_request(&commands[index].request_id, &requests[index])
            .unwrap()
            .unwrap();
        assert_eq!(outcome.original_log_index(), 2);
        assert_eq!(outcome.original_log_hash(), decided.hash);
    }
    assert_eq!(
        query(&reopened, "SELECT count(*) FROM batch_1024"),
        [vec![SqlValue::Integer(1024)]]
    );
}

#[test]
fn all_failed_batch_produces_no_effect_and_leaves_the_database_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let commands = ["missing_a", "missing_b"].map(|table| SqlCommand {
        request_id: format!("fail-{table}"),
        statements: vec![SqlStatement {
            sql: format!("INSERT INTO {table} VALUES (1)"),
            parameters: vec![],
        }],
    });
    let payloads = commands
        .iter()
        .map(|command| encode_sql_command(command).unwrap())
        .collect::<Vec<_>>();
    let members = commands
        .iter()
        .zip(&payloads)
        .map(|(command, request_payload)| SqlBatchMember {
            command,
            request_payload,
        })
        .collect::<Vec<_>>();
    let before = db.canonical_db_digest().unwrap();

    let preparation = db
        .prepare_sql_batch_effect(&members, 0, LogHash::ZERO)
        .unwrap();

    assert!(preparation.effect.is_none());
    assert!(preparation.results.iter().all(Result::is_err));
    assert_eq!(db.canonical_db_digest().unwrap(), before);
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn batch_preparation_rejects_1025_members_before_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "same".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE never_applied(value INTEGER)".into(),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();
    let members = (0..1025)
        .map(|_| SqlBatchMember {
            command: &command,
            request_payload: &payload,
        })
        .collect::<Vec<_>>();

    assert!(matches!(
        db.prepare_sql_batch_effect(&members, 0, LogHash::ZERO),
        Err(Error::InvalidCommand(_))
    ));
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn put_effect_rejects_a_request_payload_that_does_not_match_its_arguments() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();

    assert!(matches!(
        db.prepare_put_effect(
            "request-1",
            "key-1",
            "value-1",
            b"put\trequest-1\tkey-1\tdifferent",
            0,
            LogHash::ZERO,
        ),
        Err(Error::InvalidCommand(message)) if message.contains("canonical")
    ));
    assert_eq!(db.applied_tip_value().unwrap(), (0, LogHash::ZERO));
}

#[test]
fn put_request_encoder_rejects_ambiguous_or_unidentified_requests() {
    assert_eq!(
        encode_put_request("request-1", "key-1", "").unwrap(),
        b"put\trequest-1\tkey-1\t"
    );
    assert!(encode_put_request("", "key-1", "value-1").is_err());
    assert!(encode_put_request("request-1", "", "value-1").is_err());
    assert!(encode_put_request("request\t1", "key-1", "value-1").is_err());
    assert!(encode_put_request(&"x".repeat(257), "key-1", "value-1").is_err());
}

#[test]
fn existing_qwal_pair_opens_when_supplied_configuration_is_ahead() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let initial = ConfigurationState::active(1, LogHash::digest(&[b"initial"]));
    let db = SqliteStateMachine::open_with_configuration(
        &path,
        "cluster-a",
        "node-1",
        1,
        initial.clone(),
    )
    .unwrap();
    drop(db);

    let reopened = SqliteStateMachine::open_with_configuration(
        &path,
        "cluster-a",
        "node-1",
        1,
        ConfigurationState::stopped(
            1,
            initial.digest(),
            LogAnchor::new(1, LogHash::digest(&[b"stop"])),
        ),
    )
    .unwrap();

    assert_eq!(reopened.configuration_state_value().unwrap(), initial);
}

#[test]
fn qwal_effect_reproduces_complete_sqlite_behavior_from_an_exact_base() {
    let dir = tempfile::tempdir().unwrap();
    let proposer = SqliteStateMachine::open(
        dir.path().join("proposer.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let follower = SqliteStateMachine::open(
        dir.path().join("follower.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();

    let base_digest = proposer.canonical_db_digest().unwrap();
    assert_eq!(follower.canonical_db_digest().unwrap(), base_digest);

    let command = SqlCommand {
        request_id: "whole-engine-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE parents(\
                          id INTEGER PRIMARY KEY AUTOINCREMENT,\
                          nonce BLOB NOT NULL,\
                          created_at TEXT NOT NULL\
                      )"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TABLE children(\
                          id INTEGER PRIMARY KEY,\
                          parent_id INTEGER NOT NULL \
                              REFERENCES parents(id) ON DELETE CASCADE\
                      )"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TABLE audit(\
                          parent_id INTEGER NOT NULL,\
                          nonce_hex TEXT NOT NULL,\
                          created_at TEXT NOT NULL\
                      )"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "CREATE TRIGGER parents_audit AFTER INSERT ON parents BEGIN \
                          INSERT INTO audit(parent_id, nonce_hex, created_at)\
                          VALUES (NEW.id, hex(NEW.nonce), NEW.created_at);\
                      END"
                .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO parents(nonce, created_at)\
                      VALUES (randomblob(16), strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))\
                      RETURNING id, hex(nonce), created_at"
                    .into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO children(id, parent_id) VALUES (10, last_insert_rowid())".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "DELETE FROM parents WHERE id = 1 RETURNING id".into(),
                parameters: vec![],
            },
        ],
    };

    let (_request, effect) = prepared_qwal(&proposer, &command, 0, LogHash::ZERO);
    assert_eq!(
        proposer.canonical_db_digest().unwrap(),
        base_digest,
        "speculative execution must not change the proposer canonical database"
    );

    let decided = entry(1, LogHash::ZERO, &effect);
    let proposer_result = proposer
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let follower_result = follower
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();

    assert_eq!(follower_result, proposer_result);
    assert_eq!(
        follower.canonical_db_digest().unwrap(),
        proposer.canonical_db_digest().unwrap(),
        "applying the page effect must reproduce the winning staging bytes"
    );
    let inserted = &proposer_result.statement_results[4]
        .returning
        .as_ref()
        .unwrap()
        .rows[0];
    assert_eq!(inserted[0], SqlValue::Integer(1));
    assert!(matches!(&inserted[1], SqlValue::Text(value) if value.len() == 32));
    assert!(matches!(&inserted[2], SqlValue::Text(value) if !value.is_empty()));
    assert_eq!(
        query(&proposer, "SELECT count(*) FROM children"),
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(&follower, "SELECT count(*) FROM children"),
        [[SqlValue::Integer(0)]]
    );
    assert_eq!(
        query(
            &proposer,
            "SELECT seq FROM sqlite_sequence WHERE name='parents'"
        ),
        [[SqlValue::Integer(1)]]
    );
    assert_eq!(
        query(
            &proposer,
            "SELECT parent_id, nonce_hex, created_at FROM audit"
        ),
        vec![inserted.clone()]
    );
    assert_eq!(
        query(
            &follower,
            "SELECT parent_id, nonce_hex, created_at FROM audit"
        ),
        vec![inserted.clone()]
    );
}

#[test]
fn qwal_apply_rejects_an_effect_prepared_from_a_stale_base() {
    let dir = tempfile::tempdir().unwrap();
    let proposer = SqliteStateMachine::open(
        dir.path().join("proposer.sqlite"),
        "cluster-a",
        "node-1",
        1,
        1,
    )
    .unwrap();
    let follower = SqliteStateMachine::open(
        dir.path().join("follower.sqlite"),
        "cluster-a",
        "node-2",
        1,
        1,
    )
    .unwrap();
    let command = SqlCommand {
        request_id: "stale-base".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE must_not_exist(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let (_, effect) = prepared_qwal(&proposer, &command, 0, LogHash::ZERO);

    let winner = noop(1, LogHash::ZERO);
    follower.apply_entry(&winner).unwrap();
    let before = follower.canonical_db_digest().unwrap();
    let stale = entry(2, winner.hash, &effect);

    assert!(matches!(
        follower.apply_entry_with_result(&stale),
        Err(Error::InvalidEntry(_))
    ));
    assert_eq!(follower.canonical_db_digest().unwrap(), before);
    assert_eq!(
        query(
            &follower,
            "SELECT count(*) FROM sqlite_schema WHERE name='must_not_exist'"
        ),
        [[SqlValue::Integer(0)]]
    );
}

#[test]
fn pure_noop_commit_survives_reopen_and_replay_but_rejects_a_different_hash() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let winner = noop(1, LogHash::ZERO);
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();

    assert_eq!(db.apply_entry(&winner).unwrap().applied_index(), 1);
    assert_eq!(
        ControlStore::open_existing_unchecked(path.with_extension("sqlite.control"))
            .unwrap()
            .pending()
            .unwrap(),
        None
    );
    drop(db);

    let reopened = SqliteStateMachine::open_existing(&path).unwrap();
    assert_eq!(reopened.apply_entry(&winner).unwrap().applied_index(), 1);
    let mut conflicting = winner.clone();
    conflicting.payload = b"different".to_vec();
    conflicting.hash = conflicting.recompute_hash();
    assert!(matches!(
        reopened.apply_entry(&conflicting),
        Err(Error::InvalidEntry(message)) if message.contains("different hash")
    ));
    assert_eq!(reopened.applied_tip_value().unwrap(), (1, winner.hash));
}

#[test]
fn qwal_prepare_rejects_an_effect_larger_than_the_inline_limit() {
    assert_eq!(MAX_SQL_EFFECT_BYTES, 512 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "oversized-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE blobs(value BLOB NOT NULL)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: format!(
                    "INSERT INTO blobs(value) VALUES (randomblob({}))",
                    MAX_SQL_EFFECT_BYTES + 64 * 1024
                ),
                parameters: vec![],
            },
        ],
    };
    let request = encode_sql_command(&command).unwrap();
    let before = db.canonical_db_digest().unwrap();

    assert!(matches!(
        db.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command: &command,
                request_payload: &request,
            }],
            0,
            LogHash::ZERO,
        ),
        Err(Error::ResourceExhausted(_))
    ));
    assert_eq!(db.canonical_db_digest().unwrap(), before);
}

#[test]
fn qwal_duplicate_apply_returns_the_original_result_without_reapplying_pages() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "duplicate-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO items VALUES (1, 'once') RETURNING id, value".into(),
                parameters: vec![],
            },
        ],
    };
    let (request, effect) = prepared_qwal(&db, &command, 0, LogHash::ZERO);
    let decided = entry(1, LogHash::ZERO, &effect);

    let first = db.apply_entry_with_result(&decided).unwrap();
    let digest = db.canonical_db_digest().unwrap();
    let duplicate = db.apply_entry_with_result(&decided).unwrap();

    assert_eq!(duplicate.sql_result(), first.sql_result());
    assert_eq!(db.canonical_db_digest().unwrap(), digest);
    assert_eq!(
        query(&db, "SELECT id, value FROM items"),
        [[SqlValue::Integer(1), SqlValue::Text("once".into())]]
    );
    assert_eq!(
        db.check_sql_request("duplicate-effect", &request)
            .unwrap()
            .unwrap()
            .1
            .as_ref(),
        first.sql_result()
    );
}

#[test]
fn qwal_snapshot_restore_rebinds_node_identity_without_changing_user_bytes_or_result() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let source = SqliteStateMachine::open(&source_path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = SqlCommand {
        request_id: "snapshot-effect".into(),
        statements: vec![
            SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT)".into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: "INSERT INTO items VALUES (7, 'preserved') RETURNING id, value".into(),
                parameters: vec![],
            },
        ],
    };
    let (request, effect) = prepared_qwal(&source, &command, 0, LogHash::ZERO);
    let decided = entry(1, LogHash::ZERO, &effect);
    let original = source
        .apply_entry_with_result(&decided)
        .unwrap()
        .sql_result()
        .cloned()
        .unwrap();
    let source_digest = source.canonical_db_digest().unwrap();
    let snapshot = source.create_snapshot(1).unwrap();
    drop(source);

    let restored_path = dir.path().join("restored.sqlite");
    restore_snapshot_file(&restored_path, &snapshot, "node-2").unwrap();
    let restored = SqliteStateMachine::open(&restored_path, "cluster-a", "node-2", 1, 1).unwrap();

    assert_eq!(restored.canonical_db_digest().unwrap(), source_digest);
    assert_eq!(restored.applied_index_value().unwrap(), 1);
    assert_eq!(
        query(&restored, "SELECT id, value FROM items"),
        [[SqlValue::Integer(7), SqlValue::Text("preserved".into())]]
    );
    assert_eq!(
        restored
            .check_sql_request("snapshot-effect", &request)
            .unwrap()
            .unwrap()
            .1,
        Some(original)
    );
}

#[test]
fn qwal_only_apply_rejects_legacy_qsql_and_qefx_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "legacy-qsql".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE forbidden_replay(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let qsql = encode_sql_command(&command).unwrap();
    assert!(qsql.starts_with(b"QSQL\0\x02"));
    let before = db.canonical_db_digest().unwrap();

    let legacy_payloads = [qsql, b"QEFX\0\x01{}".to_vec()];
    for payload in legacy_payloads {
        assert!(matches!(
            db.apply_entry(&entry(1, LogHash::ZERO, &payload)),
            Err(Error::InvalidCommand(_))
        ));
        assert_eq!(db.canonical_db_digest().unwrap(), before);
    }
}

#[test]
fn existing_legacy_database_requires_snapshot_bootstrap_instead_of_auto_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE __rhiza_meta(key TEXT PRIMARY KEY, value BLOB NOT NULL)")
        .unwrap();
    connection
        .execute(
            "INSERT INTO __rhiza_meta(key, value) VALUES ('node_id', 'legacy-node')",
            [],
        )
        .unwrap();
    drop(connection);

    assert!(SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).is_err());
}

#[test]
fn unresolved_pending_apply_blocks_reads_and_preparation_but_exact_replay_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = SqlCommand {
        request_id: "pending".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE pending_items(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let (request, effect_bytes) = prepared_qwal(&db, &command, 0, LogHash::ZERO);
    let effect = decode_qwal_v2(&effect_bytes).unwrap();
    let decided = entry(1, LogHash::ZERO, &effect_bytes);
    let pending = PendingApply::new(
        LogAnchor::new(0, LogHash::ZERO),
        LogAnchor::new(1, decided.hash),
        effect.base_db_digest,
        effect.target_db_digest,
        effect.target_file_bytes,
    );
    let control =
        ControlStore::open_existing_unchecked(path.with_extension("sqlite.control")).unwrap();
    control.begin_pending(&pending).unwrap();

    assert!(matches!(
        db.query_sql(
            &SqlStatement {
                sql: "SELECT 1".into(),
                parameters: vec![],
            },
            1,
            16,
        ),
        Err(Error::InvalidEntry(_))
    ));
    assert!(matches!(
        db.canonical_db_digest(),
        Err(Error::InvalidEntry(_))
    ));
    assert!(matches!(db.create_snapshot(0), Err(Error::InvalidEntry(_))));
    assert!(matches!(
        db.prepare_sql_batch_effect(
            &[SqlBatchMember {
                command: &command,
                request_payload: &request,
            }],
            0,
            LogHash::ZERO,
        ),
        Err(Error::InvalidEntry(_))
    ));

    db.apply_entry(&decided).unwrap();
    assert!(db
        .query_sql(
            &SqlStatement {
                sql: "SELECT name FROM sqlite_schema WHERE name = 'pending_items'".into(),
                parameters: vec![],
            },
            1,
            1024,
        )
        .is_ok());
}

#[test]
fn open_rejects_a_pending_intent_that_no_longer_extends_the_committed_tip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    let command = SqlCommand {
        request_id: "corrupt-pending".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE corrupt_pending(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let (_, effect_bytes) = prepared_qwal(&db, &command, 0, LogHash::ZERO);
    let effect = decode_qwal_v2(&effect_bytes).unwrap();
    let decided = entry(1, LogHash::ZERO, &effect_bytes);
    drop(db);

    let control_path = path.with_extension("sqlite.control");
    let control = ControlStore::open_existing_unchecked(&control_path).unwrap();
    control
        .begin_pending(&PendingApply::new(
            LogAnchor::new(0, LogHash::ZERO),
            LogAnchor::new(1, decided.hash),
            effect.base_db_digest,
            effect.target_db_digest,
            effect.target_file_bytes,
        ))
        .unwrap();
    drop(control);
    Connection::open(&control_path)
        .unwrap()
        .execute(
            "UPDATE pending_apply SET base_index = 1 WHERE singleton = 1",
            [],
        )
        .unwrap();

    assert!(matches!(
        SqliteStateMachine::open_existing(&path),
        Err(Error::InvalidEntry(_))
    ));
}

#[test]
fn recovery_restore_overrides_the_embedded_generation_for_the_next_effect() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let source = SqliteStateMachine::open(&source_path, "cluster-a", "node-1", 1, 1).unwrap();
    let setup = SqlCommand {
        request_id: "setup-recovery".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE recovered(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let (_, setup_effect) = prepared_qwal(&source, &setup, 0, LogHash::ZERO);
    let setup_entry = entry(1, LogHash::ZERO, &setup_effect);
    source.apply_entry(&setup_entry).unwrap();
    let recovery = source.create_recovery_snapshot(7).unwrap();

    let target_path = dir.path().join("target.sqlite");
    restore_recovery_snapshot_file(
        &target_path,
        recovery.db_bytes(),
        recovery.anchor(),
        "node-2",
    )
    .unwrap();
    let target = SqliteStateMachine::open_existing(&target_path).unwrap();
    let next = SqlCommand {
        request_id: "after-recovery".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO recovered(id) VALUES (1)".into(),
            parameters: vec![],
        }],
    };
    let (_, next_effect) = prepared_qwal(&target, &next, 1, setup_entry.hash);
    assert_eq!(decode_qwal_v2(&next_effect).unwrap().recovery_generation, 7);
}
