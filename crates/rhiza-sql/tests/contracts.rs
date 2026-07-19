use rhiza_core::{EntryType, LogEntry, LogHash};
use rhiza_sql::{
    encode_sql_command, restore_snapshot_file, sql_executor_fingerprint, Error, SqlBatchMember,
    SqlCommand, SqlStatement, SqlValue, SqliteStateMachine,
};
use rusqlite::Connection;

fn apply_command(
    db: &SqliteStateMachine,
    command: &SqlCommand,
    index: u64,
    prev_hash: LogHash,
) -> LogHash {
    let request = encode_sql_command(command).unwrap();
    let preparation = db
        .prepare_sql_batch_effect(
            &[SqlBatchMember {
                command,
                request_payload: &request,
            }],
            index - 1,
            prev_hash,
        )
        .unwrap();
    preparation.results.into_iter().next().unwrap().unwrap();
    let effect = preparation.effect.unwrap();
    let hash = LogEntry::calculate_hash(
        "cluster-a",
        1,
        1,
        index,
        EntryType::Command,
        prev_hash,
        &effect,
    );
    db.apply_entry(&LogEntry {
        cluster_id: "cluster-a".into(),
        epoch: 1,
        config_id: 1,
        index,
        entry_type: EntryType::Command,
        payload: effect,
        prev_hash,
        hash,
    })
    .unwrap();
    hash
}

#[test]
fn qsql_v2_rejects_a_mismatched_executor_fingerprint_before_preparation() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let command = SqlCommand {
        request_id: "fingerprint".into(),
        statements: vec![SqlStatement {
            sql: "CREATE TABLE items(id INTEGER PRIMARY KEY)".into(),
            parameters: vec![],
        }],
    };
    let payload = encode_sql_command(&command).unwrap();
    let mut body: serde_json::Value =
        serde_json::from_slice(&payload[b"QSQL\0\x02".len()..]).unwrap();
    body["executor_fingerprint"] = serde_json::Value::String(LogHash::ZERO.to_hex());
    let mut tampered = b"QSQL\0\x02".to_vec();
    tampered.extend_from_slice(&serde_json::to_vec(&body).unwrap());

    assert_ne!(sql_executor_fingerprint().unwrap(), LogHash::ZERO);
    let preparation = db
        .prepare_sql_batch_effect(
            &[SqlBatchMember {
                command: &command,
                request_payload: &tampered,
            }],
            0,
            LogHash::ZERO,
        )
        .unwrap();
    assert!(preparation.effect.is_none());
    assert!(matches!(
        &preparation.results[0],
        Err(Error::InvalidCommand(_))
    ));
}

#[test]
fn sql_query_supports_sqlite_read_families_and_named_parameters() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();

    let values = db
        .query_sql(
            &SqlStatement {
                sql: "VALUES (1, 'one'), (2, 'two')".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(values.columns, ["column1", "column2"]);
    assert_eq!(
        values.rows,
        [
            vec![SqlValue::Integer(1), SqlValue::Text("one".into())],
            vec![SqlValue::Integer(2), SqlValue::Text("two".into())],
        ]
    );

    let explain = db
        .query_sql(
            &SqlStatement {
                sql: "EXPLAIN QUERY PLAN SELECT 1".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(explain.columns, ["id", "parent", "notused", "detail"]);
    assert_eq!(explain.rows.len(), 1);

    let recursive = db
        .query_sql(
            &SqlStatement {
                sql: "WITH RECURSIVE sequence(value) AS (VALUES (1) UNION ALL SELECT value + 1 FROM sequence WHERE value < 3) SELECT value FROM sequence".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(
        recursive.rows,
        [
            vec![SqlValue::Integer(1)],
            vec![SqlValue::Integer(2)],
            vec![SqlValue::Integer(3)],
        ]
    );

    let window = db
        .query_sql(
            &SqlStatement {
                sql: "WITH input(value) AS (VALUES (3), (1), (2)) SELECT value, sum(value) OVER (ORDER BY value) FROM input ORDER BY value".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(
        window.rows,
        [
            vec![SqlValue::Integer(1), SqlValue::Integer(1)],
            vec![SqlValue::Integer(2), SqlValue::Integer(3)],
            vec![SqlValue::Integer(3), SqlValue::Integer(6)],
        ]
    );

    let json = db
        .query_sql(
            &SqlStatement {
                sql: "SELECT json_extract('{\"name\":\"Ada\"}', '$.name'), (SELECT group_concat(value, ',') FROM json_each('[1,2,3]'))".into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(
        json.rows,
        [vec![
            SqlValue::Text("Ada".into()),
            SqlValue::Text("1,2,3".into()),
        ]]
    );

    let named = db
        .query_sql(
            &SqlStatement {
                sql: "SELECT :number, :text".into(),
                parameters: vec![SqlValue::Integer(42), SqlValue::Text("bound".into())],
            },
            10,
            1024,
        )
        .unwrap();
    assert_eq!(
        named.rows,
        [vec![SqlValue::Integer(42), SqlValue::Text("bound".into())]]
    );
}

#[test]
fn sql_query_allows_only_curated_observational_pragmas_without_state_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let db = SqliteStateMachine::open(&path, "cluster-a", "node-1", 1, 1).unwrap();
    apply_command(
        &db,
        &SqlCommand {
            request_id: "pragma-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE pragma_items(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES pragma_items(id), value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        },
        1,
        LogHash::ZERO,
    );

    for sql in [
        "PRAGMA foreign_key_check",
        "PRAGMA foreign_key_list(pragma_items)",
        "PRAGMA index_info(sqlite_autoindex_pragma_items_1)",
        "PRAGMA index_list(pragma_items)",
        "PRAGMA index_xinfo(sqlite_autoindex_pragma_items_1)",
        "PRAGMA integrity_check",
        "PRAGMA quick_check",
        "PRAGMA table_info(pragma_items)",
        "PrAgMa TaBlE_LiSt(pragma_items)",
        "PRAGMA table_xinfo(pragma_items)",
        "PRAGMA application_id",
        "PRAGMA collation_list",
        "PRAGMA compile_options",
        "PRAGMA data_version",
        "PRAGMA encoding",
        "PRAGMA freelist_count",
        "PRAGMA function_list",
        "PRAGMA module_list",
        "PRAGMA page_count",
        "PRAGMA pragma_list",
        "PRAGMA schema_version",
        "PRAGMA user_version",
    ] {
        assert!(
            db.query_sql(
                &SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                },
                1024,
                1024 * 1024,
            )
            .is_ok(),
            "rejected observational {sql}"
        );
    }

    let read_pragma = |sql: &str| {
        db.query_sql(
            &SqlStatement {
                sql: sql.into(),
                parameters: vec![],
            },
            10,
            1024,
        )
        .unwrap()
        .rows
    };
    let user_version = read_pragma("PRAGMA user_version");
    let journal_mode = Connection::open(&path)
        .unwrap()
        .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
        .unwrap();

    for sql in [
        "PRAGMA database_list",
        "PRAGMA cache_size",
        "PRAGMA table_info(__rhiza_kv)",
        "PRAGMA user_version = 7",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA journal_mode = OFF",
    ] {
        assert!(
            db.query_sql(
                &SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                },
                10,
                1024,
            )
            .is_err(),
            "accepted forbidden {sql}"
        );
    }

    assert_eq!(read_pragma("PRAGMA user_version"), user_version);
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
            .unwrap(),
        journal_mode
    );
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
fn reserved_trigger_names_and_targets_are_rejected_without_changing_the_base() {
    let dir = tempfile::tempdir().unwrap();
    let db = SqliteStateMachine::open(dir.path().join("state.sqlite"), "cluster-a", "node-1", 1, 1)
        .unwrap();
    let initial = db.canonical_db_digest().unwrap();

    for (request_id, sql) in [
        (
            "reserved-trigger-name",
            "CREATE TABLE items(id INTEGER); CREATE TRIGGER __rhiza_hidden AFTER INSERT ON items BEGIN SELECT 1; END",
        ),
        (
            "reserved-trigger-target",
            "CREATE TRIGGER user_trigger AFTER INSERT ON __rhiza_kv BEGIN SELECT 1; END",
        ),
    ] {
        let command = SqlCommand {
            request_id: request_id.into(),
            statements: sql
                .split("; ")
                .map(|sql| SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                })
                .collect(),
        };
        let request = encode_sql_command(&command).unwrap();
        let preparation = db
            .prepare_sql_batch_effect(
                &[SqlBatchMember {
                    command: &command,
                    request_payload: &request,
                }],
                0,
                LogHash::ZERO,
            )
            .unwrap();
        assert!(preparation.effect.is_none());
        assert!(preparation.results[0].is_err());
        assert_eq!(db.canonical_db_digest().unwrap(), initial);
    }
}

#[test]
fn restore_is_no_clobber_when_either_destination_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.sqlite");
    let source = SqliteStateMachine::open(&source_path, "cluster-a", "node-1", 1, 1).unwrap();
    let hash = apply_command(
        &source,
        &SqlCommand {
            request_id: "snapshot".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY)".into(),
                parameters: vec![],
            }],
        },
        1,
        LogHash::ZERO,
    );
    assert_eq!(source.applied_hash_value().unwrap(), hash);
    let snapshot = source.create_snapshot(1).unwrap();

    let target = dir.path().join("target.sqlite");
    std::fs::write(&target, b"do-not-clobber").unwrap();
    assert!(matches!(
        restore_snapshot_file(&target, &snapshot, "node-2"),
        Err(Error::InvalidSnapshot(_))
    ));
    assert_eq!(std::fs::read(&target).unwrap(), b"do-not-clobber");

    std::fs::remove_file(&target).unwrap();
    let control = target.with_extension("sqlite.control");
    std::fs::write(&control, b"do-not-clobber-control").unwrap();
    assert!(matches!(
        restore_snapshot_file(&target, &snapshot, "node-2"),
        Err(Error::InvalidSnapshot(_))
    ));
    assert_eq!(std::fs::read(&control).unwrap(), b"do-not-clobber-control");

    std::fs::remove_file(control).unwrap();
    let wal = target.with_file_name("target.sqlite-wal");
    std::fs::write(&wal, b"stale-wal").unwrap();
    assert!(matches!(
        restore_snapshot_file(&target, &snapshot, "node-2"),
        Err(Error::InvalidSnapshot(_))
    ));
    assert_eq!(std::fs::read(wal).unwrap(), b"stale-wal");
}
