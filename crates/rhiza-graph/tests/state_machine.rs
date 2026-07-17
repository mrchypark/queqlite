use std::{
    collections::BTreeMap,
    fs,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Barrier,
    },
};

use rhiza_core::{EntryType, ExecutionProfile, LogEntry, LogHash, ReplicatedCommandEnvelope};
use rhiza_graph::{
    encode_replicated_graph_command, graph_materializer_fingerprint, restore_snapshot_file, Error,
    GraphCommandResultV1, GraphCommandV1, GraphParameterValue, GraphResultValue, GraphValueV1,
    LadybugStateMachine,
};

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

fn replicated(command: &GraphCommandV1) -> Vec<u8> {
    encode_replicated_graph_command(command).unwrap()
}

#[test]
fn apply_atomically_materializes_document_request_and_log_tip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("graph.lbug");
    let state = LadybugStateMachine::open(&path, "cluster-1", "node-1", 7, 3).unwrap();
    let command = GraphCommandV1::put_document(
        "request-1",
        "document-1",
        GraphValueV1::String("hello".into()),
    )
    .unwrap();
    let payload = replicated(&command);
    let entry = entry(1, LogHash::ZERO, payload.clone());

    let outcome = state.apply_entry(&entry).unwrap();

    assert_eq!(outcome.applied_index(), 1);
    assert_eq!(outcome.applied_hash(), entry.hash);
    assert_eq!(
        outcome.result(),
        Some(&GraphCommandResultV1::PutDocument { created: true })
    );
    assert_eq!(
        state.get_document("document-1").unwrap(),
        Some(GraphValueV1::String("hello".into()))
    );
    let request = state.check_request("request-1", &payload).unwrap().unwrap();
    assert_eq!(request.original_log_index(), 1);
    assert_eq!(request.original_log_hash(), entry.hash);
    assert_eq!(request.result(), outcome.result().unwrap());
}

#[test]
fn read_only_query_returns_parameterized_rows_and_the_same_atomic_tip() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command = GraphCommandV1::put_document(
        "request-1",
        "document-1",
        GraphValueV1::String("hello".into()),
    )
    .unwrap();
    let entry = entry(1, LogHash::ZERO, replicated(&command));
    state.apply_entry(&entry).unwrap();
    let parameters = BTreeMap::from([(
        "id".to_owned(),
        GraphParameterValue::String("document-1".into()),
    )]);

    let result = state
        .query_read_only(
            "MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.id, v.string_value",
            &parameters,
            10,
            4096,
            1_000,
        )
        .unwrap();

    assert_eq!(
        result.columns,
        vec![
            rhiza_graph::GraphColumn {
                name: "v.id".into(),
                logical_type: rhiza_graph::GraphLogicalType::String,
            },
            rhiza_graph::GraphColumn {
                name: "v.string_value".into(),
                logical_type: rhiza_graph::GraphLogicalType::String,
            },
        ]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            GraphResultValue::String("document-1".into()),
            GraphResultValue::String("hello".into()),
        ]]
    );
    assert_eq!(result.applied_index, 1);
    assert_eq!(result.hash, entry.hash);
}

#[test]
fn concurrent_document_reads_return_value_index_and_hash_from_one_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap(),
    );
    let mut entries = Vec::new();
    let mut expected_hashes = vec![LogHash::ZERO];
    let mut previous = LogHash::ZERO;
    for index in 1..=64 {
        let command = GraphCommandV1::put_document(
            format!("request-{index}"),
            "document-1",
            GraphValueV1::U64(index),
        )
        .unwrap();
        let next = entry(index, previous, replicated(&command));
        previous = next.hash;
        expected_hashes.push(next.hash);
        entries.push(next);
    }
    state.apply_entry(&entries[0]).unwrap();

    let expected_hashes = Arc::new(expected_hashes);
    let start = Arc::new(Barrier::new(5));
    let done = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        let writer_state = Arc::clone(&state);
        let writer_start = Arc::clone(&start);
        let writer_done = Arc::clone(&done);
        scope.spawn(move || {
            writer_start.wait();
            for entry in &entries[1..] {
                writer_state.apply_entry(entry).unwrap();
                std::thread::yield_now();
            }
            writer_done.store(true, Ordering::Release);
        });

        for _ in 0..4 {
            let reader_state = Arc::clone(&state);
            let reader_hashes = Arc::clone(&expected_hashes);
            let reader_start = Arc::clone(&start);
            let reader_done = Arc::clone(&done);
            let reader_observations = Arc::clone(&observations);
            scope.spawn(move || {
                let parameters = BTreeMap::from([(
                    "id".to_owned(),
                    GraphParameterValue::String("document-1".into()),
                )]);
                reader_start.wait();
                loop {
                    let (value, index, hash) =
                        reader_state.get_document_with_tip("document-1").unwrap();
                    assert_eq!(value, Some(GraphValueV1::U64(index)));
                    assert_eq!(hash, reader_hashes[index as usize]);
                    let query = reader_state
                        .query_read_only(
                            "MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.u64_value LIMIT 1",
                            &parameters,
                            1,
                            4096,
                            1_000,
                        )
                        .unwrap();
                    assert_eq!(
                        query.rows,
                        [vec![GraphResultValue::U64(query.applied_index)]]
                    );
                    assert_eq!(query.hash, reader_hashes[query.applied_index as usize]);
                    reader_observations.fetch_add(1, Ordering::Relaxed);
                    if reader_done.load(Ordering::Acquire) {
                        break;
                    }
                }
            });
        }
    });

    assert!(observations.load(Ordering::Relaxed) >= 4);
}

#[test]
fn snapshot_exclusively_drains_concurrent_readers_and_reopens_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap(),
    );
    let command = GraphCommandV1::put_document(
        "request-1",
        "document-1",
        GraphValueV1::String("value".into()),
    )
    .unwrap();
    let entry = entry(1, LogHash::ZERO, replicated(&command));
    state.apply_entry(&entry).unwrap();
    let entry_hash = entry.hash;
    let start = Arc::new(Barrier::new(9));

    std::thread::scope(|scope| {
        for _ in 0..8 {
            let reader_state = Arc::clone(&state);
            let reader_start = Arc::clone(&start);
            scope.spawn(move || {
                reader_start.wait();
                for _ in 0..32 {
                    assert_eq!(
                        reader_state.get_document_with_tip("document-1").unwrap(),
                        (Some(GraphValueV1::String("value".into())), 1, entry_hash,)
                    );
                }
            });
        }
        start.wait();
        let snapshot = state.create_snapshot(1).unwrap();
        assert_eq!(snapshot.applied_index(), 1);
        assert_eq!(snapshot.applied_hash(), entry_hash);
    });

    assert_eq!(
        state.get_document_with_tip("document-1").unwrap(),
        (Some(GraphValueV1::String("value".into())), 1, entry_hash,)
    );
}

#[test]
fn read_only_query_admission_rejects_mutation_external_io_and_multiple_statements() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let unsafe_queries = [
        "BEGIN TRANSACTION",
        "RETURN 1; RETURN 2",
        "MATCH (d:RhizaDocument) DELETE d RETURN d",
        "MATCH (d:RhizaDocument) CALL show_tables() RETURN d",
        "COPY RhizaDocument FROM 'input.csv'",
        "LOAD EXTENSION httpfs",
        "RETURN __RhizaMeta",
        "RETURN `__RhizaMeta`",
        r"RETURN `__Rhi\u007AaMeta`",
    ];

    for query in unsafe_queries {
        assert!(
            matches!(
                state.query_read_only(query, &BTreeMap::new(), 10, 4096, 1_000),
                Err(Error::InvalidCommand(_))
            ),
            "unsafe query unexpectedly passed admission: {query}"
        );
    }

    assert_eq!(state.applied_index().unwrap(), 0);
    assert_eq!(state.applied_hash().unwrap(), LogHash::ZERO);
    let external_path = dir.path().join("must-not-exist.csv");
    let external_query = format!(
        "RETURN 1 COPY RhizaDocument TO '{}'",
        external_path.display()
    );
    assert!(matches!(
        state.query_read_only(&external_query, &BTreeMap::new(), 10, 4096, 1_000),
        Err(Error::InvalidCommand(_))
    ));
    assert!(!external_path.exists());
}

#[test]
fn read_only_query_accepts_labeled_patterns_functions_and_clauses_supported_by_ladybug() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command = GraphCommandV1::put_document(
        "general-query",
        "document-1",
        GraphValueV1::String("value".into()),
    )
    .unwrap();
    state
        .apply_entry(&entry(1, LogHash::ZERO, replicated(&command)))
        .unwrap();

    let supported = [
        "MATCH (d:RhizaDocument) RETURN upper(d.string_value) AS value LIMIT 1",
        "MATCH (d:RhizaDocument) WITH d RETURN d.id AS id LIMIT 1",
        "UNWIND [2, 1] AS n RETURN n ORDER BY n LIMIT 2",
        "RETURN 1 AS value LIMIT 1",
    ];

    for query in supported {
        assert!(
            state
                .query_read_only(query, &BTreeMap::new(), 10, 4096, 1_000)
                .is_ok(),
            "Ladybug-supported read query was rejected: {query}"
        );
    }

    assert!(matches!(
        state.query_read_only(
            "MATCH (d:RhizaDocument) CALL { MATCH (x:RhizaDocument) RETURN x } RETURN d",
            &BTreeMap::new(),
            10,
            4096,
            1_000,
        ),
        Err(Error::InvalidCommand(_))
    ));
}

#[test]
fn read_only_query_allows_clause_words_as_parameter_names() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command = GraphCommandV1::put_document(
        "request-context",
        "document-context",
        GraphValueV1::String("context".into()),
    )
    .unwrap();
    let entry = entry(1, LogHash::ZERO, replicated(&command));
    state.apply_entry(&entry).unwrap();
    let parameters = BTreeMap::from([
        (
            "delete".to_owned(),
            GraphParameterValue::String("document-context".into()),
        ),
        (
            "payload".to_owned(),
            GraphParameterValue::String("context".into()),
        ),
        ("blob".to_owned(), GraphParameterValue::Bytes(vec![0, 1, 2])),
    ]);

    let result = state
        .query_read_only(
            "MATCH (v:RhizaDocument) WHERE v.id = $delete RETURN $delete, $payload, $blob LIMIT 1",
            &parameters,
            10,
            4096,
            1_000,
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
}

#[test]
fn read_only_query_allows_wide_and_function_projections_with_byte_and_row_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command =
        GraphCommandV1::put_document("wide-query", "document-1", GraphValueV1::I64(7)).unwrap();
    state
        .apply_entry(&entry(1, LogHash::ZERO, replicated(&command)))
        .unwrap();
    let projections = std::iter::repeat_n("v.id", 5)
        .collect::<Vec<_>>()
        .join(", ");
    let wide = state
        .query_read_only(
            &format!("MATCH (v:RhizaDocument) RETURN {projections}"),
            &BTreeMap::new(),
            10,
            4096,
            1_000,
        )
        .unwrap();
    assert_eq!(wide.rows[0].len(), 5);
    assert!(state
        .query_read_only(
            "MATCH (v:RhizaDocument) RETURN count(v)",
            &BTreeMap::new(),
            10,
            4096,
            1_000,
        )
        .is_ok());

    assert!(matches!(
        state.query_read_only(
            "MATCH (v:RhizaDocument) RETURN repeat(v.id, 1000)",
            &BTreeMap::new(),
            10,
            16,
            1_000,
        ),
        Err(Error::InvalidCommand(message)) if message.contains("bytes")
    ));
    assert!(matches!(
        state.query_read_only(
            "MATCH (v:RhizaDocument) RETURN v.id LIMIT 1 LIMIT 2",
            &BTreeMap::new(),
            10,
            4096,
            1_000,
        ),
        Err(Error::InvalidCommand(_))
    ));

    let oversized_literal = format!(
        "MATCH (v:RhizaDocument) RETURN '{}'",
        "x".repeat(rhiza_graph::MAX_GRAPH_QUERY_BYTES)
    );
    assert!(matches!(
        state.query_read_only(&oversized_literal, &BTreeMap::new(), 10, usize::MAX, 1_000,),
        Err(Error::InvalidCommand(_))
    ));
}

#[test]
fn read_only_query_supports_joins_sorts_operators_literals_and_whole_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let first =
        GraphCommandV1::put_document("general-1", "document-1", GraphValueV1::I64(1)).unwrap();
    let first_entry = entry(1, LogHash::ZERO, replicated(&first));
    state.apply_entry(&first_entry).unwrap();
    let second =
        GraphCommandV1::put_document("general-2", "document-2", GraphValueV1::I64(2)).unwrap();
    state
        .apply_entry(&entry(2, first_entry.hash, replicated(&second)))
        .unwrap();
    let supported = [
        "MATCH (v:RhizaDocument), (x:RhizaDocument) RETURN v.id, x.id ORDER BY v.id, x.id LIMIT 2",
        "MATCH (v:RhizaDocument) RETURN v LIMIT 1",
        "MATCH (v:RhizaDocument) RETURN DISTINCT v.id ORDER BY v.id LIMIT 2",
        "MATCH (v:RhizaDocument) RETURN v.id ORDER BY v.id SKIP 1 LIMIT 1",
        "MATCH (v:RhizaDocument) RETURN v.i64_value + 1 AS incremented LIMIT 2",
        "MATCH (v:RhizaDocument) RETURN [v.id] AS ids LIMIT 1",
        "OPTIONAL MATCH (v:RhizaDocument) RETURN v.id LIMIT 1",
        "MATCH (v:RhizaDocument) WHERE v.id = 'document-1' RETURN v.id AS id LIMIT 1",
        "MATCH (v:RhizaDocument) RETURN 'literal', true, null LIMIT 1",
    ];

    for query in supported {
        assert!(
            state
                .query_read_only(query, &BTreeMap::new(), 10, 4096, 1_000)
                .is_ok(),
            "Ladybug-supported query was rejected: {query}"
        );
    }

    let lexical_adversaries = [
        format!("MATCH (v:RhizaDocument) RETURN '{}'", "x".repeat(10_000)),
        "RETURN $1bad".into(),
        "RETURN $bad-name".into(),
    ];
    for query in lexical_adversaries {
        assert!(
            matches!(
                state.query_read_only(&query, &BTreeMap::new(), 10, 4096, 1_000),
                Err(Error::InvalidCommand(_))
            ),
            "invalid or oversized query unexpectedly succeeded: {query}"
        );
    }
}

#[test]
fn read_only_query_v1_allows_bounded_property_predicates_and_scalar_projections() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command = GraphCommandV1::put_document(
        "v1-safe",
        "document-safe",
        GraphValueV1::String("safe".into()),
    )
    .unwrap();
    let entry = entry(1, LogHash::ZERO, replicated(&command));
    state.apply_entry(&entry).unwrap();
    let parameters = BTreeMap::from([(
        "id".into(),
        GraphParameterValue::String("document-safe".into()),
    )]);

    let result = state
        .query_read_only(
            "MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.id, $id LIMIT 1",
            &parameters,
            10,
            4096,
            1_000,
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].len(), 2);
}

#[test]
fn read_only_query_validates_parameter_names_and_allows_typed_containers() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let id_query = "MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.id LIMIT 1";
    for (query, parameters) in [
        (id_query, BTreeMap::new()),
        (
            "MATCH (v:RhizaDocument) RETURN v.id LIMIT 1",
            BTreeMap::from([("extra".into(), GraphParameterValue::String("x".into()))]),
        ),
    ] {
        assert!(matches!(
            state.query_read_only(query, &parameters, 10, 4096, 1_000),
            Err(Error::InvalidCommand(_))
        ));
    }

    assert!(state
        .query_read_only(
            id_query,
            &BTreeMap::from([("id".into(), GraphParameterValue::I64(1))]),
            10,
            4096,
            1_000,
        )
        .is_ok());
    let list = BTreeMap::from([(
        "value".into(),
        GraphParameterValue::List(vec![GraphParameterValue::I64(1)]),
    )]);
    assert!(state
        .query_read_only(
            "UNWIND $value AS value RETURN value LIMIT 1",
            &list,
            10,
            4096,
            1_000
        )
        .is_ok());
    let structure = BTreeMap::from([(
        "value".into(),
        GraphParameterValue::Struct(BTreeMap::from([(
            "field".into(),
            GraphParameterValue::String("value".into()),
        )])),
    )]);
    assert!(state
        .query_read_only("RETURN $value LIMIT 1", &structure, 10, 4096, 1_000)
        .is_ok());
}

#[test]
fn read_only_query_lexer_ignores_keywords_inside_comments_and_strings() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command =
        GraphCommandV1::put_document("lexer-request", "lexer-document", GraphValueV1::I64(1))
            .unwrap();
    let entry = entry(1, LogHash::ZERO, replicated(&command));
    state.apply_entry(&entry).unwrap();

    let result = state
        .query_read_only(
            "/* CALL COPY __RhizaMeta 'BEGIN; DELETE' \"LOAD EXTENSION\" */ MATCH (v:RhizaDocument) RETURN v.id LIMIT 1 // COMMIT",
            &BTreeMap::new(),
            10,
            4096,
            1_000,
        )
        .unwrap();

    assert_eq!(result.rows.len(), 1);
}

#[test]
fn read_only_query_enforces_row_byte_parameter_and_timeout_limits() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let parameters = BTreeMap::from([(
        "value".to_owned(),
        GraphParameterValue::String("bounded".into()),
    )]);
    let first = GraphCommandV1::put_document("limit-1", "limit-1", GraphValueV1::I64(1)).unwrap();
    let first_entry = entry(1, LogHash::ZERO, replicated(&first));
    state.apply_entry(&first_entry).unwrap();
    let second = GraphCommandV1::put_document("limit-2", "limit-2", GraphValueV1::I64(2)).unwrap();
    let second_entry = entry(2, first_entry.hash, replicated(&second));
    state.apply_entry(&second_entry).unwrap();

    assert!(matches!(
        state.query_read_only(
            "MATCH (v:RhizaDocument) RETURN $value LIMIT 1",
            &parameters,
            1,
            1,
            1_000
        ),
        Err(Error::InvalidCommand(message)) if message.contains("bytes")
    ));
    assert!(matches!(
        state.query_read_only(
            "MATCH (v:RhizaDocument) RETURN v.id",
            &BTreeMap::new(),
            1,
            4096,
            1_000
        ),
        Err(Error::InvalidCommand(message)) if message.contains("rows")
    ));
    assert!(matches!(
        state.query_read_only("RETURN 1", &BTreeMap::new(), 1, 4096, 0),
        Err(Error::InvalidCommand(message)) if message.contains("timeout")
    ));
}

#[test]
fn document_projection_round_trips_every_supported_scalar_type() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let values = [
        GraphValueV1::Null,
        GraphValueV1::Bool(false),
        GraphValueV1::I64(i64::MIN),
        GraphValueV1::U64(u64::MAX),
        GraphValueV1::from_f64(-19.25).unwrap(),
        GraphValueV1::String("typed".into()),
        GraphValueV1::Bytes(vec![0, 1, 2, 255]),
    ];
    let mut previous = LogHash::ZERO;

    for (offset, value) in values.into_iter().enumerate() {
        let index = offset as u64 + 1;
        let id = format!("document-{index}");
        let command =
            GraphCommandV1::put_document(format!("request-{index}"), &id, value.clone()).unwrap();
        let entry = entry(index, previous, replicated(&command));
        state.apply_entry(&entry).unwrap();
        previous = entry.hash;

        assert_eq!(state.get_document(&id).unwrap(), Some(value));
    }
}

#[test]
fn duplicate_request_replays_result_and_conflicting_payload_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let first =
        GraphCommandV1::put_document("request-1", "document-1", GraphValueV1::I64(1)).unwrap();
    let first_payload = replicated(&first);
    let first_entry = entry(1, LogHash::ZERO, first_payload.clone());
    state.apply_entry(&first_entry).unwrap();

    let replay_entry = entry(2, first_entry.hash, first_payload);
    let replay = state.apply_entry(&replay_entry).unwrap();
    assert_eq!(
        replay.result(),
        Some(&GraphCommandResultV1::PutDocument { created: true })
    );
    assert_eq!(replay.applied_index(), 2);

    let conflict =
        GraphCommandV1::put_document("request-1", "document-1", GraphValueV1::I64(2)).unwrap();
    let conflict_entry = entry(3, replay_entry.hash, replicated(&conflict));
    assert!(matches!(
        state.apply_entry(&conflict_entry),
        Err(Error::RequestConflict { .. })
    ));
    assert_eq!(state.applied_index().unwrap(), 2);
    assert_eq!(
        state.get_document("document-1").unwrap(),
        Some(GraphValueV1::I64(1))
    );
}

#[test]
fn delete_reports_observable_existence_and_is_idempotent_by_request() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let put =
        GraphCommandV1::put_document("put-1", "document-1", GraphValueV1::Bool(true)).unwrap();
    let put_entry = entry(1, LogHash::ZERO, replicated(&put));
    state.apply_entry(&put_entry).unwrap();

    let delete = GraphCommandV1::delete_document("delete-1", "document-1").unwrap();
    let delete_entry = entry(2, put_entry.hash, replicated(&delete));
    let outcome = state.apply_entry(&delete_entry).unwrap();
    assert_eq!(
        outcome.result(),
        Some(&GraphCommandResultV1::DeleteDocument { existed: true })
    );
    assert_eq!(state.get_document("document-1").unwrap(), None);
}

#[test]
fn checkpoint_snapshot_rebinds_only_node_identity_and_restores_exact_state() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lbug");
    let restored = dir.path().join("restored.lbug");
    let state = LadybugStateMachine::open(&source, "cluster-1", "node-1", 7, 3).unwrap();
    let command =
        GraphCommandV1::put_document("request-1", "document-1", GraphValueV1::U64(9)).unwrap();
    let payload = replicated(&command);
    let entry = entry(1, LogHash::ZERO, payload.clone());
    state.apply_entry(&entry).unwrap();

    let snapshot = state.create_snapshot(1).unwrap();
    assert_eq!(snapshot.cluster_id(), "cluster-1");
    assert_eq!(snapshot.created_by(), "node-1");
    assert_eq!(snapshot.epoch(), 7);
    assert_eq!(snapshot.config_id(), 3);
    assert_eq!(snapshot.applied_index(), 1);
    assert_eq!(snapshot.applied_hash(), entry.hash);
    assert_eq!(
        snapshot.materializer_fingerprint(),
        graph_materializer_fingerprint()
    );
    assert_eq!(
        state.get_document("document-1").unwrap(),
        Some(GraphValueV1::U64(9))
    );
    restore_snapshot_file(&restored, &snapshot, "node-2").unwrap();
    let restored_state = LadybugStateMachine::open(&restored, "cluster-1", "node-2", 7, 3).unwrap();

    assert_eq!(restored_state.applied_index().unwrap(), 1);
    assert_eq!(
        restored_state.get_document("document-1").unwrap(),
        Some(GraphValueV1::U64(9))
    );
    assert!(restored_state
        .check_request("request-1", &payload)
        .unwrap()
        .is_some());
}

#[test]
fn snapshot_and_restore_reject_checkpoint_wal_sidecars() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.lbug");
    let state = LadybugStateMachine::open(&source, "cluster-1", "node-1", 7, 3).unwrap();
    let command =
        GraphCommandV1::put_document("request-1", "document-1", GraphValueV1::U64(9)).unwrap();
    let entry = entry(1, LogHash::ZERO, replicated(&command));
    state.apply_entry(&entry).unwrap();
    let snapshot = state.create_snapshot(1).unwrap();

    let source_checkpoint_wal = appended_suffix(&source, ".wal.checkpoint");
    fs::write(&source_checkpoint_wal, b"stale checkpoint WAL").unwrap();
    assert!(matches!(
        state.create_snapshot(1),
        Err(Error::InvalidSnapshot(message))
            if message.contains(source_checkpoint_wal.to_string_lossy().as_ref())
    ));
    fs::remove_file(source_checkpoint_wal).unwrap();

    for (name, suffix) in [
        ("wal", ".wal"),
        ("checkpoint-wal", ".wal.checkpoint"),
        ("shadow", ".shadow"),
        ("temporary", ".tmp"),
    ] {
        let restored = dir.path().join(format!("restored-{name}.lbug"));
        let sidecar = appended_suffix(&restored, suffix);
        fs::write(&sidecar, b"stale Ladybug sidecar").unwrap();
        assert!(matches!(
            restore_snapshot_file(&restored, &snapshot, "node-2"),
            Err(Error::InvalidSnapshot(message))
                if message.contains("sidecar")
        ));
        assert!(!restored.exists());
    }
}

#[test]
fn restore_preserves_an_existing_target() {
    let dir = tempfile::tempdir().unwrap();
    let source =
        LadybugStateMachine::open(dir.path().join("source.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let snapshot = source.create_snapshot(0).unwrap();
    let target = dir.path().join("existing.lbug");
    fs::write(&target, b"existing bytes").unwrap();

    assert!(matches!(
        restore_snapshot_file(&target, &snapshot, "node-2"),
        Err(Error::InvalidSnapshot(_))
    ));
    assert_eq!(fs::read(&target).unwrap(), b"existing bytes");
}

#[test]
fn command_entries_reject_invalid_common_envelopes_without_advancing() {
    let dir = tempfile::tempdir().unwrap();
    let state =
        LadybugStateMachine::open(dir.path().join("graph.lbug"), "cluster-1", "node-1", 7, 3)
            .unwrap();
    let command = GraphCommandV1::put_document(
        "request-1",
        "document-1",
        GraphValueV1::String("value".into()),
    )
    .unwrap();

    let raw_rhgc = command.encode();
    let sqlite_profile = ReplicatedCommandEnvelope::new(
        ExecutionProfile::Sqlite,
        1,
        command.request_id(),
        command.encode(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let unknown_version = ReplicatedCommandEnvelope::new(
        ExecutionProfile::Graph,
        2,
        command.request_id(),
        command.encode(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mismatched_request = ReplicatedCommandEnvelope::new(
        ExecutionProfile::Graph,
        1,
        "different-request",
        command.encode(),
    )
    .unwrap()
    .encode()
    .unwrap();
    let mut trailing = replicated(&command);
    trailing.push(0);

    for payload in [
        raw_rhgc,
        sqlite_profile,
        unknown_version,
        mismatched_request,
        trailing,
    ] {
        let rejected = entry(1, LogHash::ZERO, payload);
        assert!(matches!(
            state.apply_entry(&rejected),
            Err(Error::InvalidCommand(_))
        ));
        assert_eq!(state.applied_index().unwrap(), 0);
        assert_eq!(state.get_document("document-1").unwrap(), None);
    }
}

fn appended_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}
