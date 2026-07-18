#![cfg(feature = "graph")]

use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{Command, CommandKind, ExecutionProfile, LogAnchor, LogHash};
use rhiza_graph::{
    apply_lgfx_to_exact_base, encode_replicated_graph_command, ControlStore, LadybugFileEffectV1,
    LadybugStateMachine, PendingApply, LGFX_V1_MAGIC,
};
use rhiza_log::{FileLogStore, LogStore};
use rhiza_node::{
    node_router, node_router_with_checkpoint_and_limits, node_router_with_limits,
    CheckpointCoordinator, ClientErrorResponse, DurabilityMode, GraphCommandResultV1,
    GraphCommandV1, GraphGetDocumentResponse, GraphMutationResponse, GraphValueDto, GraphValueV1,
    InMemoryLogPeer, NodeConfig, NodeError, NodeRuntime, PeerConfig, ReadConsistency,
    GRAPH_GET_DOCUMENT_PATH, GRAPH_PUT_DOCUMENT_PATH, GRAPH_QUERY_PATH, MAX_GRAPH_MAX_ROWS,
    MAX_HTTP_BODY_BYTES, PROTOCOL_VERSION, READYZ_PATH, VERSION_HEADER,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{RecorderFileStore, ThreeNodeConsensus};

const CLUSTER_ID: &str = "rhiza:graph:cluster-a";

#[test]
fn graph_profile_reuses_node_runtime_commit_and_reopen_lifecycle() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let config = graph_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();

    let written = runtime
        .mutate_graph(
            GraphCommandV1::put_document(
                "request-1",
                "document-1",
                GraphValueV1::String("hello".into()),
            )
            .unwrap(),
        )
        .unwrap();
    let read = runtime
        .get_graph_document("document-1", ReadConsistency::Local)
        .unwrap();

    assert_eq!(written.applied_index(), 1);
    assert_eq!(
        written.result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );
    assert_eq!(read.value, Some(GraphValueV1::String("hello".into())));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index(), written.hash())
    );
    assert_eq!(runtime.config().cluster_id(), CLUSTER_ID);
    assert!(runtime
        .log_store()
        .read(1)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(LGFX_V1_MAGIC));
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[]).unwrap();
    assert_eq!(
        reopened
            .get_graph_document("document-1", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::String("hello".into()))
    );
}

#[test]
fn graph_read_barrier_returns_value_and_tip_from_one_materializer_boundary() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        graph_config(dir.path()),
        consensus(dir.path(), "recorders"),
        &[],
    )
    .unwrap();
    runtime
        .mutate_graph(
            GraphCommandV1::put_document("request-1", "document-1", GraphValueV1::U64(42)).unwrap(),
        )
        .unwrap();

    let read = runtime
        .get_graph_document("document-1", ReadConsistency::ReadBarrier)
        .unwrap();

    assert_eq!(read.value, Some(GraphValueV1::U64(42)));
    assert_eq!(read.applied_index, 2);
    assert_eq!(read.hash, runtime.applied_hash().unwrap());
}

#[test]
fn graph_configuration_state_and_recovery_generation_survive_restart() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let config = graph_config(dir.path())
        .with_recovery_generation(7)
        .unwrap();
    let runtime = NodeRuntime::open(
        config.clone(),
        consensus(dir.path(), "configuration-recorders"),
        &[],
    )
    .unwrap();
    runtime.stop_current_configuration().unwrap();
    let stopped = runtime.configuration_state().unwrap();
    assert!(!stopped.is_active());
    drop(runtime);

    let reopened = NodeRuntime::open(
        config.clone(),
        consensus(dir.path(), "configuration-recorders"),
        &[],
    )
    .unwrap();
    assert_eq!(reopened.configuration_state().unwrap(), stopped);
    drop(reopened);

    let wrong_generation = config.with_recovery_generation(8).unwrap();
    assert!(matches!(
        NodeRuntime::open(
            wrong_generation,
            consensus(dir.path(), "configuration-recorders"),
            &[]
        ),
        Err(NodeError::Storage(message)) if message.contains("recovery_generation")
    ));
}

#[test]
fn graph_rebuild_from_uncompacted_qlog_replays_configuration_history_from_genesis() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let config = graph_config(dir.path());
    let runtime = NodeRuntime::open(
        config.clone(),
        consensus(dir.path(), "rebuild-recorders"),
        &[],
    )
    .unwrap();
    runtime
        .mutate_graph(
            GraphCommandV1::put_document("before-stop", "document", GraphValueV1::U64(7)).unwrap(),
        )
        .unwrap();
    runtime.stop_current_configuration().unwrap();
    let expected_configuration = runtime.configuration_state().unwrap();
    let expected_tip = (
        runtime.applied_index().unwrap(),
        runtime.applied_hash().unwrap(),
    );
    drop(runtime);

    let graph_path = config.data_dir().join("ladybug/graph.lbug");
    std::fs::remove_file(&graph_path).unwrap();
    std::fs::remove_file(graph_path.with_extension("lbug.control")).unwrap();

    let rebuilt =
        NodeRuntime::open(config, consensus(dir.path(), "rebuild-recorders"), &[]).unwrap();

    assert_eq!(
        rebuilt.configuration_state().unwrap(),
        expected_configuration
    );
    assert_eq!(
        (
            rebuilt.applied_index().unwrap(),
            rebuilt.applied_hash().unwrap()
        ),
        expected_tip
    );
    assert_eq!(
        rebuilt
            .get_graph_document("document", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::U64(7))
    );
}

#[test]
fn graph_startup_recovers_pending_first_lgfx_from_base_database() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    assert_graph_startup_recovers_pending_first_lgfx(false);
}

#[test]
fn graph_startup_recovers_pending_first_lgfx_from_installed_target_database() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    assert_graph_startup_recovers_pending_first_lgfx(true);
}

#[test]
fn graph_runtime_regenerates_lgfx_after_a_foreign_slot_winner() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let shared_consensus = consensus(dir.path(), "lost-slot-recorders");
    let runtime =
        NodeRuntime::open(graph_config(dir.path()), Arc::clone(&shared_consensus), &[]).unwrap();
    let first = runtime
        .mutate_graph(
            GraphCommandV1::put_document("before-winner", "first", GraphValueV1::U64(1)).unwrap(),
        )
        .unwrap();
    let winner = shared_consensus
        .propose_at(
            2,
            first.hash(),
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();
    let command =
        GraphCommandV1::put_document("after-winner", "second", GraphValueV1::U64(2)).unwrap();

    let committed = runtime.mutate_graph(command.clone()).unwrap();
    let retried = runtime.mutate_graph(command).unwrap();

    assert_eq!(committed.applied_index(), 3);
    assert_eq!(retried, committed);
    assert_eq!(runtime.log_store().read(2).unwrap(), Some(winner));
    assert!(runtime
        .log_store()
        .read(3)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(LGFX_V1_MAGIC));
    assert_eq!(
        runtime
            .get_graph_document("second", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(GraphValueV1::U64(2))
    );
}

#[test]
fn graph_startup_rejects_semantic_rhgc_decision_before_mutating_qlog() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let config = graph_config(dir.path());
    let shared_consensus = consensus(dir.path(), "legacy-startup-recorders");
    let semantic = encode_replicated_graph_command(
        &GraphCommandV1::put_document("legacy", "document", GraphValueV1::U64(1)).unwrap(),
    )
    .unwrap();
    shared_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, semantic),
        )
        .unwrap();

    assert!(matches!(
        NodeRuntime::open(config.clone(), shared_consensus, &[]),
        Err(NodeError::Invariant(message)) if message.contains("LGFX")
    ));
    let log = FileLogStore::open_with_configuration(
        config.data_dir().join("consensus/log"),
        config.cluster_id(),
        config.epoch(),
        config.log_initial_configuration().clone(),
    )
    .unwrap();
    assert_eq!(log.last_index().unwrap(), None);
}

#[test]
fn graph_peer_recovery_rejects_semantic_rhgc_winner_before_mutating_qlog() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let config = graph_config(dir.path());
    let shared_consensus = consensus(dir.path(), "legacy-peer-recorders");
    let semantic = encode_replicated_graph_command(
        &GraphCommandV1::put_document("legacy", "document", GraphValueV1::U64(1)).unwrap(),
    )
    .unwrap();
    let decided = shared_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, semantic),
        )
        .unwrap();
    let peer = InMemoryLogPeer::new(vec![decided]);

    assert!(matches!(
        NodeRuntime::open(config.clone(), shared_consensus, &[&peer]),
        Err(NodeError::Invariant(message)) if message.contains("LGFX")
    ));
    let log = FileLogStore::open_with_configuration(
        config.data_dir().join("consensus/log"),
        config.cluster_id(),
        config.epoch(),
        config.log_initial_configuration().clone(),
    )
    .unwrap();
    assert_eq!(log.last_index().unwrap(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_graph_writes_commit_sequential_lgfx_and_retry_original_outcomes() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let dir = tempfile::tempdir().unwrap();
    let config = graph_http_config(dir.path())
        .with_writer_batching(8, Duration::from_millis(20))
        .unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(config, consensus(dir.path(), "batch-recorders"), &[]).unwrap());
    let (addr, server) = serve_graph(Arc::clone(&runtime), dir.path()).await;
    let client = reqwest::Client::new();
    let first_body = serde_json::json!({
        "request_id": "batch-a",
        "id": "shared",
        "value": {"type": "string", "value": "first"}
    });
    let second_body = serde_json::json!({
        "request_id": "batch-b",
        "id": "shared",
        "value": {"type": "string", "value": "second"}
    });

    let (first, second) = tokio::join!(
        post_graph_put(&client, addr, &first_body),
        post_graph_put(&client, addr, &second_body)
    );
    assert!(first.status().is_success());
    assert!(second.status().is_success());
    let first = first.json::<GraphMutationResponse>().await.unwrap();
    let second = second.json::<GraphMutationResponse>().await.unwrap();

    let mut applied = [first.applied_index, second.applied_index];
    applied.sort_unstable();
    assert_eq!(applied, [1, 2]);
    assert_ne!(first.result, second.result);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
    for index in 1..=2 {
        assert!(runtime
            .log_store()
            .read(index)
            .unwrap()
            .unwrap()
            .payload
            .starts_with(LGFX_V1_MAGIC));
    }

    let first_retry = post_graph_put(&client, addr, &first_body)
        .await
        .json::<GraphMutationResponse>()
        .await
        .unwrap();
    let second_retry = post_graph_put(&client, addr, &second_body)
        .await
        .json::<GraphMutationResponse>()
        .await
        .unwrap();
    assert_eq!(first_retry, first);
    assert_eq!(second_retry, second);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));

    let conflict = post_graph_put(
        &client,
        addr,
        &serde_json::json!({
            "request_id": "batch-a",
            "id": "shared",
            "value": {"type": "string", "value": "conflict"}
        }),
    )
    .await;
    assert_eq!(conflict.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        conflict.json::<ClientErrorResponse>().await.unwrap().code,
        "invalid_request"
    );
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
    server.abort();
}

#[test]
fn typed_graph_batch_preserves_order_as_individual_lgfx_entries() {
    let _test_guard = graph_test_lock().blocking_lock_owned();
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        graph_config(dir.path()),
        consensus(dir.path(), "typed-batch-recorders"),
        &[],
    )
    .unwrap();
    let value = "x".repeat(128);
    let commands = (0..4)
        .map(|index| {
            GraphCommandV1::put_document(
                format!("large-{index}"),
                format!("large-{index}"),
                GraphValueV1::String(value.clone()),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let outcomes = runtime.mutate_graph_batch(commands).unwrap();
    let indices = outcomes
        .iter()
        .map(|outcome| outcome.as_ref().unwrap().applied_index())
        .collect::<Vec<_>>();
    assert_eq!(indices, [1, 2, 3, 4]);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(4));
    for index in 1..=4 {
        assert!(runtime
            .log_store()
            .read(index)
            .unwrap()
            .unwrap()
            .payload
            .starts_with(LGFX_V1_MAGIC));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_http_routes_enforce_auth_body_limits_and_return_atomic_value_with_tip() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder =
        RecorderFileStore::new_with_id(dir.path().join("http-recorder"), "n1", CLUSTER_ID, 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let put_url = format!("http://{addr}{GRAPH_PUT_DOCUMENT_PATH}");
    let query_url = format!("http://{addr}{GRAPH_QUERY_PATH}");

    let unauthorized = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .json(&serde_json::json!({
            "request_id": "request-1",
            "id": "document-1",
            "value": {"type": "string", "value": "hello"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let unauthorized_query = client
        .post(&query_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .json(&serde_json::json!({
            "statement": {"cypher": "RETURN 1", "parameters": {}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        unauthorized_query.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let too_large = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("x".repeat(MAX_HTTP_BODY_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(too_large.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);

    let too_large_query = client
        .post(&query_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("x".repeat(MAX_HTTP_BODY_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_eq!(
        too_large_query.status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );

    let malformed_query = client
        .post(&query_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_client_error(
        malformed_query,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_json",
        false,
    )
    .await;

    let put = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "request-1",
            "id": "document-1",
            "value": {"type": "string", "value": "hello"}
        }))
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success());
    let put = put.json::<GraphMutationResponse>().await.unwrap();
    assert_eq!(put.applied_index, 1);

    let get = client
        .post(format!("http://{addr}{GRAPH_GET_DOCUMENT_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "id": "document-1",
            "consistency": "read_barrier"
        }))
        .send()
        .await
        .unwrap();
    assert!(get.status().is_success());
    let get = get.json::<GraphGetDocumentResponse>().await.unwrap();
    assert_eq!(get.value, Some(GraphValueDto::String("hello".into())));
    assert_eq!(get.applied_index, 2);
    assert_ne!(get.hash, put.hash);

    let sql_route = client
        .post(format!("http://{addr}/v1/sql/query"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(sql_route.status(), reqwest::StatusCode::NOT_FOUND);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_query_returns_general_read_only_cypher_for_all_consistency_modes() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    runtime
        .mutate_graph(
            GraphCommandV1::put_document(
                "query-seed",
                "document-1",
                GraphValueV1::String("match-target".into()),
            )
            .unwrap(),
        )
        .unwrap();
    let (addr, server) = serve_graph(runtime, dir.path()).await;
    let client = reqwest::Client::new();
    let statement = serde_json::json!({
        "cypher": "MATCH (v:RhizaDocument) WHERE v.id IN $ids RETURN v.id AS id, upper(v.string_value) AS value ORDER BY v.id",
        "parameters": {
            "ids": {"type": "list", "value": [
                {"type": "string", "value": "document-1"}
            ]}
        }
    });

    let local = post_graph_query(
        &client,
        addr,
        &serde_json::json!({"statement": statement, "consistency": "local"}),
    )
    .await;
    let local_status = local.status();
    let local_body = local.text().await.unwrap();
    assert!(
        local_status.is_success(),
        "graph query failed: {local_body}"
    );
    let local = serde_json::from_str::<serde_json::Value>(&local_body).unwrap();
    assert_eq!(
        local["columns"],
        serde_json::json!([
            {"name":"id","logical_type":{"type":"string"}},
            {"name":"value","logical_type":{"type":"string"}}
        ])
    );
    assert_eq!(
        local["rows"][0][0],
        serde_json::json!({"type":"string","value":"document-1"})
    );
    assert_eq!(
        local["rows"][0][1],
        serde_json::json!({"type":"string","value":"MATCH-TARGET"})
    );
    assert_eq!(local["applied_index"], 1);

    let applied = post_graph_query(
        &client,
        addr,
        &serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                "parameters": {}
            },
            "consistency": {"applied_index": 1}
        }),
    )
    .await;
    assert!(applied.status().is_success());
    assert_eq!(
        applied.json::<serde_json::Value>().await.unwrap()["applied_index"],
        1
    );

    let future = post_graph_query(
        &client,
        addr,
        &serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                "parameters": {}
            },
            "consistency": {"applied_index": 2}
        }),
    )
    .await;
    assert_client_error(
        future,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        true,
    )
    .await;

    let barrier = post_graph_query(
        &client,
        addr,
        &serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                "parameters": {}
            },
            "consistency": "read_barrier"
        }),
    )
    .await;
    assert!(barrier.status().is_success());
    assert_eq!(
        barrier.json::<serde_json::Value>().await.unwrap()["applied_index"],
        2
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_query_rejects_unsafe_statements_without_mutating_graph_state() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    runtime
        .mutate_graph(
            GraphCommandV1::put_document(
                "query-safety-seed",
                "document-1",
                GraphValueV1::String("unchanged".into()),
            )
            .unwrap(),
        )
        .unwrap();
    let original_tip = (
        runtime.applied_index().unwrap(),
        runtime.applied_hash().unwrap(),
    );
    let (addr, server) = serve_graph(runtime.clone(), dir.path()).await;
    let client = reqwest::Client::new();

    for cypher in [
        "CREATE (:Person {name: 'Ada'})",
        "MATCH (v:RhizaDocument) SET v.string_value = 'changed' RETURN v.id",
        "MATCH (v:RhizaDocument) DELETE v",
        "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))",
        "DROP TABLE RhizaDocument",
        "BEGIN TRANSACTION",
        "CHECKPOINT",
        "CALL show_tables() RETURN *",
        "COPY (MATCH (v:RhizaDocument) RETURN v.id) TO 'graph.csv'",
        "ATTACH 'other.lbug' AS other",
        "IMPORT DATABASE 'other'",
        "MATCH (m:__RhizaMeta) RETURN m",
        "RETURN 1; RETURN 2",
    ] {
        let rejected = post_graph_query(
            &client,
            addr,
            &serde_json::json!({
                "statement": {"cypher": cypher, "parameters": {}},
                "consistency": "local"
            }),
        )
        .await;
        assert_client_error(
            rejected,
            reqwest::StatusCode::BAD_REQUEST,
            "invalid_request",
            false,
        )
        .await;
        assert_eq!(
            (
                runtime.applied_index().unwrap(),
                runtime.applied_hash().unwrap()
            ),
            original_tip,
            "unsafe graph query changed the materialized tip: {cypher}"
        );
        assert_eq!(
            runtime
                .get_graph_document("document-1", ReadConsistency::Local)
                .unwrap()
                .value,
            Some(GraphValueV1::String("unchanged".into()))
        );
        assert!(runtime.is_ready(), "client query latched runtime: {cypher}");
    }

    let huge_literal = format!("MATCH (v:RhizaDocument) RETURN '{}'", "x".repeat(64 * 1024));
    let rejected = post_graph_query(
        &client,
        addr,
        &serde_json::json!({
            "statement": {"cypher": huge_literal, "parameters": {}},
            "consistency": "local"
        }),
    )
    .await;
    assert_client_error(
        rejected,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
    )
    .await;
    assert_eq!(
        (
            runtime.applied_index().unwrap(),
            runtime.applied_hash().unwrap()
        ),
        original_tip,
        "huge graph query literal changed the materialized tip"
    );
    assert!(
        runtime.is_ready(),
        "huge graph query literal latched runtime"
    );

    for body in [
        serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) RETURN $value",
                "parameters": {}
            },
            "consistency": "local"
        }),
        serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                "parameters": {"extra": {"type": "string", "value": "unused"}}
            },
            "consistency": "local"
        }),
        serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) WHERE v.id = $id RETURN v.id",
                "parameters": {"id": {"type": "u64", "value": 1}}
            },
            "consistency": "local"
        }),
        serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) WHERE v.id = $bad-name RETURN v.id",
                "parameters": {"bad-name": {"type": "string", "value": "document-1"}}
            },
            "consistency": "local"
        }),
    ] {
        let rejected = post_graph_query(&client, addr, &body).await;
        assert_client_error(
            rejected,
            reqwest::StatusCode::BAD_REQUEST,
            "invalid_request",
            false,
        )
        .await;
        assert_eq!(
            (
                runtime.applied_index().unwrap(),
                runtime.applied_hash().unwrap()
            ),
            original_tip,
            "invalid graph query parameters changed the materialized tip"
        );
        assert!(
            runtime.is_ready(),
            "invalid graph query parameters latched runtime"
        );
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_cypher_is_rejected_without_changing_tip_or_readiness() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    runtime
        .mutate_graph(
            GraphCommandV1::put_document(
                "malformed-query-seed",
                "document-1",
                GraphValueV1::String("unchanged".into()),
            )
            .unwrap(),
        )
        .unwrap();
    let original_tip = (
        runtime.applied_index().unwrap(),
        runtime.applied_hash().unwrap(),
    );
    let (addr, server) = serve_graph(runtime.clone(), dir.path()).await;
    let client = reqwest::Client::new();

    let rejected = post_graph_query(
        &client,
        addr,
        &serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument RETURN v",
                "parameters": {}
            },
            "consistency": "local"
        }),
    )
    .await;
    assert_client_error(
        rejected,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
    )
    .await;
    assert_eq!(
        (
            runtime.applied_index().unwrap(),
            runtime.applied_hash().unwrap()
        ),
        original_tip
    );
    assert!(runtime.is_ready());
    assert!(client
        .get(format!("http://{addr}{READYZ_PATH}"))
        .send()
        .await
        .unwrap()
        .status()
        .is_success());
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_query_returns_explicit_errors_for_capacity_rows_and_invalid_values() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder =
        RecorderFileStore::new_with_id(dir.path().join("http-recorder"), "n1", CLUSTER_ID, 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 0, 8))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let overloaded = post_graph_query(
        &client,
        addr,
        &serde_json::json!({"statement":{"cypher":"RETURN 1","parameters":{}}}),
    )
    .await;
    assert_client_error(
        overloaded,
        reqwest::StatusCode::TOO_MANY_REQUESTS,
        "overloaded",
        true,
    )
    .await;
    server.abort();

    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(dir.path()),
            consensus(dir.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    for index in 0..6 {
        runtime
            .mutate_graph(
                GraphCommandV1::put_document(
                    format!("limit-seed-{index}"),
                    format!("document-{index}"),
                    GraphValueV1::String("value".into()),
                )
                .unwrap(),
            )
            .unwrap();
    }
    let (addr, server) = serve_graph(runtime, dir.path()).await;
    for (body, status) in [
        (
            serde_json::json!({
                "statement": {
                    "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                    "parameters": {}
                },
                "max_rows": 1
            }),
            reqwest::StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({
                "statement": {
                    "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                    "parameters": {}
                },
                "max_rows": 0
            }),
            reqwest::StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({
                "statement": {
                    "cypher": "MATCH (v:RhizaDocument) RETURN v.id",
                    "parameters": {}
                },
                "max_rows": MAX_GRAPH_MAX_ROWS + 1
            }),
            reqwest::StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({
                "statement": {"cypher": "MATCH (v:RhizaDocument) RETURN $blob", "parameters": {
                    "blob": {"type": "bytes", "value": "not-base64"}
                }}
            }),
            reqwest::StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({
                "statement": {"cypher": "MATCH (v:RhizaDocument) RETURN $node", "parameters": {
                    "node": {"type": "node", "value": {}}
                }}
            }),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
        ),
    ] {
        let rejected = post_graph_query(&client, addr, &body).await;
        assert_eq!(rejected.status(), status);
        assert!(
            !rejected
                .json::<ClientErrorResponse>()
                .await
                .unwrap()
                .retryable
        );
    }

    let oversized_result = post_graph_query(
        &client,
        addr,
        &serde_json::json!({
            "statement": {
                "cypher": "MATCH (v:RhizaDocument) WHERE v.id = $id RETURN $a, $b, $c, $d",
                "parameters": {
                    "id": {"type":"string", "value":"document-0"},
                    "a": {"type":"string", "value":"x".repeat(256 * 1024)},
                    "b": {"type":"string", "value":"x".repeat(256 * 1024)},
                    "c": {"type":"string", "value":"x".repeat(256 * 1024)},
                    "d": {"type":"string", "value":"x".repeat(256 * 1024)}
                }
            }
        }),
    )
    .await;
    assert_client_error(
        oversized_result,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
    )
    .await;

    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn graph_sync_checkpoint_outage_times_out_releases_capacity_and_retries_original_outcome() {
    let _test_guard = graph_test_lock().lock_owned().await;
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_backup = root.path().join("archive-backup");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = Arc::new(
        NodeRuntime::open(
            graph_http_config(root.path()),
            consensus(root.path(), "recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder =
        RecorderFileStore::new_with_id(root.path().join("http-recorder"), "n1", CLUSTER_ID, 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            node_router_with_checkpoint_and_limits(runtime, recorder, coordinator, 1, 8),
        )
        .await
        .unwrap();
    });
    std::fs::rename(&archive_root, &archive_backup).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "request_id": "request-1",
        "id": "document-1",
        "value": {"type": "string", "value": "hello"}
    });

    let first = post_graph_put(&client, addr, &body).await;

    assert_eq!(first.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        first.json::<ClientErrorResponse>().await.unwrap().code,
        "write_timeout"
    );
    let read = client
        .post(format!("http://{addr}{GRAPH_GET_DOCUMENT_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({"id": "document-1", "consistency": "local"}))
        .send()
        .await
        .unwrap();
    assert!(read.status().is_success());
    let original = read.json::<GraphGetDocumentResponse>().await.unwrap();
    assert_eq!(original.value, Some(GraphValueDto::String("hello".into())));

    restore_archive(&archive_root, &archive_backup);
    wait_ready(&client, addr).await;
    let retry = post_graph_put(&client, addr, &body).await;
    assert!(retry.status().is_success());
    let retry = retry.json::<GraphMutationResponse>().await.unwrap();
    assert_eq!(
        (retry.applied_index, retry.hash),
        (original.applied_index, original.hash)
    );
    assert_eq!(
        retry.result,
        rhiza_node::GraphMutationResultDto::PutDocument { created: true }
    );
    server.abort();
}

fn graph_config(root: &Path) -> NodeConfig {
    NodeConfig::new_embedded(
        "cluster-a",
        "n1",
        root.join("node"),
        1,
        1,
        ["n1", "n2", "n3"],
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Graph)
    .unwrap()
}

fn graph_test_lock() -> Arc<tokio::sync::Mutex<()>> {
    static LOCK: std::sync::OnceLock<Arc<tokio::sync::Mutex<()>>> = std::sync::OnceLock::new();
    Arc::clone(LOCK.get_or_init(|| Arc::new(tokio::sync::Mutex::new(()))))
}

fn assert_graph_startup_recovers_pending_first_lgfx(target_installed: bool) {
    let dir = tempfile::tempdir().unwrap();
    let config = graph_config(dir.path());
    let shared_consensus = consensus(dir.path(), "pending-startup-recorders");
    let runtime = NodeRuntime::open(config.clone(), Arc::clone(&shared_consensus), &[]).unwrap();
    drop(runtime);

    let command =
        GraphCommandV1::put_document("pending-request", "document", GraphValueV1::U64(9)).unwrap();
    let request = encode_replicated_graph_command(&command).unwrap();
    let graph_path = config.data_dir().join("ladybug/graph.lbug");
    let materializer = LadybugStateMachine::open_with_configuration(
        &graph_path,
        config.cluster_id(),
        config.node_id(),
        config.epoch(),
        config.log_initial_configuration().clone(),
        config.recovery_generation(),
    )
    .unwrap();
    let encoded = materializer
        .prepare_graph_effect(&request, 0, LogHash::ZERO)
        .unwrap();
    let effect = LadybugFileEffectV1::decode(&encoded).unwrap();
    drop(materializer);

    let entry = shared_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::Deterministic, encoded),
        )
        .unwrap();
    let log = FileLogStore::open_with_configuration(
        config.data_dir().join("consensus/log"),
        config.cluster_id(),
        config.epoch(),
        config.log_initial_configuration().clone(),
    )
    .unwrap();
    log.append(&entry).unwrap();
    drop(log);

    let pending = PendingApply::new(
        LogAnchor::new(0, LogHash::ZERO),
        LogAnchor::new(1, entry.hash),
        effect.base_db_digest,
        effect.target_db_digest,
        effect.target_file_bytes,
    );
    let control_path = graph_path.with_extension("lbug.control");
    let control = ControlStore::open_existing(&control_path).unwrap();
    control.begin_pending(&pending).unwrap();
    drop(control);

    if target_installed {
        let target = graph_path.with_extension("lbug.pending-target");
        apply_lgfx_to_exact_base(&graph_path, &target, &effect).unwrap();
        std::fs::rename(target, &graph_path).unwrap();
    }
    assert_eq!(
        ControlStore::open_existing(&control_path)
            .unwrap()
            .pending()
            .unwrap(),
        Some(pending)
    );

    let reopened = NodeRuntime::open(config, shared_consensus, &[]).unwrap();
    let read = reopened
        .get_graph_document("document", ReadConsistency::Local)
        .unwrap();
    assert_eq!(read.value, Some(GraphValueV1::U64(9)));
    assert_eq!((read.applied_index, read.hash), (1, entry.hash));
    let retried = reopened.mutate_graph(command).unwrap();
    assert_eq!((retried.applied_index(), retried.hash()), (1, entry.hash));
    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
    drop(reopened);

    assert_eq!(
        ControlStore::open_existing(control_path)
            .unwrap()
            .pending()
            .unwrap(),
        None
    );
}

fn graph_http_config(root: &Path) -> NodeConfig {
    NodeConfig::new(
        "cluster-a",
        "n1",
        root.join("node"),
        1,
        1,
        [
            PeerConfig::new("n1", "http://n1", "peer-1").unwrap(),
            PeerConfig::new("n2", "http://n2", "peer-2").unwrap(),
            PeerConfig::new("n3", "http://n3", "peer-3").unwrap(),
        ],
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Graph)
    .unwrap()
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(CLUSTER_ID, 1, 1, 1),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

async fn post_graph_put(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    body: &serde_json::Value,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{GRAPH_PUT_DOCUMENT_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn post_graph_query(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    body: &serde_json::Value,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{GRAPH_QUERY_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(body)
        .send()
        .await
        .unwrap()
}

async fn serve_graph(
    runtime: Arc<NodeRuntime>,
    root: &Path,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let recorder =
        RecorderFileStore::new_with_id(root.join("http-query-recorder"), "n1", CLUSTER_ID, 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    (addr, server)
}

async fn assert_client_error(
    response: reqwest::Response,
    status: reqwest::StatusCode,
    code: &str,
    retryable: bool,
) {
    assert_eq!(response.status(), status);
    let body = response.json::<ClientErrorResponse>().await.unwrap();
    assert_eq!(body.code, code);
    assert_eq!(body.retryable, retryable);
}

fn restore_archive(archive_root: &Path, archive_backup: &Path) {
    std::fs::remove_file(archive_root).unwrap();
    let link = archive_root.with_extension("restore-link");
    std::os::unix::fs::symlink(archive_backup, &link).unwrap();
    std::fs::rename(link, archive_root).unwrap();
}

async fn wait_ready(client: &reqwest::Client, addr: std::net::SocketAddr) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client
                .get(format!("http://{addr}{READYZ_PATH}"))
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}

fn consensus(root: &Path, recorder_dir: &str) -> Arc<ThreeNodeConsensus> {
    Arc::new(
        ThreeNodeConsensus::from_recovered_tip(
            CLUSTER_ID,
            "n1",
            1,
            1,
            [
                root.join(recorder_dir).join("n1"),
                root.join(recorder_dir).join("n2"),
                root.join(recorder_dir).join("n3"),
            ],
            1,
            LogHash::ZERO,
        )
        .unwrap(),
    )
}
