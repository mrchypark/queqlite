use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use queqlite_core::{Command, CommandKind, EntryType, LogEntry, LogHash, StoredCommand};
use queqlite_log::{FileLogStore, LogStore};
use queqlite_node::{
    catch_up_missing_entries, log_peer_router, node_router, node_router_with_limits,
    node_rpc_router_for_generation, node_rpc_router_with_limits, recorder_router, AckMode,
    ClientErrorResponse, ConfigError, FetchLogError, FetchLogRequest, FetchLogResponse,
    HttpLogPeer, HttpRecorderClient, InMemoryLogPeer, LogPeer, NodeConfig, NodeError, NodeRuntime,
    PeerConfig, ReadConsistency, ReadRequest, SqlExecuteRequest, SqlExecuteResponse, WriteRequest,
    DEFAULT_WRITER_BATCH_MAX, DEFAULT_WRITER_BATCH_WINDOW, LIVEZ_PATH, MAX_FETCH_ENTRIES,
    MAX_HTTP_BODY_BYTES, NODE_ID_HEADER, PROTOCOL_VERSION, READYZ_PATH, RECORDER_IDENTITY_PATH,
    RECORDER_PROTOCOL_VERSION, RECOVERY_GENERATION_HEADER, VERSION_HEADER,
};
use queqlite_quepaxa::{
    AcceptedSummary, Ballot, DecisionProof, DecisionRecord, FixedMembership, IsrState, Membership,
    RecordRequest, RecordSummary, RecorderFileStore, RecorderReply, RecorderRequest, RecorderRpc,
    ThreeNodeConsensus,
};
use queqlite_sqlite::{SqlCommand, SqlStatement, SqlValue};

fn test_config_digest() -> LogHash {
    FixedMembership::new(["node-1", "node-2", "node-3"])
        .unwrap()
        .digest()
}

fn assert_test_recorder_context(request: &RecorderRequest) {
    let (cluster_id, epoch, config_id, config_digest) = match request {
        RecorderRequest::Identity => return,
        RecorderRequest::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Inspect {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Observe {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Converge {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        }
        | RecorderRequest::Decide {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            ..
        } => (cluster_id, epoch, config_id, config_digest),
    };

    assert_eq!(cluster_id, "cluster-a");
    assert_eq!(*epoch, 1);
    assert_eq!(*config_id, 1);
    assert_eq!(*config_digest, test_config_digest());
}

#[test]
fn recorder_rpc_piggybacks_command_before_recording_isr_state() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorders = (1..=3)
        .map(|index| {
            Box::new(OrderingRecorder::new(
                format!("node-{index}"),
                calls.clone(),
            )) as Box<dyn RecorderRpc>
        })
        .collect();
    let consensus =
        ThreeNodeConsensus::from_recorders("cluster-a", "node-1", 1, 1, recorders).unwrap();

    consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\trequest-1\talpha\tone".to_vec(),
            ),
        )
        .unwrap();

    let calls = calls.lock().unwrap();
    let first_record = calls.iter().position(|call| *call == "record").unwrap();
    assert!(calls[..first_record].contains(&"piggyback"));
    assert!(calls.iter().all(|call| *call != "store"));
    assert!(calls.iter().filter(|call| **call == "piggyback").count() >= 2);
}

#[test]
fn catch_up_rejects_wrong_first_prev_hash() {
    let local_hash = LogHash::digest(&[b"local"]);
    let fetched = entry(2, LogHash::digest(&[b"wrong"]), b"put\tr2\tk\tv");
    let peer = OneResponsePeer::new(vec![fetched]);

    assert!(matches!(
        catch_up_missing_entries(1, local_hash, "cluster-a", 1, 1, &peer, 10),
        Err(FetchLogError::InvalidAnchor { .. })
    ));
}

#[test]
fn catch_up_rejects_tampered_hash() {
    let mut fetched = entry(1, LogHash::ZERO, b"put\tr1\tk\tv");
    fetched.hash = LogHash::ZERO;
    let peer = OneResponsePeer::new(vec![fetched]);

    assert!(matches!(
        catch_up_missing_entries(0, LogHash::ZERO, "cluster-a", 1, 1, &peer, 10),
        Err(FetchLogError::InvalidEntry { index: 1, .. })
    ));
}

#[test]
fn catch_up_rejects_foreign_identity() {
    let mut fetched = entry(1, LogHash::ZERO, b"put\tr1\tk\tv");
    fetched.cluster_id = "cluster-b".into();
    fetched.hash = fetched.recompute_hash();
    let peer = OneResponsePeer::new(vec![fetched]);

    assert!(matches!(
        catch_up_missing_entries(0, LogHash::ZERO, "cluster-a", 1, 1, &peer, 10),
        Err(FetchLogError::ForeignIdentity { index: 1 })
    ));
}

#[test]
fn runtime_commit_persists_qlog_and_sqlite_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = NodeRuntime::open(config.clone(), consensus(dir.path()), &[]).unwrap();

    let committed = runtime.write("request-1", "alpha", "one").unwrap();

    assert_eq!(committed.applied_index, 1);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
    assert_eq!(
        runtime.log_store().read(1).unwrap().unwrap().hash,
        committed.hash
    );
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), 1);
    assert_eq!(
        reopened
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("one")
    );
}

#[test]
fn runtime_regenerates_sql_effect_after_a_foreign_slot_winner() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let shared_consensus = consensus(dir.path());
    let runtime = NodeRuntime::open(config, Arc::clone(&shared_consensus), &[]).unwrap();
    let schema = runtime
        .execute_sql(SqlCommand {
            request_id: "effect-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    assert_eq!(schema.applied_index, 1);

    let winner = shared_consensus
        .propose_at(
            2,
            schema.hash,
            Command::new(
                CommandKind::Deterministic,
                b"put\tforeign-winner\tmarker\twon".to_vec(),
            ),
        )
        .unwrap();
    let committed = runtime
        .execute_sql(SqlCommand {
            request_id: "effect-after-winner".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO items(value) VALUES ('effect') RETURNING id, value".into(),
                parameters: vec![],
            }],
        })
        .unwrap();

    assert_eq!(committed.applied_index, 3);
    assert_eq!(runtime.log_store().read(2).unwrap(), Some(winner));
    assert!(runtime
        .log_store()
        .read(3)
        .unwrap()
        .unwrap()
        .payload
        .starts_with(b"QEFX\0\x01"));
    assert_eq!(
        runtime
            .query_sql(
                &SqlStatement {
                    sql: "SELECT id, value FROM items".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                10,
            )
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(1), SqlValue::Text("effect".into())]]
    );
}

#[test]
fn node_config_rejects_duplicate_peer_ids() {
    let dir = tempfile::tempdir().unwrap();
    let peers = [
        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
        PeerConfig::new("node-1", "http://node-2", "peer-token-2").unwrap(),
        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
    ];

    assert!(matches!(
        NodeConfig::new(
            "cluster-a",
            "node-1",
            dir.path().to_path_buf(),
            1,
            1,
            peers,
            "client-token",
        ),
        Err(ConfigError::DuplicatePeerNodeId(node_id)) if node_id == "node-1"
    ));
}

#[test]
fn node_config_defaults_generation_to_one_and_rejects_zero() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());

    assert_eq!(config.recovery_generation(), 1);
    let debug = format!("{config:?}");
    assert!(debug.contains("recovery_generation: 1"));
    assert!(!debug.contains("client-token"));
    assert!(!debug.contains("peer-token"));
    assert_eq!(
        config.with_recovery_generation(0),
        Err(ConfigError::InvalidRecoveryGeneration)
    );
}

#[test]
fn node_config_uses_conservative_configurable_writer_batch_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());

    assert_eq!(config.writer_batch_max(), DEFAULT_WRITER_BATCH_MAX);
    assert_eq!(config.writer_batch_window(), DEFAULT_WRITER_BATCH_WINDOW);
    assert_eq!(DEFAULT_WRITER_BATCH_MAX, 8);
    assert!(DEFAULT_WRITER_BATCH_WINDOW < Duration::from_millis(1));

    let configured = config
        .with_writer_batching(4, Duration::from_millis(10))
        .unwrap();
    assert_eq!(configured.writer_batch_max(), 4);
    assert_eq!(configured.writer_batch_window(), Duration::from_millis(10));
}

#[test]
fn dr_strong_open_is_rejected_until_synchronous_archive_is_injected() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path()).with_ack_mode(AckMode::DrStrong);

    assert!(matches!(
        NodeRuntime::open(config, consensus(dir.path()), &[]),
        Err(NodeError::UnsupportedAckMode(AckMode::DrStrong))
    ));
}

#[test]
fn node_runtime_holds_exclusive_data_root_lock() {
    let dir = tempfile::tempdir().unwrap();
    let first_consensus = consensus_named(dir.path(), "first-recorders");
    let second_consensus = consensus_named(dir.path(), "second-recorders");
    let first = NodeRuntime::open(node_config(dir.path()), first_consensus, &[]).unwrap();

    assert!(matches!(
        NodeRuntime::open(node_config(dir.path()), second_consensus, &[]),
        Err(NodeError::DataRootLocked(_))
    ));

    drop(first);
}

#[test]
fn repeated_request_returns_original_outcome_and_conflict_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();

    let first = runtime.write("request-1", "alpha", "one").unwrap();
    let repeated = runtime.write("request-1", "alpha", "one").unwrap();

    assert_eq!(repeated, first);
    assert!(matches!(
        runtime.write("request-1", "alpha", "two"),
        Err(NodeError::RequestConflict(_))
    ));
}

#[test]
fn startup_replays_qlog_ahead_of_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let log = FileLogStore::open(dir.path().join("consensus/log"), "cluster-a", 1, 1).unwrap();
    log.append(&entry(
        1,
        LogHash::ZERO,
        b"put\trequest-1\talpha\tfrom-qlog",
    ))
    .unwrap();

    let runtime = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();

    assert_eq!(runtime.applied_index().unwrap(), 1);
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("from-qlog")
    );
}

#[test]
fn startup_recovers_quorum_decision_without_qlog_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let decided_consensus = consensus(dir.path());
    let decided = decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\trequest-1\talpha\trecovered".to_vec(),
            ),
        )
        .unwrap();

    let runtime = NodeRuntime::open(config.clone(), decided_consensus, &[]).unwrap();
    assert_eq!(runtime.log_store().read(1).unwrap(), Some(decided.clone()));
    assert_eq!(runtime.applied_index().unwrap(), 1);
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap();
    assert_eq!(reopened.applied_index().unwrap(), 1);
    assert_eq!(reopened.log_store().last_index().unwrap(), Some(1));
}

#[test]
fn startup_accepts_peer_candidate_only_when_consensus_committed_exact_entry() {
    let dir = tempfile::tempdir().unwrap();
    let decided_consensus = consensus(dir.path());
    let decided = decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\trequest-candidate\talpha\tverified".to_vec(),
            ),
        )
        .unwrap();
    let peer = OneResponsePeer::new(vec![decided.clone()]);

    let runtime = NodeRuntime::open(node_config(dir.path()), decided_consensus, &[&peer]).unwrap();

    assert_eq!(runtime.log_store().read(1).unwrap(), Some(decided));
    assert_eq!(
        runtime
            .read("alpha", ReadConsistency::Local)
            .unwrap()
            .value
            .as_deref(),
        Some("verified")
    );
}

#[test]
fn startup_rejects_peer_candidate_that_differs_from_committed_decision() {
    let dir = tempfile::tempdir().unwrap();
    let decided_consensus = consensus(dir.path());
    decided_consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(
                CommandKind::Deterministic,
                b"put\trequest-committed\talpha\tcommitted".to_vec(),
            ),
        )
        .unwrap();
    let peer = OneResponsePeer::new(vec![entry(
        1,
        LogHash::ZERO,
        b"put\trequest-candidate\talpha\tuncommitted",
    )]);

    assert!(matches!(
        NodeRuntime::open(
            node_config(dir.path()),
            decided_consensus,
            &[&peer],
        ),
        Err(NodeError::Reconciliation(message)) if message.contains("candidate")
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_three_http_recorders_commit() {
    let dir = tempfile::tempdir().unwrap();
    let peers = peer_configs(["http://unused"; 3]);
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let mut servers = Vec::new();
    let mut base_urls = Vec::new();
    for index in 1..=3 {
        let store = RecorderFileStore::new_with_membership(
            dir.path().join(format!("http-recorder-{index}")),
            format!("node-{index}"),
            "cluster-a",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = recorder_router(store, peers.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
        base_urls.push(format!("http://{addr}"));
    }

    let recorders = base_urls
        .into_iter()
        .map(|base_url| {
            Box::new(HttpRecorderClient::new(base_url, "node-1", "peer-token-1").unwrap())
                as Box<dyn RecorderRpc>
        })
        .collect();
    let committed = tokio::task::spawn_blocking(move || {
        ThreeNodeConsensus::from_recorders("cluster-a", "node-1", 1, 1, recorders)
            .unwrap()
            .propose_at(
                1,
                LogHash::ZERO,
                Command::new(
                    CommandKind::Deterministic,
                    b"put\trequest-1\talpha\tone".to_vec(),
                ),
            )
    })
    .await
    .unwrap()
    .unwrap();

    assert_eq!(committed.index, 1);
    for server in servers {
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_generation_http_clients_reach_recorder_and_log_routes() {
    let dir = tempfile::tempdir().unwrap();
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("generation-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let log_peer = InMemoryLogPeer::new(Vec::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            node_rpc_router_for_generation(
                TypedOnlyRecorder(recorder),
                log_peer,
                peer_configs(["http://unused"; 3]),
                7,
            ),
        )
        .await
        .unwrap();
    });
    let base_url = format!("http://{addr}");
    let recorder =
        HttpRecorderClient::new_with_recovery_generation(&base_url, "node-1", "peer-token-1", 7)
            .unwrap();
    let (reply, summary) = tokio::task::spawn_blocking(move || {
        (recorder.recorder_id(), recorder.inspect_record_summary(1))
    })
    .await
    .unwrap();
    let reply = reply.unwrap();
    assert_eq!(reply, "node-1");
    assert!(summary.unwrap().is_none());

    let log_peer = HttpLogPeer::new(base_url, "node-1", "peer-token-1")
        .unwrap()
        .with_recovery_generation(7)
        .unwrap();
    let fetched = tokio::task::spawn_blocking(move || {
        log_peer.fetch_log(FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert!(fetched.entries.is_empty());
    server.abort();
}

#[derive(Clone)]
struct TypedOnlyRecorder(RecorderFileStore);

impl RecorderRpc for TypedOnlyRecorder {
    fn call(&self, _request: RecorderRequest) -> queqlite_quepaxa::Result<RecorderReply> {
        panic!("v2 recorder routes must not use the legacy adapter")
    }

    fn recorder_id(&self) -> queqlite_quepaxa::Result<String> {
        self.0.recorder_id()
    }

    fn store_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> queqlite_quepaxa::Result<()> {
        self.0.store_command(command_hash, command)
    }

    fn fetch_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> queqlite_quepaxa::Result<Option<StoredCommand>> {
        self.0.fetch_command(command_hash)
    }

    fn record(&self, request: RecordRequest) -> queqlite_quepaxa::Result<RecordSummary> {
        self.0.record_proposal(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> queqlite_quepaxa::Result<()> {
        self.0.install_decision_proof_record(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: u64) -> queqlite_quepaxa::Result<Option<DecisionProof>> {
        RecorderRpc::inspect_decision_proof(&self.0, slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> queqlite_quepaxa::Result<Option<RecordSummary>> {
        RecorderRpc::inspect_record_summary(&self.0, slot)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_or_missing_generation_is_rejected_before_body_and_peer_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path()).with_recovery_generation(7).unwrap();
    let runtime = Arc::new(NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("generation-order-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 0, 0))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    for (path, version) in [
        (RECORDER_IDENTITY_PATH, RECORDER_PROTOCOL_VERSION),
        ("/v1/log/fetch", PROTOCOL_VERSION),
    ] {
        for generation in [None, Some("6")] {
            let mut request = client
                .post(format!("http://{addr}{path}"))
                .header(VERSION_HEADER, version)
                .header(NODE_ID_HEADER, "node-1")
                .bearer_auth("peer-token-1")
                .header("content-type", "application/json")
                .body("{");
            if let Some(generation) = generation {
                request = request.header(RECOVERY_GENERATION_HEADER, generation);
            }
            assert_eq!(
                request.send().await.unwrap().status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }

        let at_capacity = client
            .post(format!("http://{addr}{path}"))
            .header(VERSION_HEADER, version)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "7")
            .bearer_auth("peer-token-1")
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(at_capacity.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    }

    for path in ["/v1/write", "/v1/read", "/v1/sql/execute", "/v1/sql/query"] {
        let response = client
            .post(format!("http://{addr}{path}"))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_client_error(
            response,
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            true,
            None,
        )
        .await;
    }
    server.abort();
}

#[tokio::test]
async fn unauthorized_request_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let store =
        RecorderFileStore::new_with_id(dir.path().join("recorder"), "node-1", "cluster-a", 1, 1)
            .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            recorder_router(store, peer_configs(["http://unused"; 3])),
        )
        .await
        .unwrap();
    });

    let status = reqwest::Client::new()
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("wrong-token")
        .json(&serde_json::json!({"version": 2, "body": null}))
        .send()
        .await
        .unwrap()
        .status();

    assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn health_routes_distinguish_process_liveness_from_runtime_readiness() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("health-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("http://{addr}{LIVEZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("http://{addr}{READYZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );

    runtime
        .log_store()
        .append(&entry(1, LogHash::ZERO, b"put\texternal\talpha\tone"))
        .unwrap();
    assert!(runtime.read("alpha", ReadConsistency::ReadBarrier).is_err());
    assert!(runtime.is_fatal());

    assert_eq!(
        client
            .get(format!("http://{addr}{LIVEZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        client
            .get(format!("http://{addr}{READYZ_PATH}"))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn log_fetch_is_authenticated_versioned_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    runtime.write("request-log", "alpha", "one").unwrap();
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("log-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let base_url = format!("http://{addr}");
    let peer = HttpLogPeer::new(&base_url, "node-1", "peer-token-1").unwrap();
    let fetched = tokio::task::spawn_blocking(move || {
        peer.fetch_log(FetchLogRequest {
            from_index: 1,
            max_entries: 10,
        })
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fetched.entries.len(), 1);

    let client = reqwest::Client::new();
    let request = || FetchLogRequest {
        from_index: 1,
        max_entries: 1,
    };
    let missing_version = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    let wrong_version = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, "wrong")
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    let missing_caller = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&request())
        .send()
        .await
        .unwrap();
    let wrong_bearer = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("wrong")
        .json(&request())
        .send()
        .await
        .unwrap();
    for response in [missing_version, wrong_version, missing_caller, wrong_bearer] {
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }

    let oversized_batch = client
        .post(format!("{base_url}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: MAX_FETCH_ENTRIES + 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(oversized_batch.status(), reqwest::StatusCode::BAD_REQUEST);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn client_routes_use_stable_status_mapping_and_body_limit() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("status-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/v1/write");
    let write = |request_id: &str, value: &str| WriteRequest {
        request_id: request_id.into(),
        key: "alpha".into(),
        value: value.into(),
    };

    let unauthorized = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .json(&write("unauthorized", "one"))
        .send()
        .await
        .unwrap();
    assert_client_error(
        unauthorized,
        reqwest::StatusCode::UNAUTHORIZED,
        "unauthorized",
        false,
        None,
    )
    .await;

    let invalid = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "invalid".into(),
            key: "bad\tkey".into(),
            value: "one".into(),
        })
        .send()
        .await
        .unwrap();
    assert_client_error(
        invalid,
        reqwest::StatusCode::BAD_REQUEST,
        "invalid_request",
        false,
        None,
    )
    .await;

    let first = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&write("conflict", "one"))
        .send()
        .await
        .unwrap();
    assert!(first.status().is_success());
    let conflict = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&write("conflict", "two"))
        .send()
        .await
        .unwrap();
    assert_client_error(
        conflict,
        reqwest::StatusCode::CONFLICT,
        "request_conflict",
        false,
        None,
    )
    .await;

    let unavailable = client
        .post(format!("http://{addr}/v1/read"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&ReadRequest {
            key: "alpha".into(),
            consistency: Some(ReadConsistency::AppliedIndex(100)),
        })
        .send()
        .await
        .unwrap();
    assert_client_error(
        unavailable,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        true,
        None,
    )
    .await;

    let too_large = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("x".repeat(MAX_HTTP_BODY_BYTES + 1))
        .send()
        .await
        .unwrap();
    assert_client_error(
        too_large,
        reqwest::StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        false,
        None,
    )
    .await;

    let last_index = runtime.log_store().last_index().unwrap().unwrap();
    let last_hash = runtime.log_store().read(last_index).unwrap().unwrap().hash;
    runtime
        .log_store()
        .append(&entry(
            last_index + 1,
            last_hash,
            b"put\texternal\talpha\tfatal",
        ))
        .unwrap();
    assert!(runtime.read("alpha", ReadConsistency::ReadBarrier).is_err());
    let fatal = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&write("fatal", "three"))
        .send()
        .await
        .unwrap();
    assert_client_error(
        fatal,
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "fatal",
        false,
        None,
    )
    .await;
    server.abort();
}

#[test]
fn read_consistency_uses_snake_case_and_accepts_legacy_variant_names() {
    assert_eq!(
        serde_json::to_value(ReadConsistency::Local).unwrap(),
        "local"
    );
    assert_eq!(
        serde_json::to_value(ReadConsistency::ReadBarrier).unwrap(),
        "read_barrier"
    );
    assert_eq!(
        serde_json::to_value(ReadConsistency::AppliedIndex(7)).unwrap(),
        serde_json::json!({"applied_index": 7})
    );

    assert_eq!(
        serde_json::from_value::<ReadConsistency>(serde_json::json!("Local")).unwrap(),
        ReadConsistency::Local
    );
    assert_eq!(
        serde_json::from_value::<ReadConsistency>(serde_json::json!("ReadBarrier")).unwrap(),
        ReadConsistency::ReadBarrier
    );
    assert_eq!(
        serde_json::from_value::<ReadConsistency>(serde_json::json!({"AppliedIndex": 7})).unwrap(),
        ReadConsistency::AppliedIndex(7)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnected_client_holds_its_slot_without_starving_peer_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Gate::default());
    let recorders = (1..=3)
        .map(|index| {
            Box::new(GatedRecorder::new(format!("node-{index}"), gate.clone()))
                as Box<dyn RecorderRpc>
        })
        .collect();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders("cluster-a", "node-1", 1, 1, recorders).unwrap(),
    );
    let runtime = Arc::new(NodeRuntime::open(node_config(dir.path()), consensus, &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("saturation-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 1, 1))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/v1/write");
    let slow_client = client.clone();
    let slow_url = url.clone();
    let slow = tokio::spawn(async move {
        slow_client
            .post(slow_url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&WriteRequest {
                request_id: "slow".into(),
                key: "alpha".into(),
                value: "one".into(),
            })
            .send()
            .await
    });

    let deadline = Instant::now() + Duration::from_secs(3);
    while !gate.started() {
        assert!(Instant::now() < deadline, "blocking write did not start");
        tokio::task::yield_now().await;
    }
    slow.abort();

    let saturated = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "saturated".into(),
            key: "alpha".into(),
            value: "two".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(saturated.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    let peer = client
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&serde_json::json!({"version": 2, "body": null}))
        .send()
        .await
        .unwrap();
    assert!(peer.status().is_success());

    gate.release();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let response = client
            .post(&url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&WriteRequest {
                request_id: "after-release".into(),
                key: "alpha".into(),
                value: "three".into(),
            })
            .send()
            .await
            .unwrap();
        if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS {
            assert!(response.status().is_success());
            break;
        }
        assert!(Instant::now() < deadline, "client slot was not released");
        tokio::task::yield_now().await;
    }
    server.abort();
}

#[test]
fn read_barrier_advances_index_without_changing_value() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap();
    runtime.write("request-1", "alpha", "one").unwrap();

    let read = runtime.read("alpha", ReadConsistency::ReadBarrier).unwrap();

    assert_eq!(read.value.as_deref(), Some("one"));
    assert_eq!(read.applied_index, 2);
    assert_eq!(
        runtime.log_store().read(2).unwrap().unwrap().entry_type,
        EntryType::Noop
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticated_http_write_and_read_use_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path());
    let runtime = Arc::new(NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("served-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    let write = client
        .post(format!("http://{addr}/v1/write"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "request-http".into(),
            key: "alpha".into(),
            value: "http".into(),
        })
        .send()
        .await
        .unwrap();
    assert!(write.status().is_success());

    let read = client
        .post(format!("http://{addr}/v1/read"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&ReadRequest {
            key: "alpha".into(),
            consistency: Some(ReadConsistency::Local),
        })
        .send()
        .await
        .unwrap();

    assert!(read.status().is_success());
    assert!(read.text().await.unwrap().contains("http"));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_sql_requests_share_one_entry_and_replay_distinct_returning_results() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path())
        .with_writer_batching(8, Duration::from_millis(20))
        .unwrap();
    let runtime = Arc::new(NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "batch-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batched(id INTEGER PRIMARY KEY, value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("batch-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = runtime.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/v1/sql/execute");
    let request = |request_id: &str, value: &str| SqlExecuteRequest {
        request_id: request_id.into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO batched(value) VALUES (?1) RETURNING id, value".into(),
            parameters: vec![SqlValue::Text(value.into())],
        }],
    };

    let first = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&request("sql-batch-a", "alpha"))
        .send();
    let second = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&request("sql-batch-b", "beta"))
        .send();
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap().json::<SqlExecuteResponse>().await.unwrap();
    let second = second.unwrap().json::<SqlExecuteResponse>().await.unwrap();

    assert_eq!(first.applied_index, second.applied_index);
    assert_eq!(first.hash, second.hash);
    assert_eq!(first.results.len(), 1);
    assert_eq!(second.results.len(), 1);
    assert_ne!(first.results[0].returning, second.results[0].returning);

    let last_index = runtime.log_store().last_index().unwrap();
    let replay = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&request("sql-batch-a", "alpha"))
        .send()
        .await
        .unwrap()
        .json::<SqlExecuteResponse>()
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(runtime.log_store().last_index().unwrap(), last_index);

    let duplicate = request("sql-batch-duplicate", "gamma");
    let duplicate_first = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&duplicate)
        .send();
    let duplicate_second = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&duplicate)
        .send();
    let (duplicate_first, duplicate_second) = tokio::join!(duplicate_first, duplicate_second);
    let duplicate_first = duplicate_first
        .unwrap()
        .json::<SqlExecuteResponse>()
        .await
        .unwrap();
    let duplicate_second = duplicate_second
        .unwrap()
        .json::<SqlExecuteResponse>()
        .await
        .unwrap();
    assert_eq!(duplicate_first, duplicate_second);
    assert_eq!(
        runtime.log_store().last_index().unwrap(),
        last_index.map(|index| index + 1)
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_sql_member_does_not_prevent_a_valid_request_in_the_same_window() {
    let dir = tempfile::tempdir().unwrap();
    let config = node_config(dir.path())
        .with_writer_batching(8, Duration::from_millis(20))
        .unwrap();
    let runtime = Arc::new(NodeRuntime::open(config, consensus(dir.path()), &[]).unwrap());
    runtime
        .execute_sql(SqlCommand {
            request_id: "partial-setup".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE partial(value TEXT UNIQUE)".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    runtime
        .execute_sql(SqlCommand {
            request_id: "partial-existing".into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO partial(value) VALUES ('existing')".into(),
                parameters: vec![],
            }],
        })
        .unwrap();
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("partial-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let url = format!("http://{addr}/v1/sql/execute");
    let request = |request_id: &str, value: &str| SqlExecuteRequest {
        request_id: request_id.into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO partial(value) VALUES (?1)".into(),
            parameters: vec![SqlValue::Text(value.into())],
        }],
    };

    let invalid = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&request("partial-invalid", "existing"))
        .send();
    let valid = client
        .post(&url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&request("partial-valid", "fresh"))
        .send();
    let (invalid, valid) = tokio::join!(invalid, valid);

    assert_eq!(invalid.unwrap().status(), reqwest::StatusCode::BAD_REQUEST);
    let valid = valid.unwrap();
    assert!(valid.status().is_success());
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn log_fetch_saturation_does_not_consume_recorder_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let gate = Arc::new(Gate::default());
    let log_peer = BlockingLogPeer { gate: gate.clone() };
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("separate-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let peers = peer_configs(["http://unused"; 3]);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            node_rpc_router_with_limits(recorder, log_peer, peers, 1, 1),
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    let fetch_url = format!("http://{addr}/v1/log/fetch");
    let blocking_client = client.clone();
    let blocking_fetch = tokio::spawn(async move {
        blocking_client
            .post(fetch_url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-1")
            .json(&FetchLogRequest {
                from_index: 1,
                max_entries: 1,
            })
            .send()
            .await
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    while !gate.started() {
        assert!(
            Instant::now() < deadline,
            "blocking log fetch did not start"
        );
        tokio::task::yield_now().await;
    }

    let saturated_fetch = client
        .post(format!("http://{addr}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(
        saturated_fetch.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );

    let recorder_response = client
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&serde_json::json!({"version": 2, "body": null}))
        .send()
        .await
        .unwrap();
    assert!(recorder_response.status().is_success());

    gate.release();
    assert!(blocking_fetch.await.unwrap().unwrap().status().is_success());
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_backend_errors_return_non_success_statuses() {
    let failures = [
        (
            queqlite_quepaxa::Error::Io("recorder unavailable".into()),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            queqlite_quepaxa::Error::ConflictingCertificates,
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];
    for (error, expected_status) in failures {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                recorder_router(
                    FailingRecorder { error },
                    peer_configs(["http://unused"; 3]),
                ),
            )
            .await
            .unwrap();
        });
        let response = reqwest::Client::new()
            .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
            .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-1")
            .json(&serde_json::json!({"version": 2, "body": null}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected_status);
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn log_backend_errors_are_non_success_and_preserved_by_http_peer() {
    let failures = [
        (
            FetchLogError::Transport {
                message: "log unavailable".into(),
            },
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            FetchLogError::InvalidEntry {
                index: 1,
                message: "corrupt committed file".into(),
            },
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];
    for (expected_error, expected_status) in failures {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_error = expected_error.clone();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                log_peer_router(
                    FailingLogPeer {
                        error: server_error,
                    },
                    peer_configs(["http://unused"; 3]),
                ),
            )
            .await
            .unwrap();
        });
        let base_url = format!("http://{addr}");
        let direct = reqwest::Client::new()
            .post(format!("{base_url}/v1/log/fetch"))
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .header(NODE_ID_HEADER, "node-1")
            .header(RECOVERY_GENERATION_HEADER, "1")
            .bearer_auth("peer-token-1")
            .json(&FetchLogRequest {
                from_index: 1,
                max_entries: 1,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(direct.status(), expected_status);

        let peer = HttpLogPeer::new(base_url, "node-1", "peer-token-1").unwrap();
        let returned = tokio::task::spawn_blocking(move || {
            peer.fetch_log(FetchLogRequest {
                from_index: 1,
                max_entries: 1,
            })
        })
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(returned, expected_error);
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unauthenticated_invalid_bodies_are_rejected_before_body_or_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(node_config(dir.path()), consensus(dir.path()), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("auth-order-recorder"),
        "node-1",
        "cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router_with_limits(runtime, recorder, 0, 0))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();

    for path in ["/v1/write", RECORDER_IDENTITY_PATH, "/v1/log/fetch"] {
        let malformed = client
            .post(format!("http://{addr}{path}"))
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    let oversized_status = tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        write!(
            stream,
            "POST /v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_HTTP_BODY_BYTES + 1
        )
        .unwrap();
        stream.flush().unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        response.lines().next().unwrap_or_default().to_string()
    })
    .await
    .unwrap();
    assert!(oversized_status.contains(" 401 "), "{oversized_status}");

    let authenticated_malformed = client
        .post(format!("http://{addr}/v1/write"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(
        authenticated_malformed.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );
    let valid_recorder = client
        .post(format!("http://{addr}{RECORDER_IDENTITY_PATH}"))
        .header(VERSION_HEADER, RECORDER_PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&serde_json::json!({"version": 2, "body": null}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        valid_recorder.status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );
    let valid_fetch = client
        .post(format!("http://{addr}/v1/log/fetch"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "node-1")
        .header(RECOVERY_GENERATION_HEADER, "1")
        .bearer_auth("peer-token-1")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(valid_fetch.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    server.abort();
}

fn node_config(root: &Path) -> NodeConfig {
    NodeConfig::new(
        "cluster-a",
        "node-1",
        root.to_path_buf(),
        1,
        1,
        peer_configs(["http://node-1", "http://node-2", "http://node-3"]),
        "client-token",
    )
    .unwrap()
}

fn peer_configs(urls: [&str; 3]) -> [PeerConfig; 3] {
    [
        PeerConfig::new("node-1", urls[0], "peer-token-1").unwrap(),
        PeerConfig::new("node-2", urls[1], "peer-token-2").unwrap(),
        PeerConfig::new("node-3", urls[2], "peer-token-3").unwrap(),
    ]
}

fn consensus(root: &Path) -> Arc<ThreeNodeConsensus> {
    consensus_named(root, "recorders")
}

async fn assert_client_error(
    response: reqwest::Response,
    status: reqwest::StatusCode,
    code: &str,
    retryable: bool,
    statement_index: Option<usize>,
) {
    assert_eq!(response.status(), status);
    assert_eq!(
        response.headers()[reqwest::header::CONTENT_TYPE],
        "application/json"
    );
    let body = response.json::<ClientErrorResponse>().await.unwrap();
    assert_eq!(body.code, code);
    assert_eq!(body.retryable, retryable);
    assert!(!body.message.is_empty());
    assert_eq!(body.statement_index, statement_index);
}

fn consensus_named(root: &Path, recorder_dir: &str) -> Arc<ThreeNodeConsensus> {
    Arc::new(
        ThreeNodeConsensus::from_recovered_tip(
            "cluster-a",
            "node-1",
            1,
            1,
            [
                root.join(recorder_dir).join("node-1"),
                root.join(recorder_dir).join("node-2"),
                root.join(recorder_dir).join("node-3"),
            ],
            1,
            LogHash::ZERO,
        )
        .unwrap(),
    )
}

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

struct OneResponsePeer {
    entries: Vec<LogEntry>,
}

impl OneResponsePeer {
    fn new(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }
}

impl LogPeer for OneResponsePeer {
    fn fetch_log(&self, _request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        Ok(FetchLogResponse {
            last_index: self.entries.last().map_or(0, |entry| entry.index),
            entries: self.entries.clone(),
        })
    }
}

struct OrderingRecorder {
    id: String,
    commands: Mutex<HashMap<LogHash, StoredCommand>>,
    isr: Mutex<HashMap<u64, IsrState>>,
    proofs: Mutex<HashMap<u64, DecisionProof>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl OrderingRecorder {
    fn new(id: String, calls: Arc<Mutex<Vec<&'static str>>>) -> Self {
        Self {
            id,
            commands: Mutex::new(HashMap::new()),
            isr: Mutex::new(HashMap::new()),
            proofs: Mutex::new(HashMap::new()),
            calls,
        }
    }

    fn reply(
        &self,
        slot: u64,
        highest_promised: Option<Ballot>,
        accepted: Option<AcceptedSummary>,
        decided: Option<DecisionRecord>,
        command: Option<StoredCommand>,
    ) -> RecorderReply {
        RecorderReply {
            recorder_id: self.id.clone(),
            slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: 1,
            highest_promised,
            accepted,
            decided,
            command,
        }
    }
}

impl RecorderRpc for OrderingRecorder {
    fn call(&self, request: RecorderRequest) -> queqlite_quepaxa::Result<RecorderReply> {
        assert_test_recorder_context(&request);
        match request {
            RecorderRequest::Identity => Ok(self.reply(0, None, None, None, None)),
            RecorderRequest::StoreCommand {
                command_hash,
                command,
                ..
            } => {
                assert_eq!(command.hash(), command_hash);
                self.commands.lock().unwrap().insert(command_hash, command);
                self.calls.lock().unwrap().push("store");
                Ok(self.reply(0, None, None, None, None))
            }
            RecorderRequest::FetchCommand { command_hash, .. } => Ok(self.reply(
                0,
                None,
                None,
                None,
                self.commands.lock().unwrap().get(&command_hash).cloned(),
            )),
            RecorderRequest::Observe { slot, ballot, .. } => {
                self.calls.lock().unwrap().push("observe");
                Ok(self.reply(slot, Some(ballot), None, None, None))
            }
            RecorderRequest::Converge {
                slot,
                ballot,
                value,
                ..
            } => {
                assert!(self
                    .commands
                    .lock()
                    .unwrap()
                    .contains_key(&value.command_hash));
                self.calls.lock().unwrap().push("observe");
                Ok(self.reply(
                    slot,
                    Some(ballot.clone()),
                    Some(AcceptedSummary { ballot, value }),
                    None,
                    None,
                ))
            }
            RecorderRequest::Decide { slot, decision, .. } => {
                Ok(self.reply(slot, None, None, Some(decision), None))
            }
            RecorderRequest::Inspect { slot, .. } => Ok(self.reply(slot, None, None, None, None)),
        }
    }

    fn record(&self, request: RecordRequest) -> queqlite_quepaxa::Result<RecordSummary> {
        if let Some(command) = request.command.as_ref() {
            let value = request.proposal.value.as_ref().unwrap();
            assert_eq!(command.hash(), value.command_hash);
            self.commands
                .lock()
                .unwrap()
                .insert(command.hash(), command.clone());
            self.calls.lock().unwrap().push("piggyback");
        }
        assert!(request.proposal.value.as_ref().is_none_or(|value| self
            .commands
            .lock()
            .unwrap()
            .contains_key(&value.command_hash)));
        self.calls.lock().unwrap().push("record");
        let mut states = self.isr.lock().unwrap();
        let current = states.entry(request.slot).or_default().clone();
        let (next, reply) = current.record(request.step, request.proposal);
        states.insert(request.slot, next);
        Ok(RecordSummary {
            recorder_id: self.id.clone(),
            slot: request.slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: reply.step,
            first_current: reply.first_current,
            aggregate_prior: reply.aggregate_prior,
            decided: self.proofs.lock().unwrap().get(&request.slot).cloned(),
        })
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> queqlite_quepaxa::Result<()> {
        let slot = match &proof {
            DecisionProof::FastPath { slot, .. } | DecisionProof::Phase2 { slot, .. } => *slot,
        };
        self.proofs.lock().unwrap().insert(slot, proof);
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> queqlite_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.proofs.lock().unwrap().get(&slot).cloned())
    }
}

#[derive(Clone)]
struct BlockingLogPeer {
    gate: Arc<Gate>,
}

impl LogPeer for BlockingLogPeer {
    fn fetch_log(&self, _request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        self.gate.wait();
        Ok(FetchLogResponse {
            entries: Vec::new(),
            last_index: 0,
        })
    }
}

#[derive(Clone)]
struct FailingRecorder {
    error: queqlite_quepaxa::Error,
}

impl RecorderRpc for FailingRecorder {
    fn call(&self, _request: RecorderRequest) -> queqlite_quepaxa::Result<RecorderReply> {
        Err(self.error.clone())
    }
}

#[derive(Clone)]
struct FailingLogPeer {
    error: FetchLogError,
}

impl LogPeer for FailingLogPeer {
    fn fetch_log(&self, _request: FetchLogRequest) -> Result<FetchLogResponse, FetchLogError> {
        Err(self.error.clone())
    }
}

#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    started: bool,
    released: bool,
}

impl Gate {
    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.started = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn started(&self) -> bool {
        self.state.lock().unwrap().started
    }

    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}

struct GatedRecorder {
    id: String,
    commands: Mutex<HashMap<LogHash, StoredCommand>>,
    isr: Mutex<HashMap<u64, IsrState>>,
    proofs: Mutex<HashMap<u64, DecisionProof>>,
    gate: Arc<Gate>,
}

impl GatedRecorder {
    fn new(id: String, gate: Arc<Gate>) -> Self {
        Self {
            id,
            commands: Mutex::new(HashMap::new()),
            isr: Mutex::new(HashMap::new()),
            proofs: Mutex::new(HashMap::new()),
            gate,
        }
    }

    fn reply(
        &self,
        slot: u64,
        highest_promised: Option<Ballot>,
        accepted: Option<AcceptedSummary>,
        decided: Option<DecisionRecord>,
        command: Option<StoredCommand>,
    ) -> RecorderReply {
        RecorderReply {
            recorder_id: self.id.clone(),
            slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: 1,
            highest_promised,
            accepted,
            decided,
            command,
        }
    }
}

impl RecorderRpc for GatedRecorder {
    fn call(&self, request: RecorderRequest) -> queqlite_quepaxa::Result<RecorderReply> {
        assert_test_recorder_context(&request);
        match request {
            RecorderRequest::Identity => Ok(self.reply(0, None, None, None, None)),
            RecorderRequest::StoreCommand {
                command_hash,
                command,
                ..
            } => {
                self.gate.wait();
                self.commands.lock().unwrap().insert(command_hash, command);
                Ok(self.reply(0, None, None, None, None))
            }
            RecorderRequest::FetchCommand { command_hash, .. } => Ok(self.reply(
                0,
                None,
                None,
                None,
                self.commands.lock().unwrap().get(&command_hash).cloned(),
            )),
            RecorderRequest::Inspect { slot, .. } => Ok(self.reply(slot, None, None, None, None)),
            RecorderRequest::Observe { slot, ballot, .. } => {
                Ok(self.reply(slot, Some(ballot), None, None, None))
            }
            RecorderRequest::Converge {
                slot,
                ballot,
                value,
                ..
            } => Ok(self.reply(
                slot,
                Some(ballot.clone()),
                Some(AcceptedSummary { ballot, value }),
                None,
                None,
            )),
            RecorderRequest::Decide { slot, decision, .. } => {
                Ok(self.reply(slot, None, None, Some(decision), None))
            }
        }
    }

    fn record(&self, request: RecordRequest) -> queqlite_quepaxa::Result<RecordSummary> {
        if let Some(command) = request.command.as_ref() {
            let value = request.proposal.value.as_ref().unwrap();
            assert_eq!(command.hash(), value.command_hash);
            self.commands
                .lock()
                .unwrap()
                .insert(command.hash(), command.clone());
        }
        self.gate.wait();
        let mut states = self.isr.lock().unwrap();
        let current = states.entry(request.slot).or_default().clone();
        let (next, reply) = current.record(request.step, request.proposal);
        states.insert(request.slot, next);
        Ok(RecordSummary {
            recorder_id: self.id.clone(),
            slot: request.slot,
            config_id: 1,
            config_digest: test_config_digest(),
            step: reply.step,
            first_current: reply.first_current,
            aggregate_prior: reply.aggregate_prior,
            decided: self.proofs.lock().unwrap().get(&request.slot).cloned(),
        })
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> queqlite_quepaxa::Result<()> {
        let slot = match &proof {
            DecisionProof::FastPath { slot, .. } | DecisionProof::Phase2 { slot, .. } => *slot,
        };
        self.proofs.lock().unwrap().insert(slot, proof);
        Ok(())
    }

    fn inspect_decision_proof(&self, slot: u64) -> queqlite_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.proofs.lock().unwrap().get(&slot).cloned())
    }
}

#[test]
fn in_memory_peer_still_supports_bounded_happy_path() {
    let first = entry(1, LogHash::ZERO, b"put\tr1\talpha\tone");
    let second = entry(2, first.hash, b"put\tr2\talpha\ttwo");
    let peer = InMemoryLogPeer::new(vec![first.clone(), second.clone()]);

    assert_eq!(
        catch_up_missing_entries(1, first.hash, "cluster-a", 1, 1, &peer, 10).unwrap(),
        vec![second]
    );
}
