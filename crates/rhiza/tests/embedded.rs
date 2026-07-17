#[cfg(feature = "graph")]
use std::collections::BTreeMap;
use std::{
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

#[cfg(feature = "kv")]
use rhiza::KvCommandResultV1;
use rhiza::{
    effective_cluster_id, BatchWriteError, CheckpointCoordinator, DurabilityHealth, DurabilityMode,
    EmbeddedConfig, EmbeddedIdentity, Error, ExecutionProfile, NodeError, ReadConsistency,
    RecorderRpc, Rhiza, SqlCommand, SqlStatement, SqlValue,
};
#[cfg(feature = "graph")]
use rhiza::{
    GraphCommandResultV1, GraphCommandV1, GraphParameterValue, GraphResultValue, GraphValueV1,
};
use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    DecisionProof, Membership, RecordRequest, RecordSummary, RecorderFileStore, RecorderReply,
};

#[tokio::test(flavor = "multi_thread")]
async fn executes_and_queries_sql_with_in_process_recorders() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();

    handle
        .execute_sql(SqlCommand {
            request_id: "schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let insert = SqlCommand {
        request_id: "insert".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2) RETURNING id, name".into(),
            parameters: vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
        }],
    };
    let first = handle.execute_sql(insert.clone()).await.unwrap();
    let replay = handle.execute_sql(insert).await.unwrap();

    assert_eq!(replay, first);
    assert_eq!(
        first.results[0].returning.as_ref().unwrap().rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );

    let result = handle
        .query(
            SqlStatement {
                sql: "SELECT id, name FROM items".into(),
                parameters: vec![],
            },
            ReadConsistency::Local,
            10,
        )
        .await
        .unwrap();

    assert_eq!(result.columns, ["id", "name"]);
    assert_eq!(
        result.rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );
    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_sql_batch_coalesces_in_order_and_retries_unchanged_vector() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();
    handle
        .execute_sql(SqlCommand {
            request_id: "batch-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE batch_items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let commands = (1..=3)
        .map(|id| SqlCommand {
            request_id: format!("batch-insert-{id}"),
            statements: vec![SqlStatement {
                sql: "INSERT INTO batch_items(id, name) VALUES (?1, ?2) RETURNING id".into(),
                parameters: vec![SqlValue::Integer(id), SqlValue::Text(format!("name-{id}"))],
            }],
        })
        .collect::<Vec<_>>();

    let first = handle.execute_sql_batch(commands.clone()).await.unwrap();
    let replay = handle.execute_sql_batch(commands).await.unwrap();

    assert_eq!(first, replay);
    assert!(first.iter().all(Result::is_ok));
    assert!(first
        .iter()
        .map(|result| result.as_ref().unwrap().applied_index)
        .all(|index| index == 2));
    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn embedded_sql_batch_preflight_failure_is_not_attempted() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();
    let statement = SqlStatement {
        sql: "CREATE TABLE batch_preflight(id INTEGER PRIMARY KEY)".into(),
        parameters: vec![],
    };

    let error = handle
        .execute_sql_batch(vec![
            SqlCommand {
                request_id: "would-be-valid".into(),
                statements: vec![statement.clone()],
            },
            SqlCommand {
                request_id: String::new(),
                statements: vec![statement.clone()],
            },
        ])
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        BatchWriteError::NotAttempted(Error::Node(NodeError::InvalidRequest(_)))
    ));
    handle
        .execute_sql(SqlCommand {
            request_id: "after-preflight".into(),
            statements: vec![statement],
        })
        .await
        .unwrap();
    rhiza.shutdown().await.unwrap();
}

#[cfg(feature = "graph")]
#[tokio::test(flavor = "multi_thread")]
async fn graph_profile_executes_semantic_writes_and_read_only_queries_in_process() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config_for_profile(root.path(), ExecutionProfile::Graph))
        .await
        .unwrap();
    let handle = rhiza.handle();

    let write = handle
        .mutate_graph(
            GraphCommandV1::put_document(
                "graph-put",
                "document-1",
                GraphValueV1::String("Ada".into()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        write.result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );

    let result = handle
        .query_graph(
            "MATCH (d:RhizaDocument) WHERE d.id = $id RETURN d.string_value AS value",
            BTreeMap::from([(
                "id".into(),
                GraphParameterValue::String("document-1".into()),
            )]),
            ReadConsistency::Local,
            10,
        )
        .await
        .unwrap();
    assert_eq!(result.rows, [vec![GraphResultValue::String("Ada".into())]]);

    assert!(matches!(
        handle
            .execute_sql(SqlCommand {
                request_id: "wrong-profile".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE forbidden(id INTEGER)".into(),
                    parameters: vec![],
                }],
            })
            .await,
        Err(Error::ExecutionProfileMismatch {
            expected: ExecutionProfile::Sqlite,
            actual: ExecutionProfile::Graph,
        })
    ));

    assert!(matches!(
        handle
            .execute_sql_batch(vec![SqlCommand {
                request_id: "wrong-profile-batch".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE forbidden_batch(id INTEGER)".into(),
                    parameters: vec![],
                }],
            }])
            .await,
        Err(BatchWriteError::NotAttempted(
            Error::ExecutionProfileMismatch {
                expected: ExecutionProfile::Sqlite,
                actual: ExecutionProfile::Graph,
            }
        ))
    ));

    rhiza.shutdown().await.unwrap();
}

#[cfg(feature = "kv")]
#[tokio::test(flavor = "multi_thread")]
async fn kv_profile_puts_gets_scans_and_deletes_in_process() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config_for_profile(root.path(), ExecutionProfile::Kv))
        .await
        .unwrap();
    let handle = rhiza.handle();

    let first = handle
        .put_kv("kv-put-a", b"a".to_vec(), b"one".to_vec())
        .await
        .unwrap();
    assert_eq!(first.result(), &KvCommandResultV1::Put { replaced: false });
    handle
        .put_kv("kv-put-aa", b"aa".to_vec(), b"two".to_vec())
        .await
        .unwrap();

    let get = handle.get_kv(b"a", ReadConsistency::Local).await.unwrap();
    assert_eq!(get.value, Some(b"one".to_vec()));

    let scan = handle
        .scan_kv_prefix(b"a", 10, None, ReadConsistency::Local)
        .await
        .unwrap();
    assert_eq!(
        scan.rows()
            .iter()
            .map(|row| (row.key(), row.value()))
            .collect::<Vec<_>>(),
        vec![
            (b"a".as_slice(), b"one".as_slice()),
            (b"aa".as_slice(), b"two".as_slice()),
        ]
    );

    let deleted = handle.delete_kv("kv-delete", b"a".to_vec()).await.unwrap();
    assert_eq!(
        deleted.result(),
        &KvCommandResultV1::Delete { existed: true }
    );
    assert_eq!(
        handle
            .get_kv(b"a", ReadConsistency::Local)
            .await
            .unwrap()
            .value,
        None
    );

    rhiza.shutdown().await.unwrap();
}

#[cfg(feature = "graph")]
#[tokio::test(flavor = "multi_thread")]
async fn graph_sync_write_waits_for_transient_archive_recovery() {
    sync_embedded_profile_write_waits_for_transient_archive_recovery(ExecutionProfile::Graph).await;
}

#[cfg(feature = "kv")]
#[tokio::test(flavor = "multi_thread")]
async fn kv_sync_write_waits_for_transient_archive_recovery() {
    sync_embedded_profile_write_waits_for_transient_archive_recovery(ExecutionProfile::Kv).await;
}

#[cfg(any(feature = "graph", feature = "kv"))]
async fn sync_embedded_profile_write_waits_for_transient_archive_recovery(
    profile: ExecutionProfile,
) {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive_backup = root.path().join("archive-backup");
    let archive = initialized_profile_checkpoint(&archive_root, profile).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let mut config = config_for_profile(root.path(), profile);
    config.coordinator = Some(coordinator.clone());
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    std::fs::rename(&archive_root, &archive_backup).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();
    let write = tokio::spawn(async move {
        match profile {
            #[cfg(feature = "graph")]
            ExecutionProfile::Graph => handle
                .mutate_graph(
                    GraphCommandV1::put_document(
                        "transient-archive",
                        "document-1",
                        GraphValueV1::String("value".into()),
                    )
                    .unwrap(),
                )
                .await
                .map(|_| ()),
            #[cfg(feature = "kv")]
            ExecutionProfile::Kv => handle
                .put_kv("transient-archive", b"key".to_vec(), b"value".to_vec())
                .await
                .map(|_| ()),
            _ => unreachable!("helper is only called for an enabled embedded profile"),
        }
    });

    tokio::time::timeout(Duration::from_secs(2), async {
        while coordinator.checkpoint_publication_attempts() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sync durability must attempt archive publication");
    assert!(
        !write.is_finished(),
        "a transient archive failure must not finish an acknowledged sync write"
    );

    std::fs::remove_file(&archive_root).unwrap();
    std::fs::rename(&archive_backup, &archive_root).unwrap();
    tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("sync write must resume after archive recovery")
        .unwrap()
        .unwrap();
    assert_eq!(coordinator.health(), DurabilityHealth::Available);
    rhiza.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_is_closed_after_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();

    rhiza.shutdown().await.unwrap();

    assert!(matches!(
        handle.put("request", "key", "value").await,
        Err(Error::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_preserves_sql_and_idempotent_returning_results() {
    let root = tempfile::tempdir().unwrap();
    let rhiza = Rhiza::open(config(root.path())).await.unwrap();
    let handle = rhiza.handle();
    handle
        .execute_sql(SqlCommand {
            request_id: "schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .await
        .unwrap();
    let insert = SqlCommand {
        request_id: "insert".into(),
        statements: vec![SqlStatement {
            sql: "INSERT INTO items(id, name) VALUES (?1, ?2) RETURNING id, name".into(),
            parameters: vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())],
        }],
    };
    let first = handle.execute_sql(insert.clone()).await.unwrap();
    rhiza.shutdown().await.unwrap();

    let reopened = Rhiza::open(config(root.path())).await.unwrap();
    let handle = reopened.handle();
    let replay = handle.execute_sql(insert).await.unwrap();

    assert_eq!(replay, first);
    assert_eq!(
        handle
            .query(
                SqlStatement {
                    sql: "SELECT id, name FROM items".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                10,
            )
            .await
            .unwrap()
            .rows,
        [vec![SqlValue::Integer(7), SqlValue::Text("Ada".into())]]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_cancels_a_sync_write_blocked_on_checkpoint_storage() {
    const OUTER_HANG_GUARD: Duration = Duration::from_secs(10);
    const BEHAVIOR_DEADLINE: Duration = Duration::from_secs(1);

    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let mut config = config(root.path());
    config.coordinator = Some(coordinator.clone());
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();
    let status_handle = handle.clone();
    std::fs::remove_dir_all(&archive_root).unwrap();
    std::fs::write(&archive_root, b"archive unavailable").unwrap();

    let retry_cap_attempt = coordinator
        .checkpoint_publication_attempts()
        .checked_add(7)
        .unwrap();
    let write = tokio::spawn(async move { handle.put("request", "key", "value").await });
    tokio::time::timeout(OUTER_HANG_GUARD, async {
        while coordinator.checkpoint_publication_attempts() < retry_cap_attempt {
            assert!(
                !write.is_finished(),
                "the sync write finished before reaching the capped retry backoff"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint publication retries must not hang the test");
    tokio::time::timeout(BEHAVIOR_DEADLINE, async {
        while coordinator.health() != DurabilityHealth::Unavailable {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint storage failure must make durability unavailable");
    assert!(!status_handle.status().await.unwrap().ready);

    let shutdown = tokio::time::timeout(BEHAVIOR_DEADLINE, rhiza.shutdown())
        .await
        .expect("shutdown must not wait forever for the blocked write");
    assert!(shutdown.is_err());
    assert!(write.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_a_minority_rpc_before_releasing_storage() {
    let root = tempfile::tempdir().unwrap();
    let (blocked_config, started, release) = config_with_blocked_minority(root.path());
    let rhiza = Rhiza::open(blocked_config).await.unwrap();
    let handle = rhiza.handle();

    handle.put("request", "key", "value").await.unwrap();
    tokio::task::spawn_blocking(move || started.recv().unwrap())
        .await
        .unwrap();

    let status = handle.clone();
    let shutdown = tokio::spawn(rhiza.shutdown());
    loop {
        match status.status().await {
            Err(Error::Closed) => break,
            Ok(_) => tokio::task::yield_now().await,
            Err(error) => panic!("unexpected status error during shutdown: {error}"),
        }
    }
    assert!(
        !shutdown.is_finished(),
        "shutdown released runtime storage while a minority RPC was still running"
    );

    release.release();
    shutdown.await.unwrap().unwrap();

    let reopened = Rhiza::open(config(root.path())).await.unwrap();
    reopened.shutdown().await.unwrap();
    root.close().unwrap();
}

#[test]
fn shutdown_consensus_drain_is_not_queued_behind_a_saturated_blocking_pool() {
    const HANG_GUARD: Duration = Duration::from_secs(10);

    let root = tempfile::tempdir().unwrap();
    let root_path = root.path().to_path_buf();
    let (release_blocker_tx, release_blocker_rx) = mpsc::channel();
    let (saturated_tx, saturated_rx) = mpsc::channel();
    let (shutdown_finished_tx, shutdown_finished_rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let executor = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        executor.block_on(async move {
            let rhiza = Rhiza::open(config(&root_path)).await.unwrap();
            let (blocker_started_tx, blocker_started_rx) = mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                blocker_started_tx.send(()).unwrap();
                release_blocker_rx.recv().unwrap();
            });
            blocker_started_rx.recv().unwrap();
            saturated_tx.send(()).unwrap();
            shutdown_finished_tx.send(rhiza.shutdown().await).unwrap();
            blocker.await.unwrap();
        });
    });
    saturated_rx
        .recv_timeout(HANG_GUARD)
        .expect("blocking-pool saturation must be established");

    let result = shutdown_finished_rx.recv_timeout(HANG_GUARD);
    release_blocker_tx.send(()).unwrap();
    worker.join().unwrap();

    result
        .expect("shutdown consensus drain must start despite blocking-pool saturation")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn open_rejects_recorder_membership_before_creating_runtime_storage() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.members = vec!["node-1".into(), "node-2".into(), "node-4".into()];

    assert!(matches!(
        Rhiza::open(config).await,
        Err(Error::Config(
            rhiza_node::ConfigError::PeerMembershipMismatch
        ))
    ));
    assert!(!root.path().join("node").exists());
}

fn config(root: &std::path::Path) -> EmbeddedConfig {
    config_for_profile(root, ExecutionProfile::Sqlite)
}

fn config_for_profile(
    root: &std::path::Path,
    execution_profile: ExecutionProfile,
) -> EmbeddedConfig {
    let identity = EmbeddedIdentity::new("cluster-a", "node-1", 1, 1);
    let recorder_cluster_id = effective_cluster_id(execution_profile, "cluster-a").unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                &recorder_cluster_id,
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect();
    EmbeddedConfig::new(
        identity,
        root.join("node"),
        execution_profile,
        membership.members().to_vec(),
        recorders,
        vec![],
        None,
    )
}

fn config_with_blocked_minority(
    root: &std::path::Path,
) -> (EmbeddedConfig, mpsc::Receiver<()>, BlockingRelease) {
    let identity = EmbeddedIdentity::new("cluster-a", "node-1", 1, 1);
    let recorder_cluster_id = effective_cluster_id(ExecutionProfile::Sqlite, "cluster-a").unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let release = BlockingRelease::default();
    let recorders = membership
        .members()
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                &recorder_cluster_id,
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            let recorder: Box<dyn RecorderRpc> = if index == 2 {
                Box::new(BlockingRecorder {
                    inner: recorder,
                    started: started_tx.clone(),
                    release: release.clone(),
                })
            } else {
                Box::new(recorder)
            };
            (id.clone(), recorder)
        })
        .collect();
    (
        EmbeddedConfig::new(
            identity,
            root.join("node"),
            ExecutionProfile::Sqlite,
            membership.members().to_vec(),
            recorders,
            vec![],
            None,
        ),
        started_rx,
        release,
    )
}

#[derive(Clone, Default)]
struct BlockingRelease(Arc<(Mutex<bool>, Condvar)>);

impl BlockingRelease {
    fn wait(&self) {
        let (released, condition) = &*self.0;
        let mut released = released.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
    }

    fn release(&self) {
        let (released, condition) = &*self.0;
        *released.lock().unwrap() = true;
        condition.notify_all();
    }
}

struct BlockingRecorder {
    inner: RecorderFileStore,
    started: mpsc::Sender<()>,
    release: BlockingRelease,
}

impl RecorderRpc for BlockingRecorder {
    fn call(
        &self,
        request: rhiza_quepaxa::RecorderRequest,
    ) -> rhiza_quepaxa::Result<RecorderReply> {
        self.inner.call(request)
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        let _ = self.started.send(());
        self.release.wait();
        self.inner.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.inner.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.inner.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.inner.inspect_record_summary(slot)
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

async fn initialized_checkpoint(root: &std::path::Path) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new("cluster-a", 1, 1, 1),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

#[cfg(any(feature = "graph", feature = "kv"))]
async fn initialized_profile_checkpoint(
    root: &std::path::Path,
    profile: ExecutionProfile,
) -> ObjectArchiveStore {
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.to_path_buf(),
    })
    .unwrap();
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new(effective_cluster_id(profile, "cluster-a").unwrap(), 1, 1, 1),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}
