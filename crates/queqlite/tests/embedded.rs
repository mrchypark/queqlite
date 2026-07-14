use std::{
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

use queqlite::{
    CheckpointCoordinator, DurabilityHealth, DurabilityMode, EmbeddedConfig, EmbeddedIdentity,
    Error, Queqlite, ReadConsistency, RecorderRpc, SqlCommand, SqlStatement, SqlValue,
};
use queqlite_archive::{CheckpointIdentity, ObjectArchiveStore};
use queqlite_obj_store::{ObjStore, ObjStoreConfig};
use queqlite_quepaxa::{
    DecisionProof, Membership, RecordRequest, RecordSummary, RecorderFileStore, RecorderReply,
};

#[tokio::test(flavor = "multi_thread")]
async fn executes_and_queries_sql_with_in_process_recorders() {
    let root = tempfile::tempdir().unwrap();
    let queqlite = Queqlite::open(config(root.path())).await.unwrap();
    let handle = queqlite.handle();

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
    queqlite.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn handle_is_closed_after_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let queqlite = Queqlite::open(config(root.path())).await.unwrap();
    let handle = queqlite.handle();

    queqlite.shutdown().await.unwrap();

    assert!(matches!(
        handle.put("request", "key", "value").await,
        Err(Error::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_preserves_sql_and_idempotent_returning_results() {
    let root = tempfile::tempdir().unwrap();
    let queqlite = Queqlite::open(config(root.path())).await.unwrap();
    let handle = queqlite.handle();
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
    queqlite.shutdown().await.unwrap();

    let reopened = Queqlite::open(config(root.path())).await.unwrap();
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
    let queqlite = Queqlite::open(config).await.unwrap();
    let handle = queqlite.handle();
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

    let shutdown = tokio::time::timeout(BEHAVIOR_DEADLINE, queqlite.shutdown())
        .await
        .expect("shutdown must not wait forever for the blocked write");
    assert!(shutdown.is_err());
    assert!(write.await.unwrap().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_waits_for_a_minority_rpc_before_releasing_storage() {
    let root = tempfile::tempdir().unwrap();
    let (blocked_config, started, release) = config_with_blocked_minority(root.path());
    let queqlite = Queqlite::open(blocked_config).await.unwrap();
    let handle = queqlite.handle();

    handle.put("request", "key", "value").await.unwrap();
    tokio::task::spawn_blocking(move || started.recv().unwrap())
        .await
        .unwrap();

    let status = handle.clone();
    let shutdown = tokio::spawn(queqlite.shutdown());
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

    let reopened = Queqlite::open(config(root.path())).await.unwrap();
    reopened.shutdown().await.unwrap();
    root.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn open_rejects_recorder_membership_before_creating_runtime_storage() {
    let root = tempfile::tempdir().unwrap();
    let mut config = config(root.path());
    config.members = vec!["node-1".into(), "node-2".into(), "node-4".into()];

    assert!(matches!(
        Queqlite::open(config).await,
        Err(Error::Config(
            queqlite_node::ConfigError::PeerMembershipMismatch
        ))
    ));
    assert!(!root.path().join("node").exists());
}

fn config(root: &std::path::Path) -> EmbeddedConfig {
    let identity = EmbeddedIdentity::new("cluster-a", "node-1", 1, 1);
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                "cluster-a",
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
                "cluster-a",
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
        request: queqlite_quepaxa::RecorderRequest,
    ) -> queqlite_quepaxa::Result<RecorderReply> {
        self.inner.call(request)
    }

    fn record(&self, request: RecordRequest) -> queqlite_quepaxa::Result<RecordSummary> {
        let _ = self.started.send(());
        self.release.wait();
        self.inner.record(request)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> queqlite_quepaxa::Result<()> {
        self.inner.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(&self, slot: u64) -> queqlite_quepaxa::Result<Option<DecisionProof>> {
        self.inner.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(&self, slot: u64) -> queqlite_quepaxa::Result<Option<RecordSummary>> {
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
