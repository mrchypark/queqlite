#![cfg(feature = "kv")]

use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{ExecutionProfile, LogHash};
use rhiza_node::{
    node_router, node_router_with_checkpoint_and_limits, CheckpointCoordinator,
    ClientErrorResponse, DurabilityMode, KvCommandResultV1, KvCommandV1, KvGetResponse,
    KvMutationResponse, NodeConfig, NodeRuntime, PeerConfig, ReadConsistency, KV_GET_PATH,
    KV_PUT_PATH, PROTOCOL_VERSION, READYZ_PATH, VERSION_HEADER,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{RecorderFileStore, ThreeNodeConsensus};

const CLUSTER_ID: &str = "rhiza:kv:cluster-a";

#[test]
fn kv_profile_reuses_node_runtime_commit_and_reopen_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let config = kv_config(dir.path());
    let runtime =
        NodeRuntime::open(config.clone(), consensus(dir.path(), "recorders"), &[]).unwrap();

    let written = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();
    let read = runtime.get_kv(b"key", ReadConsistency::Local).unwrap();

    assert_eq!(written.applied_index(), 1);
    assert_eq!(
        written.result(),
        &KvCommandResultV1::Put { replaced: false }
    );
    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index(), written.hash())
    );
    assert_eq!(runtime.config().cluster_id(), CLUSTER_ID);
    drop(runtime);

    let reopened = NodeRuntime::open(config, consensus(dir.path(), "recorders"), &[]).unwrap();
    assert_eq!(
        reopened
            .get_kv(b"key", ReadConsistency::Local)
            .unwrap()
            .value,
        Some(b"value".to_vec())
    );
}

#[test]
fn kv_read_barrier_returns_value_and_tip_from_one_materializer_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        kv_config(dir.path()),
        consensus(dir.path(), "recorders"),
        &[],
    )
    .unwrap();
    runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();

    let read = runtime
        .get_kv(b"key", ReadConsistency::ReadBarrier)
        .unwrap();

    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(read.applied_index, 2);
    assert_eq!(read.hash, runtime.applied_hash().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_http_routes_use_base64_and_map_invalid_input_without_mutating_state() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            kv_http_config(dir.path()),
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
    let put_url = format!("http://{addr}{KV_PUT_PATH}");

    let invalid = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "invalid",
            "key": "***",
            "value": "dmFsdWU="
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        invalid.json::<ClientErrorResponse>().await.unwrap().code,
        "invalid_request"
    );

    let put = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "request-1",
            "key": "a2V5",
            "value": "dmFsdWU="
        }))
        .send()
        .await
        .unwrap();
    assert!(put.status().is_success());
    let put = put.json::<KvMutationResponse>().await.unwrap();
    assert_eq!(put.applied_index, 1);

    let get = client
        .post(format!("http://{addr}{KV_GET_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "key": "a2V5",
            "consistency": "read_barrier"
        }))
        .send()
        .await
        .unwrap();
    assert!(get.status().is_success());
    let get = get.json::<KvGetResponse>().await.unwrap();
    assert_eq!(get.value.as_deref(), Some("dmFsdWU="));
    assert_eq!(get.applied_index, 2);
    assert_ne!(get.hash, put.hash);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_sync_checkpoint_outage_times_out_releases_capacity_and_retries_original_outcome() {
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
            kv_http_config(root.path()),
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
        "key": "a2V5",
        "value": "dmFsdWU="
    });

    let first = post_kv_put(&client, addr, &body).await;

    assert_eq!(first.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        first.json::<ClientErrorResponse>().await.unwrap().code,
        "write_timeout"
    );
    let read = client
        .post(format!("http://{addr}{KV_GET_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({"key": "a2V5", "consistency": "local"}))
        .send()
        .await
        .unwrap();
    assert!(read.status().is_success());
    let original = read.json::<KvGetResponse>().await.unwrap();
    assert_eq!(original.value.as_deref(), Some("dmFsdWU="));

    restore_archive(&archive_root, &archive_backup);
    wait_ready(&client, addr).await;
    let retry = post_kv_put(&client, addr, &body).await;
    assert!(retry.status().is_success());
    let retry = retry.json::<KvMutationResponse>().await.unwrap();
    assert_eq!(
        (retry.applied_index, retry.hash),
        (original.applied_index, original.hash)
    );
    assert_eq!(
        retry.result,
        rhiza_node::KvMutationResultDto::Put { replaced: false }
    );
    server.abort();
}

fn kv_config(root: &Path) -> NodeConfig {
    NodeConfig::new_embedded(
        "cluster-a",
        "n1",
        root.join("node"),
        1,
        1,
        ["n1", "n2", "n3"],
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Kv)
    .unwrap()
}

fn kv_http_config(root: &Path) -> NodeConfig {
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
    .with_execution_profile(ExecutionProfile::Kv)
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

async fn post_kv_put(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    body: &serde_json::Value,
) -> reqwest::Response {
    client
        .post(format!("http://{addr}{KV_PUT_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(body)
        .send()
        .await
        .unwrap()
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
