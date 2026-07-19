#![cfg(feature = "kv")]

use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{ExecutionProfile, LogHash};
use rhiza_kv::{encode_replicated_kv_batch, encode_replicated_kv_command, MAX_KV_VALUE_BYTES};
use rhiza_log::LogStore;
use rhiza_node::{
    node_router, node_router_with_checkpoint_and_limits, CheckpointCoordinator,
    ClientErrorResponse, DurabilityMode, KvCommandResultV1, KvCommandV1, KvGetResponse,
    KvMutationResponse, KvScanResponse, NodeConfig, NodeRuntime, PeerConfig, ReadConsistency,
    KV_GET_PATH, KV_PUT_PATH, KV_SCAN_PATH, MAX_COMMAND_BYTES, MAX_KV_SCAN_ROWS, PROTOCOL_VERSION,
    READYZ_PATH, VERSION_HEADER,
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
    let reopened_read = reopened
        .get_kv(b"key", ReadConsistency::ReadBarrier)
        .unwrap();
    assert_eq!(reopened_read.value, Some(b"value".to_vec()));
    assert_eq!(
        (reopened_read.applied_index, reopened_read.hash),
        (written.applied_index(), written.hash())
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
    let written = runtime
        .mutate_kv(KvCommandV1::put("request-1", b"key".to_vec(), b"value".to_vec()).unwrap())
        .unwrap();

    let read = runtime
        .get_kv(b"key", ReadConsistency::ReadBarrier)
        .unwrap();

    assert_eq!(read.value, Some(b"value".to_vec()));
    assert_eq!(
        (read.applied_index, read.hash),
        (written.applied_index(), written.hash())
    );
}

#[test]
fn kv_scan_pages_ranges_and_prefixes_with_the_exact_snapshot_tip() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = NodeRuntime::open(
        kv_config(dir.path()),
        consensus(dir.path(), "recorders"),
        &[],
    )
    .unwrap();
    for (request, key, value) in [
        ("put-a", b"a".as_slice(), b"one".as_slice()),
        ("put-aa", b"aa".as_slice(), b"two".as_slice()),
        ("put-b", b"b".as_slice(), b"three".as_slice()),
    ] {
        runtime
            .mutate_kv(KvCommandV1::put(request, key.to_vec(), value.to_vec()).unwrap())
            .unwrap();
    }

    let first = runtime
        .scan_kv_range(b"a", Some(b"b"), 1, None, ReadConsistency::Local)
        .unwrap();
    assert_eq!(first.rows()[0].key(), b"a");
    assert_eq!(first.next_cursor(), Some(b"a".as_slice()));
    assert_eq!(first.tip().applied_index(), 3);
    assert_eq!(first.tip().applied_hash(), runtime.applied_hash().unwrap());

    let second = runtime
        .scan_kv_range(
            b"a",
            Some(b"b"),
            1,
            first.next_cursor(),
            ReadConsistency::AppliedIndex(first.tip().applied_index()),
        )
        .unwrap();
    assert_eq!(second.rows()[0].key(), b"aa");
    assert_eq!(second.next_cursor(), None);

    let prefix = runtime
        .scan_kv_prefix(b"a", 10, None, ReadConsistency::ReadBarrier)
        .unwrap();
    assert_eq!(
        prefix
            .rows()
            .iter()
            .map(|entry| entry.key())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"aa".as_slice()]
    );
    assert_eq!(prefix.tip().applied_index(), 3);
    assert_eq!(prefix.tip().applied_hash(), runtime.applied_hash().unwrap());
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
    assert_eq!(get.applied_index, 1);
    assert_eq!(get.hash, put.hash);

    let second_put = client
        .post(&put_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "request_id": "request-2",
            "key": "a2V5Mg==",
            "value": "dmFsdWUy"
        }))
        .send()
        .await
        .unwrap();
    assert!(second_put.status().is_success());

    let scan_url = format!("http://{addr}{KV_SCAN_PATH}");
    let scan = client
        .post(&scan_url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({
            "prefix": "a2V5",
            "limit": 1,
            "consistency": "local"
        }))
        .send()
        .await
        .unwrap();
    assert!(scan.status().is_success());
    let scan = scan.json::<KvScanResponse>().await.unwrap();
    assert_eq!(scan.entries[0].key, "a2V5");
    assert_eq!(scan.entries[0].value, "dmFsdWU=");
    assert_eq!(scan.next_cursor.as_deref(), Some("a2V5"));
    assert_eq!(scan.applied_index, 2);

    for invalid in [
        serde_json::json!({"prefix":"a2V5", "start":"aQ=="}),
        serde_json::json!({"start":"aQ==", "cursor":"***"}),
        serde_json::json!({"prefix":"a2V5", "limit":0}),
        serde_json::json!({"prefix":"a2V5", "limit":MAX_KV_SCAN_ROWS + 1}),
    ] {
        let response = client
            .post(&scan_url)
            .header(VERSION_HEADER, PROTOCOL_VERSION)
            .bearer_auth("client-token")
            .json(&invalid)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(
            response.json::<ClientErrorResponse>().await.unwrap().code,
            "invalid_request"
        );
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn largest_node_valid_kv_record_scans_without_latching_readiness() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(
            kv_http_config(dir.path()),
            consensus(dir.path(), "largest-recorders"),
            &[],
        )
        .unwrap(),
    );
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("largest-http-recorder"),
        "n1",
        CLUSTER_ID,
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = Arc::clone(&runtime);
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let request_id = "largest-http";
    let key = b"k";
    let value = vec![b'v'; MAX_KV_VALUE_BYTES];
    let command = KvCommandV1::put(request_id, key.to_vec(), value.clone()).unwrap();
    assert_eq!(value.len(), MAX_KV_VALUE_BYTES);
    assert!(encode_replicated_kv_command(&command).unwrap().len() <= MAX_COMMAND_BYTES);
    let client = reqwest::Client::new();

    let put = post_kv_put(
        &client,
        addr,
        &serde_json::json!({
            "request_id": request_id,
            "key": encode_base64(key),
            "value": encode_base64(&value)
        }),
    )
    .await;
    assert!(
        put.status().is_success(),
        "put failed: {}",
        put.text().await.unwrap()
    );

    let scan = client
        .post(format!("http://{addr}{KV_SCAN_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&serde_json::json!({"prefix": "", "limit": 1}))
        .send()
        .await
        .unwrap();
    assert!(
        scan.status().is_success(),
        "scan failed: {}",
        scan.text().await.unwrap()
    );
    let ready = client
        .get(format!("http://{addr}{READYZ_PATH}"))
        .send()
        .await
        .unwrap();
    assert!(ready.status().is_success());
    assert!(runtime.is_ready());
    assert!(!runtime.is_fatal());
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_kv_writes_share_one_entry_and_retry_distinct_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let config = kv_http_config(dir.path())
        .with_writer_batching(8, Duration::from_millis(20))
        .unwrap();
    let runtime =
        Arc::new(NodeRuntime::open(config, consensus(dir.path(), "batch-recorders"), &[]).unwrap());
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("batch-http-recorder"),
        "n1",
        CLUSTER_ID,
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = Arc::clone(&runtime);
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let first_body = serde_json::json!({
        "request_id": "batch-a",
        "key": "c2hhcmVk",
        "value": "Zmlyc3Q="
    });
    let second_body = serde_json::json!({
        "request_id": "batch-b",
        "key": "c2hhcmVk",
        "value": "c2Vjb25k"
    });

    let (first, second) = tokio::join!(
        post_kv_put(&client, addr, &first_body),
        post_kv_put(&client, addr, &second_body)
    );
    let first = first.json::<KvMutationResponse>().await.unwrap();
    let second = second.json::<KvMutationResponse>().await.unwrap();

    assert_eq!(first.applied_index, second.applied_index);
    assert_eq!(first.hash, second.hash);
    assert_ne!(first.result, second.result);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));

    let first_retry = post_kv_put(&client, addr, &first_body)
        .await
        .json::<KvMutationResponse>()
        .await
        .unwrap();
    let second_retry = post_kv_put(&client, addr, &second_body)
        .await
        .json::<KvMutationResponse>()
        .await
        .unwrap();
    assert_eq!(first_retry, first);
    assert_eq!(second_retry, second);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));

    let conflict = post_kv_put(
        &client,
        addr,
        &serde_json::json!({
            "request_id": "batch-a",
            "key": "c2hhcmVk",
            "value": "Y29uZmxpY3Q="
        }),
    )
    .await;
    assert_eq!(conflict.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        conflict.json::<ClientErrorResponse>().await.unwrap().code,
        "invalid_request"
    );
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(1));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn kv_batch_byte_cap_uses_largest_ordered_fitting_sub_batches() {
    let dir = tempfile::tempdir().unwrap();
    let config = kv_http_config(dir.path())
        .with_writer_batching(4, Duration::from_millis(50))
        .unwrap();
    let runtime = Arc::new(
        NodeRuntime::open(config, consensus(dir.path(), "byte-cap-recorders"), &[]).unwrap(),
    );
    let recorder = RecorderFileStore::new_with_id(
        dir.path().join("byte-cap-http-recorder"),
        "n1",
        CLUSTER_ID,
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served_runtime = Arc::clone(&runtime);
    let server = tokio::spawn(async move {
        axum::serve(listener, node_router(served_runtime, recorder))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    let value = vec![0; 130 * 1024];
    let commands = (0..4)
        .map(|index| {
            KvCommandV1::put(
                format!("large-{index}"),
                format!("key-{index}").into_bytes(),
                value.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert!(commands
        .iter()
        .all(|command| encode_replicated_kv_command(command).unwrap().len() <= MAX_COMMAND_BYTES));
    assert!(encode_replicated_kv_batch(&commands).unwrap().len() > MAX_COMMAND_BYTES);
    assert!(encode_replicated_kv_batch(&commands[..3]).unwrap().len() <= MAX_COMMAND_BYTES);

    let encoded_value = "A".repeat(value.len() / 3 * 4);
    let encoded_keys = ["a2V5LTA=", "a2V5LTE=", "a2V5LTI=", "a2V5LTM="];
    let mut requests = tokio::task::JoinSet::new();
    for (index, key) in encoded_keys.into_iter().enumerate() {
        let client = client.clone();
        let value = encoded_value.clone();
        requests.spawn(async move {
            let response = post_kv_put(
                &client,
                addr,
                &serde_json::json!({
                    "request_id": format!("large-{index}"),
                    "key": key,
                    "value": value
                }),
            )
            .await;
            let status = response.status();
            let body = response.text().await.unwrap();
            (status, body)
        });
    }
    let mut indices = Vec::new();
    while let Some(response) = requests.join_next().await {
        let (status, body) = response.unwrap();
        assert!(status.is_success(), "KV write failed with {status}: {body}");
        indices.push(
            serde_json::from_str::<KvMutationResponse>(&body)
                .unwrap()
                .applied_index,
        );
    }
    indices.sort_unstable();
    indices.dedup();

    assert_eq!(indices, [1, 2]);
    assert_eq!(runtime.log_store().last_index().unwrap(), Some(2));
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

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[usize::from(first >> 2)] as char);
        encoded.push(ALPHABET[usize::from(((first & 0x03) << 4) | (second >> 4))] as char);
        encoded.push(if chunk.len() > 1 {
            ALPHABET[usize::from(((second & 0x0f) << 2) | (third >> 6))] as char
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            ALPHABET[usize::from(third & 0x3f)] as char
        } else {
            '='
        });
    }
    encoded
}
