use std::{net::SocketAddr, path::Path, sync::Arc};

use axum::Router;
use queqlite_archive::{CheckpointIdentity, ObjectArchiveStore};
use queqlite_core::{ConfigurationState, LogAnchor, LogHash};
use queqlite_log::{IndexRange, LogStore};
use queqlite_node::{
    install_successor_recorder, node_router, node_router_with_admin,
    node_router_with_checkpoint_and_admin, AdminActivateRequest, AdminActivateResponse,
    AdminCompactRequest, AdminConfig, AdminInstallSuccessorRequest, AdminInstallSuccessorResponse,
    AdminStatusResponse, AdminStopRequest, AdminStopResponse, AdminSuccessorBundle,
    CheckpointCoordinator, DurabilityMode, NodeConfig, NodeRuntime, PeerConfig,
    RuntimeConfigurationStatus, WriteRequest, ADMIN_ACTIVATE_PATH, ADMIN_COMPACT_PATH,
    ADMIN_INSTALL_SUCCESSOR_PATH, ADMIN_STATUS_PATH, ADMIN_STOP_PATH, PROTOCOL_VERSION,
    VERSION_HEADER, WRITE_PATH,
};
use queqlite_obj_store::{ObjStore, ObjStoreConfig};
use queqlite_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};

const ADMIN_TOKEN: &str = "admin-secret";

#[tokio::test(flavor = "multi_thread")]
async fn admin_authentication_precedes_body_parsing_and_routes_are_optional() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(root.path(), "node", 1, peers(), None);
    let admin_recorder = recorder(root.path(), "admin-recorder", "node-1", 1, old_membership());
    let (addr, server) = serve(node_router_with_admin(
        runtime.clone(),
        admin_recorder,
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;

    let malformed = reqwest::Client::new()
        .post(format!("http://{addr}{ADMIN_STOP_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("wrong-secret")
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(runtime.applied_index().unwrap(), 0);

    let authorized_malformed = reqwest::Client::new()
        .post(format!("http://{addr}{ADMIN_STOP_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(ADMIN_TOKEN)
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(
        authorized_malformed.status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    server.abort();

    let disabled = node_router(
        runtime,
        recorder(
            root.path(),
            "disabled-recorder",
            "node-1",
            1,
            old_membership(),
        ),
    );
    let (addr, server) = serve(disabled).await;
    let response = reqwest::Client::new()
        .get(format!("http://{addr}{ADMIN_STATUS_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_is_idempotent_conflict_checked_and_closes_old_config_writes() {
    let root = tempfile::tempdir().unwrap();
    let runtime = runtime(root.path(), "node", 1, peers(), None);
    runtime.write("before-stop", "key", "value").unwrap();
    let recorder = recorder(root.path(), "admin-recorder", "node-1", 1, old_membership());
    let (addr, server) = serve(node_router_with_admin(
        runtime.clone(),
        recorder,
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;
    let request = AdminStopRequest {
        operation_id: "stop-001".into(),
        expected_config_id: 1,
        successor: successor_bundle(2, old_membership()),
    };

    let first = admin_post(addr, ADMIN_STOP_PATH, &request).await;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first = first.json::<AdminStopResponse>().await.unwrap();
    let replay = admin_post(addr, ADMIN_STOP_PATH, &request).await;
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    assert_eq!(replay.json::<AdminStopResponse>().await.unwrap(), first);

    let conflict = admin_post(
        addr,
        ADMIN_STOP_PATH,
        &AdminStopRequest {
            operation_id: request.operation_id,
            expected_config_id: 2,
            successor: successor_bundle(3, old_membership()),
        },
    )
    .await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);

    let status = admin_status(addr).await;
    assert_eq!(
        status.node.configuration_status,
        RuntimeConfigurationStatus::Stopped
    );
    let stop_anchor = LogAnchor::new(first.stop.entry.index, first.stop.entry.hash);
    assert_eq!(status.node.stop_anchor, Some(stop_anchor));
    assert_eq!(status.members, old_membership().members());
    assert_eq!(status.qlog_root, stop_anchor);
    assert_eq!(status.stopped_transition.as_ref().unwrap().stop, first.stop);
    assert_eq!(
        status.stopped_transition.as_ref().unwrap().successor,
        first.successor
    );

    let write = reqwest::Client::new()
        .post(format!("http://{addr}{WRITE_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth("client-token")
        .json(&WriteRequest {
            request_id: "after-stop".into(),
            key: "key".into(),
            value: "changed".into(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(write.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(runtime.applied_index().unwrap(), first.stop.entry.index);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_success_and_changed_body_conflict_survive_router_and_runtime_restart() {
    let root = tempfile::tempdir().unwrap();
    let first_runtime = runtime(root.path(), "node", 1, peers(), None);
    let admin_recorder = recorder(root.path(), "admin-recorder", "node-1", 1, old_membership());
    let request = AdminStopRequest {
        operation_id: "stop-restart-001".into(),
        expected_config_id: 1,
        successor: successor_bundle(2, old_membership()),
    };
    let (addr, shutdown, server) = serve_until_shutdown(node_router_with_admin(
        first_runtime.clone(),
        admin_recorder.clone(),
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;
    let first = admin_post(addr, ADMIN_STOP_PATH, &request).await;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first = first.json::<AdminStopResponse>().await.unwrap();
    let _ = shutdown.send(());
    let _ = server.await;
    drop(first_runtime);

    let runtime = runtime(root.path(), "node", 1, peers(), None);
    let (addr, server) = serve(node_router_with_admin(
        runtime,
        admin_recorder,
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;
    let replay = admin_post(addr, ADMIN_STOP_PATH, &request).await;
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    assert_eq!(replay.json::<AdminStopResponse>().await.unwrap(), first);
    let mut changed = request;
    changed.successor.config_id = 3;
    let conflict = admin_post(addr, ADMIN_STOP_PATH, &changed).await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn install_successor_replays_exact_result_and_rejects_changed_bundle() {
    let root = tempfile::tempdir().unwrap();
    let first_runtime = runtime(root.path(), "node", 1, peers(), None);
    let successor = Membership::new(["node-1", "node-2", "node-4"]).unwrap();
    let stop = first_runtime
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    let recorder = recorder(root.path(), "admin-recorder", "node-1", 1, old_membership());
    let (addr, shutdown, server) = serve_until_shutdown(node_router_with_admin(
        first_runtime.clone(),
        recorder.clone(),
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;
    let request = AdminInstallSuccessorRequest {
        operation_id: "install-001".into(),
        expected_config_id: 1,
        expected_stopped_anchor: LogAnchor::new(stop.entry.index, stop.entry.hash),
        old_members: old_membership().members().to_vec(),
        stop: stop.clone(),
        successor: AdminSuccessorBundle {
            config_id: 2,
            members: successor.members().to_vec(),
            digest: successor.digest(),
        },
    };
    let request: AdminInstallSuccessorRequest =
        serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();

    let first = admin_post(addr, ADMIN_INSTALL_SUCCESSOR_PATH, &request).await;
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first = first.json::<AdminInstallSuccessorResponse>().await.unwrap();
    let _ = shutdown.send(());
    let _ = server.await;
    drop(first_runtime);

    let runtime = runtime(root.path(), "node", 1, peers(), None);
    let (addr, server) = serve(node_router_with_admin(
        runtime,
        recorder,
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;
    let replay = admin_post(addr, ADMIN_INSTALL_SUCCESSOR_PATH, &request).await;
    assert_eq!(replay.status(), reqwest::StatusCode::OK);
    assert_eq!(
        replay
            .json::<AdminInstallSuccessorResponse>()
            .await
            .unwrap(),
        first
    );

    let mut changed = request;
    changed.successor.digest = LogHash::ZERO;
    let conflict = admin_post(addr, ADMIN_INSTALL_SUCCESSOR_PATH, &changed).await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stopped_successor_activates_through_live_admin_and_reopens_writes() {
    let root = tempfile::tempdir().unwrap();
    let old_runtime = runtime(root.path(), "node", 1, peers(), None);
    let successor = old_membership();
    let stop = old_runtime
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    let stopped = old_runtime.configuration_state().unwrap();
    drop(old_runtime);

    let recorder_ids = ["node-1", "node-2", "node-3"];
    let mut recorders = Vec::new();
    let mut local = None;
    for id in recorder_ids {
        let recorder = recorder(
            root.path(),
            &format!("successor-{id}"),
            id,
            1,
            old_membership(),
        );
        install_successor_recorder(&recorder, 2, successor.clone(), &stop).unwrap();
        if id == "node-1" {
            local = Some(recorder.clone());
        }
        recorders.push((id.to_string(), Box::new(recorder) as Box<dyn RecorderRpc>));
    }
    let successor_peers = vec![
        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
        PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
    ];
    let config = NodeConfig::new_with_configuration(
        "cluster-a",
        "node-1",
        root.path().join("node"),
        1,
        successor,
        stopped,
        successor_peers,
        "client-token",
    )
    .unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids("cluster-a", "node-1", 1, 2, recorders)
            .unwrap(),
    );
    let runtime = Arc::new(NodeRuntime::open(config, consensus, &[]).unwrap());
    let (addr, server) = serve(node_router_with_admin(
        runtime.clone(),
        local.unwrap(),
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;

    let before = admin_status(addr).await;
    assert_eq!(
        before.node.configuration_status,
        RuntimeConfigurationStatus::AwaitingActivation
    );
    let activate = admin_post(
        addr,
        ADMIN_ACTIVATE_PATH,
        &AdminActivateRequest {
            operation_id: "activate-001".into(),
            expected_config_id: 2,
        },
    )
    .await;
    assert_eq!(activate.status(), reqwest::StatusCode::OK);
    let activated = activate.json::<AdminActivateResponse>().await.unwrap();
    assert_eq!(activated.entry.config_id, 2);
    assert_eq!(
        admin_status(addr).await.node.configuration_status,
        RuntimeConfigurationStatus::Active
    );
    runtime.write("after-activate", "key", "new").unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn compact_archive_failure_preserves_local_qlog() {
    let root = tempfile::tempdir().unwrap();
    let archive_root = root.path().join("archive");
    let archive = initialized_checkpoint(&archive_root).await;
    let coordinator = Arc::new(
        CheckpointCoordinator::open(archive, DurabilityMode::Sync)
            .await
            .unwrap(),
    );
    let runtime = runtime(root.path(), "node", 1, peers(), None);
    runtime.write("request-1", "key", "value").unwrap();
    let expected_root = LogAnchor::new(
        runtime.applied_index().unwrap(),
        runtime.applied_hash().unwrap(),
    );
    let recorder = recorder(root.path(), "admin-recorder", "node-1", 1, old_membership());
    let (addr, server) = serve(node_router_with_checkpoint_and_admin(
        runtime.clone(),
        recorder,
        coordinator,
        AdminConfig::new(ADMIN_TOKEN).unwrap(),
    ))
    .await;
    std::fs::remove_dir_all(&archive_root).unwrap();
    std::fs::write(&archive_root, b"unavailable").unwrap();

    let response = admin_post(
        addr,
        ADMIN_COMPACT_PATH,
        &AdminCompactRequest {
            operation_id: "compact-001".into(),
            expected_config_id: 1,
            expected_recovery_generation: 1,
            expected_root,
        },
    )
    .await;

    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        runtime
            .log_store()
            .read_range(IndexRange::new(1, 1).unwrap())
            .unwrap()
            .len(),
        1
    );
    server.abort();
}

async fn admin_status(addr: SocketAddr) -> AdminStatusResponse {
    let response = reqwest::Client::new()
        .get(format!("http://{addr}{ADMIN_STATUS_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.json().await.unwrap()
}

async fn admin_post<T: serde::Serialize>(
    addr: SocketAddr,
    path: &str,
    request: &T,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(ADMIN_TOKEN)
        .json(request)
        .send()
        .await
        .unwrap()
}

async fn serve(router: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (addr, server)
}

async fn serve_until_shutdown(
    router: Router,
) -> (
    SocketAddr,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown, wait_for_shutdown) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                let _ = wait_for_shutdown.await;
            })
            .await
            .unwrap()
    });
    (addr, shutdown, server)
}

async fn initialized_checkpoint(root: &Path) -> ObjectArchiveStore {
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

fn runtime(
    root: &Path,
    data_dir: &str,
    config_id: u64,
    peers: Vec<PeerConfig>,
    state: Option<ConfigurationState>,
) -> Arc<NodeRuntime> {
    let membership =
        Membership::from_voters(peers.iter().map(|peer| peer.node_id().to_owned())).unwrap();
    let config = match state {
        Some(state) => NodeConfig::new_with_configuration(
            "cluster-a",
            "node-1",
            root.join(data_dir),
            1,
            membership.clone(),
            state,
            peers,
            "client-token",
        ),
        None => NodeConfig::new(
            "cluster-a",
            "node-1",
            root.join(data_dir),
            1,
            config_id,
            peers,
            "client-token",
        ),
    }
    .unwrap();
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            let recorder = recorder(
                root,
                &format!("consensus-{id}"),
                id,
                config_id,
                membership.clone(),
            );
            (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids("cluster-a", "node-1", 1, config_id, recorders)
            .unwrap(),
    );
    Arc::new(NodeRuntime::open(config, consensus, &[]).unwrap())
}

fn recorder(
    root: &Path,
    name: &str,
    id: &str,
    config_id: u64,
    membership: Membership,
) -> RecorderFileStore {
    RecorderFileStore::new_with_membership(
        root.join(name),
        id,
        "cluster-a",
        1,
        config_id,
        membership,
    )
    .unwrap()
}

fn old_membership() -> Membership {
    Membership::new(["node-1", "node-2", "node-3"]).unwrap()
}

fn successor_bundle(config_id: u64, membership: Membership) -> AdminSuccessorBundle {
    AdminSuccessorBundle {
        config_id,
        members: membership.members().to_vec(),
        digest: membership.digest(),
    }
}

fn peers() -> Vec<PeerConfig> {
    vec![
        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
        PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
    ]
}
