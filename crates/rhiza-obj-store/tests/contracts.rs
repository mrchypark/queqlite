use std::{env, process, time::SystemTime};

use rhiza_obj_store::{Error, ObjStore, ObjStoreConfig, UpdateVersion};

const ISOLATED_TEST_CHILD: &str = "RHIZA_ISOLATED_TEST_CHILD";

#[tokio::test]
async fn local_store_put_get_and_list_round_trips_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);

    store.put("cluster-a/log/0001.qlog", b"qlog").await.unwrap();

    assert_eq!(store.get("cluster-a/log/0001.qlog").await.unwrap(), b"qlog");
    assert_eq!(
        store.list("cluster-a/log").await.unwrap(),
        vec!["cluster-a/log/0001.qlog".to_string()]
    );
}

#[tokio::test]
async fn metadata_listing_exposes_stable_delete_identity() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);
    store.put("cluster-a/log/0001.qlog", b"qlog").await.unwrap();

    let listed = store.list_metadata("cluster-a/log").await.unwrap();

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key(), "cluster-a/log/0001.qlog");
    assert_eq!(listed[0].size_bytes(), 4);
    assert!(listed[0].last_modified_ms() > 0);
    assert!(listed[0].version().e_tag().is_some() || listed[0].version().version().is_some());
}

#[tokio::test]
async fn exact_delete_refuses_a_replaced_object_version() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);
    store.put("candidate.qlog", b"old").await.unwrap();
    let listed = store.list_metadata("").await.unwrap();
    store.put("candidate.qlog", b"replacement").await.unwrap();

    assert!(matches!(
        store
            .delete_exact("candidate.qlog", listed[0].version())
            .await,
        Err(Error::Precondition { .. })
    ));
    assert_eq!(store.get("candidate.qlog").await.unwrap(), b"replacement");
}

#[tokio::test]
async fn exact_delete_is_idempotent_after_the_expected_version_is_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);
    store.put("candidate.qlog", b"old").await.unwrap();
    let listed = store.list_metadata("").await.unwrap();

    assert!(store
        .delete_exact("candidate.qlog", listed[0].version())
        .await
        .unwrap());
    assert!(!store
        .delete_exact("candidate.qlog", listed[0].version())
        .await
        .unwrap());
}

#[test]
fn local_store_reports_process_local_compare_and_swap() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);

    assert!(!store.supports_strong_cross_process_cas());
}

#[test]
fn s3_store_reports_strong_cross_process_compare_and_swap() {
    let store = ObjStore::new(ObjStoreConfig::S3 {
        endpoint: Some("http://127.0.0.1:1".to_string()),
        bucket: "test".to_string(),
        access_key: Some("test".to_string()),
        secret_key: Some("test".to_string()),
        region: "us-east-1".to_string(),
        allow_http: true,
    })
    .unwrap();

    assert!(store.supports_strong_cross_process_cas());
}

#[test]
fn s3_store_constructs_without_static_credentials_for_default_credential_chain() {
    if env::var_os(ISOLATED_TEST_CHILD).is_none() {
        run_isolated_test(
            "s3_store_constructs_without_static_credentials_for_default_credential_chain",
            &[],
        );
        return;
    }

    let store = ObjStore::new(ObjStoreConfig::S3 {
        endpoint: None,
        bucket: "test".to_string(),
        access_key: None,
        secret_key: None,
        region: "us-east-1".to_string(),
        allow_http: true,
    })
    .unwrap();

    assert!(store.supports_strong_cross_process_cas());
}

#[test]
fn s3_store_constructs_without_endpoint_for_static_credentials() {
    let store = ObjStore::new(ObjStoreConfig::S3 {
        endpoint: None,
        bucket: "test".to_string(),
        access_key: Some("test".to_string()),
        secret_key: Some("test".to_string()),
        region: "us-east-1".to_string(),
        allow_http: false,
    })
    .unwrap();

    assert!(store.supports_strong_cross_process_cas());
}

#[tokio::test]
async fn explicit_s3_endpoint_overrides_ambient_service_endpoint() {
    if env::var_os(ISOLATED_TEST_CHILD).is_none() {
        run_isolated_test(
            "explicit_s3_endpoint_overrides_ambient_service_endpoint",
            &[
                ("AWS_ACCESS_KEY_ID", "test"),
                ("AWS_SECRET_ACCESS_KEY", "test"),
                ("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:2"),
            ],
        );
        return;
    }

    let store = ObjStore::new(ObjStoreConfig::S3 {
        endpoint: Some("http://127.0.0.1:1".to_string()),
        bucket: "test".to_string(),
        access_key: None,
        secret_key: None,
        region: "us-east-1".to_string(),
        allow_http: true,
    })
    .unwrap();

    let result = store.get("endpoint-probe").await;
    assert!(
        matches!(
            &result,
            Err(Error::Transport { message, .. }) if message.contains("127.0.0.1:1")
        ),
        "{result:?}"
    );
}

#[test]
fn gcs_store_constructs_without_live_credentials_and_reports_strong_cas() {
    let store = ObjStore::new(ObjStoreConfig::Gcs {
        bucket: "test".to_string(),
        service_account_path: None,
        service_account_key: None,
    })
    .unwrap();

    assert!(store.supports_strong_cross_process_cas());
}

#[test]
fn azure_blob_store_constructs_without_live_credentials_and_reports_strong_cas() {
    let store = ObjStore::new(ObjStoreConfig::AzureBlob {
        account: "testaccount".to_string(),
        container: "testcontainer".to_string(),
        access_key: None,
    })
    .unwrap();

    assert!(store.supports_strong_cross_process_cas());
}

#[test]
fn object_store_config_debug_redacts_credentials() {
    let config = ObjStoreConfig::S3 {
        endpoint: Some("https://objects.example".to_string()),
        bucket: "bucket".to_string(),
        access_key: Some("visible-access-key".to_string()),
        secret_key: Some("visible-secret-key".to_string()),
        region: "us-east-1".to_string(),
        allow_http: false,
    };

    let debug = format!("{config:?}");
    assert!(!debug.contains("visible-access-key"));
    assert!(!debug.contains("visible-secret-key"));
    assert!(debug.contains("[redacted]"));
}

#[test]
fn cloud_store_configuration_rejects_missing_or_conflicting_values() {
    let cases = [
        ObjStoreConfig::S3 {
            endpoint: Some("".to_string()),
            bucket: "test".to_string(),
            access_key: Some("test".to_string()),
            secret_key: Some("test".to_string()),
            region: "us-east-1".to_string(),
            allow_http: true,
        },
        ObjStoreConfig::S3 {
            endpoint: Some("http://127.0.0.1:1".to_string()),
            bucket: "test".to_string(),
            access_key: Some("test".to_string()),
            secret_key: None,
            region: "us-east-1".to_string(),
            allow_http: true,
        },
        ObjStoreConfig::S3 {
            endpoint: Some("http://127.0.0.1:1".to_string()),
            bucket: "test".to_string(),
            access_key: None,
            secret_key: Some("test".to_string()),
            region: "us-east-1".to_string(),
            allow_http: true,
        },
        ObjStoreConfig::S3 {
            endpoint: Some("http://127.0.0.1:1".to_string()),
            bucket: "test".to_string(),
            access_key: Some("".to_string()),
            secret_key: Some("test".to_string()),
            region: "us-east-1".to_string(),
            allow_http: true,
        },
        ObjStoreConfig::S3 {
            endpoint: Some("http://127.0.0.1:1".to_string()),
            bucket: "test".to_string(),
            access_key: Some("test".to_string()),
            secret_key: Some(" ".to_string()),
            region: "us-east-1".to_string(),
            allow_http: true,
        },
        ObjStoreConfig::Gcs {
            bucket: "".to_string(),
            service_account_path: None,
            service_account_key: None,
        },
        ObjStoreConfig::Gcs {
            bucket: "test".to_string(),
            service_account_path: Some("credentials.json".to_string()),
            service_account_key: Some("{}".to_string()),
        },
        ObjStoreConfig::AzureBlob {
            account: "testaccount".to_string(),
            container: "".to_string(),
            access_key: None,
        },
    ];

    for config in cases {
        assert!(matches!(
            ObjStore::new(config),
            Err(Error::Configuration(_))
        ));
    }
}

#[tokio::test]
async fn create_returns_existing_version_when_immutable_bytes_are_identical() {
    let dir = tempfile::tempdir().unwrap();
    let first = local_store(&dir);
    let second = local_store(&dir);

    let created = first.create("segment.qlog", b"immutable").await.unwrap();
    let retried = second.create("segment.qlog", b"immutable").await.unwrap();
    let observed = second.get_with_version("segment.qlog").await.unwrap();

    assert_eq!(retried, created);
    assert_eq!(observed.bytes(), b"immutable");
    assert_eq!(observed.version(), &created);
}

#[tokio::test]
async fn versioned_get_returns_bytes_while_get_remains_vec_compatible() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);
    store.put("segment.qlog", b"immutable").await.unwrap();

    let versioned = store.get_with_version("segment.qlog").await.unwrap();
    let versioned_bytes: &[u8] = versioned.bytes();
    let compatible_bytes: Vec<u8> = store.get("segment.qlog").await.unwrap();

    assert_eq!(versioned_bytes, b"immutable");
    assert_eq!(compatible_bytes, b"immutable");
}

#[tokio::test]
async fn independent_local_clients_allow_exactly_one_differing_create() {
    let dir = tempfile::tempdir().unwrap();
    let first = local_store(&dir);
    let second = local_store(&dir);

    let (first_result, second_result) = tokio::join!(
        first.create("manifest.json", b"from-first"),
        second.create("manifest.json", b"from-second")
    );
    let (winner_bytes, winner_version) = one_create_winner(
        (b"from-first", first_result),
        (b"from-second", second_result),
    );

    let observed = first.get_with_version("manifest.json").await.unwrap();
    assert_eq!(observed.bytes(), winner_bytes);
    assert_eq!(observed.version(), &winner_version);
}

#[tokio::test]
async fn shared_local_client_allows_exactly_one_update_from_one_version() {
    let dir = tempfile::tempdir().unwrap();
    let first = local_store(&dir);
    let second = first.clone();
    let version = first.create("manifest.json", b"v1").await.unwrap();

    let (first_result, second_result) = tokio::join!(
        first.update("manifest.json", b"from-first", version.clone()),
        second.update("manifest.json", b"from-second", version)
    );
    let (winner_bytes, winner_version) = one_update_winner(
        (b"from-first", first_result),
        (b"from-second", second_result),
    );

    let observed = second.get_with_version("manifest.json").await.unwrap();
    assert_eq!(observed.bytes(), winner_bytes);
    assert_eq!(observed.version(), &winner_version);
}

#[tokio::test]
async fn missing_object_is_typed_as_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let store = local_store(&dir);

    assert!(matches!(
        store.get_with_version("missing").await,
        Err(Error::NotFound { key }) if key == "missing"
    ));
}

#[tokio::test]
async fn unreachable_s3_endpoint_is_typed_as_transport() {
    let store = ObjStore::new(ObjStoreConfig::S3 {
        endpoint: Some("http://127.0.0.1:1".to_string()),
        bucket: "test".to_string(),
        access_key: Some("test".to_string()),
        secret_key: Some("test".to_string()),
        region: "us-east-1".to_string(),
        allow_http: true,
    })
    .unwrap();

    let result = store.get("transport-probe").await;
    assert!(
        matches!(
            &result,
            Err(Error::Transport { key, message })
                if key == "transport-probe"
                    && message.contains("http://127.0.0.1:1/test/transport-probe")
        ),
        "{result:?}"
    );
}

#[tokio::test]
async fn live_cloud_store_independent_clients_enforce_create_and_update_cas() {
    let Some(config) = live_store_config() else {
        return;
    };

    let first = ObjStore::new(config.clone()).unwrap();
    let second = ObjStore::new(config).unwrap();
    let key = live_test_key();

    let (first_create, second_create) = tokio::join!(
        first.create(&key, b"created-by-first"),
        second.create(&key, b"created-by-second")
    );
    let (created_bytes, created_version) = one_create_winner(
        (b"created-by-first", first_create),
        (b"created-by-second", second_create),
    );
    let created = second.get_with_version(&key).await.unwrap();
    assert_eq!(created.bytes(), created_bytes);
    assert_eq!(created.version(), &created_version);

    let shared_version = created.version().clone();
    let (first_update, second_update) = tokio::join!(
        first.update(&key, b"updated-by-first", shared_version.clone()),
        second.update(&key, b"updated-by-second", shared_version)
    );
    let (updated_bytes, updated_version) = one_update_winner(
        (b"updated-by-first", first_update),
        (b"updated-by-second", second_update),
    );
    let updated = first.get_with_version(&key).await.unwrap();
    assert_eq!(updated.bytes(), updated_bytes);
    assert_eq!(updated.version(), &updated_version);
}

fn local_store(dir: &tempfile::TempDir) -> ObjStore {
    ObjStore::new(ObjStoreConfig::Local {
        root: dir.path().to_path_buf(),
    })
    .unwrap()
}

fn run_isolated_test(name: &str, environment: &[(&str, &str)]) {
    let status = process::Command::new(env::current_exe().unwrap())
        .args(["--exact", name])
        .env_clear()
        .env(ISOLATED_TEST_CHILD, "1")
        .envs(environment.iter().copied())
        .status()
        .unwrap();
    assert!(status.success());
}

fn one_create_winner<'a>(
    first: (&'a [u8], rhiza_obj_store::Result<UpdateVersion>),
    second: (&'a [u8], rhiza_obj_store::Result<UpdateVersion>),
) -> (&'a [u8], UpdateVersion) {
    match (first, second) {
        ((bytes, Ok(version)), (_, Err(Error::AlreadyExists { .. })))
        | ((_, Err(Error::AlreadyExists { .. })), (bytes, Ok(version))) => (bytes, version),
        results => panic!("expected one create success and one AlreadyExists, got {results:?}"),
    }
}

fn one_update_winner<'a>(
    first: (&'a [u8], rhiza_obj_store::Result<UpdateVersion>),
    second: (&'a [u8], rhiza_obj_store::Result<UpdateVersion>),
) -> (&'a [u8], UpdateVersion) {
    match (first, second) {
        ((bytes, Ok(version)), (_, Err(Error::Precondition { .. })))
        | ((_, Err(Error::Precondition { .. })), (bytes, Ok(version))) => (bytes, version),
        results => panic!("expected one update success and one Precondition, got {results:?}"),
    }
}

fn s3_config_from_env() -> ObjStoreConfig {
    ObjStoreConfig::S3 {
        endpoint: env::var("RHIZA_S3_ENDPOINT").ok(),
        bucket: required_env("RHIZA_S3_BUCKET"),
        access_key: env::var("RHIZA_S3_ACCESS_KEY").ok(),
        secret_key: env::var("RHIZA_S3_SECRET_KEY").ok(),
        region: env::var("RHIZA_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        allow_http: env::var("RHIZA_S3_ALLOW_HTTP")
            .map(|value| value == "true" || value == "1")
            .unwrap_or(false),
    }
}

fn gcs_config_from_env() -> ObjStoreConfig {
    ObjStoreConfig::Gcs {
        bucket: required_env("RHIZA_GCS_BUCKET"),
        service_account_path: env::var("RHIZA_GCS_SERVICE_ACCOUNT_PATH").ok(),
        service_account_key: env::var("RHIZA_GCS_SERVICE_ACCOUNT_KEY").ok(),
    }
}

fn azure_config_from_env() -> ObjStoreConfig {
    ObjStoreConfig::AzureBlob {
        account: required_env("RHIZA_AZURE_ACCOUNT"),
        container: required_env("RHIZA_AZURE_CONTAINER"),
        access_key: env::var("RHIZA_AZURE_ACCESS_KEY").ok(),
    }
}

fn live_store_config() -> Option<ObjStoreConfig> {
    let provider = match env::var("RHIZA_LIVE_STORE") {
        Ok(provider) => provider,
        Err(env::VarError::NotPresent) if env::var("RUN_S3_TESTS").as_deref() == Ok("1") => {
            "s3".to_string()
        }
        Err(env::VarError::NotPresent) => return None,
        Err(error) => panic!("RHIZA_LIVE_STORE is not valid Unicode: {error}"),
    };

    Some(match provider.as_str() {
        "s3" => s3_config_from_env(),
        "gcs" => gcs_config_from_env(),
        "azure" => azure_config_from_env(),
        _ => panic!("RHIZA_LIVE_STORE must be one of: s3, gcs, azure"),
    })
}

fn required_env(name: &str) -> String {
    env::var(name)
        .unwrap_or_else(|_| panic!("{name} must be set for the selected live object store"))
}

fn live_test_key() -> String {
    let timestamp = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos();
    format!("rhiza-obj-store-contract/{timestamp}-{}", process::id())
}
