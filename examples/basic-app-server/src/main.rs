//! Minimal local-development HTTP server backed by embedded Rhiza.
//!
//! ```text
//! RHIZA_BIND_ADDR=127.0.0.1:3000 RHIZA_DATA_DIR=./rhiza-data \
//!   cargo run -p rhiza-basic-app-server
//! ```
//!
//! All three file-backed recorders run in this process and data directory. This demonstrates the
//! embedded API, but it is a single failure domain and is not a highly available deployment.

use std::{
    env,
    error::Error as StdError,
    ffi::OsString,
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
};

use axum::{
    extract::{rejection::JsonRejection, Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
    Json, Router,
};
use rhiza::{
    EmbeddedConfig, Error, ErrorCategory, ExecutionProfile, ReadConsistency, Rhiza, RhizaHandle,
};
use serde::{Deserialize, Serialize};

const CLUSTER_ID: &str = "basic-app";
const MAX_ESCAPED_ERROR_DETAIL_BYTES: usize = 4 * 1024;
const ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER: &str = "...[truncated]";

fn parse_bind_addr(value: &str) -> io::Result<SocketAddr> {
    let address: SocketAddr = value.parse().map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid RHIZA_BIND_ADDR {value:?}: {error}"),
        )
    })?;
    if !address.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "RHIZA_BIND_ADDR {value:?} must use a loopback IP address; remote binding is unsupported"
            ),
        ));
    }
    Ok(address)
}

fn parse_data_dir(value: Option<OsString>) -> io::Result<PathBuf> {
    match value {
        Some(value) if value.is_empty() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RHIZA_DATA_DIR must not be empty",
        )),
        Some(value) => Ok(PathBuf::from(value)),
        None => Ok(PathBuf::from("./rhiza-data")),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PutItemRequest {
    request_id: String,
    value: String,
}

#[derive(Serialize)]
struct ItemResponse {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct ReadyResponse {
    ready: bool,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    retryable: bool,
    message: String,
}

fn build_router(handle: RhizaHandle) -> Router {
    Router::new()
        .route("/items/{key}", put(put_item).get(get_item))
        .route("/ready", get(ready))
        .with_state(handle)
}

async fn put_item(
    AxumPath(key): AxumPath<String>,
    State(handle): State<RhizaHandle>,
    request: Result<Json<PutItemRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(error) => {
            eprintln!("invalid item request: {}", escaped_error_detail(&error));
            return (
                error.status(),
                Json(ErrorResponse {
                    error: "invalid_request".to_owned(),
                    retryable: false,
                    message: "request body is invalid".to_owned(),
                }),
            )
                .into_response();
        }
    };
    match handle.put(&request.request_id, &key, &request.value).await {
        Ok(_) => Json(ItemResponse {
            key,
            value: request.value,
        })
        .into_response(),
        Err(error) => rhiza_error(error),
    }
}

async fn get_item(AxumPath(key): AxumPath<String>, State(handle): State<RhizaHandle>) -> Response {
    match handle.read(&key, ReadConsistency::Local).await {
        Ok(response) => match response.value {
            Some(value) => Json(ItemResponse { key, value }).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "not_found".to_owned(),
                    retryable: false,
                    message: format!("item {key:?} was not found"),
                }),
            )
                .into_response(),
        },
        Err(error) => rhiza_error(error),
    }
}

async fn ready(State(handle): State<RhizaHandle>) -> Response {
    match handle.status().await {
        Ok(status) => (
            if status.ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            Json(ReadyResponse {
                ready: status.ready,
            }),
        )
            .into_response(),
        Err(error) => rhiza_error(error),
    }
}

fn rhiza_error(error: Error) -> Response {
    let classification = error.classification();
    let (status, message) =
        rhiza_error_details(classification.category(), classification.retryable());
    if should_log_rhiza_error(&error) {
        eprintln!(
            "Rhiza request failed (code={}, retryable={}): {}",
            classification.code(),
            classification.retryable(),
            escaped_error_detail(&error)
        );
    }
    (
        status,
        Json(ErrorResponse {
            error: classification.code().to_owned(),
            retryable: classification.retryable(),
            message: message.to_owned(),
        }),
    )
        .into_response()
}

fn should_log_rhiza_error(error: &Error) -> bool {
    !matches!(error, Error::Node(rhiza::NodeError::Fatal(_)))
}

fn escaped_error_detail(error: &dyn std::fmt::Display) -> String {
    let detail = error.to_string();
    let mut escaped = String::with_capacity(detail.len().min(MAX_ESCAPED_ERROR_DETAIL_BYTES));
    for character in detail.chars() {
        let character_start = escaped.len();
        for escaped_character in character.escape_default() {
            if escaped.len()
                + escaped_character.len_utf8()
                + ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER.len()
                > MAX_ESCAPED_ERROR_DETAIL_BYTES
            {
                escaped.truncate(character_start);
                escaped.push_str(ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER);
                return escaped;
            }
            escaped.push(escaped_character);
        }
    }
    escaped
}

fn rhiza_error_details(category: ErrorCategory, retryable: bool) -> (StatusCode, &'static str) {
    match category {
        ErrorCategory::InvalidRequest => (StatusCode::BAD_REQUEST, "request is invalid"),
        ErrorCategory::Authentication => (StatusCode::UNAUTHORIZED, "authentication failed"),
        ErrorCategory::Conflict => (StatusCode::CONFLICT, "request conflicts with current state"),
        ErrorCategory::Unavailable | ErrorCategory::ResourceExhausted => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service is temporarily unavailable",
        ),
        _ if retryable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service is temporarily unavailable",
        ),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal server error"),
    }
}

async fn open_rhiza(data_dir: &Path) -> Result<Rhiza, Box<dyn StdError>> {
    let config = EmbeddedConfig::local_file_backed(CLUSTER_ID, data_dir, ExecutionProfile::Sqlite)?;
    Ok(Rhiza::open(config).await?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let bind_addr = match env::var("RHIZA_BIND_ADDR") {
        Ok(value) => parse_bind_addr(&value)?,
        Err(env::VarError::NotPresent) => parse_bind_addr("127.0.0.1:3000")?,
        Err(error) => return Err(error.into()),
    };
    let data_dir = parse_data_dir(env::var_os("RHIZA_DATA_DIR"))?;
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let rhiza = open_rhiza(&data_dir).await?;
    let app = build_router(rhiza.handle());
    let listener_addr = listener.local_addr()?;

    {
        let mut stdout = io::stdout().lock();
        writeln!(stdout, "RHIZA_LISTEN_ADDR={listener_addr}")?;
        stdout.flush()?;
    }

    eprintln!(
        "basic Rhiza app listening on http://{} with data in {}",
        listener_addr,
        data_dir.display()
    );
    eprintln!("local development only: all three recorders share one process and failure domain");

    let server_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    let shutdown_result = rhiza.shutdown().await;
    server_result?;
    shutdown_result?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => report_ctrl_c_error(result),
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                eprintln!(
                    "failed to install SIGTERM handler: {}",
                    escaped_error_detail(&error)
                );
                wait_for_ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    wait_for_ctrl_c().await;
}

async fn wait_for_ctrl_c() {
    report_ctrl_c_error(tokio::signal::ctrl_c().await);
}

fn report_ctrl_c_error(result: io::Result<()>) {
    if let Err(error) = result {
        eprintln!(
            "failed to install Ctrl-C handler: {}",
            escaped_error_detail(&error)
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsString, path::PathBuf};

    use axum::{
        body::{to_bytes, Body},
        http::{header::CONTENT_TYPE, Method, Request, StatusCode},
        response::Response,
        Router,
    };
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use rhiza::{Error, NodeError};

    use super::{
        build_router, escaped_error_detail, open_rhiza, parse_bind_addr, parse_data_dir,
        rhiza_error, should_log_rhiza_error,
    };

    async fn get(app: &Router, uri: &str) -> Response {
        app.clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    async fn put(app: &Router, key: &str, request_id: &str, value: &str) -> Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri(format!("/items/{key}"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({"request_id": request_id, "value": value}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn json_body(response: Response) -> Value {
        serde_json::from_slice(&to_bytes(response.into_body(), 64 * 1024).await.unwrap()).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn items_returns_stored_value_when_put_succeeds() {
        let root = tempfile::tempdir().unwrap();
        let rhiza = open_rhiza(root.path()).await.unwrap();
        let app = build_router(rhiza.handle());

        assert_eq!(
            put(&app, "greeting", "put-greeting", "hello")
                .await
                .status(),
            StatusCode::OK
        );

        let response = get(&app, "/items/greeting").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["key"], "greeting");
        assert_eq!(body["value"], "hello");

        rhiza.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ready_returns_ok_when_embedded_node_is_ready() {
        let root = tempfile::tempdir().unwrap();
        let rhiza = open_rhiza(root.path()).await.unwrap();
        let response = get(&build_router(rhiza.handle()), "/ready").await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await, json!({"ready": true}));

        rhiza.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn items_returns_not_found_when_key_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let rhiza = open_rhiza(root.path()).await.unwrap();
        let response = get(&build_router(rhiza.handle()), "/items/missing").await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(json_body(response).await["error"], "not_found");

        rhiza.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn items_accepts_idempotent_replay_when_request_id_and_payload_match() {
        let root = tempfile::tempdir().unwrap();
        let rhiza = open_rhiza(root.path()).await.unwrap();
        let app = build_router(rhiza.handle());

        assert_eq!(
            put(&app, "greeting", "same-request", "hello")
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            put(&app, "greeting", "same-request", "hello")
                .await
                .status(),
            StatusCode::OK
        );

        rhiza.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn items_returns_conflict_when_request_id_is_reused_for_different_payload() {
        let root = tempfile::tempdir().unwrap();
        let rhiza = open_rhiza(root.path()).await.unwrap();
        let app = build_router(rhiza.handle());

        assert_eq!(
            put(&app, "greeting", "same-request", "hello")
                .await
                .status(),
            StatusCode::OK
        );
        let response = put(&app, "greeting", "same-request", "goodbye").await;
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = json_body(response).await;
        assert_eq!(body["error"], "request_conflict");
        assert_eq!(body["retryable"], false);

        rhiza.shutdown().await.unwrap();
    }

    async fn assert_invalid_request_body(body: String) {
        let root = tempfile::tempdir().unwrap();
        let rhiza = open_rhiza(root.path()).await.unwrap();
        let response = build_router(rhiza.handle())
            .oneshot(
                Request::builder()
                    .method(Method::PUT)
                    .uri("/items/greeting")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(response.status().is_client_error());
        let body = json_body(response).await;
        assert_eq!(body["error"], "invalid_request");
        assert_eq!(body["retryable"], false);
        assert_eq!(body["message"], "request body is invalid");

        rhiza.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn items_returns_client_error_when_json_is_malformed() {
        assert_invalid_request_body(String::from("{")).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn items_returns_client_error_when_json_has_unknown_field() {
        assert_invalid_request_body(
            json!({"request_id": "put-greeting", "value": "hello", "extra": true}).to_string(),
        )
        .await;
    }

    #[tokio::test]
    async fn rhiza_error_response_does_not_expose_node_display_details() {
        let detail = "/srv/rhiza/private/consensus/log: checksum mismatch";
        let response = rhiza_error(Error::Node(NodeError::Storage(detail.into())));

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = json_body(response).await;
        assert_eq!(body["error"], "storage_error");
        assert_eq!(body["retryable"], false);
        assert_eq!(body["message"], "internal server error");
        assert!(!body.to_string().contains(detail));

        let response = rhiza_error(Error::Node(NodeError::DataRootLocked(PathBuf::from(
            "/srv/rhiza/private",
        ))));
        let body = json_body(response).await;
        assert_eq!(body["message"], "internal server error");
        assert!(!body.to_string().contains("/srv/rhiza/private"));
    }

    #[test]
    fn rhiza_error_log_decision_skips_only_fatal_node_errors() {
        assert!(!should_log_rhiza_error(&Error::Node(NodeError::Fatal(
            "fatal runtime detail".into(),
        ))));
        assert!(should_log_rhiza_error(&Error::Node(NodeError::Storage(
            "storage detail".into(),
        ))));
    }

    #[test]
    fn escaped_error_detail_escapes_control_characters() {
        let detail = "checksum mismatch\nforged entry\r\u{1b}[2J";

        assert_eq!(
            escaped_error_detail(&detail),
            r"checksum mismatch\nforged entry\r\u{1b}[2J"
        );
    }

    #[test]
    fn escaped_error_detail_bounds_escape_expansion() {
        let detail = "\n".repeat(super::MAX_ESCAPED_ERROR_DETAIL_BYTES / 2 + 1);
        let escaped = escaped_error_detail(&detail);

        assert!(
            escaped.len() <= super::MAX_ESCAPED_ERROR_DETAIL_BYTES,
            "escaped detail must stay within the log budget"
        );
        assert!(
            escaped.ends_with(super::ESCAPED_ERROR_DETAIL_TRUNCATION_MARKER),
            "truncated details must be explicit"
        );
        assert!(!escaped.contains('\n'));
    }

    #[test]
    fn bind_address_rejects_non_loopback_address() {
        assert!(parse_bind_addr("0.0.0.0:3000").is_err());
    }

    #[test]
    fn data_directory_rejects_empty_environment_value() {
        assert!(parse_data_dir(Some(OsString::new())).is_err());
    }
}
