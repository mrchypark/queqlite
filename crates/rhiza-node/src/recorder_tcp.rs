use std::{
    fmt,
    future::Future,
    io::{Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use rhiza_core::{LogHash, StoredCommand};
use rhiza_quepaxa::{
    DecisionProof, Error, Membership, RecordRequest, RecordSummary, RecorderRpc, RejectReason,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

use crate::{
    peer_credentials_authenticated, valid_recorder_command, valid_recorder_record, PeerConfig,
    DEFAULT_PEER_CONCURRENCY, MAX_HTTP_BODY_BYTES,
};

const WIRE_VERSION: u16 = 1;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTIONS_PER_LANE: usize = 2;
const MAX_SERVER_CONNECTIONS: usize = DEFAULT_PEER_CONCURRENCY * 4;
const RECORDER_TLS_ALPN: &[u8] = b"rhiza-recorder/1";

#[derive(Clone)]
pub struct RecorderTlsServerConfig {
    inner: Arc<rustls::ServerConfig>,
}

impl fmt::Debug for RecorderTlsServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecorderTlsServerConfig")
            .finish_non_exhaustive()
    }
}

impl RecorderTlsServerConfig {
    pub fn from_pem(certificate_chain_pem: &[u8], private_key_pem: &[u8]) -> Result<Self, String> {
        let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(certificate_chain_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid recorder TLS certificate PEM".to_string())?;
        if certificates.is_empty() {
            return Err("recorder TLS certificate chain is empty".into());
        }
        let mut key_reader = std::io::Cursor::new(private_key_pem);
        let private_key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|_| "invalid recorder TLS private key PEM".to_string())?
            .ok_or_else(|| "recorder TLS private key is missing".to_string())?;
        if rustls_pemfile::private_key(&mut key_reader)
            .map_err(|_| "invalid recorder TLS private key PEM".to_string())?
            .is_some()
        {
            return Err("recorder TLS private key PEM contains multiple keys".into());
        }
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| "recorder TLS crypto provider does not support TLS 1.3".to_string())?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| {
            "recorder TLS certificate and private key are invalid or mismatched".to_string()
        })?;
        config.alpn_protocols = vec![RECORDER_TLS_ALPN.to_vec()];
        config.max_early_data_size = 0;
        Ok(Self {
            inner: Arc::new(config),
        })
    }
}

#[derive(Clone)]
pub struct RecorderTlsClientConfig {
    inner: Arc<rustls::ClientConfig>,
    server_name: rustls::pki_types::ServerName<'static>,
}

impl fmt::Debug for RecorderTlsClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecorderTlsClientConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl RecorderTlsClientConfig {
    pub fn from_ca_pem(ca_bundle_pem: &[u8], server_name: &str) -> Result<Self, String> {
        let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(ca_bundle_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid recorder TLS CA bundle PEM".to_string())?;
        if certificates.is_empty() {
            return Err("recorder TLS CA bundle is empty".into());
        }
        let mut roots = rustls::RootCertStore::empty();
        for certificate in certificates {
            roots.add(certificate).map_err(|_| {
                "recorder TLS CA bundle contains an invalid certificate".to_string()
            })?;
        }
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|_| "invalid recorder TLS server name".to_string())?;
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| "recorder TLS crypto provider does not support TLS 1.3".to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![RECORDER_TLS_ALPN.to_vec()];
        config.enable_early_data = false;
        Ok(Self {
            inner: Arc::new(config),
            server_name,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Hello {
    version: u16,
    node_id: String,
    recovery_generation: u64,
    token: String,
}

#[derive(Debug, Deserialize, Serialize)]
enum HelloReply {
    Accepted { version: u16, recorder_id: String },
    Rejected,
}

#[derive(Debug, Deserialize, Serialize)]
struct RequestFrame {
    version: u16,
    request_id: u64,
    remaining_deadline_ms: u32,
    body: RecorderRequestBody,
}

#[derive(Debug, Deserialize, Serialize)]
enum RecorderRequestBody {
    Identity,
    StoreCommand {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    },
    FetchCommand {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    },
    Record(RecordRequest),
    InstallDecisionProof {
        proof: DecisionProof,
        members: Vec<String>,
    },
    InspectDecisionProof {
        slot: u64,
    },
    InspectRecordSummary {
        slot: u64,
    },
}

#[derive(Debug, Deserialize, Serialize)]
struct ResponseFrame {
    version: u16,
    request_id: u64,
    body: RecorderResponseBody,
}

#[derive(Debug, Deserialize, Serialize)]
enum RecorderResponseBody {
    Identity(RpcResult<String>),
    StoreCommand(RpcResult<()>),
    FetchCommand(RpcResult<Option<StoredCommand>>),
    Record(RpcResult<RecordSummary>),
    InstallDecisionProof(RpcResult<()>),
    InspectDecisionProof(RpcResult<Option<DecisionProof>>),
    InspectRecordSummary(RpcResult<Option<RecordSummary>>),
}

#[derive(Debug, Deserialize, Serialize)]
enum RpcResult<T> {
    Ok(T),
    Rejected(RejectReason),
    Error(String),
    Overloaded,
}

impl<T> RpcResult<T> {
    fn from_result(result: rhiza_quepaxa::Result<T>) -> Self {
        match result {
            Ok(value) => Self::Ok(value),
            Err(Error::Rejected(reason)) => Self::Rejected(reason),
            Err(error) => Self::Error(error.to_string()),
        }
    }

    fn into_result(self) -> rhiza_quepaxa::Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Rejected(reason) => Err(Error::Rejected(reason)),
            Self::Error(message) => Err(Error::Io(message)),
            Self::Overloaded => Err(Error::Io("recorder RPC overloaded".into())),
        }
    }
}

pub async fn serve_recorder_tcp<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    serve_recorder_tcp_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        None,
        shutdown,
    )
    .await
}

pub async fn serve_recorder_tcp_tls<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: RecorderTlsServerConfig,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    serve_recorder_tcp_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        Some(tls.inner),
        shutdown,
    )
    .await
}

async fn serve_recorder_tcp_inner<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: Option<Arc<rustls::ServerConfig>>,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    let slots = Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY));
    let connections = Arc::new(tokio::sync::Semaphore::new(MAX_SERVER_CONNECTIONS));
    let reported_connection_error = Arc::new(AtomicBool::new(false));
    let mut tasks = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| format!("recorder TCP accept failed: {error}"))?;
                let Ok(connection) = connections.clone().try_acquire_owned() else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let recorder = recorder.clone();
                let peers = peers.clone();
                let slots = slots.clone();
                let tls = tls.clone();
                let reported_connection_error = Arc::clone(&reported_connection_error);
                tasks.spawn(async move {
                    let _connection = connection;
                    let result = if let Some(config) = tls {
                        let acceptor = TlsAcceptor::from(config);
                        match tokio::time::timeout(CONNECT_TIMEOUT, acceptor.accept(stream)).await {
                            Ok(Ok(tls_stream)) => {
                                if tls_stream.get_ref().1.alpn_protocol() != Some(RECORDER_TLS_ALPN) {
                                    Err("recorder TLS ALPN negotiation failed".to_string())
                                } else {
                                    serve_connection(tls_stream, recorder, peers, recovery_generation, slots).await
                                }
                            }
                            Ok(Err(_)) => Err("recorder TLS handshake failed".to_string()),
                            Err(_) => Err("recorder TLS handshake timed out".to_string()),
                        }
                    } else {
                        serve_connection(stream, recorder, peers, recovery_generation, slots).await
                    };
                    if let Err(error) = result {
                        if error != "connection closed"
                            && !reported_connection_error.swap(true, Ordering::Relaxed)
                        {
                            eprintln!("recorder TCP connection rejected: {error}");
                        }
                    }
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let _drained = slots
        .acquire_many_owned(u32::try_from(DEFAULT_PEER_CONCURRENCY).unwrap_or(u32::MAX))
        .await
        .map_err(|_| "recorder operation semaphore closed during shutdown".to_string())?;
    Ok(())
}

async fn serve_connection<R, S>(
    mut stream: S,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let hello_bytes = tokio::time::timeout(CALL_TIMEOUT, read_frame_async(&mut stream))
        .await
        .map_err(|_| "recorder HELLO timed out".to_string())??;
    let hello: Hello = decode_exact(&hello_bytes)?;
    if !hello_authenticated(&hello, &peers, recovery_generation) {
        let _ = write_value_async_with_timeout(
            &mut stream,
            &HelloReply::Rejected,
            "recorder HELLO rejection",
        )
        .await;
        return Err("recorder HELLO rejected".into());
    }
    let identity_recorder = recorder.clone();
    let recorder_id = tokio::task::spawn_blocking(move || identity_recorder.recorder_id())
        .await
        .map_err(|error| format!("recorder identity task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    write_value_async_with_timeout(
        &mut stream,
        &HelloReply::Accepted {
            version: WIRE_VERSION,
            recorder_id,
        },
        "recorder HELLO response",
    )
    .await?;

    loop {
        let request = match read_frame_async(&mut stream).await {
            Ok(bytes) => decode_exact::<RequestFrame>(&bytes)?,
            Err(error) if error == "connection closed" => return Ok(()),
            Err(error) => return Err(error),
        };
        if request.version != WIRE_VERSION || request.remaining_deadline_ms == 0 {
            return Err("invalid recorder request envelope".into());
        }
        let request_id = request.request_id;
        let operation = response_operation(&request.body);
        let permit = match slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                write_value_async_with_timeout(
                    &mut stream,
                    &ResponseFrame {
                        version: WIRE_VERSION,
                        request_id,
                        body: overloaded_response(operation),
                    },
                    "recorder overload response",
                )
                .await?;
                continue;
            }
        };
        let call_recorder = recorder.clone();
        let body = match tokio::task::spawn_blocking(move || {
            let _permit = permit;
            dispatch(call_recorder, request.body)
        })
        .await
        {
            Ok(body) => body,
            Err(error) => error_response(operation, error.to_string()),
        };
        write_value_async_with_timeout(
            &mut stream,
            &ResponseFrame {
                version: WIRE_VERSION,
                request_id,
                body,
            },
            "recorder response",
        )
        .await?;
    }
}

fn hello_authenticated(hello: &Hello, peers: &[PeerConfig], recovery_generation: u64) -> bool {
    hello.version == WIRE_VERSION
        && hello.recovery_generation == recovery_generation
        && peer_credentials_authenticated(&hello.node_id, &hello.token, peers)
}

#[derive(Clone, Copy)]
enum Operation {
    Identity,
    StoreCommand,
    FetchCommand,
    Record,
    InstallDecisionProof,
    InspectDecisionProof,
    InspectRecordSummary,
}

fn response_operation(request: &RecorderRequestBody) -> Operation {
    match request {
        RecorderRequestBody::Identity => Operation::Identity,
        RecorderRequestBody::StoreCommand { .. } => Operation::StoreCommand,
        RecorderRequestBody::FetchCommand { .. } => Operation::FetchCommand,
        RecorderRequestBody::Record(_) => Operation::Record,
        RecorderRequestBody::InstallDecisionProof { .. } => Operation::InstallDecisionProof,
        RecorderRequestBody::InspectDecisionProof { .. } => Operation::InspectDecisionProof,
        RecorderRequestBody::InspectRecordSummary { .. } => Operation::InspectRecordSummary,
    }
}

fn dispatch<R: RecorderRpc>(recorder: R, request: RecorderRequestBody) -> RecorderResponseBody {
    match request {
        RecorderRequestBody::Identity => {
            RecorderResponseBody::Identity(RpcResult::from_result(recorder.recorder_id()))
        }
        RecorderRequestBody::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        } => {
            let result = if !valid_recorder_command(&command) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                recorder.store_command_for(
                    cluster_id,
                    epoch,
                    config_id,
                    config_digest,
                    command_hash,
                    command,
                )
            };
            RecorderResponseBody::StoreCommand(RpcResult::from_result(result))
        }
        RecorderRequestBody::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        } => RecorderResponseBody::FetchCommand(RpcResult::from_result(
            recorder.fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash),
        )),
        RecorderRequestBody::Record(request) => {
            let result = if !valid_recorder_record(&request) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                recorder.record(request)
            };
            RecorderResponseBody::Record(RpcResult::from_result(result))
        }
        RecorderRequestBody::InstallDecisionProof { proof, members } => {
            let result = Membership::from_voters(members)
                .and_then(|membership| recorder.install_decision_proof(proof, &membership));
            RecorderResponseBody::InstallDecisionProof(RpcResult::from_result(result))
        }
        RecorderRequestBody::InspectDecisionProof { slot } => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::from_result(
                recorder.inspect_decision_proof(slot),
            ))
        }
        RecorderRequestBody::InspectRecordSummary { slot } => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::from_result(
                recorder.inspect_record_summary(slot),
            ))
        }
    }
}

fn overloaded_response(operation: Operation) -> RecorderResponseBody {
    match operation {
        Operation::Identity => RecorderResponseBody::Identity(RpcResult::Overloaded),
        Operation::StoreCommand => RecorderResponseBody::StoreCommand(RpcResult::Overloaded),
        Operation::FetchCommand => RecorderResponseBody::FetchCommand(RpcResult::Overloaded),
        Operation::Record => RecorderResponseBody::Record(RpcResult::Overloaded),
        Operation::InstallDecisionProof => {
            RecorderResponseBody::InstallDecisionProof(RpcResult::Overloaded)
        }
        Operation::InspectDecisionProof => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::Overloaded)
        }
        Operation::InspectRecordSummary => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::Overloaded)
        }
    }
}

fn error_response(operation: Operation, message: String) -> RecorderResponseBody {
    match operation {
        Operation::Identity => RecorderResponseBody::Identity(RpcResult::Error(message)),
        Operation::StoreCommand => RecorderResponseBody::StoreCommand(RpcResult::Error(message)),
        Operation::FetchCommand => RecorderResponseBody::FetchCommand(RpcResult::Error(message)),
        Operation::Record => RecorderResponseBody::Record(RpcResult::Error(message)),
        Operation::InstallDecisionProof => {
            RecorderResponseBody::InstallDecisionProof(RpcResult::Error(message))
        }
        Operation::InspectDecisionProof => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::Error(message))
        }
        Operation::InspectRecordSummary => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::Error(message))
        }
    }
}

async fn read_frame_async<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, String> {
    let mut length = [0_u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err("connection closed".into())
        }
        Err(error) => return Err(error.to_string()),
    }
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length == 0 || length > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|error| error.to_string())?;
    Ok(frame)
}

async fn write_value_async<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), String> {
    let encoded = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    write_frame_async(writer, &encoded).await
}

async fn write_value_async_with_timeout<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    operation: &str,
) -> Result<(), String> {
    tokio::time::timeout(CALL_TIMEOUT, write_value_async(writer, value))
        .await
        .map_err(|_| format!("{operation} timed out"))?
}

async fn write_frame_async<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> Result<(), String> {
    let length = frame_length(frame)?;
    writer
        .write_all(&length)
        .await
        .map_err(|error| error.to_string())?;
    writer
        .write_all(frame)
        .await
        .map_err(|error| error.to_string())
}

fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let (value, trailing) = postcard::take_from_bytes(bytes).map_err(|error| error.to_string())?;
    if !trailing.is_empty() {
        return Err("trailing recorder frame bytes".into());
    }
    Ok(value)
}

fn frame_length(frame: &[u8]) -> Result<[u8; 4], String> {
    if frame.is_empty() || frame.len() > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let length = u32::try_from(frame.len()).map_err(|_| "recorder frame is too large")?;
    Ok(length.to_be_bytes())
}

struct ConnectionPool {
    state: Mutex<PoolState>,
    available: Condvar,
}

#[derive(Default)]
struct PoolState {
    idle: Vec<RecorderClientStream>,
    open: usize,
}

enum RecorderClientStream {
    Plain(TcpStream),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, TcpStream>>),
}

impl RecorderClientStream {
    fn socket(&self) -> &TcpStream {
        match self {
            Self::Plain(socket) => socket,
            Self::Tls(stream) => &stream.sock,
        }
    }
}

impl Read for RecorderClientStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for RecorderClientStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

#[derive(Clone)]
enum ClientTransport {
    Plain,
    Tls(RecorderTlsClientConfig),
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        }
    }
}

pub struct TcpPostcardRecorderClient {
    address: String,
    expected_recorder_id: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    transport: ClientTransport,
    consensus: ConnectionPool,
    control: ConnectionPool,
    next_request_id: AtomicU64,
}

impl fmt::Debug for TcpPostcardRecorderClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpPostcardRecorderClient")
            .field("address", &self.address)
            .field("expected_recorder_id", &self.expected_recorder_id)
            .field("local_node_id", &self.local_node_id)
            .field("peer_token", &"[redacted]")
            .field("recovery_generation", &self.recovery_generation)
            .field(
                "transport",
                &match self.transport {
                    ClientTransport::Plain => "plain",
                    ClientTransport::Tls(_) => "tls",
                },
            )
            .finish()
    }
}

impl TcpPostcardRecorderClient {
    pub fn new(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
    ) -> Result<Self, String> {
        Self::new_with_transport(
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            ClientTransport::Plain,
        )
    }

    pub fn new_tls(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
        tls: RecorderTlsClientConfig,
    ) -> Result<Self, String> {
        Self::new_with_transport(
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            ClientTransport::Tls(tls),
        )
    }

    fn new_with_transport(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
        transport: ClientTransport,
    ) -> Result<Self, String> {
        let address = address.to_string();
        validate_recorder_tcp_endpoint(&address)?;
        let expected_recorder_id = expected_recorder_id.into();
        let local_node_id = local_node_id.into();
        let peer_token = peer_token.into();
        if expected_recorder_id.trim().is_empty()
            || local_node_id.trim().is_empty()
            || peer_token.trim().is_empty()
            || recovery_generation == 0
        {
            return Err("invalid recorder TCP client identity".into());
        }
        Ok(Self {
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            transport,
            consensus: ConnectionPool::new(),
            control: ConnectionPool::new(),
            next_request_id: AtomicU64::new(1),
        })
    }

    fn exchange(
        &self,
        request: RecorderRequestBody,
        consensus: bool,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        let deadline = Instant::now() + CALL_TIMEOUT;
        let pool = if consensus {
            &self.consensus
        } else {
            &self.control
        };
        let mut stream = self.checkout(pool, deadline)?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let operation = response_operation(&request);
        let remaining = deadline.saturating_duration_since(Instant::now());
        let remaining_deadline_ms = u32::try_from(remaining.as_millis())
            .unwrap_or(u32::MAX)
            .max(1);
        if let Err(error) = set_timeouts(stream.socket(), remaining) {
            self.discard(pool);
            return Err(error);
        }
        let frame = RequestFrame {
            version: WIRE_VERSION,
            request_id,
            remaining_deadline_ms,
            body: request,
        };
        let result = write_value_sync(&mut stream, &frame)
            .and_then(|()| read_frame_sync(&mut stream))
            .and_then(|bytes| decode_exact::<ResponseFrame>(&bytes));
        match result {
            Ok(response)
                if response.version == WIRE_VERSION
                    && response.request_id == request_id
                    && response_matches(operation, &response.body) =>
            {
                self.checkin(pool, stream);
                Ok(response.body)
            }
            Ok(_) => {
                self.discard(pool);
                Err(Error::Decode("recorder response envelope mismatch".into()))
            }
            Err(error) => {
                self.discard(pool);
                Err(Error::Io(error))
            }
        }
    }

    fn checkout(
        &self,
        pool: &ConnectionPool,
        deadline: Instant,
    ) -> rhiza_quepaxa::Result<RecorderClientStream> {
        loop {
            let mut state = pool
                .state
                .lock()
                .map_err(|_| Error::Io("recorder connection pool lock poisoned".into()))?;
            if let Some(stream) = state.idle.pop() {
                return Ok(stream);
            }
            if state.open < CONNECTIONS_PER_LANE {
                state.open += 1;
                drop(state);
                return match self.connect(deadline) {
                    Ok(stream) => Ok(stream),
                    Err(error) => {
                        self.discard(pool);
                        Err(Error::Io(error))
                    }
                };
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Io("recorder connection checkout timed out".into()));
            }
            let (next, wait) = pool
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| Error::Io("recorder connection pool lock poisoned".into()))?;
            drop(next);
            if wait.timed_out() {
                return Err(Error::Io("recorder connection checkout timed out".into()));
            }
        }
    }

    fn connect(&self, deadline: Instant) -> Result<RecorderClientStream, String> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = CONNECT_TIMEOUT.min(remaining);
        if connect_timeout.is_zero() {
            return Err("recorder connect deadline exceeded".into());
        }
        let mut last_error = None;
        let mut socket = None;
        let resolved_addresses = self
            .address
            .to_socket_addrs()
            .map_err(|error| format!("cannot resolve recorder TCP address: {error}"))?
            .collect::<Vec<SocketAddr>>();
        if resolved_addresses.is_empty() {
            return Err("recorder TCP address resolved to no endpoints".into());
        }
        for address in &resolved_addresses {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match TcpStream::connect_timeout(address, connect_timeout.min(remaining)) {
                Ok(connected) => {
                    socket = Some(connected);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let socket = socket.ok_or_else(|| {
            format!(
                "recorder TCP connect failed: {}",
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "deadline exceeded".into())
            )
        })?;
        socket
            .set_nodelay(true)
            .map_err(|error| format!("cannot set recorder TCP_NODELAY: {error}"))?;
        set_timeouts(&socket, deadline.saturating_duration_since(Instant::now()))
            .map_err(|error| error.to_string())?;
        let mut stream = match &self.transport {
            ClientTransport::Plain => RecorderClientStream::Plain(socket),
            ClientTransport::Tls(tls) => {
                let connection =
                    rustls::ClientConnection::new(Arc::clone(&tls.inner), tls.server_name.clone())
                        .map_err(|_| "cannot initialize recorder TLS connection".to_string())?;
                let mut stream = rustls::StreamOwned::new(connection, socket);
                while stream.conn.is_handshaking() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err("recorder TLS handshake timed out".into());
                    }
                    set_timeouts(&stream.sock, remaining).map_err(|error| error.to_string())?;
                    stream
                        .conn
                        .complete_io(&mut stream.sock)
                        .map_err(|_| "recorder TLS handshake failed".to_string())?;
                }
                if stream.conn.alpn_protocol() != Some(RECORDER_TLS_ALPN) {
                    return Err("recorder TLS ALPN negotiation failed".into());
                }
                RecorderClientStream::Tls(Box::new(stream))
            }
        };
        write_value_sync(
            &mut stream,
            &Hello {
                version: WIRE_VERSION,
                node_id: self.local_node_id.clone(),
                recovery_generation: self.recovery_generation,
                token: self.peer_token.clone(),
            },
        )?;
        let reply: HelloReply = decode_exact(&read_frame_sync(&mut stream)?)?;
        match reply {
            HelloReply::Accepted {
                version,
                recorder_id,
            } if version == WIRE_VERSION && recorder_id == self.expected_recorder_id => Ok(stream),
            HelloReply::Accepted { .. } => Err("recorder identity mismatch".into()),
            HelloReply::Rejected => Err("recorder HELLO rejected".into()),
        }
    }

    fn checkin(&self, pool: &ConnectionPool, stream: RecorderClientStream) {
        if let Ok(mut state) = pool.state.lock() {
            state.idle.push(stream);
            pool.available.notify_one();
        }
    }

    fn discard(&self, pool: &ConnectionPool) {
        if let Ok(mut state) = pool.state.lock() {
            state.open = state.open.saturating_sub(1);
            pool.available.notify_one();
        }
    }
}

pub fn validate_recorder_tcp_endpoint(address: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(&format!("tcp://{address}"))
        .map_err(|_| "invalid recorder TCP address".to_string())?;
    if parsed.host_str().is_none()
        || parsed.port().is_none()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("invalid recorder TCP address".into());
    }
    Ok(())
}

fn response_matches(operation: Operation, response: &RecorderResponseBody) -> bool {
    matches!(
        (operation, response),
        (Operation::Identity, RecorderResponseBody::Identity(_))
            | (
                Operation::StoreCommand,
                RecorderResponseBody::StoreCommand(_)
            )
            | (
                Operation::FetchCommand,
                RecorderResponseBody::FetchCommand(_)
            )
            | (Operation::Record, RecorderResponseBody::Record(_))
            | (
                Operation::InstallDecisionProof,
                RecorderResponseBody::InstallDecisionProof(_)
            )
            | (
                Operation::InspectDecisionProof,
                RecorderResponseBody::InspectDecisionProof(_)
            )
            | (
                Operation::InspectRecordSummary,
                RecorderResponseBody::InspectRecordSummary(_)
            )
    )
}

impl RecorderRpc for TcpPostcardRecorderClient {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        match self.exchange(RecorderRequestBody::Identity, false)? {
            RecorderResponseBody::Identity(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn store_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        let request = RecorderRequestBody::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        };
        match self.exchange(request, false)? {
            RecorderResponseBody::StoreCommand(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        let request = RecorderRequestBody::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        };
        match self.exchange(request, false)? {
            RecorderResponseBody::FetchCommand(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        match self.exchange(RecorderRequestBody::Record(request), true)? {
            RecorderResponseBody::Record(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        let request = RecorderRequestBody::InstallDecisionProof {
            proof,
            members: membership.members().to_vec(),
        };
        match self.exchange(request, true)? {
            RecorderResponseBody::InstallDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        let request = RecorderRequestBody::InspectDecisionProof { slot };
        match self.exchange(request, false)? {
            RecorderResponseBody::InspectDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        let request = RecorderRequestBody::InspectRecordSummary { slot };
        match self.exchange(request, false)? {
            RecorderResponseBody::InspectRecordSummary(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn uses_typed_protocol(&self) -> bool {
        true
    }
}

fn set_timeouts(socket: &TcpStream, timeout: Duration) -> rhiza_quepaxa::Result<()> {
    if timeout.is_zero() {
        return Err(Error::Io("recorder RPC deadline exceeded".into()));
    }
    socket
        .set_read_timeout(Some(timeout))
        .and_then(|()| socket.set_write_timeout(Some(timeout)))
        .map_err(|error| Error::Io(error.to_string()))
}

fn read_frame_sync(reader: &mut impl Read) -> Result<Vec<u8>, String> {
    let mut length = [0_u8; 4];
    reader
        .read_exact(&mut length)
        .map_err(|error| error.to_string())?;
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length == 0 || length > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .map_err(|error| error.to_string())?;
    Ok(frame)
}

fn write_value_sync(writer: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    let encoded = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    let length = frame_length(&encoded)?;
    writer
        .write_all(&length)
        .map_err(|error| error.to_string())?;
    writer
        .write_all(&encoded)
        .map_err(|error| error.to_string())?;
    writer.flush().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postcard_decoder_rejects_trailing_bytes_and_wrong_hello_version() {
        let hello = Hello {
            version: WIRE_VERSION,
            node_id: "node-1".into(),
            recovery_generation: 7,
            token: "peer-token-1".into(),
        };
        let mut encoded = postcard::to_allocvec(&hello).unwrap();
        encoded.push(0);
        assert!(decode_exact::<Hello>(&encoded).is_err());

        let wrong_version = Hello {
            version: WIRE_VERSION + 1,
            ..hello
        };
        assert!(!hello_authenticated(&wrong_version, &[], 7));
    }

    #[test]
    fn recorder_tcp_endpoint_accepts_socket_and_dns_addresses_without_paths() {
        assert!(validate_recorder_tcp_endpoint("127.0.0.1:8082").is_ok());
        assert!(validate_recorder_tcp_endpoint("node-1.internal:8082").is_ok());
        assert!(validate_recorder_tcp_endpoint("[::1]:8082").is_ok());
        assert!(validate_recorder_tcp_endpoint("127.0.0.1").is_err());
        assert!(validate_recorder_tcp_endpoint("127.0.0.1:8082/path").is_err());
    }

    #[tokio::test]
    async fn frame_reader_rejects_zero_oversize_and_truncated_frames() {
        for length in [0_u32, u32::try_from(MAX_HTTP_BODY_BYTES + 1).unwrap()] {
            let (mut writer, mut reader) = tokio::io::duplex(16);
            writer.write_all(&length.to_be_bytes()).await.unwrap();
            assert!(read_frame_async(&mut reader).await.is_err());
        }

        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&4_u32.to_be_bytes()).await.unwrap();
        writer.write_all(&[1, 2]).await.unwrap();
        drop(writer);
        assert!(read_frame_async(&mut reader).await.is_err());
    }
}
