use std::{
    env,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    process::{self, Command},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, Bytes},
    http::{header::CONTENT_TYPE, StatusCode},
    response::Response,
    routing::post,
    Router,
};
use prost::Message;
use quinn::{Connection, Endpoint};
use rcgen::CertifiedKey;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const PREV_HASH: [u8; 32] = [0x5a; 32];
const MAX_FRAME: usize = 1024 * 1024;
const CALL_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_ALPN: &[u8] = b"http/1.1";
const RPC_ALPN: &[u8] = b"rhiza-bench/1";
const TLS_STREAM_OBSERVATION: &str =
    "server-side negotiated TLS 1.3 and ALPN observed and verified";
const QUIC_OBSERVATION: &str =
    "server-side ALPN observed and verified; TLS 1.3 verified by QUIC invariant and TLS1.3-only config";
const TLS_CONDITIONS_OBSERVATION: &str =
    "HTTPS/TCP negotiated TLS 1.3 and ALPN observed; Quinn ALPN observed with TLS 1.3 enforced by QUIC invariant and config";

#[derive(Clone)]
struct TlsMaterial {
    certificate: CertificateDer<'static>,
    private_key: Vec<u8>,
}

fn generate_tls_material() -> TlsMaterial {
    let CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    TlsMaterial {
        certificate: CertificateDer::from(cert),
        private_key: signing_key.serialize_der(),
    }
}

fn certificate_sha256(tls: &TlsMaterial) -> String {
    Sha256::digest(tls.certificate.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn server_tls_config(tls: &TlsMaterial, alpn: &[u8]) -> Result<rustls::ServerConfig, String> {
    let mut config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(
                vec![tls.certificate.clone()],
                PrivatePkcs8KeyDer::from(tls.private_key.clone()).into(),
            )
            .map_err(|error| error.to_string())?;
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(config)
}

fn client_tls_config(tls: &TlsMaterial, alpn: &[u8]) -> Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(tls.certificate.clone())
        .map_err(|error| error.to_string())?;
    let mut config =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_no_client_auth();
    config.alpn_protocols = vec![alpn.to_vec()];
    Ok(config)
}

#[derive(Default)]
struct TlsTelemetry {
    successful_handshakes: AtomicU64,
    negotiation_mismatches: AtomicU64,
}

impl TlsTelemetry {
    fn snapshot(&self) -> TlsSnapshot {
        TlsSnapshot {
            successful_handshakes: self.successful_handshakes.load(Ordering::Relaxed),
            negotiation_mismatches: self.negotiation_mismatches.load(Ordering::Relaxed),
        }
    }

    fn record(&self, negotiated_as_expected: bool) {
        self.successful_handshakes.fetch_add(1, Ordering::Relaxed);
        if !negotiated_as_expected {
            self.negotiation_mismatches.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy)]
struct TlsSnapshot {
    successful_handshakes: u64,
    negotiation_mismatches: u64,
}

#[derive(Clone)]
struct HttpsObservingAcceptor {
    inner: axum_server::tls_rustls::RustlsAcceptor,
    telemetry: Arc<TlsTelemetry>,
}

impl<I, S> axum_server::accept::Accept<I, S> for HttpsObservingAcceptor
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    S: Send + 'static,
{
    type Stream = tokio_rustls::server::TlsStream<I>;
    type Service = S;
    type Future = Pin<Box<dyn Future<Output = io::Result<(Self::Stream, S)>> + Send>>;

    fn accept(&self, stream: I, service: S) -> Self::Future {
        let acceptor = self.inner.clone();
        let telemetry = self.telemetry.clone();
        Box::pin(async move {
            let (stream, service) =
                axum_server::accept::Accept::accept(&acceptor, stream, service).await?;
            let session = stream.get_ref().1;
            telemetry.record(
                session.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3)
                    && session.alpn_protocol() == Some(HTTP_ALPN),
            );
            Ok((stream, service))
        })
    }
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
struct WireRequest {
    #[prost(string, tag = "1")]
    request_id: String,
    #[prost(uint64, tag = "2")]
    slot: u64,
    #[prost(bytes = "vec", tag = "3")]
    prev_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message, Serialize, Deserialize)]
struct WireAck {
    #[prost(uint64, tag = "1")]
    slot: u64,
    #[prost(bool, tag = "2")]
    accepted: bool,
    #[prost(bytes = "vec", tag = "3")]
    hash: Vec<u8>,
    #[prost(string, tag = "4")]
    request_id: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Codec {
    Json,
    Postcard,
    Prost,
}

impl Codec {
    fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::Postcard => "application/vnd.rhiza.postcard",
            Self::Prost => "application/vnd.rhiza.protobuf",
        }
    }

    fn encode_request(self, value: &WireRequest) -> Result<Vec<u8>, String> {
        match self {
            Self::Json => serde_json::to_vec(value).map_err(|error| error.to_string()),
            Self::Postcard => postcard::to_allocvec(value).map_err(|error| error.to_string()),
            Self::Prost => Ok(value.encode_to_vec()),
        }
    }

    fn decode_request(self, bytes: &[u8]) -> Result<WireRequest, String> {
        match self {
            Self::Json => serde_json::from_slice(bytes).map_err(|error| error.to_string()),
            Self::Postcard => postcard::from_bytes(bytes).map_err(|error| error.to_string()),
            Self::Prost => WireRequest::decode(bytes).map_err(|error| error.to_string()),
        }
    }

    fn encode_ack(self, value: &WireAck) -> Result<Vec<u8>, String> {
        match self {
            Self::Json => serde_json::to_vec(value).map_err(|error| error.to_string()),
            Self::Postcard => postcard::to_allocvec(value).map_err(|error| error.to_string()),
            Self::Prost => Ok(value.encode_to_vec()),
        }
    }

    fn decode_ack(self, bytes: &[u8]) -> Result<WireAck, String> {
        match self {
            Self::Json => serde_json::from_slice(bytes).map_err(|error| error.to_string()),
            Self::Postcard => postcard::from_bytes(bytes).map_err(|error| error.to_string()),
            Self::Prost => WireAck::decode(bytes).map_err(|error| error.to_string()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Candidate {
    HttpJson,
    HttpPostcard,
    HttpProst,
    HttpsJson,
    HttpsPostcard,
    HttpsProst,
    TcpPostcard,
    TcpTlsPostcard,
    QuinnRpcStream,
    QuinnLane,
}

impl Candidate {
    const ALL: [Self; 10] = [
        Self::HttpJson,
        Self::HttpPostcard,
        Self::HttpProst,
        Self::HttpsJson,
        Self::HttpsPostcard,
        Self::HttpsProst,
        Self::TcpPostcard,
        Self::TcpTlsPostcard,
        Self::QuinnRpcStream,
        Self::QuinnLane,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::HttpJson => "http-json",
            Self::HttpPostcard => "http-postcard",
            Self::HttpProst => "http-prost",
            Self::HttpsJson => "https-json",
            Self::HttpsPostcard => "https-postcard",
            Self::HttpsProst => "https-prost",
            Self::TcpPostcard => "tcp-postcard",
            Self::TcpTlsPostcard => "tcp-tls-postcard",
            Self::QuinnRpcStream => "quinn-rpc-stream",
            Self::QuinnLane => "quinn-lane",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "tcp-postcard-persistent-worker" => Some(Self::TcpPostcard),
            "tcp-tls-postcard-persistent-worker" => Some(Self::TcpTlsPostcard),
            "quinn-lane-persistent-worker" => Some(Self::QuinnLane),
            _ => Self::ALL
                .into_iter()
                .find(|candidate| candidate.name() == value),
        }
    }

    fn codec(self) -> Codec {
        match self {
            Self::HttpJson | Self::HttpsJson => Codec::Json,
            Self::HttpProst | Self::HttpsProst => Codec::Prost,
            _ => Codec::Postcard,
        }
    }

    fn transport(self) -> &'static str {
        match self {
            Self::HttpJson | Self::HttpPostcard | Self::HttpProst => "HTTP/1.1 over TCP",
            Self::HttpsJson | Self::HttpsPostcard | Self::HttpsProst => "HTTP/1.1 over TLS/TCP",
            Self::TcpPostcard | Self::TcpTlsPostcard => "length-prefixed TCP",
            Self::QuinnRpcStream | Self::QuinnLane => "QUIC",
        }
    }

    fn tls(self) -> &'static str {
        match self {
            Self::HttpsJson
            | Self::HttpsPostcard
            | Self::HttpsProst
            | Self::TcpTlsPostcard
            | Self::QuinnRpcStream
            | Self::QuinnLane => "TLS server authentication; shared benchmark certificate",
            Self::HttpJson | Self::HttpPostcard | Self::HttpProst | Self::TcpPostcard => "none",
        }
    }

    fn is_tls(self) -> bool {
        !matches!(
            self,
            Self::HttpJson | Self::HttpPostcard | Self::HttpProst | Self::TcpPostcard
        )
    }

    fn topology(self) -> &'static str {
        match self {
            Self::HttpJson
            | Self::HttpPostcard
            | Self::HttpProst
            | Self::HttpsJson
            | Self::HttpsPostcard
            | Self::HttpsProst => "shared reqwest pool; blocking worker per concurrency slot",
            Self::TcpPostcard | Self::TcpTlsPostcard => {
                "one warmed persistent connection per worker"
            }
            Self::QuinnRpcStream => "one QUIC connection; one bidirectional stream per RPC",
            Self::QuinnLane => "one QUIC connection; one persistent stream per worker",
        }
    }
}

#[derive(Debug)]
struct Config {
    warmup: usize,
    operations: usize,
    payloads: Vec<usize>,
    concurrencies: Vec<usize>,
    candidates: Vec<Candidate>,
    candidate_order_offset: usize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        Self::parse_from(env::args().skip(1))
    }

    fn parse_from<I, S>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut config = Self {
            warmup: 4_096,
            operations: 60_000,
            payloads: vec![128, 4 * 1024],
            concurrencies: vec![1, 8, 64],
            candidates: Candidate::ALL.to_vec(),
            candidate_order_offset: 0,
        };
        let args: Vec<String> = args.into_iter().map(Into::into).collect();
        let mut index = 0;
        while index < args.len() {
            let flag = &args[index];
            if matches!(flag.as_str(), "--help" | "-h") {
                print_usage();
                process::exit(0);
            }
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("{flag} requires a value"))?;
            match flag.as_str() {
                "--warmup" => config.warmup = parse_positive(value, flag)?,
                "--operations" => config.operations = parse_positive(value, flag)?,
                "--payloads" => config.payloads = parse_list(value, flag)?,
                "--concurrency" => config.concurrencies = parse_list(value, flag)?,
                "--candidate-order-offset" => {
                    config.candidate_order_offset = parse_nonnegative(value, flag)?
                }
                "--candidates" => {
                    config.candidates = value
                        .split(',')
                        .map(|name| {
                            Candidate::parse(name)
                                .ok_or_else(|| format!("unknown candidate {name:?}"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if config.candidates.is_empty() {
                        return Err("--candidates must not be empty".into());
                    }
                }
                _ => return Err(format!("unknown option {flag:?}")),
            }
            index += 2;
        }
        let max_concurrency = config.concurrencies.iter().copied().max().unwrap_or(0);
        if config.warmup < max_concurrency {
            return Err(format!(
                "--warmup ({}) must be at least the maximum concurrency ({max_concurrency})",
                config.warmup
            ));
        }
        rotate_candidates(&mut config.candidates, config.candidate_order_offset);
        Ok(config)
    }
}

fn parse_nonnegative(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("{flag} requires a non-negative integer"))
}

fn rotate_candidates(candidates: &mut [Candidate], offset: usize) {
    if !candidates.is_empty() {
        candidates.rotate_left(offset % candidates.len());
    }
}

fn parse_positive(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{flag} requires a positive integer"))
}

fn parse_list(value: &str, flag: &str) -> Result<Vec<usize>, String> {
    let values = value
        .split(',')
        .map(|value| parse_positive(value, flag))
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        Err(format!("{flag} must not be empty"))
    } else {
        Ok(values)
    }
}

fn print_usage() {
    eprintln!(
        "Usage: rhiza-transport [--warmup N] [--operations N] \
         [--payloads N,N] [--concurrency N,N] [--candidates NAME,NAME]\n\
         [--candidate-order-offset N]\n\
         Candidates: http-json,http-postcard,http-prost,https-json,https-postcard,https-prost,\n\
         tcp-postcard,tcp-tls-postcard,quinn-rpc-stream,quinn-lane"
    );
}

#[derive(Serialize)]
struct Report {
    schema_version: u8,
    generated_at_epoch_seconds: f64,
    diagnostic_valid: bool,
    comparison_valid: bool,
    production_valid: bool,
    comparison_blockers: Vec<String>,
    environment: Environment,
    conditions: Conditions,
    metrics: Vec<Metric>,
}

#[derive(Serialize)]
struct Environment {
    git_commit: Option<String>,
    git_dirty: Option<bool>,
    rustc: Option<String>,
    os: Option<String>,
    cpu: Option<String>,
}

#[derive(Serialize)]
struct Conditions {
    host: &'static str,
    security: &'static str,
    includes: &'static str,
    excludes: &'static str,
    warmup_operations_per_metric: usize,
    measured_operations_per_metric: usize,
    payload_bytes: Vec<usize>,
    concurrency: Vec<usize>,
    candidates: Vec<&'static str>,
    candidate_order_offset: usize,
    candidate_order: &'static str,
    server_reuse: &'static str,
    call_timeout_seconds: u64,
    tls: TlsConditions,
}

#[derive(Serialize)]
struct TlsConditions {
    certificate_sha256: String,
    trust_roots: &'static str,
    protocol_version: &'static str,
    https_alpn: &'static str,
    tcp_rpc_alpn: &'static str,
    quinn_alpn: &'static str,
    negotiation_observation: &'static str,
    handshake_observation: &'static str,
}

#[derive(Serialize)]
struct Metric {
    candidate: &'static str,
    codec: &'static str,
    transport: &'static str,
    tls: &'static str,
    topology: &'static str,
    payload_bytes: usize,
    encoded_request_bytes: usize,
    encoded_response_bytes: usize,
    concurrency: usize,
    warmup_attempts: usize,
    warmup_errors: usize,
    attempts: usize,
    successes: usize,
    errors: usize,
    wall_seconds: f64,
    throughput_ops_per_second: f64,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    p999_us: f64,
    max_us: f64,
    tls_handshakes_before_measurement: Option<u64>,
    tls_handshakes_after_measurement: Option<u64>,
    tls_handshakes_during_measurement: Option<u64>,
    tls_negotiation_mismatches: Option<u64>,
    tls_negotiation_observation: &'static str,
    diagnostic_valid: bool,
    comparison_valid: bool,
}

struct PhaseResult {
    samples: Vec<u64>,
    errors: usize,
    wall: Duration,
    warmup_attempts: usize,
    warmup_errors: usize,
    tls_before: Option<TlsSnapshot>,
    tls_after: Option<TlsSnapshot>,
}

struct HttpTls<'a> {
    root_certificate: &'a CertificateDer<'static>,
    telemetry: Arc<TlsTelemetry>,
}

struct HttpPhaseInput {
    url: Arc<String>,
    codec: Codec,
    payload: Arc<Vec<u8>>,
    root_certificate: Option<Arc<Vec<u8>>>,
    telemetry: Option<Arc<TlsTelemetry>>,
}

struct TcpTlsClient {
    connector: tokio_rustls::TlsConnector,
    telemetry: Arc<TlsTelemetry>,
}

struct QuinnClient {
    _endpoint: Endpoint,
    connection: Connection,
    telemetry: Arc<TlsTelemetry>,
}

struct QuinnServer {
    _endpoint: Endpoint,
    addr: SocketAddr,
    telemetry: Arc<TlsTelemetry>,
}

type WorkerOutcome = (Vec<u64>, usize, usize, usize);
type WorkerTask = tokio::task::JoinHandle<WorkerOutcome>;

impl Metric {
    fn from_samples(
        candidate: Candidate,
        payload_bytes: usize,
        concurrency: usize,
        mut phase: PhaseResult,
    ) -> Self {
        phase.samples.sort_unstable();
        let attempts = phase.samples.len();
        let handshake_delta = phase
            .tls_before
            .zip(phase.tls_after)
            .map(|(before, after)| {
                after
                    .successful_handshakes
                    .saturating_sub(before.successful_handshakes)
            });
        let negotiation_verified = phase.tls_after.is_none_or(|after| {
            after.successful_handshakes > 0 && after.negotiation_mismatches == 0
        });
        let diagnostic_valid = phase.errors == 0
            && phase.warmup_errors == 0
            && handshake_delta.is_none_or(|delta| delta == 0)
            && negotiation_verified;
        let (encoded_request_bytes, encoded_response_bytes) =
            encoded_sizes(candidate, payload_bytes);
        Self {
            candidate: candidate.name(),
            codec: candidate.codec().content_type(),
            transport: candidate.transport(),
            tls: candidate.tls(),
            topology: candidate.topology(),
            payload_bytes,
            encoded_request_bytes,
            encoded_response_bytes,
            concurrency,
            warmup_attempts: phase.warmup_attempts,
            warmup_errors: phase.warmup_errors,
            attempts,
            successes: attempts - phase.errors,
            errors: phase.errors,
            wall_seconds: phase.wall.as_secs_f64(),
            throughput_ops_per_second: (attempts - phase.errors) as f64 / phase.wall.as_secs_f64(),
            p50_us: percentile_us(&phase.samples, 0.5),
            p95_us: percentile_us(&phase.samples, 0.95),
            p99_us: percentile_us(&phase.samples, 0.99),
            p999_us: percentile_us(&phase.samples, 0.999),
            max_us: phase.samples[attempts - 1] as f64 / 1_000.0,
            tls_handshakes_before_measurement: phase
                .tls_before
                .map(|snapshot| snapshot.successful_handshakes),
            tls_handshakes_after_measurement: phase
                .tls_after
                .map(|snapshot| snapshot.successful_handshakes),
            tls_handshakes_during_measurement: handshake_delta,
            tls_negotiation_mismatches: phase
                .tls_after
                .map(|snapshot| snapshot.negotiation_mismatches),
            tls_negotiation_observation: if candidate.is_tls() {
                if negotiation_verified {
                    if matches!(candidate, Candidate::QuinnRpcStream | Candidate::QuinnLane) {
                        QUIC_OBSERVATION
                    } else {
                        TLS_STREAM_OBSERVATION
                    }
                } else {
                    "negotiation missing or mismatched"
                }
            } else {
                "not-applicable"
            },
            diagnostic_valid,
            comparison_valid: false,
        }
    }
}

fn percentile_us(sorted_samples_ns: &[u64], quantile: f64) -> f64 {
    let rank = ((sorted_samples_ns.len() as f64 * quantile).ceil() as usize).max(1) - 1;
    sorted_samples_ns[rank.min(sorted_samples_ns.len() - 1)] as f64 / 1_000.0
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap_or_else(|_| panic!("rustls CryptoProvider was already set incompatibly"));
    let config = Config::parse().unwrap_or_else(|error| {
        eprintln!("configuration error: {error}");
        print_usage();
        process::exit(2);
    });
    let tls = generate_tls_material();
    let http_addr = start_http().await;
    let (https_addr, https_telemetry) = start_https(&tls).await.unwrap_or_else(|error| {
        eprintln!("HTTPS setup error: {error}");
        process::exit(1);
    });
    let tcp_addr = start_tcp().await;
    let (tcp_tls_addr, tcp_tls_telemetry) = start_tcp_tls(&tls).await.unwrap_or_else(|error| {
        eprintln!("TLS/TCP setup error: {error}");
        process::exit(1);
    });
    let quic_server = start_quic_server(&tls).await.unwrap_or_else(|error| {
        eprintln!("QUIC setup error: {error}");
        process::exit(1);
    });

    let mut metrics = Vec::new();
    for &payload_bytes in &config.payloads {
        for &concurrency in &config.concurrencies {
            for &candidate in &config.candidates {
                let metric = match candidate {
                    Candidate::HttpJson | Candidate::HttpsJson => {
                        bench_http(
                            if candidate == Candidate::HttpJson {
                                http_addr
                            } else {
                                https_addr
                            },
                            Codec::Json,
                            candidate,
                            (candidate == Candidate::HttpsJson).then(|| HttpTls {
                                root_certificate: &tls.certificate,
                                telemetry: https_telemetry.clone(),
                            }),
                            payload_bytes,
                            concurrency,
                            &config,
                        )
                        .await
                    }
                    Candidate::HttpPostcard | Candidate::HttpsPostcard => {
                        bench_http(
                            if candidate == Candidate::HttpPostcard {
                                http_addr
                            } else {
                                https_addr
                            },
                            Codec::Postcard,
                            candidate,
                            (candidate == Candidate::HttpsPostcard).then(|| HttpTls {
                                root_certificate: &tls.certificate,
                                telemetry: https_telemetry.clone(),
                            }),
                            payload_bytes,
                            concurrency,
                            &config,
                        )
                        .await
                    }
                    Candidate::HttpProst | Candidate::HttpsProst => {
                        bench_http(
                            if candidate == Candidate::HttpProst {
                                http_addr
                            } else {
                                https_addr
                            },
                            Codec::Prost,
                            candidate,
                            (candidate == Candidate::HttpsProst).then(|| HttpTls {
                                root_certificate: &tls.certificate,
                                telemetry: https_telemetry.clone(),
                            }),
                            payload_bytes,
                            concurrency,
                            &config,
                        )
                        .await
                    }
                    Candidate::TcpPostcard => {
                        bench_tcp(tcp_addr, candidate, payload_bytes, concurrency, &config).await
                    }
                    Candidate::TcpTlsPostcard => {
                        bench_tcp_tls(
                            tcp_tls_addr,
                            &tls,
                            tcp_tls_telemetry.clone(),
                            candidate,
                            payload_bytes,
                            concurrency,
                            &config,
                        )
                        .await
                    }
                    Candidate::QuinnRpcStream | Candidate::QuinnLane => {
                        bench_quinn(
                            &quic_server,
                            &tls,
                            candidate,
                            payload_bytes,
                            concurrency,
                            &config,
                        )
                        .await
                    }
                };
                metrics.push(metric);
            }
        }
    }
    let diagnostic_valid = metrics.iter().all(|metric| metric.diagnostic_valid);
    let environment = environment();
    let mut comparison_blockers =
        vec!["single raw run; three independent runs are required".to_owned()];
    if environment.git_dirty != Some(false) {
        comparison_blockers.push("Git tree is dirty or its state is unknown".to_owned());
    }
    if config
        .candidates
        .iter()
        .any(|candidate| !candidate.is_tls())
    {
        comparison_blockers.push("candidate set mixes plaintext and TLS controls".to_owned());
    }
    if !diagnostic_valid {
        comparison_blockers.push("one or more rows failed warmup or measurement".to_owned());
    }
    let report = Report {
        schema_version: 2,
        generated_at_epoch_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        diagnostic_valid,
        comparison_valid: false,
        production_valid: false,
        comparison_blockers,
        environment,
        conditions: Conditions {
            host: "127.0.0.1 loopback; client and servers in one process",
            security: "equal-TLS candidates use one generated certificate/root with TLS server authentication; plaintext HTTP/TCP remain decomposition controls; no mTLS",
            includes: "request construction and encode, loopback transport, decode, semantic validation, SHA-256 response, response decode and validation",
            excludes: "TLS certificate generation and handshakes, QuePaxa quorum, persistence, fsync, materialization, remote network, resource profiling",
            warmup_operations_per_metric: config.warmup,
            measured_operations_per_metric: config.operations,
            payload_bytes: config.payloads,
            concurrency: config.concurrencies,
            candidates: config.candidates.iter().map(|candidate| candidate.name()).collect(),
            candidate_order_offset: config.candidate_order_offset,
            candidate_order: "the CLI/default order is rotated left by candidate_order_offset; candidates records the effective order",
            server_reuse: "servers are reused; HTTPS and TLS/TCP use per-worker connections and Quinn creates one fresh client endpoint/connection per metric cell; all are warmed before measurement",
            call_timeout_seconds: CALL_TIMEOUT.as_secs(),
            tls: TlsConditions {
                certificate_sha256: certificate_sha256(&tls),
                trust_roots: "generated benchmark certificate only; platform roots disabled",
                protocol_version: "TLS 1.3 only",
                https_alpn: "http/1.1",
                tcp_rpc_alpn: "rhiza-bench/1",
                quinn_alpn: "rhiza-bench/1",
                negotiation_observation: TLS_CONDITIONS_OBSERVATION,
                handshake_observation: "server-side successful handshake counters sampled immediately before and after measurement",
            },
        },
        metrics,
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

fn request(sequence: u64, payload: &[u8]) -> WireRequest {
    WireRequest {
        request_id: format!("transport-{sequence:020}"),
        slot: sequence,
        prev_hash: PREV_HASH.to_vec(),
        payload: payload.to_vec(),
    }
}

fn handle_request(request: WireRequest) -> Result<WireAck, String> {
    if request.request_id.is_empty()
        || request.prev_hash.len() != PREV_HASH.len()
        || request.payload.len() > MAX_FRAME
    {
        return Err("invalid request".into());
    }
    let request_id = request.request_id.clone();
    Ok(WireAck {
        slot: request.slot,
        accepted: true,
        hash: ack_hash(request.slot, &request.prev_hash, &request.payload).to_vec(),
        request_id,
    })
}

fn valid_ack(ack: &WireAck, sequence: u64, payload: &[u8]) -> bool {
    ack.accepted
        && ack.slot == sequence
        && ack.request_id == format!("transport-{sequence:020}")
        && ack.hash == ack_hash(sequence, &PREV_HASH, payload).as_slice()
}

fn encoded_sizes(candidate: Candidate, payload_bytes: usize) -> (usize, usize) {
    let codec = candidate.codec();
    let request = request(
        representative_sequence(candidate),
        &vec![0xa5; payload_bytes],
    );
    let ack = handle_request(request.clone()).unwrap();
    let mut request_bytes = codec.encode_request(&request).unwrap().len();
    let mut response_bytes = codec.encode_ack(&ack).unwrap().len();
    if matches!(
        candidate,
        Candidate::TcpPostcard
            | Candidate::TcpTlsPostcard
            | Candidate::QuinnRpcStream
            | Candidate::QuinnLane
    ) {
        request_bytes += 4;
        response_bytes += 4;
    }
    (request_bytes, response_bytes)
}

fn representative_sequence(candidate: Candidate) -> u64 {
    match candidate {
        Candidate::HttpJson
        | Candidate::HttpPostcard
        | Candidate::HttpProst
        | Candidate::HttpsJson
        | Candidate::HttpsPostcard
        | Candidate::HttpsProst => 1_000_000,
        Candidate::TcpPostcard | Candidate::TcpTlsPostcard => 2_000_000,
        Candidate::QuinnRpcStream | Candidate::QuinnLane => 3_000_000,
    }
}

fn environment() -> Environment {
    Environment {
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: git_dirty(),
        rustc: command_output("rustc", &["--version"]),
        os: command_output("uname", &["-a"]),
        cpu: command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
            .or_else(|| command_output("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo"])),
    }
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn git_dirty() -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    output.status.success().then_some(!output.stdout.is_empty())
}

fn ack_hash(slot: u64, prev_hash: &[u8], payload: &[u8]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(slot.to_be_bytes());
    digest.update(prev_hash);
    digest.update(payload);
    digest.finalize().into()
}

async fn start_http() -> SocketAddr {
    let app = http_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn start_https(tls: &TlsMaterial) -> Result<(SocketAddr, Arc<TlsTelemetry>), String> {
    let app = http_router();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let config = axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(server_tls_config(
        tls, HTTP_ALPN,
    )?));
    let telemetry = Arc::new(TlsTelemetry::default());
    let acceptor = HttpsObservingAcceptor {
        inner: axum_server::tls_rustls::RustlsAcceptor::new(config),
        telemetry: telemetry.clone(),
    };
    let server = axum_server::from_tcp(listener)
        .map_err(|error| error.to_string())?
        .acceptor(acceptor);
    tokio::spawn(async move {
        if let Err(error) = server.serve(app.into_make_service()).await {
            eprintln!("HTTPS server error: {error}");
        }
    });
    Ok((addr, telemetry))
}

fn http_router() -> Router {
    async fn json(body: Bytes) -> Result<Response, StatusCode> {
        http_handler(Codec::Json, &body)
    }
    async fn postcard(body: Bytes) -> Result<Response, StatusCode> {
        http_handler(Codec::Postcard, &body)
    }
    async fn prost(body: Bytes) -> Result<Response, StatusCode> {
        http_handler(Codec::Prost, &body)
    }
    Router::new()
        .route("/json", post(json))
        .route("/postcard", post(postcard))
        .route("/prost", post(prost))
}

fn http_handler(codec: Codec, body: &[u8]) -> Result<Response, StatusCode> {
    let request = codec
        .decode_request(body)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let ack = handle_request(request).map_err(|_| StatusCode::BAD_REQUEST)?;
    let body = codec
        .encode_ack(&ack)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Response::builder()
        .header(CONTENT_TYPE, codec.content_type())
        .body(Body::from(body))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn bench_http(
    addr: SocketAddr,
    codec: Codec,
    candidate: Candidate,
    tls: Option<HttpTls<'_>>,
    payload_bytes: usize,
    concurrency: usize,
    config: &Config,
) -> Metric {
    let path = match codec {
        Codec::Json => "json",
        Codec::Postcard => "postcard",
        Codec::Prost => "prost",
    };
    let scheme = if tls.is_some() { "https" } else { "http" };
    let url = Arc::new(format!("{scheme}://localhost:{}/{path}", addr.port()));
    let payload = Arc::new(vec![0xa5; payload_bytes]);
    let root_certificate = tls
        .as_ref()
        .map(|tls| Arc::new(tls.root_certificate.as_ref().to_vec()));
    let telemetry = tls.map(|tls| tls.telemetry);
    let warmup = config.warmup;
    let operations = config.operations;
    let phase = tokio::task::spawn_blocking(move || {
        http_persistent_phase(
            HttpPhaseInput {
                url,
                codec,
                payload,
                root_certificate,
                telemetry,
            },
            concurrency,
            warmup,
            operations,
            1_000_000,
        )
    })
    .await
    .unwrap();
    Metric::from_samples(candidate, payload_bytes, concurrency, phase)
}

fn http_client(root_certificate: Option<&[u8]>) -> Result<reqwest::blocking::Client, String> {
    let mut builder = reqwest::blocking::Client::builder()
        .connect_timeout(CALL_TIMEOUT)
        .timeout(CALL_TIMEOUT)
        .http1_only()
        .tls_version_min(reqwest::tls::Version::TLS_1_3)
        .tls_version_max(reqwest::tls::Version::TLS_1_3);
    if let Some(certificate) = root_certificate {
        let certificate =
            reqwest::Certificate::from_der(certificate).map_err(|error| error.to_string())?;
        builder = builder.tls_certs_only([certificate]);
    }
    builder.build().map_err(|error| error.to_string())
}

fn http_persistent_phase(
    input: HttpPhaseInput,
    concurrency: usize,
    warmup: usize,
    operations: usize,
    offset: u64,
) -> PhaseResult {
    let HttpPhaseInput {
        url,
        codec,
        payload,
        root_certificate,
        telemetry,
    } = input;
    let warmup_start = Arc::new(Barrier::new(concurrency + 1));
    let measurement_ready = Arc::new(Barrier::new(concurrency + 1));
    let measurement_start = Arc::new(Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let client = http_client(root_certificate.as_deref().map(Vec::as_slice)).ok();
        let url = url.clone();
        let payload = payload.clone();
        let warmup_start = warmup_start.clone();
        let measurement_ready = measurement_ready.clone();
        let measurement_start = measurement_start.clone();
        let warmup_count = worker_count(warmup, concurrency, worker);
        let count = worker_count(operations, concurrency, worker);
        workers.push(thread::spawn(move || {
            let mut samples = Vec::with_capacity(count);
            let mut errors = 0;
            warmup_start.wait();
            for local in 0..warmup_count {
                let sequence = sequence(concurrency, worker, local) as u64;
                let ok = client
                    .as_ref()
                    .is_some_and(|client| http_exchange(client, &url, codec, &payload, sequence));
                errors += usize::from(!ok);
            }
            let warmup_errors = errors;
            errors = 0;
            measurement_ready.wait();
            measurement_start.wait();
            for local in 0..count {
                let sequence = offset + sequence(concurrency, worker, local) as u64;
                let started = Instant::now();
                let ok = client
                    .as_ref()
                    .is_some_and(|client| http_exchange(client, &url, codec, &payload, sequence));
                samples.push(started.elapsed().as_nanos() as u64);
                errors += usize::from(!ok);
            }
            (samples, errors, warmup_count, warmup_errors)
        }));
    }
    warmup_start.wait();
    measurement_ready.wait();
    let tls_before = telemetry.as_ref().map(|telemetry| telemetry.snapshot());
    let started = Instant::now();
    measurement_start.wait();
    let mut samples = Vec::with_capacity(operations);
    let mut errors = 0;
    let mut warmup_attempts = 0;
    let mut warmup_errors = 0;
    for worker in workers {
        let (mut one, worker_errors, worker_warmup_attempts, worker_warmup_errors) =
            worker.join().unwrap();
        samples.append(&mut one);
        errors += worker_errors;
        warmup_attempts += worker_warmup_attempts;
        warmup_errors += worker_warmup_errors;
    }
    let wall = started.elapsed();
    let tls_after = telemetry.as_ref().map(|telemetry| telemetry.snapshot());
    PhaseResult {
        samples,
        errors,
        wall,
        warmup_attempts,
        warmup_errors,
        tls_before,
        tls_after,
    }
}

fn http_exchange(
    client: &reqwest::blocking::Client,
    url: &str,
    codec: Codec,
    payload: &[u8],
    sequence: u64,
) -> bool {
    let encoded = codec.encode_request(&request(sequence, payload)).unwrap();
    client
        .post(url)
        .header(CONTENT_TYPE, codec.content_type())
        .body(encoded)
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .and_then(reqwest::blocking::Response::bytes)
        .ok()
        .and_then(|bytes| codec.decode_ack(&bytes).ok())
        .is_some_and(|ack| valid_ack(&ack, sequence, payload))
}

async fn start_tcp() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(serve_tcp_stream(stream));
        }
    });
    addr
}

async fn start_tcp_tls(tls: &TlsMaterial) -> Result<(SocketAddr, Arc<TlsTelemetry>), String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let addr = listener.local_addr().map_err(|error| error.to_string())?;
    let server_config = server_tls_config(tls, RPC_ALPN)?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let telemetry = Arc::new(TlsTelemetry::default());
    let server_telemetry = telemetry.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let acceptor = acceptor.clone();
            let telemetry = server_telemetry.clone();
            tokio::spawn(async move {
                if let Ok(stream) = acceptor.accept(stream).await {
                    let session = stream.get_ref().1;
                    telemetry.record(
                        session.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3)
                            && session.alpn_protocol() == Some(RPC_ALPN),
                    );
                    serve_tcp_stream(stream).await;
                }
            });
        }
    });
    Ok((addr, telemetry))
}

async fn serve_tcp_stream<S>(mut stream: S)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let Ok(Some(frame)) = read_tcp_frame(&mut stream).await else {
            break;
        };
        let result = Codec::Postcard
            .decode_request(&frame)
            .and_then(handle_request)
            .and_then(|ack| Codec::Postcard.encode_ack(&ack));
        let Ok(response) = result else { break };
        if write_tcp_frame(&mut stream, &response).await.is_err() {
            break;
        }
    }
}

async fn bench_tcp(
    addr: SocketAddr,
    candidate: Candidate,
    payload_bytes: usize,
    concurrency: usize,
    config: &Config,
) -> Metric {
    let payload = Arc::new(vec![0xa5; payload_bytes]);
    let phase = tcp_phase(
        addr,
        payload,
        concurrency,
        config.warmup,
        config.operations,
        2_000_000,
    )
    .await;
    Metric::from_samples(candidate, payload_bytes, concurrency, phase)
}

async fn bench_tcp_tls(
    addr: SocketAddr,
    tls: &TlsMaterial,
    telemetry: Arc<TlsTelemetry>,
    candidate: Candidate,
    payload_bytes: usize,
    concurrency: usize,
    config: &Config,
) -> Metric {
    let payload = Arc::new(vec![0xa5; payload_bytes]);
    let client_config = client_tls_config(tls, RPC_ALPN).unwrap();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
    let phase = tcp_tls_phase(
        addr,
        TcpTlsClient {
            connector,
            telemetry,
        },
        payload,
        concurrency,
        config.warmup,
        config.operations,
        2_000_000,
    )
    .await;
    Metric::from_samples(candidate, payload_bytes, concurrency, phase)
}

async fn tcp_tls_phase(
    addr: SocketAddr,
    client: TcpTlsClient,
    payload: Arc<Vec<u8>>,
    concurrency: usize,
    warmup: usize,
    operations: usize,
    offset: u64,
) -> PhaseResult {
    let warmup_start = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let measurement_ready = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let measurement_start = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let tcp = tokio::time::timeout(CALL_TIMEOUT, tokio::net::TcpStream::connect(addr))
            .await
            .ok()
            .and_then(Result::ok);
        if let Some(stream) = tcp.as_ref() {
            let _ = stream.set_nodelay(true);
        }
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .unwrap()
            .to_owned();
        let mut stream = match tcp {
            Some(stream) => tokio::time::timeout(
                CALL_TIMEOUT,
                client.connector.clone().connect(server_name, stream),
            )
            .await
            .ok()
            .and_then(Result::ok),
            None => None,
        };
        let payload = payload.clone();
        let warmup_start = warmup_start.clone();
        let measurement_ready = measurement_ready.clone();
        let measurement_start = measurement_start.clone();
        let warmup_count = worker_count(warmup, concurrency, worker);
        let count = worker_count(operations, concurrency, worker);
        workers.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(count);
            let mut errors = 0;
            warmup_start.wait().await;
            if let Some(stream) = stream.as_mut() {
                for local in 0..warmup_count {
                    let sequence = sequence(concurrency, worker, local) as u64;
                    errors += usize::from(!tcp_exchange(stream, sequence, &payload).await);
                }
            } else {
                errors += warmup_count;
            }
            let warmup_errors = errors;
            errors = 0;
            measurement_ready.wait().await;
            measurement_start.wait().await;
            for local in 0..count {
                let sequence = offset + sequence(concurrency, worker, local) as u64;
                let started = Instant::now();
                let ok = match stream.as_mut() {
                    Some(stream) => tcp_exchange(stream, sequence, &payload).await,
                    None => false,
                };
                samples.push(started.elapsed().as_nanos() as u64);
                errors += usize::from(!ok);
            }
            (samples, errors, warmup_count, warmup_errors)
        }));
    }
    warmup_start.wait().await;
    measurement_ready.wait().await;
    let tls_before = client.telemetry.snapshot();
    let started = Instant::now();
    measurement_start.wait().await;
    let (samples, errors, warmup_attempts, warmup_errors, wall) =
        collect_phase_tasks(workers, operations, started).await;
    let tls_after = client.telemetry.snapshot();
    PhaseResult {
        samples,
        errors,
        wall,
        warmup_attempts,
        warmup_errors,
        tls_before: Some(tls_before),
        tls_after: Some(tls_after),
    }
}

async fn tcp_phase(
    addr: SocketAddr,
    payload: Arc<Vec<u8>>,
    concurrency: usize,
    warmup: usize,
    operations: usize,
    offset: u64,
) -> PhaseResult {
    let warmup_start = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let measurement_ready = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let measurement_start = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let mut stream = tokio::time::timeout(CALL_TIMEOUT, tokio::net::TcpStream::connect(addr))
            .await
            .ok()
            .and_then(Result::ok);
        if let Some(stream) = stream.as_mut() {
            let _ = stream.set_nodelay(true);
        }
        let payload = payload.clone();
        let warmup_start = warmup_start.clone();
        let measurement_ready = measurement_ready.clone();
        let measurement_start = measurement_start.clone();
        let warmup_count = worker_count(warmup, concurrency, worker);
        let count = worker_count(operations, concurrency, worker);
        workers.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(count);
            let mut errors = 0;
            warmup_start.wait().await;
            if let Some(stream) = stream.as_mut() {
                for local in 0..warmup_count {
                    let sequence = sequence(concurrency, worker, local) as u64;
                    errors += usize::from(!tcp_exchange(stream, sequence, &payload).await);
                }
            } else {
                errors += warmup_count;
            }
            let warmup_errors = errors;
            errors = 0;
            measurement_ready.wait().await;
            measurement_start.wait().await;
            for local in 0..count {
                let sequence = offset + sequence(concurrency, worker, local) as u64;
                let started = Instant::now();
                let ok = match stream.as_mut() {
                    Some(stream) => tcp_exchange(stream, sequence, &payload).await,
                    None => false,
                };
                samples.push(started.elapsed().as_nanos() as u64);
                errors += usize::from(!ok);
            }
            (samples, errors, warmup_count, warmup_errors)
        }));
    }
    warmup_start.wait().await;
    measurement_ready.wait().await;
    let started = Instant::now();
    measurement_start.wait().await;
    let (samples, errors, warmup_attempts, warmup_errors, wall) =
        collect_phase_tasks(workers, operations, started).await;
    PhaseResult {
        samples,
        errors,
        wall,
        warmup_attempts,
        warmup_errors,
        tls_before: None,
        tls_after: None,
    }
}

async fn tcp_exchange<S>(stream: &mut S, sequence: u64, payload: &[u8]) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let encoded = Codec::Postcard
        .encode_request(&request(sequence, payload))
        .unwrap();
    tokio::time::timeout(CALL_TIMEOUT, async {
        write_tcp_frame(stream, &encoded).await.ok()?;
        let response = read_tcp_frame(stream).await.ok()??;
        let ack = Codec::Postcard.decode_ack(&response).ok()?;
        Some(valid_ack(&ack, sequence, payload))
    })
    .await
    .ok()
    .flatten()
    .unwrap_or(false)
}

async fn read_tcp_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>, String> {
    let mut length = [0_u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.to_string()),
    }
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        return Err("oversize frame".into());
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|error| error.to_string())?;
    Ok(Some(frame))
}

async fn write_tcp_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> Result<(), String> {
    let bytes = framed(frame)?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| error.to_string())
}

async fn start_quic_server(tls: &TlsMaterial) -> Result<QuinnServer, String> {
    let server_crypto = server_tls_config(tls, RPC_ALPN)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .map_err(|error| error.to_string())?,
    ));
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .max_concurrent_bidi_streams(1_024_u32.into());
    let server = Endpoint::server(
        server_config,
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
    )
    .map_err(|error| error.to_string())?;
    let server_addr = server.local_addr().map_err(|error| error.to_string())?;
    let telemetry = Arc::new(TlsTelemetry::default());
    let server_telemetry = telemetry.clone();
    let accept_endpoint = server.clone();
    tokio::spawn(async move {
        while let Some(incoming) = accept_endpoint.accept().await {
            let telemetry = server_telemetry.clone();
            tokio::spawn(async move {
                let Ok(connection) = incoming.await else {
                    return;
                };
                let alpn_matches = connection
                    .handshake_data()
                    .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                    .is_some_and(|data| data.protocol.as_deref() == Some(RPC_ALPN));
                telemetry.record(alpn_matches);
                while let Ok((send, recv)) = connection.accept_bi().await {
                    tokio::spawn(handle_quic_stream(send, recv));
                }
            });
        }
    });
    Ok(QuinnServer {
        _endpoint: server,
        addr: server_addr,
        telemetry,
    })
}

async fn connect_quic(server: &QuinnServer, tls: &TlsMaterial) -> Result<QuinnClient, String> {
    let client_crypto = client_tls_config(tls, RPC_ALPN)?;
    let client_config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_crypto)
            .map_err(|error| error.to_string())?,
    ));
    let mut client = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
        .map_err(|error| error.to_string())?;
    client.set_default_client_config(client_config);
    let connection = client
        .connect(server.addr, "localhost")
        .map_err(|error| error.to_string())?
        .await
        .map_err(|error| error.to_string())?;
    Ok(QuinnClient {
        _endpoint: client,
        connection,
        telemetry: server.telemetry.clone(),
    })
}

async fn handle_quic_stream(mut send: quinn::SendStream, mut recv: quinn::RecvStream) {
    while let Ok(frame) = read_quic_frame(&mut recv).await {
        let result = Codec::Postcard
            .decode_request(&frame)
            .and_then(handle_request)
            .and_then(|ack| Codec::Postcard.encode_ack(&ack));
        let Ok(response) = result else { break };
        if write_quic_frame(&mut send, &response).await.is_err() {
            break;
        }
    }
    let _ = send.finish();
}

async fn bench_quinn(
    server: &QuinnServer,
    tls: &TlsMaterial,
    candidate: Candidate,
    payload_bytes: usize,
    concurrency: usize,
    config: &Config,
) -> Metric {
    let client = connect_quic(server, tls)
        .await
        .unwrap_or_else(|error| panic!("QUIC cell setup failed: {error}"));
    let payload = Arc::new(vec![0xa5; payload_bytes]);
    let phase = quinn_phase(
        client,
        candidate,
        payload,
        concurrency,
        config.warmup,
        config.operations,
        3_000_000,
    )
    .await;
    Metric::from_samples(candidate, payload_bytes, concurrency, phase)
}

async fn quinn_phase(
    client: QuinnClient,
    candidate: Candidate,
    payload: Arc<Vec<u8>>,
    concurrency: usize,
    warmup: usize,
    operations: usize,
    offset: u64,
) -> PhaseResult {
    let warmup_start = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let measurement_ready = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let measurement_start = Arc::new(tokio::sync::Barrier::new(concurrency + 1));
    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let connection = client.connection.clone();
        let payload = payload.clone();
        let warmup_start = warmup_start.clone();
        let measurement_ready = measurement_ready.clone();
        let measurement_start = measurement_start.clone();
        let warmup_count = worker_count(warmup, concurrency, worker);
        let count = worker_count(operations, concurrency, worker);
        workers.push(tokio::spawn(async move {
            let mut lane = if candidate == Candidate::QuinnLane {
                tokio::time::timeout(CALL_TIMEOUT, connection.open_bi())
                    .await
                    .ok()
                    .and_then(Result::ok)
            } else {
                None
            };
            let mut samples = Vec::with_capacity(count);
            let mut errors = 0;
            warmup_start.wait().await;
            for local in 0..warmup_count {
                let sequence = sequence(concurrency, worker, local) as u64;
                errors += usize::from(
                    !quinn_call(&connection, candidate, &mut lane, sequence, &payload).await,
                );
            }
            let warmup_errors = errors;
            errors = 0;
            measurement_ready.wait().await;
            measurement_start.wait().await;
            for local in 0..count {
                let sequence = offset + sequence(concurrency, worker, local) as u64;
                let started = Instant::now();
                let ok = quinn_call(&connection, candidate, &mut lane, sequence, &payload).await;
                samples.push(started.elapsed().as_nanos() as u64);
                errors += usize::from(!ok);
            }
            if let Some((send, _)) = lane.as_mut() {
                let _ = send.finish();
            }
            (samples, errors, warmup_count, warmup_errors)
        }));
    }
    warmup_start.wait().await;
    measurement_ready.wait().await;
    let tls_before = client.telemetry.snapshot();
    let started = Instant::now();
    measurement_start.wait().await;
    let (samples, errors, warmup_attempts, warmup_errors, wall) =
        collect_phase_tasks(workers, operations, started).await;
    client
        .connection
        .close(0_u32.into(), b"metric cell complete");
    client._endpoint.wait_idle().await;
    let tls_after = client.telemetry.snapshot();
    PhaseResult {
        samples,
        errors,
        wall,
        warmup_attempts,
        warmup_errors,
        tls_before: Some(tls_before),
        tls_after: Some(tls_after),
    }
}

async fn quinn_call(
    connection: &Connection,
    candidate: Candidate,
    lane: &mut Option<(quinn::SendStream, quinn::RecvStream)>,
    sequence: u64,
    payload: &[u8],
) -> bool {
    let encoded = Codec::Postcard
        .encode_request(&request(sequence, payload))
        .unwrap();
    tokio::time::timeout(CALL_TIMEOUT, async {
        if candidate == Candidate::QuinnLane {
            match lane.as_mut() {
                Some((send, recv)) => quinn_exchange(send, recv, &encoded, sequence, payload).await,
                None => false,
            }
        } else {
            match connection.open_bi().await {
                Ok((mut send, mut recv)) => {
                    let ok =
                        quinn_exchange(&mut send, &mut recv, &encoded, sequence, payload).await;
                    let _ = send.finish();
                    ok
                }
                Err(_) => false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

async fn quinn_exchange(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    encoded: &[u8],
    sequence: u64,
    payload: &[u8],
) -> bool {
    let result = async {
        write_quic_frame(send, encoded).await.ok()?;
        let response = read_quic_frame(recv).await.ok()?;
        let ack = Codec::Postcard.decode_ack(&response).ok()?;
        Some(valid_ack(&ack, sequence, payload))
    }
    .await;
    result.unwrap_or(false)
}

async fn read_quic_frame(reader: &mut quinn::RecvStream) -> Result<Vec<u8>, String> {
    let mut length = [0_u8; 4];
    reader
        .read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME {
        return Err("oversize frame".into());
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|error| error.to_string())?;
    Ok(frame)
}

async fn write_quic_frame(writer: &mut quinn::SendStream, frame: &[u8]) -> Result<(), String> {
    let bytes = framed(frame)?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|error| error.to_string())
}

fn framed(payload: &[u8]) -> Result<Vec<u8>, String> {
    if payload.len() > MAX_FRAME || payload.len() > u32::MAX as usize {
        return Err("oversize frame".into());
    }
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

async fn collect_phase_tasks(
    workers: Vec<WorkerTask>,
    operations: usize,
    started: Instant,
) -> (Vec<u64>, usize, usize, usize, Duration) {
    let mut samples = Vec::with_capacity(operations);
    let mut errors = 0;
    let mut warmup_attempts = 0;
    let mut warmup_errors = 0;
    for worker in workers {
        let (mut one, count, worker_warmup_attempts, worker_warmup_errors) = worker.await.unwrap();
        samples.append(&mut one);
        errors += count;
        warmup_attempts += worker_warmup_attempts;
        warmup_errors += worker_warmup_errors;
    }
    (
        samples,
        errors,
        warmup_attempts,
        warmup_errors,
        started.elapsed(),
    )
}

fn worker_count(total: usize, concurrency: usize, worker: usize) -> usize {
    total / concurrency + usize::from(worker < total % concurrency)
}

fn sequence(concurrency: usize, worker: usize, local: usize) -> usize {
    local * concurrency + worker
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codecs_preserve_request_and_valid_response() {
        let request = request(7, &[0xa5; 128]);
        for codec in [Codec::Json, Codec::Postcard, Codec::Prost] {
            let decoded = codec
                .decode_request(&codec.encode_request(&request).unwrap())
                .unwrap();
            assert_eq!(decoded, request);
            let ack = handle_request(decoded).unwrap();
            let decoded_ack = codec.decode_ack(&codec.encode_ack(&ack).unwrap()).unwrap();
            assert!(valid_ack(&decoded_ack, 7, &[0xa5; 128]));
        }
    }

    #[test]
    fn framing_is_length_prefixed_and_bounded() {
        let payload = b"rhiza";
        let frame = framed(payload).unwrap();
        assert_eq!(&frame[..4], &(payload.len() as u32).to_be_bytes());
        assert_eq!(&frame[4..], payload);
        assert!(framed(&vec![0; MAX_FRAME + 1]).is_err());
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        let samples = [1_000, 2_000, 3_000, 4_000];
        assert_eq!(percentile_us(&samples, 0.50), 2.0);
        assert_eq!(percentile_us(&samples, 0.99), 4.0);
    }

    #[tokio::test]
    async fn frame_reader_rejects_truncated_and_oversize_frames() {
        let (mut writer, mut reader) = tokio::io::duplex(32);
        writer.write_all(&5_u32.to_be_bytes()).await.unwrap();
        writer.write_all(b"no").await.unwrap();
        writer.shutdown().await.unwrap();
        assert!(read_tcp_frame(&mut reader).await.is_err());

        let (mut writer, mut reader) = tokio::io::duplex(32);
        writer
            .write_all(&((MAX_FRAME + 1) as u32).to_be_bytes())
            .await
            .unwrap();
        assert!(read_tcp_frame(&mut reader).await.is_err());
    }

    #[test]
    fn short_candidate_aliases_are_accepted() {
        assert_eq!(
            Candidate::parse("tcp-postcard"),
            Some(Candidate::TcpPostcard)
        );
        assert_eq!(
            Candidate::parse("tcp-tls-postcard"),
            Some(Candidate::TcpTlsPostcard)
        );
        assert_eq!(Candidate::parse("quinn-lane"), Some(Candidate::QuinnLane));
    }

    #[test]
    fn plaintext_tcp_candidate_is_canonical_and_isolates_tls_overhead() {
        assert_eq!(Candidate::TcpPostcard.name(), "tcp-postcard");
        assert_eq!(
            Candidate::TcpPostcard.codec(),
            Candidate::TcpTlsPostcard.codec()
        );
        assert_eq!(
            Candidate::TcpPostcard.transport(),
            Candidate::TcpTlsPostcard.transport()
        );
        assert_eq!(
            Candidate::TcpPostcard.topology(),
            Candidate::TcpTlsPostcard.topology()
        );
        assert_eq!(Candidate::TcpPostcard.tls(), "none");
        assert!(Candidate::TcpTlsPostcard.is_tls());
    }

    #[test]
    fn candidate_order_offset_is_parsed_and_rotates_selected_candidates() {
        let config = Config::parse_from([
            "--candidates",
            "http-json,https-json,tcp-tls-postcard",
            "--candidate-order-offset",
            "4",
        ])
        .unwrap();
        assert_eq!(config.candidate_order_offset, 4);
        assert_eq!(
            config.candidates,
            [
                Candidate::HttpsJson,
                Candidate::TcpTlsPostcard,
                Candidate::HttpJson,
            ]
        );
        assert!(Config::parse_from(["--candidate-order-offset", "-1"]).is_err());
    }

    #[test]
    fn warmup_covers_every_concurrency_worker() {
        assert!(
            Config::parse_from(["--warmup", "7", "--concurrency", "1,8"])
                .unwrap_err()
                .contains("must be at least")
        );
        assert!(Config::parse_from(["--warmup", "8", "--concurrency", "1,8"]).is_ok());
    }

    #[test]
    fn tls_configs_pin_certificate_tls13_and_protocol_alpn() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls = generate_tls_material();
        let https = server_tls_config(&tls, HTTP_ALPN).unwrap();
        let tcp = server_tls_config(&tls, RPC_ALPN).unwrap();
        let client = client_tls_config(&tls, RPC_ALPN).unwrap();
        assert_eq!(https.alpn_protocols, [HTTP_ALPN]);
        assert_eq!(tcp.alpn_protocols, [RPC_ALPN]);
        assert_eq!(client.alpn_protocols, [RPC_ALPN]);
        assert_eq!(certificate_sha256(&tls).len(), 64);
        assert_eq!(
            TLS_STREAM_OBSERVATION,
            "server-side negotiated TLS 1.3 and ALPN observed and verified"
        );
        assert!(QUIC_OBSERVATION.contains("QUIC invariant"));
    }

    #[test]
    fn tls_telemetry_counts_successes_and_negotiation_mismatches() {
        let telemetry = TlsTelemetry::default();
        telemetry.record(true);
        telemetry.record(false);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.successful_handshakes, 2);
        assert_eq!(snapshot.negotiation_mismatches, 1);
    }

    #[test]
    fn equal_tls_candidates_report_shared_server_authentication() {
        for candidate in [
            Candidate::HttpsJson,
            Candidate::HttpsPostcard,
            Candidate::HttpsProst,
            Candidate::TcpTlsPostcard,
            Candidate::QuinnRpcStream,
            Candidate::QuinnLane,
        ] {
            assert_eq!(
                candidate.tls(),
                "TLS server authentication; shared benchmark certificate"
            );
        }
        for candidate in [
            Candidate::HttpJson,
            Candidate::HttpPostcard,
            Candidate::HttpProst,
            Candidate::TcpPostcard,
        ] {
            assert_eq!(candidate.tls(), "none");
        }
    }

    #[test]
    fn encoded_sizes_use_each_transport_measurement_sequence() {
        for candidate in Candidate::ALL {
            let sequence = representative_sequence(candidate);
            assert_eq!(
                sequence,
                match candidate {
                    Candidate::HttpJson
                    | Candidate::HttpPostcard
                    | Candidate::HttpProst
                    | Candidate::HttpsJson
                    | Candidate::HttpsPostcard
                    | Candidate::HttpsProst => 1_000_000,
                    Candidate::TcpPostcard | Candidate::TcpTlsPostcard => 2_000_000,
                    Candidate::QuinnRpcStream | Candidate::QuinnLane => 3_000_000,
                }
            );
            let codec = candidate.codec();
            let request = request(sequence, &[0xa5; 128]);
            let ack = handle_request(request.clone()).unwrap();
            let framing = usize::from(matches!(
                candidate,
                Candidate::TcpPostcard
                    | Candidate::TcpTlsPostcard
                    | Candidate::QuinnRpcStream
                    | Candidate::QuinnLane
            )) * 4;
            assert_eq!(
                encoded_sizes(candidate, 128),
                (
                    codec.encode_request(&request).unwrap().len() + framing,
                    codec.encode_ack(&ack).unwrap().len() + framing,
                )
            );
        }
    }
}
