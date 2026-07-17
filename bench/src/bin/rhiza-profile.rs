use std::{
    collections::BTreeMap,
    env,
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rhiza::{
    effective_cluster_id, EmbeddedConfig, EmbeddedIdentity, ExecutionProfile, GraphCommandV1,
    GraphParameterValue, GraphValueV1, ReadConsistency, Rhiza, RhizaHandle, SqlCommand,
    SqlStatement, SqlValue,
};
use rhiza_core::{Command as ConsensusCommand, CommandKind, EntryType, LogEntry, LogHash};
use rhiza_graph::{encode_replicated_graph_command, LadybugStateMachine};
use rhiza_kv::{encode_replicated_kv_command, KvCommandV1, RedbStateMachine};
use rhiza_node::{NodeConfig, NodeRuntime};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
use rhiza_sql::{encode_sql_command, SqliteStateMachine};
use serde::Serialize;

const KEYSPACE: u64 = 256;
const RAW_RESULT_BYTES: usize = 1024 * 1024;
const RAW_GRAPH_TIMEOUT_MS: u64 = 5_000;
type RecorderSet = Vec<(String, Box<dyn RecorderRpc>)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Profile {
    Sql,
    Graph,
    Kv,
}

impl Profile {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "sql" => Ok(Self::Sql),
            "graph" => Ok(Self::Graph),
            "kv" => Ok(Self::Kv),
            _ => Err("--profile must be sql, graph, or kv".into()),
        }
    }

    const fn execution_profile(self) -> ExecutionProfile {
        match self {
            Self::Sql => ExecutionProfile::Sqlite,
            Self::Graph => ExecutionProfile::Graph,
            Self::Kv => ExecutionProfile::Kv,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    Write,
    Get,
    DocumentGet,
    NativeRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Layer {
    Handle,
    Runtime,
    Raw,
    Consensus,
}

impl Layer {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "handle" => Ok(Self::Handle),
            "runtime" => Ok(Self::Runtime),
            "raw" => Ok(Self::Raw),
            "consensus" => Ok(Self::Consensus),
            _ => Err("--layer must be handle, runtime, raw, or consensus".into()),
        }
    }

    const fn scope(self) -> &'static str {
        match self {
            Self::Handle => "public RhizaHandle embedded API",
            Self::Runtime => {
                "NodeRuntime API with one blocking thread per benchmark worker; excludes RhizaHandle"
            }
            Self::Raw => {
                "direct materializer reads; writes encode a command, construct LogEntry, and apply it"
            }
            Self::Consensus => {
                "generic ThreeNodeConsensus::propose_at with deterministic payload; selected profile is not exercised; excludes qlog and materializer"
            }
        }
    }

    const fn consensus(self) -> &'static str {
        match self {
            Self::Handle | Self::Runtime => {
                "in-process QuePaxa with three file-backed RecorderRpc voters"
            }
            Self::Raw => {
                "excluded; writes include command encode, LogEntry construction, and materializer apply"
            }
            Self::Consensus => "in-process QuePaxa with three file-backed RecorderRpc voters",
        }
    }

    const fn durability(self) -> &'static str {
        match self {
            Self::Handle | Self::Runtime => {
                "RecorderFileStore local fsync plus local qlog/materializer"
            }
            Self::Raw => "materializer-native local commit only",
            Self::Consensus => {
                "three RecorderFileStore voter commits; excludes local qlog and materializer"
            }
        }
    }
}

impl Workload {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "write" => Ok(Self::Write),
            "get" => Ok(Self::Get),
            "document-get" => Ok(Self::DocumentGet),
            "native-read" => Ok(Self::NativeRead),
            _ => Err("--workload must be write, get, document-get, or native-read".into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    layer: Layer,
    profile: Profile,
    workload: Workload,
    batch_size: usize,
    operations: u64,
    warmup: u64,
    concurrency: usize,
    value_bytes: usize,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let values: Vec<_> = args.into_iter().collect();
        let mut profile = None;
        let mut workload = None;
        let mut layer = Layer::Handle;
        let mut batch_size = 1;
        let mut operations = 1_000;
        let mut warmup = 100;
        let mut concurrency = 1;
        let mut value_bytes = 128;
        let mut index = 0;
        while index < values.len() {
            let flag = &values[index];
            let next = || {
                values
                    .get(index + 1)
                    .ok_or_else(|| format!("{flag} requires a value"))
            };
            match flag.as_str() {
                "--layer" => layer = Layer::parse(next()?)?,
                "--profile" => profile = Some(Profile::parse(next()?)?),
                "--workload" => workload = Some(Workload::parse(next()?)?),
                "--batch-size" => batch_size = parse_usize(next()?, flag)?,
                "--operations" => operations = parse_u64(next()?, flag)?,
                "--warmup" => warmup = parse_u64_allow_zero(next()?, flag)?,
                "--concurrency" => concurrency = parse_usize(next()?, flag)?,
                "--value-bytes" => value_bytes = parse_usize(next()?, flag)?,
                "--help" | "-h" => return Err(usage()),
                _ => return Err(format!("unknown option: {flag}\n\n{}", usage())),
            }
            index += 2;
        }
        if !(16..=4_096).contains(&value_bytes) {
            return Err("--value-bytes must be between 16 and 4096".into());
        }
        if matches!(layer, Layer::Raw | Layer::Consensus) && concurrency != 1 {
            return Err(format!("--layer {layer:?} requires --concurrency 1").to_lowercase());
        }
        let profile = profile.ok_or_else(|| "--profile is required".to_string())?;
        let workload = workload.ok_or_else(|| "--workload is required".to_string())?;
        if workload == Workload::DocumentGet && profile != Profile::Graph {
            return Err("--workload document-get requires --profile graph".into());
        }
        if layer == Layer::Consensus && workload != Workload::Write {
            return Err("--layer consensus supports only --workload write".into());
        }
        if !matches!(batch_size, 1 | 2 | 4 | 8 | 16 | 32 | 64) {
            return Err("--batch-size must be 1, 2, 4, 8, 16, 32, or 64".into());
        }
        if batch_size != 1 && workload != Workload::Write {
            return Err("--batch-size greater than 1 requires --workload write".into());
        }
        if batch_size != 1 && !matches!(layer, Layer::Handle | Layer::Runtime) {
            return Err("--batch-size greater than 1 requires --layer handle or runtime".into());
        }
        Ok(Self {
            layer,
            profile,
            workload,
            batch_size,
            operations,
            warmup,
            concurrency,
            value_bytes,
        })
    }
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        Err(format!("{flag} must be a positive integer"))
    } else {
        Ok(parsed)
    }
}

fn parse_u64_allow_zero(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        Err(format!("{flag} must be a positive integer"))
    } else {
        Ok(parsed)
    }
}

fn usage() -> String {
    "usage: rhiza-profile --profile sql|graph|kv --workload write|get|document-get|native-read \
     [--layer handle|runtime|raw|consensus] [--operations N] [--warmup N] [--concurrency N] \
     [--batch-size 1|2|4|8|16|32|64] [--value-bytes N]"
        .into()
}

#[derive(Clone, Debug, Default)]
struct Samples {
    successes: u64,
    errors: u64,
    latency_us: BTreeMap<u64, u64>,
    error_classes: BTreeMap<String, u64>,
}

impl Samples {
    fn record(&mut self, latency: Duration, result: Result<(), String>) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        match result {
            Ok(()) => {
                self.successes += 1;
                *self.latency_us.entry(micros).or_default() += 1;
            }
            Err(error) => {
                self.errors += 1;
                let mut class = error.replace(['\n', '\r'], " ");
                class.truncate(160);
                if self.error_classes.len() < 16 || self.error_classes.contains_key(&class) {
                    *self.error_classes.entry(class).or_default() += 1;
                } else {
                    *self.error_classes.entry("other".into()).or_default() += 1;
                }
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.successes += other.successes;
        self.errors += other.errors;
        for (latency, count) in other.latency_us {
            *self.latency_us.entry(latency).or_default() += count;
        }
        for (class, count) in other.error_classes {
            *self.error_classes.entry(class).or_default() += count;
        }
    }

    fn percentile(&self, permille: u64) -> Option<u64> {
        if self.successes == 0 {
            return None;
        }
        let rank = self
            .successes
            .saturating_mul(permille)
            .div_ceil(1_000)
            .max(1);
        let mut seen = 0;
        self.latency_us.iter().find_map(|(latency, count)| {
            seen += count;
            (seen >= rank).then_some(*latency)
        })
    }

    fn metrics(&self, elapsed: Duration, qlog_entries: Option<u64>) -> Metrics {
        Metrics {
            attempts: self.successes + self.errors,
            successes: self.successes,
            errors: self.errors,
            elapsed_seconds: elapsed.as_secs_f64(),
            operations_per_second: if elapsed.is_zero() {
                0.0
            } else {
                self.successes as f64 / elapsed.as_secs_f64()
            },
            qlog_entries,
            latency_us: Latencies {
                p50: self.percentile(500),
                p95: self.percentile(950),
                p99: self.percentile(990),
                p99_9: self.percentile(999),
                max: self.latency_us.last_key_value().map(|(value, _)| *value),
            },
            error_classes: self.error_classes.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    benchmark: &'static str,
    generated_at_unix_ms: u128,
    provenance: Provenance,
    system: System,
    configuration: ReportConfig,
    measurement: Metrics,
    limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct Provenance {
    git_commit: String,
    git_dirty: bool,
    rustc: String,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct System {
    os: &'static str,
    architecture: &'static str,
    logical_cpus: usize,
    kernel: String,
    cpu_model: String,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    layer: Layer,
    measurement_scope: &'static str,
    profile: Profile,
    workload: Workload,
    batch_size: usize,
    logical_operations: u64,
    operations: u64,
    warmup_operations: u64,
    concurrency: usize,
    keyspace: u64,
    value_bytes: usize,
    read_consistency: &'static str,
    consensus: &'static str,
    durability: &'static str,
}

#[derive(Clone, Debug, Serialize)]
struct Metrics {
    attempts: u64,
    successes: u64,
    errors: u64,
    elapsed_seconds: f64,
    operations_per_second: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    qlog_entries: Option<u64>,
    latency_us: Latencies,
    error_classes: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Latencies {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    #[serde(rename = "p99.9")]
    p99_9: Option<u64>,
    max: Option<u64>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = match Config::parse(env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    match run(config).await {
        Ok(report) => {
            let failed = report.measurement.errors > 0;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            if failed {
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("rhiza-profile: {error}");
            std::process::exit(1);
        }
    }
}

async fn run(config: Config) -> Result<Report, String> {
    // Capture before setup or measurement so benchmark-created files cannot
    // change the source provenance reported for this run.
    let provenance = provenance();
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (samples, elapsed, qlog_entries) = match config.layer {
        Layer::Handle => {
            let rhiza = Rhiza::open(embedded_config(root.path(), config.profile)?)
                .await
                .map_err(|error| error.to_string())?;
            let measured = measure_target(Target::Handle(rhiza.handle()), &config).await;
            let shutdown = rhiza.shutdown().await.map_err(|error| error.to_string());
            let measured = measured?;
            shutdown?;
            measured
        }
        Layer::Runtime => {
            let runtime = runtime(root.path(), config.profile)?;
            measure_target(Target::Runtime(runtime), &config).await?
        }
        Layer::Raw => {
            let (samples, elapsed) =
                measure_raw(RawTarget::open(root.path(), config.profile)?, &config)?;
            (samples, elapsed, None)
        }
        Layer::Consensus => {
            let (samples, elapsed) =
                measure_consensus(ConsensusTarget::open(root.path())?, &config)?;
            (samples, elapsed, None)
        }
    };

    Ok(Report {
        schema_version: 1,
        benchmark: "rhiza-profile-direct",
        generated_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        provenance,
        system: system(),
        configuration: ReportConfig {
            layer: config.layer,
            measurement_scope: config.layer.scope(),
            profile: config.profile,
            workload: config.workload,
            batch_size: config.batch_size,
            logical_operations: config.operations,
            operations: config.operations,
            warmup_operations: config.warmup,
            concurrency: config.concurrency,
            keyspace: KEYSPACE,
            value_bytes: config.value_bytes,
            read_consistency: "local",
            consensus: config.layer.consensus(),
            durability: config.layer.durability(),
        },
        measurement: samples.metrics(elapsed, qlog_entries),
        limitations: vec![
            "single process on one host",
            "excludes HTTP serialization and transport",
            "excludes node-to-node network latency",
            "local reads exclude a consensus read barrier",
            "excludes remote checkpoint upload",
        ],
    })
}

fn embedded_config(root: &std::path::Path, profile: Profile) -> Result<EmbeddedConfig, String> {
    let execution_profile = profile.execution_profile();
    let membership =
        Membership::new(["node-1", "node-2", "node-3"]).map_err(|error| error.to_string())?;
    let cluster_id = effective_cluster_id(execution_profile, "profile-bench")
        .map_err(|error| error.to_string())?;
    let recorders = recorders(root, &membership, &cluster_id)?;
    Ok(EmbeddedConfig::new(
        EmbeddedIdentity::new("profile-bench", "node-1", 1, 1),
        root.join("node"),
        execution_profile,
        membership.members().to_vec(),
        recorders,
        vec![],
        None,
    ))
}

fn recorders(
    root: &Path,
    membership: &Membership,
    cluster_id: &str,
) -> Result<RecorderSet, String> {
    membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                cluster_id,
                1,
                1,
                membership.clone(),
            )
            .map(|recorder| (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>))
            .map_err(|error| error.to_string())
        })
        .collect()
}

fn runtime(root: &Path, profile: Profile) -> Result<Arc<NodeRuntime>, String> {
    let execution_profile = profile.execution_profile();
    let membership =
        Membership::new(["node-1", "node-2", "node-3"]).map_err(|error| error.to_string())?;
    let node_config = NodeConfig::new_embedded(
        "profile-bench",
        "node-1",
        root.join("node"),
        1,
        1,
        membership.members().to_vec(),
    )
    .map_err(|error| error.to_string())?
    .with_execution_profile(execution_profile)
    .map_err(|error| error.to_string())?;
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            node_config.cluster_id().to_owned(),
            "node-1",
            1,
            1,
            recorders(root, &membership, node_config.cluster_id())?,
        )
        .map_err(|error| error.to_string())?,
    );
    if node_config.membership() != consensus.membership() {
        return Err("runtime benchmark membership mismatch".into());
    }
    NodeRuntime::open(node_config, consensus, &[])
        .map(Arc::new)
        .map_err(|error| error.to_string())
}

#[derive(Clone)]
enum Target {
    Handle(RhizaHandle),
    Runtime(Arc<NodeRuntime>),
}

async fn measure_target(
    target: Target,
    config: &Config,
) -> Result<(Samples, Duration, Option<u64>), String> {
    setup(&target, config).await?;
    if config.warmup > 0 {
        let (warmup, _) = run_phase(target.clone(), config, config.warmup, "warmup").await;
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
    }
    let qlog_before = match &target {
        Target::Handle(_) => None,
        Target::Runtime(runtime) => {
            Some(runtime.applied_index().map_err(|error| error.to_string())?)
        }
    };
    let measured = run_phase(target.clone(), config, config.operations, "measure").await;
    let qlog_entries = match (&target, qlog_before) {
        (Target::Runtime(runtime), Some(before)) => Some(
            runtime
                .applied_index()
                .map_err(|error| error.to_string())?
                .saturating_sub(before),
        ),
        _ => None,
    };
    Ok((measured.0, measured.1, qlog_entries))
}

async fn setup(target: &Target, config: &Config) -> Result<(), String> {
    if config.profile == Profile::Sql {
        execute_sql(
            target,
            SqlCommand {
                request_id: "profile-bench-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    }
    for index in 0..KEYSPACE {
        write_one(
            target,
            config.profile,
            index,
            &format!("setup-{index:016x}"),
            config.value_bytes,
        )
        .await?;
    }
    Ok(())
}

async fn run_phase(
    target: Target,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    match target {
        Target::Handle(handle) => run_phase_handle(handle, config, operations, phase).await,
        Target::Runtime(runtime) => run_phase_runtime(runtime, config, operations, phase).await,
    }
}

async fn run_phase_handle(
    handle: RhizaHandle,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now() + Duration::from_millis(20);
    let mut workers = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let target = Target::Handle(handle.clone());
        let counter = counter.clone();
        let config = config.clone();
        workers.push(tokio::spawn(async move {
            tokio::time::sleep_until(start.into()).await;
            let mut samples = Samples::default();
            loop {
                let claim = if config.workload == Workload::Write {
                    config.batch_size as u64
                } else {
                    1
                };
                let sequence = counter.fetch_add(claim, Ordering::Relaxed);
                if sequence >= operations {
                    break;
                }
                let count = claim.min(operations - sequence) as usize;
                let began = Instant::now();
                let results = if config.workload == Workload::Write {
                    write_batch(&target, &config, sequence, count, phase).await
                } else {
                    let request_id = format!("{phase}-{sequence:016x}");
                    vec![operate(&target, &config, sequence, &request_id).await]
                };
                let elapsed = began.elapsed();
                for result in results {
                    samples.record(elapsed, result);
                }
            }
            samples
        }));
    }
    let mut combined = Samples::default();
    for worker in workers {
        match worker.await {
            Ok(samples) => combined.merge(samples),
            Err(error) => {
                combined.errors += 1;
                combined
                    .error_classes
                    .insert(format!("worker join: {error}"), 1);
            }
        }
    }
    (combined, start.elapsed())
}

async fn run_phase_runtime(
    runtime: Arc<NodeRuntime>,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now() + Duration::from_millis(20);
    let mut workers = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let runtime = Arc::clone(&runtime);
        let counter = Arc::clone(&counter);
        let config = config.clone();
        workers.push(tokio::task::spawn_blocking(move || {
            std::thread::sleep(start.saturating_duration_since(Instant::now()));
            let mut samples = Samples::default();
            loop {
                let claim = if config.workload == Workload::Write {
                    config.batch_size as u64
                } else {
                    1
                };
                let sequence = counter.fetch_add(claim, Ordering::Relaxed);
                if sequence >= operations {
                    break;
                }
                let count = claim.min(operations - sequence) as usize;
                let began = Instant::now();
                let results = if config.workload == Workload::Write {
                    write_batch_runtime(&runtime, &config, sequence, count, phase)
                } else {
                    let request_id = format!("{phase}-{sequence:016x}");
                    vec![operate_runtime(&runtime, &config, sequence, &request_id)]
                };
                let elapsed = began.elapsed();
                for result in results {
                    samples.record(elapsed, result);
                }
            }
            samples
        }));
    }
    let mut combined = Samples::default();
    for worker in workers {
        match worker.await {
            Ok(samples) => combined.merge(samples),
            Err(error) => {
                combined.errors += 1;
                combined
                    .error_classes
                    .insert(format!("runtime worker join: {error}"), 1);
            }
        }
    }
    (combined, start.elapsed())
}

fn operate_runtime(
    runtime: &NodeRuntime,
    config: &Config,
    sequence: u64,
    request_id: &str,
) -> Result<(), String> {
    let key_index = sequence % KEYSPACE;
    match config.workload {
        Workload::Write => write_one_runtime(
            runtime,
            config.profile,
            key_index,
            request_id,
            config.value_bytes,
        ),
        Workload::Get => get_one_runtime(runtime, config.profile, key_index),
        Workload::DocumentGet => get_graph_document_runtime(runtime, config.profile, key_index),
        Workload::NativeRead => native_read_runtime(runtime, config.profile),
    }
}

fn write_one_runtime(
    runtime: &NodeRuntime,
    profile: Profile,
    key_index: u64,
    request_id: &str,
    value_bytes: usize,
) -> Result<(), String> {
    let key = key(key_index);
    let value = value(key_index, request_id, value_bytes);
    match profile {
        Profile::Sql => runtime
            .execute_sql(SqlCommand {
                request_id: request_id.into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                          ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                        .into(),
                    parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
                }],
            })
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Profile::Graph => runtime
            .mutate_graph(
                GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?,
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Profile::Kv => runtime
            .mutate_kv(
                KvCommandV1::put(request_id, key.into_bytes(), value.into_bytes())
                    .map_err(|error| error.to_string())?,
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
    }
}

fn write_batch_runtime(
    runtime: &NodeRuntime,
    config: &Config,
    first_sequence: u64,
    count: usize,
    phase: &str,
) -> Vec<Result<(), String>> {
    match config.profile {
        Profile::Sql => logical_batch_results(
            count,
            runtime.execute_sql_batch(
                (0..count)
                    .map(|offset| {
                        sql_write_command(first_sequence + offset as u64, phase, config.value_bytes)
                    })
                    .collect(),
            ),
        ),
        Profile::Graph => match (0..count)
            .map(|offset| {
                graph_write_command(first_sequence + offset as u64, phase, config.value_bytes)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(commands) => logical_batch_results(count, runtime.mutate_graph_batch(commands)),
            Err(error) => repeated_batch_error(count, error),
        },
        Profile::Kv => match (0..count)
            .map(|offset| {
                kv_write_command(first_sequence + offset as u64, phase, config.value_bytes)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(commands) => logical_batch_results(count, runtime.mutate_kv_batch(commands)),
            Err(error) => repeated_batch_error(count, error),
        },
    }
}

fn get_one_runtime(runtime: &NodeRuntime, profile: Profile, key_index: u64) -> Result<(), String> {
    let key = key(key_index);
    let present = match profile {
        Profile::Sql => {
            runtime
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                        parameters: vec![SqlValue::Text(key)],
                    },
                    ReadConsistency::Local,
                    1,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len()
                == 1
        }
        Profile::Graph => {
            runtime
                .query_graph(
                    "MATCH (d:RhizaDocument) WHERE d.id = $id \
                 RETURN d.string_value AS value LIMIT 1",
                    &BTreeMap::from([("id".into(), GraphParameterValue::String(key))]),
                    ReadConsistency::Local,
                    1,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len()
                == 1
        }
        Profile::Kv => runtime
            .get_kv(key.as_bytes(), ReadConsistency::Local)
            .map_err(|error| error.to_string())?
            .value
            .is_some(),
    };
    if present {
        Ok(())
    } else {
        Err("runtime get returned no row".into())
    }
}

fn get_graph_document_runtime(
    runtime: &NodeRuntime,
    profile: Profile,
    key_index: u64,
) -> Result<(), String> {
    if profile != Profile::Graph {
        return Err("document get requires graph profile".into());
    }
    if runtime
        .get_graph_document(&key(key_index), ReadConsistency::Local)
        .map_err(|error| error.to_string())?
        .value
        .is_some()
    {
        Ok(())
    } else {
        Err("runtime document get returned no value".into())
    }
}

fn native_read_runtime(runtime: &NodeRuntime, profile: Profile) -> Result<(), String> {
    let rows = match profile {
        Profile::Sql => runtime
            .query_sql(
                &SqlStatement {
                    sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                16,
            )
            .map_err(|error| error.to_string())?
            .rows
            .len(),
        Profile::Graph => runtime
            .query_graph(
                "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16",
                &BTreeMap::new(),
                ReadConsistency::Local,
                16,
            )
            .map_err(|error| error.to_string())?
            .rows
            .len(),
        Profile::Kv => runtime
            .scan_kv_prefix(b"bench-key-", 16, None, ReadConsistency::Local)
            .map_err(|error| error.to_string())?
            .rows()
            .len(),
    };
    if rows == 0 {
        Err("runtime native read returned no rows".into())
    } else {
        Ok(())
    }
}

async fn operate(
    target: &Target,
    config: &Config,
    sequence: u64,
    request_id: &str,
) -> Result<(), String> {
    let key_index = sequence % KEYSPACE;
    match config.workload {
        Workload::Write => {
            write_one(
                target,
                config.profile,
                key_index,
                request_id,
                config.value_bytes,
            )
            .await
        }
        Workload::Get => get_one(target, config.profile, key_index).await,
        Workload::DocumentGet => get_graph_document(target, config.profile, key_index).await,
        Workload::NativeRead => native_read(target, config.profile).await,
    }
}

async fn write_one(
    target: &Target,
    profile: Profile,
    key_index: u64,
    request_id: &str,
    value_bytes: usize,
) -> Result<(), String> {
    let key = key(key_index);
    let value = value(key_index, request_id, value_bytes);
    match profile {
        Profile::Sql => {
            execute_sql(
                target,
                SqlCommand {
                    request_id: request_id.into(),
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                              ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                            .into(),
                        parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
                    }],
                },
            )
            .await?;
        }
        Profile::Graph => {
            let command =
                GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?;
            match target {
                Target::Handle(handle) => {
                    handle
                        .mutate_graph(command)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.mutate_graph(command).map(|_| ())
                    })
                    .await?;
                }
            }
        }
        Profile::Kv => match target {
            Target::Handle(handle) => {
                handle
                    .put_kv(request_id, key.into_bytes(), value.into_bytes())
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Target::Runtime(runtime) => {
                let command = KvCommandV1::put(request_id, key.into_bytes(), value.into_bytes())
                    .map_err(|error| error.to_string())?;
                runtime_call(runtime, move |runtime| {
                    runtime.mutate_kv(command).map(|_| ())
                })
                .await?;
            }
        },
    }
    Ok(())
}

async fn write_batch(
    target: &Target,
    config: &Config,
    first_sequence: u64,
    count: usize,
    phase: &'static str,
) -> Vec<Result<(), String>> {
    match (target, config.profile) {
        (Target::Handle(handle), Profile::Sql) => logical_batch_results(
            count,
            handle
                .execute_sql_batch(
                    (0..count)
                        .map(|offset| {
                            sql_write_command(
                                first_sequence + offset as u64,
                                phase,
                                config.value_bytes,
                            )
                        })
                        .collect(),
                )
                .await,
        ),
        (Target::Handle(handle), Profile::Graph) => {
            let commands = (0..count)
                .map(|offset| {
                    graph_write_command(first_sequence + offset as u64, phase, config.value_bytes)
                })
                .collect::<Result<Vec<_>, _>>();
            match commands {
                Ok(commands) => {
                    logical_batch_results(count, handle.mutate_graph_batch(commands).await)
                }
                Err(error) => repeated_batch_error(count, error),
            }
        }
        (Target::Handle(handle), Profile::Kv) => {
            let commands = (0..count)
                .map(|offset| {
                    kv_write_command(first_sequence + offset as u64, phase, config.value_bytes)
                })
                .collect::<Result<Vec<_>, _>>();
            match commands {
                Ok(commands) => {
                    logical_batch_results(count, handle.mutate_kv_batch(commands).await)
                }
                Err(error) => repeated_batch_error(count, error),
            }
        }
        (Target::Runtime(runtime), _) => {
            let runtime = Arc::clone(runtime);
            let config = config.clone();
            match tokio::task::spawn_blocking(move || {
                write_batch_runtime(&runtime, &config, first_sequence, count, phase)
            })
            .await
            {
                Ok(results) => results,
                Err(error) => repeated_batch_error(count, format!("runtime worker join: {error}")),
            }
        }
    }
}

fn sql_write_command(sequence: u64, phase: &str, value_bytes: usize) -> SqlCommand {
    let request_id = format!("{phase}-{sequence:016x}");
    let key = key(sequence % KEYSPACE);
    let value = value(sequence % KEYSPACE, &request_id, value_bytes);
    SqlCommand {
        request_id,
        statements: vec![SqlStatement {
            sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                  ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                .into(),
            parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
        }],
    }
}

fn graph_write_command(
    sequence: u64,
    phase: &str,
    value_bytes: usize,
) -> Result<GraphCommandV1, String> {
    let request_id = format!("{phase}-{sequence:016x}");
    let key_index = sequence % KEYSPACE;
    GraphCommandV1::put_document(
        request_id.clone(),
        key(key_index),
        GraphValueV1::String(value(key_index, &request_id, value_bytes)),
    )
    .map_err(|error| error.to_string())
}

fn kv_write_command(sequence: u64, phase: &str, value_bytes: usize) -> Result<KvCommandV1, String> {
    let request_id = format!("{phase}-{sequence:016x}");
    let key_index = sequence % KEYSPACE;
    KvCommandV1::put(
        request_id.clone(),
        key(key_index).into_bytes(),
        value(key_index, &request_id, value_bytes).into_bytes(),
    )
    .map_err(|error| error.to_string())
}

fn logical_batch_results<T, ItemError, BatchError>(
    count: usize,
    result: Result<Vec<Result<T, ItemError>>, BatchError>,
) -> Vec<Result<(), String>>
where
    ItemError: ToString,
    BatchError: ToString,
{
    match result {
        Ok(results) if results.len() == count => results
            .into_iter()
            .map(|result| result.map(|_| ()).map_err(|error| error.to_string()))
            .collect(),
        Ok(results) => repeated_batch_error(
            count,
            format!(
                "batch returned {} results for {count} operations",
                results.len()
            ),
        ),
        Err(error) => repeated_batch_error(count, error.to_string()),
    }
}

fn repeated_batch_error(count: usize, error: impl Into<String>) -> Vec<Result<(), String>> {
    let error = error.into();
    (0..count).map(|_| Err(error.clone())).collect()
}

async fn execute_sql(target: &Target, command: SqlCommand) -> Result<(), String> {
    match target {
        Target::Handle(handle) => handle
            .execute_sql(command)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Target::Runtime(runtime) => {
            runtime_call(runtime, move |runtime| {
                runtime.execute_sql(command).map(|_| ())
            })
            .await
        }
    }
}

async fn runtime_call<T, E, F>(runtime: &Arc<NodeRuntime>, operation: F) -> Result<T, String>
where
    T: Send + 'static,
    E: ToString + Send + 'static,
    F: FnOnce(Arc<NodeRuntime>) -> Result<T, E> + Send + 'static,
{
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || operation(runtime))
        .await
        .map_err(|error| format!("runtime worker join: {error}"))?
        .map_err(|error| error.to_string())
}

async fn get_one(target: &Target, profile: Profile, key_index: u64) -> Result<(), String> {
    let key = key(key_index);
    match profile {
        Profile::Sql => {
            let statement = SqlStatement {
                sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                parameters: vec![SqlValue::Text(key)],
            };
            let rows = match target {
                Target::Handle(handle) => handle
                    .query(statement, ReadConsistency::Local, 1)
                    .await
                    .map_err(|error| error.to_string())?
                    .rows
                    .len(),
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime
                            .query_sql(&statement, ReadConsistency::Local, 1)
                            .map(|result| result.rows.len())
                    })
                    .await?
                }
            };
            if rows != 1 {
                return Err("sql get returned no row".into());
            }
        }
        Profile::Graph => {
            let statement = "MATCH (d:RhizaDocument) WHERE d.id = $id \
                             RETURN d.string_value AS value LIMIT 1";
            let parameters = BTreeMap::from([("id".into(), GraphParameterValue::String(key))]);
            let rows = match target {
                Target::Handle(handle) => handle
                    .query_graph(statement, parameters, ReadConsistency::Local, 1)
                    .await
                    .map_err(|error| error.to_string())?
                    .rows
                    .len(),
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime
                            .query_graph(statement, &parameters, ReadConsistency::Local, 1)
                            .map(|result| result.rows.len())
                    })
                    .await?
                }
            };
            if rows != 1 {
                return Err("graph get returned no row".into());
            }
        }
        Profile::Kv => {
            let present = match target {
                Target::Handle(handle) => handle
                    .get_kv(key.as_bytes(), ReadConsistency::Local)
                    .await
                    .map_err(|error| error.to_string())?
                    .value
                    .is_some(),
                Target::Runtime(runtime) => {
                    let key = key.into_bytes();
                    runtime_call(runtime, move |runtime| {
                        runtime
                            .get_kv(&key, ReadConsistency::Local)
                            .map(|result| result.value.is_some())
                    })
                    .await?
                }
            };
            if !present {
                return Err("kv get returned no value".into());
            }
        }
    }
    Ok(())
}

async fn get_graph_document(
    target: &Target,
    profile: Profile,
    key_index: u64,
) -> Result<(), String> {
    if profile != Profile::Graph {
        return Err("document get requires graph profile".into());
    }
    let id = key(key_index);
    let present = match target {
        Target::Handle(handle) => handle
            .get_graph_document(id, ReadConsistency::Local)
            .await
            .map_err(|error| error.to_string())?
            .value
            .is_some(),
        Target::Runtime(runtime) => {
            runtime_call(runtime, move |runtime| {
                runtime
                    .get_graph_document(&id, ReadConsistency::Local)
                    .map(|result| result.value.is_some())
            })
            .await?
        }
    };
    if present {
        Ok(())
    } else {
        Err("document get returned no value".into())
    }
}

async fn native_read(target: &Target, profile: Profile) -> Result<(), String> {
    let rows = match profile {
        Profile::Sql => {
            let statement = SqlStatement {
                sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                parameters: vec![],
            };
            match target {
                Target::Handle(handle) => handle
                    .query(statement, ReadConsistency::Local, 16)
                    .await
                    .map_err(|error| error.to_string())?
                    .rows
                    .len(),
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime
                            .query_sql(&statement, ReadConsistency::Local, 16)
                            .map(|result| result.rows.len())
                    })
                    .await?
                }
            }
        }
        Profile::Graph => {
            let statement = "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16";
            let parameters = BTreeMap::new();
            match target {
                Target::Handle(handle) => handle
                    .query_graph(statement, parameters, ReadConsistency::Local, 16)
                    .await
                    .map_err(|error| error.to_string())?
                    .rows
                    .len(),
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime
                            .query_graph(statement, &parameters, ReadConsistency::Local, 16)
                            .map(|result| result.rows.len())
                    })
                    .await?
                }
            }
        }
        Profile::Kv => match target {
            Target::Handle(handle) => handle
                .scan_kv_prefix(b"bench-key-", 16, None, ReadConsistency::Local)
                .await
                .map_err(|error| error.to_string())?
                .rows()
                .len(),
            Target::Runtime(runtime) => {
                runtime_call(runtime, move |runtime| {
                    runtime
                        .scan_kv_prefix(b"bench-key-", 16, None, ReadConsistency::Local)
                        .map(|result| result.rows().len())
                })
                .await?
            }
        },
    };
    if rows == 0 {
        Err("native read returned no rows".into())
    } else {
        Ok(())
    }
}

enum RawState {
    Sql(SqliteStateMachine),
    Graph(LadybugStateMachine),
    Kv(RedbStateMachine),
}

struct RawTarget {
    cluster_id: String,
    next_index: u64,
    previous_hash: LogHash,
    state: RawState,
}

impl RawTarget {
    fn open(root: &Path, profile: Profile) -> Result<Self, String> {
        let cluster_id = effective_cluster_id(profile.execution_profile(), "profile-bench")
            .map_err(|error| error.to_string())?;
        let state = match profile {
            Profile::Sql => {
                SqliteStateMachine::open(root.join("raw/sql.db"), &cluster_id, "node-1", 1, 1)
                    .map(RawState::Sql)
                    .map_err(|error| error.to_string())?
            }
            Profile::Graph => {
                LadybugStateMachine::open(root.join("raw/graph.lbug"), &cluster_id, "node-1", 1, 1)
                    .map(RawState::Graph)
                    .map_err(|error| error.to_string())?
            }
            Profile::Kv => {
                RedbStateMachine::open(root.join("raw/kv.redb"), &cluster_id, "node-1", 1, 1)
                    .map(RawState::Kv)
                    .map_err(|error| error.to_string())?
            }
        };
        Ok(Self {
            cluster_id,
            next_index: 1,
            previous_hash: LogHash::ZERO,
            state,
        })
    }

    fn apply(&mut self, payload: Vec<u8>) -> Result<(), String> {
        let hash = LogEntry::calculate_hash(
            &self.cluster_id,
            self.next_index,
            1,
            1,
            EntryType::Command,
            self.previous_hash,
            &payload,
        );
        let entry = LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: 1,
            config_id: 1,
            index: self.next_index,
            entry_type: EntryType::Command,
            payload,
            prev_hash: self.previous_hash,
            hash,
        };
        match &self.state {
            RawState::Sql(state) => state
                .apply_entry(&entry)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            RawState::Graph(state) => state
                .apply_entry(&entry)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            RawState::Kv(state) => state
                .apply_entry(&entry)
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }?;
        self.next_index += 1;
        self.previous_hash = hash;
        Ok(())
    }

    fn write_one(
        &mut self,
        profile: Profile,
        key_index: u64,
        request_id: &str,
        value_bytes: usize,
    ) -> Result<(), String> {
        let key = key(key_index);
        let value = value(key_index, request_id, value_bytes);
        let payload = match profile {
            Profile::Sql => encode_sql_command(&SqlCommand {
                request_id: request_id.into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                          ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                        .into(),
                    parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
                }],
            })
            .map_err(|error| error.to_string())?,
            Profile::Graph => encode_replicated_graph_command(
                &GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?,
            Profile::Kv => encode_replicated_kv_command(
                &KvCommandV1::put(request_id, key.into_bytes(), value.into_bytes())
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?,
        };
        self.apply(payload)
    }

    fn operate(&mut self, config: &Config, sequence: u64, request_id: &str) -> Result<(), String> {
        let key_index = sequence % KEYSPACE;
        match config.workload {
            Workload::Write => {
                self.write_one(config.profile, key_index, request_id, config.value_bytes)
            }
            Workload::Get => self.get_one(config.profile, key_index),
            Workload::DocumentGet => self.get_graph_document(config.profile, key_index),
            Workload::NativeRead => self.native_read(config.profile),
        }
    }

    fn get_one(&self, profile: Profile, key_index: u64) -> Result<(), String> {
        let key = key(key_index);
        let rows = match (&self.state, profile) {
            (RawState::Sql(state), Profile::Sql) => state
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                        parameters: vec![SqlValue::Text(key)],
                    },
                    1,
                    RAW_RESULT_BYTES,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Graph(state), Profile::Graph) => state
                .query_read_only(
                    "MATCH (d:RhizaDocument) WHERE d.id = $id \
                     RETURN d.string_value AS value LIMIT 1",
                    &BTreeMap::from([("id".into(), GraphParameterValue::String(key))]),
                    1,
                    RAW_RESULT_BYTES,
                    RAW_GRAPH_TIMEOUT_MS,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Kv(state), Profile::Kv) => usize::from(
                state
                    .get_with_tip(key.as_bytes())
                    .map_err(|error| error.to_string())?
                    .value()
                    .is_some(),
            ),
            _ => return Err("raw benchmark profile/state mismatch".into()),
        };
        if rows == 1 {
            Ok(())
        } else {
            Err("raw get returned no row".into())
        }
    }

    fn get_graph_document(&self, profile: Profile, key_index: u64) -> Result<(), String> {
        if profile != Profile::Graph {
            return Err("document get requires graph profile".into());
        }
        let RawState::Graph(state) = &self.state else {
            return Err("raw benchmark profile/state mismatch".into());
        };
        if state
            .get_document_with_tip(&key(key_index))
            .map_err(|error| error.to_string())?
            .0
            .is_some()
        {
            Ok(())
        } else {
            Err("raw document get returned no value".into())
        }
    }

    fn native_read(&self, profile: Profile) -> Result<(), String> {
        let rows = match (&self.state, profile) {
            (RawState::Sql(state), Profile::Sql) => state
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                        parameters: vec![],
                    },
                    16,
                    RAW_RESULT_BYTES,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Graph(state), Profile::Graph) => state
                .query_read_only(
                    "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16",
                    &BTreeMap::new(),
                    16,
                    RAW_RESULT_BYTES,
                    RAW_GRAPH_TIMEOUT_MS,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Kv(state), Profile::Kv) => state
                .scan_prefix(b"bench-key-", 16, None)
                .map_err(|error| error.to_string())?
                .rows()
                .len(),
            _ => return Err("raw benchmark profile/state mismatch".into()),
        };
        if rows == 0 {
            Err("raw native read returned no rows".into())
        } else {
            Ok(())
        }
    }
}

fn measure_raw(mut target: RawTarget, config: &Config) -> Result<(Samples, Duration), String> {
    if config.profile == Profile::Sql {
        let payload = encode_sql_command(&SqlCommand {
            request_id: "profile-bench-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })
        .map_err(|error| error.to_string())?;
        target.apply(payload)?;
    }
    for index in 0..KEYSPACE {
        target.write_one(
            config.profile,
            index,
            &format!("setup-{index:016x}"),
            config.value_bytes,
        )?;
    }
    if config.warmup > 0 {
        let (warmup, _) = run_phase_raw(&mut target, config, config.warmup, "warmup");
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
    }
    Ok(run_phase_raw(
        &mut target,
        config,
        config.operations,
        "measure",
    ))
}

fn run_phase_raw(
    target: &mut RawTarget,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let start = Instant::now();
    let mut samples = Samples::default();
    for sequence in 0..operations {
        let request_id = format!("{phase}-{sequence:016x}");
        let began = Instant::now();
        let result = target.operate(config, sequence, &request_id);
        samples.record(began.elapsed(), result);
    }
    (samples, start.elapsed())
}

struct ConsensusTarget {
    consensus: ThreeNodeConsensus,
    next_index: u64,
    previous_hash: LogHash,
}

impl ConsensusTarget {
    fn open(root: &Path) -> Result<Self, String> {
        let cluster_id = "profile-bench-consensus";
        let membership =
            Membership::new(["node-1", "node-2", "node-3"]).map_err(|error| error.to_string())?;
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            cluster_id,
            "node-1",
            1,
            1,
            recorders(root, &membership, cluster_id)?,
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            consensus,
            next_index: 1,
            previous_hash: LogHash::ZERO,
        })
    }

    fn write(&mut self, sequence: u64, request_id: &str, bytes: usize) -> Result<(), String> {
        let payload = value(sequence, request_id, bytes).into_bytes();
        let entry = self
            .consensus
            .propose_at(
                self.next_index,
                self.previous_hash,
                ConsensusCommand::new(CommandKind::Deterministic, payload),
            )
            .map_err(|error| error.to_string())?;
        self.next_index = entry
            .index
            .checked_add(1)
            .ok_or_else(|| "consensus benchmark index exhausted".to_string())?;
        self.previous_hash = entry.hash;
        Ok(())
    }
}

fn measure_consensus(
    mut target: ConsensusTarget,
    config: &Config,
) -> Result<(Samples, Duration), String> {
    if config.warmup > 0 {
        let (warmup, _) = run_phase_consensus(&mut target, config, config.warmup, "warmup");
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
    }
    Ok(run_phase_consensus(
        &mut target,
        config,
        config.operations,
        "measure",
    ))
}

fn run_phase_consensus(
    target: &mut ConsensusTarget,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let start = Instant::now();
    let mut samples = Samples::default();
    for sequence in 0..operations {
        let request_id = format!("{phase}-{sequence:016x}");
        let began = Instant::now();
        let result = target.write(sequence, &request_id, config.value_bytes);
        samples.record(began.elapsed(), result);
    }
    (samples, start.elapsed())
}

fn key(index: u64) -> String {
    format!("bench-key-{index:08}")
}

fn value(index: u64, request_id: &str, bytes: usize) -> String {
    // Stable FNV-1a keeps the changing portion at the front even for the
    // minimum payload size. The same request/key pair therefore produces the
    // exact same bytes for SQL, graph, and KV without adding a hash dependency.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in index.to_be_bytes().iter().chain(request_id.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let pattern = format!("{hash:016x}");
    pattern.chars().cycle().take(bytes).collect()
}

fn provenance() -> Provenance {
    Provenance {
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: !command_output("git", &["status", "--porcelain"]).is_empty(),
        rustc: command_output("rustc", &["--version"]),
        source: "bench/src/bin/rhiza-profile.rs",
    }
}

fn system() -> System {
    let cpu_model = if cfg!(target_os = "macos") {
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        command_output(
            "sh",
            &[
                "-c",
                "sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1",
            ],
        )
    };
    System {
        os: env::consts::OS,
        architecture: env::consts::ARCH,
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        kernel: command_output("uname", &["-srm"]),
        cpu_model,
    }
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_parses_required_and_optional_values() {
        let config = Config::parse(
            [
                "--profile",
                "graph",
                "--workload",
                "native-read",
                "--operations",
                "20",
                "--warmup",
                "0",
                "--concurrency",
                "4",
                "--value-bytes",
                "64",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(
            config,
            Config {
                layer: Layer::Handle,
                profile: Profile::Graph,
                workload: Workload::NativeRead,
                batch_size: 1,
                operations: 20,
                warmup: 0,
                concurrency: 4,
                value_bytes: 64,
            }
        );
    }

    #[test]
    fn config_selects_runtime_and_rejects_concurrent_raw_measurement() {
        let runtime = Config::parse(
            ["--profile", "kv", "--workload", "get", "--layer", "runtime"].map(str::to_owned),
        )
        .unwrap();
        assert_eq!(runtime.layer, Layer::Runtime);

        assert!(Config::parse(
            [
                "--profile",
                "kv",
                "--workload",
                "get",
                "--layer",
                "raw",
                "--concurrency",
                "2",
            ]
            .map(str::to_owned),
        )
        .is_err());
    }

    #[test]
    fn consensus_layer_accepts_only_single_worker_writes() {
        let consensus = Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "consensus",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(consensus.layer, Layer::Consensus);

        for invalid in [
            vec![
                "--profile",
                "sql",
                "--workload",
                "get",
                "--layer",
                "consensus",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "consensus",
                "--concurrency",
                "2",
            ],
        ] {
            assert!(Config::parse(invalid.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn document_get_accepts_only_graph_profile() {
        for layer in ["handle", "runtime", "raw"] {
            let config = Config::parse(
                [
                    "--profile",
                    "graph",
                    "--workload",
                    "document-get",
                    "--layer",
                    layer,
                ]
                .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(config.workload, Workload::DocumentGet);
        }

        for profile in ["sql", "kv"] {
            assert!(Config::parse(
                ["--profile", profile, "--workload", "document-get"].map(str::to_owned)
            )
            .is_err());
        }
    }

    #[test]
    fn raw_document_get_reads_seeded_graph_document() {
        let root = tempfile::tempdir().unwrap();
        let mut target = RawTarget::open(root.path(), Profile::Graph).unwrap();
        target
            .write_one(Profile::Graph, 7, "setup-graph-document", 64)
            .unwrap();

        target.get_graph_document(Profile::Graph, 7).unwrap();
    }

    #[test]
    fn config_rejects_zero_operations_and_unbounded_values() {
        assert!(Config::parse(
            ["--profile", "sql", "--workload", "get", "--operations", "0"].map(str::to_owned)
        )
        .is_err());
        assert!(Config::parse(
            [
                "--profile",
                "kv",
                "--workload",
                "write",
                "--value-bytes",
                "4097",
            ]
            .map(str::to_owned)
        )
        .is_err());
    }

    #[test]
    fn write_batch_size_accepts_powers_of_two_and_rejects_other_workloads() {
        for batch_size in [1, 2, 4, 8, 16, 32, 64] {
            let config = Config::parse(
                [
                    "--profile",
                    "kv",
                    "--workload",
                    "write",
                    "--batch-size",
                    &batch_size.to_string(),
                ]
                .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(config.batch_size, batch_size);
        }
        for args in [
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--batch-size",
                "3",
            ],
            vec!["--profile", "sql", "--workload", "get", "--batch-size", "2"],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "raw",
                "--batch-size",
                "2",
            ],
        ] {
            assert!(Config::parse(args.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn percentiles_use_nearest_rank_and_merge_worker_histograms() {
        let mut left = Samples::default();
        left.record(Duration::from_micros(10), Ok(()));
        left.record(Duration::from_micros(20), Ok(()));
        let mut right = Samples::default();
        right.record(Duration::from_micros(30), Ok(()));
        right.record(Duration::from_micros(40), Ok(()));
        right.record(Duration::from_micros(50), Err("failed".into()));
        left.merge(right);

        assert_eq!(left.successes, 4);
        assert_eq!(left.errors, 1);
        assert_eq!(left.percentile(500), Some(20));
        assert_eq!(left.percentile(950), Some(40));
        assert_eq!(
            left.metrics(Duration::from_secs(2), None)
                .operations_per_second,
            2.0
        );
    }

    #[test]
    fn report_latency_keys_are_stable() {
        let json = serde_json::to_value(Latencies {
            p50: Some(1),
            p95: Some(2),
            p99: Some(3),
            p99_9: Some(4),
            max: Some(5),
        })
        .unwrap();
        assert_eq!(json["p99.9"], 4);
        assert_eq!(json["max"], 5);
    }

    #[test]
    fn measured_write_values_change_without_changing_payload_size() {
        let first = value(7, "measure-0000000000000001", 128);
        let second = value(7, "measure-0000000000000002", 128);

        assert_eq!(first.len(), 128);
        assert_eq!(second.len(), 128);
        assert_ne!(first, second);
        assert_eq!(first, value(7, "measure-0000000000000001", 128));
    }
}
