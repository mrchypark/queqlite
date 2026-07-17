use std::{
    collections::BTreeMap,
    env,
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rhiza::{
    effective_cluster_id, EmbeddedConfig, EmbeddedIdentity, ExecutionProfile, GraphCommandV1,
    GraphParameterValue, GraphValueV1, ReadConsistency, RecorderRpc, Rhiza, RhizaHandle,
    SqlCommand, SqlStatement, SqlValue,
};
use rhiza_quepaxa::{Membership, RecorderFileStore};
use serde::Serialize;

const KEYSPACE: u64 = 256;

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
    NativeRead,
}

impl Workload {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "write" => Ok(Self::Write),
            "get" => Ok(Self::Get),
            "native-read" => Ok(Self::NativeRead),
            _ => Err("--workload must be write, get, or native-read".into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    profile: Profile,
    workload: Workload,
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
                "--profile" => profile = Some(Profile::parse(next()?)?),
                "--workload" => workload = Some(Workload::parse(next()?)?),
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
        Ok(Self {
            profile: profile.ok_or_else(|| "--profile is required".to_string())?,
            workload: workload.ok_or_else(|| "--workload is required".to_string())?,
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
    "usage: rhiza-profile --profile sql|graph|kv --workload write|get|native-read \
     [--operations N] [--warmup N] [--concurrency N] [--value-bytes N]"
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

    fn metrics(&self, elapsed: Duration) -> Metrics {
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
    profile: Profile,
    workload: Workload,
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
    let rhiza = Rhiza::open(embedded_config(root.path(), config.profile)?)
        .await
        .map_err(|error| error.to_string())?;
    let handle = rhiza.handle();
    let measured = async {
        setup(&handle, &config).await?;
        if config.warmup > 0 {
            let (warmup, _) = run_phase(handle.clone(), &config, config.warmup, "warmup").await;
            if warmup.errors > 0 {
                return Err(format!("warmup failed with {} errors", warmup.errors));
            }
        }
        let (samples, elapsed) =
            run_phase(handle.clone(), &config, config.operations, "measure").await;
        Ok((samples, elapsed))
    }
    .await;
    let shutdown = rhiza.shutdown().await.map_err(|error| error.to_string());
    let (samples, elapsed) = measured?;
    shutdown?;

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
            profile: config.profile,
            workload: config.workload,
            operations: config.operations,
            warmup_operations: config.warmup,
            concurrency: config.concurrency,
            keyspace: KEYSPACE,
            value_bytes: config.value_bytes,
            read_consistency: "local",
            consensus: "in-process QuePaxa with three file-backed RecorderRpc voters",
            durability: "RecorderFileStore local fsync plus local qlog/materializer",
        },
        measurement: samples.metrics(elapsed),
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
    let recorders = membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                &cluster_id,
                1,
                1,
                membership.clone(),
            )
            .map(|recorder| (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>))
            .map_err(|error| error.to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
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

async fn setup(handle: &RhizaHandle, config: &Config) -> Result<(), String> {
    if config.profile == Profile::Sql {
        handle
            .execute_sql(SqlCommand {
                request_id: "profile-bench-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .await
            .map_err(|error| error.to_string())?;
    }
    for index in 0..KEYSPACE {
        write_one(
            handle,
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
    handle: RhizaHandle,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now() + Duration::from_millis(20);
    let mut workers = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let handle = handle.clone();
        let counter = counter.clone();
        let config = config.clone();
        workers.push(tokio::spawn(async move {
            tokio::time::sleep_until(start.into()).await;
            let mut samples = Samples::default();
            loop {
                let sequence = counter.fetch_add(1, Ordering::Relaxed);
                if sequence >= operations {
                    break;
                }
                let request_id = format!("{phase}-{sequence:016x}");
                let began = Instant::now();
                let result = operate(&handle, &config, sequence, &request_id).await;
                samples.record(began.elapsed(), result);
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

async fn operate(
    handle: &RhizaHandle,
    config: &Config,
    sequence: u64,
    request_id: &str,
) -> Result<(), String> {
    let key_index = sequence % KEYSPACE;
    match config.workload {
        Workload::Write => {
            write_one(
                handle,
                config.profile,
                key_index,
                request_id,
                config.value_bytes,
            )
            .await
        }
        Workload::Get => get_one(handle, config.profile, key_index).await,
        Workload::NativeRead => native_read(handle, config.profile).await,
    }
}

async fn write_one(
    handle: &RhizaHandle,
    profile: Profile,
    key_index: u64,
    request_id: &str,
    value_bytes: usize,
) -> Result<(), String> {
    let key = key(key_index);
    let value = value(key_index, request_id, value_bytes);
    match profile {
        Profile::Sql => {
            handle
                .execute_sql(SqlCommand {
                    request_id: request_id.into(),
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                              ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                            .into(),
                        parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
                    }],
                })
                .await
                .map_err(|error| error.to_string())?;
        }
        Profile::Graph => {
            let command =
                GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?;
            handle
                .mutate_graph(command)
                .await
                .map_err(|error| error.to_string())?;
        }
        Profile::Kv => {
            handle
                .put_kv(request_id, key.into_bytes(), value.into_bytes())
                .await
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

async fn get_one(handle: &RhizaHandle, profile: Profile, key_index: u64) -> Result<(), String> {
    let key = key(key_index);
    match profile {
        Profile::Sql => {
            let result = handle
                .query(
                    SqlStatement {
                        sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                        parameters: vec![SqlValue::Text(key)],
                    },
                    ReadConsistency::Local,
                    1,
                )
                .await
                .map_err(|error| error.to_string())?;
            if result.rows.len() != 1 {
                return Err("sql get returned no row".into());
            }
        }
        Profile::Graph => {
            let result = handle
                .query_graph(
                    "MATCH (d:RhizaDocument) WHERE d.id = $id \
                     RETURN d.string_value AS value LIMIT 1",
                    BTreeMap::from([("id".into(), GraphParameterValue::String(key))]),
                    ReadConsistency::Local,
                    1,
                )
                .await
                .map_err(|error| error.to_string())?;
            if result.rows.len() != 1 {
                return Err("graph get returned no row".into());
            }
        }
        Profile::Kv => {
            let result = handle
                .get_kv(key.as_bytes(), ReadConsistency::Local)
                .await
                .map_err(|error| error.to_string())?;
            if result.value.is_none() {
                return Err("kv get returned no value".into());
            }
        }
    }
    Ok(())
}

async fn native_read(handle: &RhizaHandle, profile: Profile) -> Result<(), String> {
    let rows = match profile {
        Profile::Sql => handle
            .query(
                SqlStatement {
                    sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                    parameters: vec![],
                },
                ReadConsistency::Local,
                16,
            )
            .await
            .map_err(|error| error.to_string())?
            .rows
            .len(),
        Profile::Graph => handle
            .query_graph(
                "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16",
                BTreeMap::new(),
                ReadConsistency::Local,
                16,
            )
            .await
            .map_err(|error| error.to_string())?
            .rows
            .len(),
        Profile::Kv => handle
            .scan_kv_prefix(b"bench-key-", 16, None, ReadConsistency::Local)
            .await
            .map_err(|error| error.to_string())?
            .rows()
            .len(),
    };
    if rows == 0 {
        Err("native read returned no rows".into())
    } else {
        Ok(())
    }
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
                profile: Profile::Graph,
                workload: Workload::NativeRead,
                operations: 20,
                warmup: 0,
                concurrency: 4,
                value_bytes: 64,
            }
        );
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
            left.metrics(Duration::from_secs(2)).operations_per_second,
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
