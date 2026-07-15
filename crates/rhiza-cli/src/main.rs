use std::{
    env, fmt, fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process,
    sync::Arc,
    time::Duration,
};

use reqwest::{header, Method, RequestBuilder, Response};
use rhiza_archive::{
    CheckpointIdentity, CheckpointPublisherOptions, CheckpointTip, GcPlan, GcPolicy,
    ObjectArchiveStore,
};
use rhiza_core::{
    ConfigChange, ConfigurationState, ExecutionProfile, LogAnchor, LogEntry, StoredCommand,
};
use rhiza_log::LogStore;
use rhiza_node::{
    effective_cluster_id, install_successor_recorder, node_router,
    node_router_with_admin_and_tasks, node_router_with_checkpoint,
    node_router_with_checkpoint_and_admin_tasks, recorder_router_for_generation,
    recover_successor_recorder_after_checkpoint, rehydrate_recorder_after_checkpoint,
    restore_checkpoint_to_fresh_data_dir_for_node, restore_successor_checkpoint_to_fresh_data_dir,
    run_e2e, serve_recorder_tcp, validate_recorder_tcp_endpoint, AdminActivateRequest,
    AdminActivateResponse, AdminCompactRequest, AdminCompactResponse, AdminConfig,
    AdminErrorResponse, AdminInstallSuccessorRequest, AdminInstallSuccessorResponse,
    AdminStatusResponse, AdminStopRequest, AdminStopResponse, AdminSuccessorBundle,
    AdminTaskTracker, CheckpointCoordinator, DurabilityMode, E2eConfig, HttpLogPeer,
    HttpRecorderClient, LogPeer, NodeConfig, NodeError, NodeRuntime, PeerConfig, ReadConsistency,
    ReadRequest, ReadResponse, SqlExecuteRequest, SqlExecuteResponse, SqlQueryRequest,
    SqlQueryResponse, StopInformation, TcpPostcardRecorderClient, WriteRequest, WriteResponse,
    ADMIN_ACTIVATE_PATH, ADMIN_COMPACT_PATH, ADMIN_INSTALL_SUCCESSOR_PATH, ADMIN_STATUS_PATH,
    ADMIN_STOP_PATH, LIVEZ_PATH, PROTOCOL_VERSION, READYZ_PATH, READ_PATH, SQL_EXECUTE_PATH,
    SQL_QUERY_PATH, VERSION_HEADER, WRITE_PATH,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    DecisionProof, Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus,
};
use rhiza_sql::{SqlStatement, SqlValue};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[tokio::main]
async fn main() {
    process::exit(run(env::args().skip(1)).await);
}

async fn run(args: impl IntoIterator<Item = String>) -> i32 {
    let command = match parse_command(args) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            usage();
            return 2;
        }
    };

    match command {
        Command::Status(args) => match request_health(&args).await {
            Ok(()) => {
                println!("ok");
                0
            }
            Err(error) => fail("status", error),
        },
        Command::E2e(config) => match run_e2e(config).await {
            Ok(report) => {
                println!(
                    "rhiza e2e ok: applied_index={} restored_value={} objects={}",
                    report.applied_index,
                    report.restored_value,
                    report.object_keys.len()
                );
                0
            }
            Err(error) => fail("e2e", error),
        },
        Command::Serve(config) => match serve(*config).await {
            Ok(()) => 0,
            Err(error) => fail("serve", error),
        },
        Command::InitCheckpoint(config) => match run_init_checkpoint(config).await {
            Ok(tip) => {
                println!("checkpoint initialized: durable_tip={}", tip.index());
                0
            }
            Err(error) => fail("init-checkpoint", error),
        },
        Command::RollCheckpoint(config) => match run_roll_checkpoint(config).await {
            Ok((old, new)) => {
                println!(
                    "checkpoint rolled: old_durable_tip={} new_durable_tip={}",
                    old.index(),
                    new.index()
                );
                0
            }
            Err(error) => fail("roll-checkpoint", error),
        },
        Command::CheckpointInspect(config) => match run_checkpoint_inspect(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("checkpoint inspect", error),
        },
        Command::CheckpointForkSuccessor(config) => {
            match run_checkpoint_fork_successor(config).await {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("checkpoint fork-successor", error),
            }
        }
        Command::CheckpointCompact(config) => match run_checkpoint_compact(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("checkpoint compact", error),
        },
        Command::ValidateConfigBundle(config_id) => match config_id
            .map(Ok)
            .unwrap_or_else(validate_config_bundle_stdin)
        {
            Ok(config_id) => {
                println!("{{\"config_id\":{config_id}}}");
                0
            }
            Err(error) => fail("validate-config-bundle", error),
        },
        Command::GcPlan(config) => match run_gc_plan(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("gc plan", error),
        },
        Command::GcInspect(config) => match run_gc_inspect(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("gc inspect", error),
        },
        Command::GcApply(config) => match run_gc_apply(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("gc apply", error),
        },
        Command::MembershipStatus(config) => match run_membership_status(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("membership status", error),
        },
        Command::MembershipStop(config) => match run_membership_stop(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("membership stop", error),
        },
        Command::MembershipInstallSuccessor(config) => {
            match run_membership_install_successor(*config).await {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("membership install-successor", error),
            }
        }
        Command::MembershipActivate(config) => match run_membership_activate(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("membership activate", error),
        },
        Command::Write(args) => match request_write(&args).await {
            Ok(response) => {
                println!(
                    "applied_index={} hash={}",
                    response.applied_index,
                    response.hash.to_hex()
                );
                0
            }
            Err(error) => fail("write", error),
        },
        Command::Read(args) => match request_read(&args).await {
            Ok(response) => finish_read(&args, response),
            Err(error) => fail("read", error),
        },
        Command::SqlExecute(args) => match request_sql_execute(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("sql execute", error.to_string()),
            },
            Err(error) => fail("sql execute", error),
        },
        Command::SqlQuery(args) => match request_sql_query(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("sql query", error.to_string()),
            },
            Err(error) => fail("sql query", error),
        },
        Command::Health(args) => match request_health(&args).await {
            Ok(()) => {
                println!("ok");
                0
            }
            Err(error) => fail("health", error),
        },
    }
}

enum Command {
    Status(HealthArgs),
    E2e(E2eConfig),
    Serve(Box<ServeConfig>),
    InitCheckpoint(CheckpointCommandConfig),
    RollCheckpoint(RollCheckpointConfig),
    CheckpointInspect(CheckpointCommandConfig),
    CheckpointForkSuccessor(CheckpointForkSuccessorConfig),
    CheckpointCompact(Box<AdminCommandConfig>),
    ValidateConfigBundle(Option<u64>),
    GcPlan(GcPlanConfig),
    GcInspect(GcInspectConfig),
    GcApply(GcInspectConfig),
    MembershipStatus(Box<AdminCommandConfig>),
    MembershipStop(Box<AdminCommandConfig>),
    MembershipInstallSuccessor(Box<AdminCommandConfig>),
    MembershipActivate(Box<AdminCommandConfig>),
    Write(WriteArgs),
    Read(ReadArgs),
    SqlExecute(SqlExecuteArgs),
    SqlQuery(SqlQueryArgs),
    Health(HealthArgs),
}

struct WriteArgs {
    urls: Vec<String>,
    token: String,
    request_id: String,
    key: String,
    value: String,
}

struct ReadArgs {
    urls: Vec<String>,
    token: String,
    key: String,
    consistency: Option<ReadConsistency>,
    expect: Option<String>,
}

struct SqlExecuteArgs {
    urls: Vec<String>,
    token: String,
    request_id: String,
    statement: SqlStatement,
}

struct SqlQueryArgs {
    urls: Vec<String>,
    token: String,
    statement: SqlStatement,
    consistency: Option<ReadConsistency>,
    max_rows: Option<u32>,
}

struct HealthArgs {
    url: String,
    ready: bool,
}

#[derive(Clone)]
struct AdminClientConfig {
    url: String,
    token: String,
}

const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const HEALTH_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HEALTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const SERVE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const LOCAL_CHECKPOINT_IDENTITY_FILE: &str = ".rhiza-checkpoint-identity-v1.json";
const MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES: u64 = 4 * 1024;

impl fmt::Debug for AdminClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminClientConfig")
            .field("url", &self.url)
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug)]
enum AdminTarget {
    Live(AdminClientConfig),
    Offline,
}

#[derive(Clone, Debug)]
struct AdminCommandConfig {
    target: AdminTarget,
    serve: Option<Box<ServeConfig>>,
    operation_id: Option<String>,
    successor: Option<ConfigurationBundle>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigurationBundleDocument {
    version: u32,
    config_id: u64,
    members: Vec<MemberDocument>,
    #[serde(default)]
    predecessor: Option<PredecessorDocument>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberDocument {
    node_id: String,
    url: String,
    #[serde(default)]
    log_url: Option<String>,
    #[serde(default)]
    recorder_tcp_addr: Option<String>,
    token: String,
}

#[derive(Clone, Debug)]
struct RecorderTcpPeer {
    address: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PredecessorDocument {
    version: u16,
    members: Vec<String>,
    stop_entry: LogEntry,
    stop_proof: DecisionProof,
}

#[derive(Clone)]
struct ConfigurationBundle {
    config_id: u64,
    peers: Vec<PeerConfig>,
    recorder_tcp_peers: Vec<Option<RecorderTcpPeer>>,
    membership: Membership,
    configuration_state: ConfigurationState,
    predecessor: Option<PredecessorConfiguration>,
}

impl fmt::Debug for ConfigurationBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigurationBundle")
            .field("config_id", &self.config_id)
            .field("peers", &self.peers)
            .field("recorder_tcp_peers", &self.recorder_tcp_peers)
            .field("membership", &self.membership.members())
            .field("configuration_state", &self.configuration_state)
            .field("predecessor", &self.predecessor)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecorderTransport {
    Http,
    TcpPostcard,
}

#[derive(Clone, Debug)]
struct RecorderTcpConfig {
    listen: String,
}

#[derive(Clone, Debug)]
struct PredecessorConfiguration {
    membership: Membership,
    stop: StopInformation,
}

impl ConfigurationBundle {
    fn require_predecessor(&self) -> Result<&PredecessorConfiguration, String> {
        self.predecessor
            .as_ref()
            .ok_or_else(|| "configuration bundle is missing predecessor transition material".into())
    }
}

#[derive(Clone)]
struct ServeConfig {
    execution_profile: ExecutionProfile,
    logical_cluster_id: String,
    cluster_id: String,
    node_id: String,
    data_dir: PathBuf,
    epoch: u64,
    bundle: ConfigurationBundle,
    client_token: String,
    admin_token: Option<String>,
    client_listen: String,
    recorder_listen: String,
    recorder_transport: RecorderTransport,
    recorder_tcp: Option<RecorderTcpConfig>,
    recovery_generation: u64,
    remote: Option<RemoteCheckpointConfig>,
}

impl fmt::Debug for ServeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServeConfig")
            .field("execution_profile", &self.execution_profile)
            .field("logical_cluster_id", &self.logical_cluster_id)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("data_dir", &self.data_dir)
            .field("epoch", &self.epoch)
            .field("configuration", &self.bundle)
            .field("client_token", &"[redacted]")
            .field(
                "admin_token",
                &self.admin_token.as_ref().map(|_| "[redacted]"),
            )
            .field("client_listen", &self.client_listen)
            .field("recorder_listen", &self.recorder_listen)
            .field("recorder_transport", &self.recorder_transport)
            .field("recorder_tcp", &self.recorder_tcp)
            .field("recovery_generation", &self.recovery_generation)
            .field(
                "object_store",
                &self.remote.as_ref().map(RemoteCheckpointConfig::provider),
            )
            .finish()
    }
}

#[derive(Clone)]
struct RemoteCheckpointConfig {
    object_store: ObjStoreConfig,
    durability: DurabilityMode,
    lease_duration_ms: u64,
    startup: StartupMode,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LocalCheckpointIdentityMarker {
    format_version: u32,
    cluster_id: String,
    execution_profile: ExecutionProfile,
    epoch: u64,
    config_id: u64,
    recovery_generation: u64,
}

impl RemoteCheckpointConfig {
    fn provider(&self) -> &'static str {
        match self.object_store {
            ObjStoreConfig::Local { .. } => "local",
            ObjStoreConfig::S3 { .. } => "s3",
            ObjStoreConfig::Gcs { .. } => "gcs",
            ObjStoreConfig::AzureBlob { .. } => "azure",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupMode {
    Bootstrap,
    Rejoin,
    Disaster,
}

#[derive(Clone)]
struct CheckpointCommandConfig {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    recovery_generation: u64,
    object_store: ObjStoreConfig,
}

impl CheckpointCommandConfig {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let execution_profile = execution_profile(&mut lookup)?;
        let logical_cluster_id = required_env(&mut lookup, "RHIZA_CLUSTER_ID")?;
        let cluster_id = effective_cluster_id(execution_profile, &logical_cluster_id)
            .map_err(|error| error.to_string())?;
        let epoch = positive_env(&mut lookup, "RHIZA_EPOCH")?;
        let config_id = configuration_id(&mut lookup)?;
        let recovery_generation = positive_env(&mut lookup, "RHIZA_RECOVERY_GENERATION")?;
        let mode = required_env(&mut lookup, "RHIZA_OBJECT_STORE")?;
        let object_store = parse_object_store_with_lookup(&mode, false, &mut lookup)?;
        Ok(Self {
            cluster_id,
            epoch,
            config_id,
            recovery_generation,
            object_store,
        })
    }

    fn identity(&self) -> CheckpointIdentity {
        CheckpointIdentity::new(
            self.cluster_id.clone(),
            self.epoch,
            self.config_id,
            self.recovery_generation,
        )
    }

    fn archive(&self) -> Result<ObjectArchiveStore, String> {
        let store = open_object_store(&self.object_store)?;
        if !store.supports_strong_cross_process_cas() {
            return Err("serving checkpoints require strong cross-process compare-and-swap".into());
        }
        ObjectArchiveStore::new_checkpoint(store, self.identity())
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone)]
struct RollCheckpointConfig {
    base: CheckpointCommandConfig,
    from_generation: u64,
    to_generation: u64,
}

#[derive(Clone)]
struct CheckpointForkSuccessorConfig {
    target: CheckpointCommandConfig,
    source_config_id: u64,
    source_generation: u64,
    stop_entry: LogEntry,
}

#[derive(Clone)]
struct GcPlanConfig {
    base: CheckpointCommandConfig,
    operation_id: String,
    retain_generations: usize,
    grace_ms: u64,
    min_age_ms: u64,
}

#[derive(Clone)]
struct GcInspectConfig {
    base: CheckpointCommandConfig,
    plan_hash: String,
}

impl ServeConfig {
    fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let execution_profile = execution_profile(&mut lookup)?;
        let cluster_id_source = required_env(&mut lookup, "RHIZA_CLUSTER_ID")?;
        let cluster_id = effective_cluster_id(execution_profile, &cluster_id_source)
            .map_err(|error| error.to_string())?;
        let profile_prefix = format!("rhiza:{}:", execution_profile.as_str());
        let logical_cluster_id = cluster_id
            .strip_prefix(&profile_prefix)
            .expect("effective cluster id has the requested profile")
            .to_owned();
        let node_id = required_env(&mut lookup, "RHIZA_NODE_ID")?;
        let data_dir = PathBuf::from(required_env(&mut lookup, "RHIZA_DATA_DIR")?);
        let epoch = positive_env(&mut lookup, "RHIZA_EPOCH")?;
        let client_token = required_env(&mut lookup, "RHIZA_CLIENT_TOKEN")?;
        let admin_token = lookup("RHIZA_ADMIN_TOKEN")
            .map(|token| {
                AdminConfig::new(token.clone())
                    .map(|_| token)
                    .map_err(|error| format!("invalid RHIZA_ADMIN_TOKEN: {error}"))
            })
            .transpose()?;
        let bundle = load_configuration_bundle(&mut lookup, |path| fs::read_to_string(path))?;
        let client_listen = lookup("RHIZA_CLIENT_LISTEN").unwrap_or_else(|| "0.0.0.0:8080".into());
        let recorder_listen =
            lookup("RHIZA_RECORDER_LISTEN").unwrap_or_else(|| "0.0.0.0:8081".into());
        let recorder_transport = match lookup("RHIZA_RECORDER_TRANSPORT").as_deref() {
            None | Some("http") => RecorderTransport::Http,
            Some("tcp-postcard") => RecorderTransport::TcpPostcard,
            Some(_) => return Err("RHIZA_RECORDER_TRANSPORT must be http|tcp-postcard".into()),
        };
        for removed in [
            "RHIZA_RECORDER_TLS_CERT_FILE",
            "RHIZA_RECORDER_TLS_KEY_FILE",
            "RHIZA_RECORDER_TLS_CA_FILE",
        ] {
            if lookup(removed).is_some() {
                return Err(format!(
                    "{removed} is no longer supported; tcp-postcard is plaintext and in-cluster only"
                ));
            }
        }
        let tcp_listen = optional_env(&mut lookup, "RHIZA_RECORDER_TCP_LISTEN")?;
        let tcp_requested =
            recorder_transport == RecorderTransport::TcpPostcard || tcp_listen.is_some();
        let recorder_tcp = if tcp_requested {
            let listen = tcp_listen.ok_or_else(|| {
                "RHIZA_RECORDER_TCP_LISTEN is required for recorder TCP".to_string()
            })?;
            listen.parse::<std::net::SocketAddr>().map_err(|_| {
                "RHIZA_RECORDER_TCP_LISTEN must be an IP socket address".to_string()
            })?;
            Some(RecorderTcpConfig { listen })
        } else {
            None
        };
        if recorder_transport == RecorderTransport::TcpPostcard
            && bundle.recorder_tcp_peers.iter().any(Option::is_none)
        {
            return Err("tcp-postcard requires recorder_tcp_addr for every bundle member".into());
        }
        let object_store_mode = lookup("RHIZA_OBJECT_STORE");
        let (recovery_generation, remote) = match object_store_mode {
            Some(mode) => {
                let object_store = parse_object_store_with_lookup(&mode, false, &mut lookup)?;
                let recovery_generation = positive_env(&mut lookup, "RHIZA_RECOVERY_GENERATION")?;
                let startup =
                    parse_startup_mode(required_env(&mut lookup, "RHIZA_STARTUP_MODE")?.as_str())?;
                let durability = parse_durability(&mut lookup)?;
                let lease_duration_ms =
                    optional_positive_env(&mut lookup, "RHIZA_CHECKPOINT_LEASE_MS")?
                        .unwrap_or(300_000);
                (
                    recovery_generation,
                    Some(RemoteCheckpointConfig {
                        object_store,
                        durability,
                        lease_duration_ms,
                        startup,
                    }),
                )
            }
            None => {
                for name in [
                    "RHIZA_DURABILITY_MODE",
                    "RHIZA_DURABILITY_MAX_LAG",
                    "RHIZA_DURABILITY_INTERVAL",
                    "RHIZA_CHECKPOINT_LEASE_MS",
                    "RHIZA_STARTUP_MODE",
                ] {
                    if lookup(name).is_some() {
                        return Err(format!("{name} is irrelevant without RHIZA_OBJECT_STORE"));
                    }
                }
                let generation =
                    optional_positive_env(&mut lookup, "RHIZA_RECOVERY_GENERATION")?.unwrap_or(1);
                (generation, None)
            }
        };

        let config = Self {
            execution_profile,
            logical_cluster_id,
            cluster_id,
            node_id,
            data_dir,
            epoch,
            bundle,
            client_token,
            admin_token,
            client_listen,
            recorder_listen,
            recorder_transport,
            recorder_tcp,
            recovery_generation,
            remote,
        };
        config.node_config()?;
        if let Some(admin_token) = &config.admin_token {
            if admin_token == &config.client_token
                || config
                    .bundle
                    .peers
                    .iter()
                    .any(|peer| peer.token() == admin_token)
            {
                return Err(
                    "RHIZA_ADMIN_TOKEN must be distinct from client and peer tokens".into(),
                );
            }
        }
        Ok(config)
    }

    fn local_peer_token(&self) -> Result<&str, String> {
        self.bundle
            .peers
            .iter()
            .find(|peer| peer.node_id() == self.node_id)
            .map(PeerConfig::token)
            .ok_or_else(|| "peer set must include RHIZA_NODE_ID".into())
    }

    fn node_config(&self) -> Result<NodeConfig, String> {
        let mut config = NodeConfig::new_with_configuration(
            self.logical_cluster_id.clone(),
            self.node_id.clone(),
            self.data_dir.clone(),
            self.epoch,
            self.bundle.membership.clone(),
            self.bundle.configuration_state.clone(),
            self.bundle.peers.clone(),
            self.client_token.clone(),
        )
        .map_err(|error| error.to_string())?
        .with_execution_profile(self.execution_profile)
        .map_err(|error| error.to_string())?;
        if let Some(predecessor) = &self.bundle.predecessor {
            config = config.with_log_initial_configuration(ConfigurationState::active(
                predecessor.stop.entry.config_id,
                predecessor.membership.digest(),
            ));
            config = config.with_predecessor_stop_entry(predecessor.stop.entry.clone());
        }
        config
            .with_recovery_generation(self.recovery_generation)
            .map_err(|error| error.to_string())
    }

    fn admin_config(&self) -> Result<Option<AdminConfig>, String> {
        self.admin_token
            .clone()
            .map(AdminConfig::new)
            .transpose()
            .map_err(|error| error.to_string())
    }
}

fn parse_command<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let Some(command) = args.next() else {
        return Err("missing command".into());
    };
    match command.as_str() {
        "status" => parse_status(args).map(Command::Status),
        "e2e" => parse_e2e(args).map(Command::E2e),
        "serve" => {
            reject_extra_args(args)?;
            ServeConfig::from_env().map(Box::new).map(Command::Serve)
        }
        "init-checkpoint" => {
            reject_extra_args(args)?;
            CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())
                .map(Command::InitCheckpoint)
        }
        "roll-checkpoint" => parse_roll_checkpoint(args).map(Command::RollCheckpoint),
        "checkpoint" => parse_checkpoint_command(args),
        "validate-config-bundle" => {
            let args = args.collect::<Vec<_>>();
            if args == ["--stdin"] {
                Ok(Command::ValidateConfigBundle(None))
            } else {
                reject_extra_args(args.into_iter())?;
                parse_validate_config_bundle(
                    |name| env::var(name).ok(),
                    |path| fs::read_to_string(path),
                )
            }
        }
        "gc" => parse_gc_command(args),
        "membership" => parse_membership_command(args),
        "write" => parse_write(args).map(Command::Write),
        "read" => parse_read(args).map(Command::Read),
        "sql" => parse_sql_command(args),
        "health" => parse_health(args).map(Command::Health),
        _ => Err(format!("unknown command: {command}")),
    }
}

fn parse_validate_config_bundle(
    lookup: impl FnMut(&str) -> Option<String>,
    read_file: impl FnOnce(&str) -> std::io::Result<String>,
) -> Result<Command, String> {
    load_configuration_bundle(lookup, read_file)
        .map(|bundle| Command::ValidateConfigBundle(Some(bundle.config_id)))
}

fn validate_config_bundle_stdin() -> Result<u64, String> {
    let mut json = String::new();
    io::stdin()
        .read_to_string(&mut json)
        .map_err(|error| format!("cannot read configuration bundle from stdin: {error}"))?;
    parse_configuration_bundle(&json).map(|bundle| bundle.config_id)
}

fn parse_write(args: impl IntoIterator<Item = String>) -> Result<WriteArgs, String> {
    parse_write_with_lookup(args, |name| env::var(name).ok())
}

fn parse_write_with_lookup(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<WriteArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut key = None;
    let mut value = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--key" => key = Some(next_value(&mut args, "--key")?),
            "--value" => value = Some(next_value(&mut args, "--value")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(WriteArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request_id: required_arg(request_id, "--request-id")?,
        key: required_arg(key, "--key")?,
        value: required_arg(value, "--value")?,
    })
}

fn parse_read(args: impl IntoIterator<Item = String>) -> Result<ReadArgs, String> {
    parse_read_with_lookup(args, |name| env::var(name).ok())
}

fn parse_read_with_lookup(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<ReadArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut key = None;
    let mut consistency = None;
    let mut expect = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--key" => key = Some(next_value(&mut args, "--key")?),
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?);
            }
            "--expect" => expect = Some(next_value(&mut args, "--expect")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(ReadArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        key: required_arg(key, "--key")?,
        consistency,
        expect,
    })
}

fn parse_sql_command(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("execute") => {
            parse_sql_execute(args, |name| env::var(name).ok()).map(Command::SqlExecute)
        }
        Some("query") => parse_sql_query(args, |name| env::var(name).ok()).map(Command::SqlQuery),
        Some(other) => Err(format!("unknown sql command: {other}")),
        None => Err("missing sql command: execute|query".into()),
    }
}

fn parse_sql_execute(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<SqlExecuteArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut sql = None;
    let mut parameters = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--sql" => sql = Some(next_value(&mut args, "--sql")?),
            "--params-json" => {
                parameters = parse_sql_parameters(&next_value(&mut args, "--params-json")?)?
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(SqlExecuteArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request_id: required_arg(request_id, "--request-id")?,
        statement: SqlStatement {
            sql: required_arg(sql, "--sql")?,
            parameters,
        },
    })
}

fn parse_sql_query(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<SqlQueryArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut sql = None;
    let mut parameters = Vec::new();
    let mut consistency = None;
    let mut max_rows = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--sql" => sql = Some(next_value(&mut args, "--sql")?),
            "--params-json" => {
                parameters = parse_sql_parameters(&next_value(&mut args, "--params-json")?)?
            }
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?)
            }
            "--max-rows" => {
                max_rows = Some(
                    next_value(&mut args, "--max-rows")?
                        .parse::<u32>()
                        .map_err(|_| "--max-rows must be a positive integer".to_string())?,
                )
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(SqlQueryArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        statement: SqlStatement {
            sql: required_arg(sql, "--sql")?,
            parameters,
        },
        consistency,
        max_rows,
    })
}

fn parse_sql_parameters(value: &str) -> Result<Vec<SqlValue>, String> {
    serde_json::from_str(value).map_err(|error| format!("invalid --params-json: {error}"))
}

fn parse_read_consistency(value: &str) -> Result<ReadConsistency, String> {
    match value {
        "local" => Ok(ReadConsistency::Local),
        "read_barrier" => Ok(ReadConsistency::ReadBarrier),
        _ => Err("consistency must be `local` or `read_barrier`".into()),
    }
}

fn parse_status(args: impl IntoIterator<Item = String>) -> Result<HealthArgs, String> {
    parse_health(std::iter::once("--ready".into()).chain(args))
}

fn parse_health(args: impl IntoIterator<Item = String>) -> Result<HealthArgs, String> {
    let mut url = None;
    let mut ready = false;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = Some(next_value(&mut args, "--url")?),
            "--ready" => ready = true,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(HealthArgs {
        url: required_arg(url, "--url")?,
        ready,
    })
}

fn parse_e2e(args: impl IntoIterator<Item = String>) -> Result<E2eConfig, String> {
    let mut data_dir = env::var("RHIZA_DATA_DIR").unwrap_or_else(|_| "./.rhiza-e2e".into());
    let mut object_store =
        env::var("RHIZA_OBJECT_STORE").unwrap_or_else(|_| "local:./.rhiza-objects".into());
    let mut cluster_id = env::var("RHIZA_CLUSTER_ID").unwrap_or_else(|_| "cluster-a".into());
    let mut node_id = env::var("RHIZA_NODE_ID").unwrap_or_else(|_| "node-1".into());
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = next_value(&mut args, "--data-dir")?,
            "--object-store" => object_store = next_value(&mut args, "--object-store")?,
            "--cluster-id" => cluster_id = next_value(&mut args, "--cluster-id")?,
            "--node-id" => node_id = next_value(&mut args, "--node-id")?,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(E2eConfig {
        data_dir: PathBuf::from(data_dir),
        object_store: parse_object_store(&object_store)?,
        cluster_id,
        node_id,
    })
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {name}"))
}

fn required_arg(value: Option<String>, name: &str) -> Result<String, String> {
    value.ok_or_else(|| format!("missing required argument: {name}"))
}

fn required_urls(urls: Vec<String>) -> Result<Vec<String>, String> {
    if urls.is_empty() {
        Err("missing required argument: --url".into())
    } else {
        Ok(urls)
    }
}

fn client_token(
    flag: Option<String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<String, String> {
    let token = flag
        .or_else(|| lookup("RHIZA_CLIENT_TOKEN"))
        .ok_or_else(|| {
            "missing client token: pass --token or set RHIZA_CLIENT_TOKEN".to_string()
        })?;
    validate_auth_token(&token, "client token")?;
    Ok(token)
}

fn validate_auth_token(token: &str, name: &str) -> Result<(), String> {
    if token.trim().is_empty()
        || token.chars().any(char::is_whitespace)
        || !header::HeaderValue::try_from(token).is_ok_and(|value| value.to_str().is_ok())
    {
        return Err(format!(
            "{name} must be a nonempty whitespace-free HTTP header value"
        ));
    }
    Ok(())
}

fn validate_origin_url(url: String, name: &str) -> Result<String, String> {
    let url = url.trim_end_matches('/').to_string();
    let parsed = reqwest::Url::parse(&url).map_err(|_| format!("invalid {name}"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(format!("invalid {name}"));
    }
    Ok(url)
}

fn reject_extra_args(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    match args.next() {
        Some(arg) => Err(format!("unknown argument: {arg}")),
        None => Ok(()),
    }
}

fn required_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<String, String> {
    lookup(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

fn execution_profile(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<ExecutionProfile, String> {
    required_env(lookup, "RHIZA_EXECUTION_PROFILE")?
        .parse()
        .map_err(|_| "RHIZA_EXECUTION_PROFILE must be sql|graph|kv".to_string())
}

fn positive_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<u64, String> {
    required_env(lookup, name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn optional_positive_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<u64>, String> {
    let Some(value) = lookup(name) else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .map(Some)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn optional_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<String>, String> {
    match lookup(name) {
        Some(value) if value.is_empty() => Err(format!("{name} must not be empty")),
        value => Ok(value),
    }
}

fn configuration_id(lookup: &mut impl FnMut(&str) -> Option<String>) -> Result<u64, String> {
    load_configuration_bundle(&mut *lookup, |path| fs::read_to_string(path))
        .map(|bundle| bundle.config_id)
}

fn parse_startup_mode(value: &str) -> Result<StartupMode, String> {
    match value {
        "bootstrap" => Ok(StartupMode::Bootstrap),
        "rejoin" => Ok(StartupMode::Rejoin),
        "disaster" => Ok(StartupMode::Disaster),
        _ => Err("RHIZA_STARTUP_MODE must be bootstrap|rejoin|disaster".into()),
    }
}

fn parse_durability(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<DurabilityMode, String> {
    let mode = required_env(lookup, "RHIZA_DURABILITY_MODE")?;
    let max_lag = optional_env(lookup, "RHIZA_DURABILITY_MAX_LAG")?;
    let interval = optional_env(lookup, "RHIZA_DURABILITY_INTERVAL")?;
    match mode.as_str() {
        "sync" => {
            reject_irrelevant_duration(max_lag, "RHIZA_DURABILITY_MAX_LAG", "sync")?;
            reject_irrelevant_duration(interval, "RHIZA_DURABILITY_INTERVAL", "sync")?;
            Ok(DurabilityMode::Sync)
        }
        "bounded" => {
            reject_irrelevant_duration(interval, "RHIZA_DURABILITY_INTERVAL", "bounded")?;
            let value = max_lag.ok_or_else(|| {
                "RHIZA_DURABILITY_MAX_LAG is required for bounded durability".to_string()
            })?;
            Ok(DurabilityMode::Bounded {
                max_lag: parse_positive_duration(&value)
                    .map_err(|error| format!("RHIZA_DURABILITY_MAX_LAG {error}"))?,
            })
        }
        "periodic" => {
            reject_irrelevant_duration(max_lag, "RHIZA_DURABILITY_MAX_LAG", "periodic")?;
            let value = interval.ok_or_else(|| {
                "RHIZA_DURABILITY_INTERVAL is required for periodic durability".to_string()
            })?;
            Ok(DurabilityMode::Periodic {
                interval: parse_positive_duration(&value)
                    .map_err(|error| format!("RHIZA_DURABILITY_INTERVAL {error}"))?,
            })
        }
        _ => Err("RHIZA_DURABILITY_MODE must be sync|bounded|periodic".into()),
    }
}

fn reject_irrelevant_duration(value: Option<String>, name: &str, mode: &str) -> Result<(), String> {
    if value.is_some() {
        Err(format!("{name} is irrelevant for {mode} durability"))
    } else {
        Ok(())
    }
}

fn parse_positive_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        return Err("must be a positive duration with ms/s/m/h suffix".into());
    };
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("must be a positive duration with ms/s/m/h suffix".into());
    }
    let amount = number
        .parse::<u64>()
        .ok()
        .filter(|amount| *amount > 0)
        .ok_or_else(|| "must be a positive duration with ms/s/m/h suffix".to_string())?;
    let millis = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration is too large".to_string())?;
    Ok(Duration::from_millis(millis))
}

fn parse_roll_checkpoint(
    args: impl IntoIterator<Item = String>,
) -> Result<RollCheckpointConfig, String> {
    parse_roll_checkpoint_with_lookup(args, |name| env::var(name).ok())
}

fn parse_roll_checkpoint_with_lookup(
    args: impl IntoIterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<RollCheckpointConfig, String> {
    let mut from = None;
    let mut to = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from-generation" => from = Some(next_value(&mut args, "--from-generation")?),
            "--to-generation" => to = Some(next_value(&mut args, "--to-generation")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let from_generation = parse_positive_value(
        from.or_else(|| lookup("RHIZA_FROM_GENERATION")),
        "--from-generation or RHIZA_FROM_GENERATION",
    )?;
    let to_generation = parse_positive_value(
        to.or_else(|| lookup("RHIZA_TO_GENERATION")),
        "--to-generation or RHIZA_TO_GENERATION",
    )?;
    if from_generation.checked_add(1) != Some(to_generation) {
        return Err("target recovery generation must equal source generation + 1".into());
    }
    let generation = from_generation.to_string();
    let base = CheckpointCommandConfig::from_lookup(|name| {
        if name == "RHIZA_RECOVERY_GENERATION" {
            Some(generation.clone())
        } else {
            lookup(name)
        }
    })?;
    Ok(RollCheckpointConfig {
        base,
        from_generation,
        to_generation,
    })
}

fn parse_checkpoint_command(mut args: impl Iterator<Item = String>) -> Result<Command, String> {
    let subcommand = args
        .next()
        .ok_or_else(|| "missing checkpoint subcommand".to_string())?;
    match subcommand.as_str() {
        "inspect" => {
            reject_extra_args(args)?;
            CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())
                .map(Command::CheckpointInspect)
        }
        "fork-successor" => parse_checkpoint_fork_successor(args, |name| env::var(name).ok())
            .map(Command::CheckpointForkSuccessor),
        "compact" => parse_admin_command_config(args, true, true, |name| env::var(name).ok())
            .map(Box::new)
            .map(Command::CheckpointCompact),
        _ => Err(format!("unknown checkpoint subcommand: {subcommand}")),
    }
}

fn parse_checkpoint_fork_successor(
    args: impl IntoIterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<CheckpointForkSuccessorConfig, String> {
    let mut source_config_id = None;
    let mut source_generation = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from-config-id" => {
                source_config_id = Some(next_value(&mut args, "--from-config-id")?)
            }
            "--from-generation" => {
                source_generation = Some(next_value(&mut args, "--from-generation")?)
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let source_config_id = parse_positive_value(source_config_id, "--from-config-id")?;
    let source_generation = parse_positive_value(source_generation, "--from-generation")?;
    let target = CheckpointCommandConfig::from_lookup(&mut lookup)?;
    let bundle = load_configuration_bundle(&mut lookup, |path| fs::read_to_string(path))?;
    let predecessor = bundle.require_predecessor()?;
    if bundle.config_id != target.config_id
        || source_config_id.checked_add(1) != Some(target.config_id)
        || predecessor.stop.entry.config_id != source_config_id
        || predecessor.stop.entry.cluster_id != target.cluster_id
        || predecessor.stop.entry.epoch != target.epoch
    {
        return Err(
            "predecessor proof/bundle does not match the exact source and target identity".into(),
        );
    }
    Ok(CheckpointForkSuccessorConfig {
        target,
        source_config_id,
        source_generation,
        stop_entry: predecessor.stop.entry.clone(),
    })
}

fn parse_membership_command(args: impl Iterator<Item = String>) -> Result<Command, String> {
    parse_membership_command_with_lookup(args, |name| env::var(name).ok())
}

fn parse_membership_command_with_lookup(
    mut args: impl Iterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<Command, String> {
    let subcommand = args
        .next()
        .ok_or_else(|| "missing membership subcommand".to_string())?;
    if !matches!(
        subcommand.as_str(),
        "status" | "stop" | "install-successor" | "activate"
    ) {
        return Err(format!("unknown membership subcommand: {subcommand}"));
    }
    let mutating = subcommand != "status";
    let config = Box::new(parse_admin_command_config(
        args,
        mutating,
        mutating,
        &mut lookup,
    )?);
    match subcommand.as_str() {
        "status" => Ok(Command::MembershipStatus(config)),
        "stop" => Ok(Command::MembershipStop(config)),
        "install-successor" => Ok(Command::MembershipInstallSuccessor(config)),
        "activate" => Ok(Command::MembershipActivate(config)),
        _ => unreachable!("membership subcommand validated"),
    }
}

fn parse_admin_command_config(
    args: impl Iterator<Item = String>,
    need_serve: bool,
    require_operation_id: bool,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<AdminCommandConfig, String> {
    let mut offline = false;
    let mut url = None;
    let mut token = None;
    let mut operation_id = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--offline" => offline = true,
            "--admin-url" => url = Some(next_value(&mut args, "--admin-url")?),
            "--admin-token" => token = Some(next_value(&mut args, "--admin-token")?),
            "--operation-id" => operation_id = Some(next_value(&mut args, "--operation-id")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    let successor = load_optional_successor_bundle(&mut lookup)?;

    if offline {
        if url.is_some() || token.is_some() || operation_id.is_some() {
            return Err("admin URL, token, and operation id are irrelevant with --offline".into());
        }
        let serve = ServeConfig::from_lookup(&mut lookup)?;
        return Ok(AdminCommandConfig {
            target: AdminTarget::Offline,
            serve: Some(Box::new(serve)),
            operation_id: None,
            successor,
        });
    }

    let url = url
        .or_else(|| lookup("RHIZA_ADMIN_URL"))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing admin URL: pass --admin-url or set RHIZA_ADMIN_URL".to_string())?;
    let url = validate_origin_url(url, "admin URL")?;
    let token = token
        .or_else(|| lookup("RHIZA_ADMIN_TOKEN"))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            "missing admin token: pass --admin-token or set RHIZA_ADMIN_TOKEN".to_string()
        })?;
    AdminConfig::new(token.clone()).map_err(|error| format!("invalid admin token: {error}"))?;
    let operation_id = operation_id
        .or_else(|| lookup("RHIZA_ADMIN_OPERATION_ID"))
        .filter(|value| !value.trim().is_empty());
    if require_operation_id && operation_id.is_none() {
        return Err(
            "missing operation id: pass --operation-id or set RHIZA_ADMIN_OPERATION_ID".into(),
        );
    }
    let serve = need_serve
        .then(|| ServeConfig::from_lookup(&mut lookup))
        .transpose()?
        .map(Box::new);
    if let Some(serve) = &serve {
        if token == serve.client_token
            || serve.bundle.peers.iter().any(|peer| peer.token() == token)
        {
            return Err("admin token must be distinct from client and peer tokens".into());
        }
    }
    Ok(AdminCommandConfig {
        target: AdminTarget::Live(AdminClientConfig { url, token }),
        serve,
        operation_id,
        successor,
    })
}

fn load_optional_successor_bundle(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<ConfigurationBundle>, String> {
    let inline = optional_env(&mut *lookup, "RHIZA_SUCCESSOR_CONFIG_BUNDLE")?;
    let file = optional_env(&mut *lookup, "RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE")?;
    match (inline, file) {
        (Some(_), Some(_)) => Err(
            "RHIZA_SUCCESSOR_CONFIG_BUNDLE and RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE are mutually exclusive"
                .into(),
        ),
        (Some(json), None) => parse_configuration_bundle(&json).map(Some),
        (None, Some(path)) => fs::read_to_string(path)
            .map_err(|error| format!("cannot read RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE: {error}"))
            .and_then(|json| parse_configuration_bundle(&json))
            .map(Some),
        (None, None) => Ok(None),
    }
}

fn parse_gc_command(mut args: impl Iterator<Item = String>) -> Result<Command, String> {
    let subcommand = args
        .next()
        .ok_or_else(|| "missing gc subcommand".to_string())?;
    match subcommand.as_str() {
        "plan" => parse_gc_plan(args, |name| env::var(name).ok()).map(Command::GcPlan),
        "inspect" | "evidence" => {
            let plan_hash = parse_plan_hash_flags(args, false)?;
            let base = CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())?;
            Ok(Command::GcInspect(GcInspectConfig { base, plan_hash }))
        }
        "apply" => {
            let plan_hash = parse_gc_apply_flags(args)?;
            let base = CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())?;
            Ok(Command::GcApply(GcInspectConfig { base, plan_hash }))
        }
        _ => Err(format!("unknown gc subcommand: {subcommand}")),
    }
}

fn parse_gc_plan(
    args: impl IntoIterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<GcPlanConfig, String> {
    let mut operation_id = None;
    let mut retain_generations = None;
    let mut grace_ms = None;
    let mut min_age_ms = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--operation-id" => operation_id = Some(next_value(&mut args, "--operation-id")?),
            "--retain-generations" => {
                retain_generations = Some(next_value(&mut args, "--retain-generations")?)
            }
            "--grace-ms" => grace_ms = Some(next_value(&mut args, "--grace-ms")?),
            "--min-age-ms" => min_age_ms = Some(next_value(&mut args, "--min-age-ms")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let operation_id = required_arg(operation_id, "--operation-id")?;
    if operation_id.trim().is_empty() {
        return Err("--operation-id must not be empty".into());
    }
    let retain_generations =
        parse_nonnegative_usize(retain_generations, "--retain-generations")?.unwrap_or(1);
    let grace_ms = parse_nonnegative_u64(grace_ms, "--grace-ms")?.unwrap_or(60_000);
    let min_age_ms = parse_nonnegative_u64(min_age_ms, "--min-age-ms")?.unwrap_or(86_400_000);
    let base = CheckpointCommandConfig::from_lookup(&mut lookup)?;
    Ok(GcPlanConfig {
        base,
        operation_id,
        retain_generations,
        grace_ms,
        min_age_ms,
    })
}

fn parse_nonnegative_u64(value: Option<String>, name: &str) -> Result<Option<u64>, String> {
    value
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| format!("{name} must be a nonnegative integer"))
        })
        .transpose()
}

fn parse_nonnegative_usize(value: Option<String>, name: &str) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("{name} must be a nonnegative integer"))
        })
        .transpose()
}

fn parse_gc_apply_flags(args: impl IntoIterator<Item = String>) -> Result<String, String> {
    parse_plan_hash_flags(args, true)
}

fn parse_plan_hash_flags(
    args: impl IntoIterator<Item = String>,
    require_confirmation: bool,
) -> Result<String, String> {
    let mut plan_hash = None;
    let mut confirmed = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--plan-hash" => plan_hash = Some(next_value(&mut args, "--plan-hash")?),
            "--confirm" if require_confirmation => confirmed = true,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let plan_hash = required_arg(plan_hash, "--plan-hash")?;
    if plan_hash.len() != 64
        || !plan_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("--plan-hash must be an exact 64-character lowercase SHA-256 hex value".into());
    }
    if require_confirmation && !confirmed {
        return Err("gc apply requires --confirm".into());
    }
    Ok(plan_hash)
}

fn parse_positive_value(value: Option<String>, name: &str) -> Result<u64, String> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn load_configuration_bundle(
    mut lookup: impl FnMut(&str) -> Option<String>,
    read_file: impl FnOnce(&str) -> std::io::Result<String>,
) -> Result<ConfigurationBundle, String> {
    let inline = optional_env(&mut lookup, "RHIZA_CONFIG_BUNDLE")?;
    let file = optional_env(&mut lookup, "RHIZA_CONFIG_BUNDLE_FILE")?;
    if inline.is_some() && file.is_some() {
        return Err(
            "RHIZA_CONFIG_BUNDLE and RHIZA_CONFIG_BUNDLE_FILE are mutually exclusive".into(),
        );
    }
    if lookup("RHIZA_CONFIG_ID").is_some()
        || (1..=7).any(|index| {
            ["ID", "URL", "LOG_URL", "TOKEN"]
                .iter()
                .any(|suffix| lookup(&format!("RHIZA_PEER_{index}_{suffix}")).is_some())
        })
    {
        return Err(
            "RHIZA_CONFIG_ID and RHIZA_PEER_* are unsupported; use RHIZA_CONFIG_BUNDLE or RHIZA_CONFIG_BUNDLE_FILE"
                .into(),
        );
    }
    let json = match (inline, file) {
        (Some(json), None) => json,
        (None, Some(path)) => read_file(&path)
            .map_err(|error| format!("cannot read RHIZA_CONFIG_BUNDLE_FILE: {error}"))?,
        (None, None) => {
            return Err("RHIZA_CONFIG_BUNDLE or RHIZA_CONFIG_BUNDLE_FILE is required".into())
        }
        _ => unreachable!("bundle source exclusivity checked"),
    };
    parse_configuration_bundle(&json)
}

fn parse_configuration_bundle(json: &str) -> Result<ConfigurationBundle, String> {
    let document: ConfigurationBundleDocument = serde_json::from_str(json)
        .map_err(|error| format!("invalid configuration bundle JSON: {error}"))?;
    if document.version != 1 {
        return Err(format!(
            "unsupported configuration bundle version: {}",
            document.version
        ));
    }
    if document.config_id == 0 {
        return Err("configuration bundle config_id must be positive".into());
    }
    let mut members = document
        .members
        .into_iter()
        .map(|member| {
            let recorder_tcp = match member.recorder_tcp_addr {
                None => None,
                Some(address) => {
                    validate_recorder_tcp_endpoint(&address)?;
                    Some(RecorderTcpPeer { address })
                }
            };
            let log_url = member.log_url.unwrap_or_else(|| member.url.clone());
            let peer =
                PeerConfig::new_with_log_url(member.node_id, member.url, log_url, member.token)
                    .map_err(|error| error.to_string())?;
            Ok::<_, String>((peer, recorder_tcp))
        })
        .collect::<Result<Vec<_>, _>>()?;
    members.sort_by(|left, right| left.0.node_id().cmp(right.0.node_id()));
    let (peers, recorder_tcp_peers): (Vec<_>, Vec<_>) = members.into_iter().unzip();
    let membership = Membership::from_voters(
        peers
            .iter()
            .map(|peer| peer.node_id().to_string())
            .collect::<Vec<_>>(),
    )
    .map_err(|error| error.to_string())?;

    let predecessor = document
        .predecessor
        .map(|predecessor| parse_predecessor(document.config_id, &membership, predecessor))
        .transpose()?;
    let configuration_state = predecessor
        .as_ref()
        .map(|predecessor| {
            ConfigurationState::stopped(
                predecessor.stop.entry.config_id,
                predecessor.membership.digest(),
                rhiza_core::LogAnchor::new(
                    predecessor.stop.entry.index,
                    predecessor.stop.entry.hash,
                ),
            )
        })
        .unwrap_or_else(|| ConfigurationState::active(document.config_id, membership.digest()));
    Ok(ConfigurationBundle {
        config_id: document.config_id,
        peers,
        recorder_tcp_peers,
        membership,
        configuration_state,
        predecessor,
    })
}

fn parse_predecessor(
    successor_config_id: u64,
    successor_membership: &Membership,
    predecessor: PredecessorDocument,
) -> Result<PredecessorConfiguration, String> {
    let membership = Membership::from_voters(predecessor.members)
        .map_err(|error| format!("invalid predecessor membership: {error}"))?;
    let entry = predecessor.stop_entry;
    if predecessor.version != 2 {
        return Err("predecessor transition document must use version 2".into());
    }
    let proof = predecessor.stop_proof;
    if entry.config_id.checked_add(1) != Some(successor_config_id) {
        return Err("successor config_id must equal predecessor config_id + 1".into());
    }
    proof
        .validate_for(entry.index, entry.epoch, entry.config_id, &membership)
        .map_err(|error| format!("invalid predecessor Stop proof: {error:?}"))?;
    let value = proof
        .proposal()
        .value
        .as_ref()
        .ok_or_else(|| "predecessor Stop proof has no value".to_string())?;
    let command = StoredCommand::new(entry.entry_type, entry.payload.clone());
    let bound_successor = ConfigChange::recognize(&command)
        .ok()
        .and_then(|change| change.successor().cloned())
        .ok_or_else(|| "predecessor Stop must be bound to an exact successor".to_string())?;
    let transitioned = ConfigurationState::active(entry.config_id, membership.digest())
        .validate_entry(&entry)
        .map_err(|error| format!("invalid predecessor stop entry: {error}"))?;
    if entry.recompute_hash() != entry.hash
        || value.command_hash != command.hash()
        || value.prev_hash != entry.prev_hash
        || value.entry_hash != entry.hash
        || bound_successor.config_id() != successor_config_id
        || bound_successor.members() != successor_membership.members()
        || bound_successor.digest() != successor_membership.digest()
        || transitioned.stop().map(|stop| (stop.index(), stop.hash()))
            != Some((entry.index, entry.hash))
    {
        return Err("predecessor Stop entry and proof do not match".into());
    }
    Ok(PredecessorConfiguration {
        membership,
        stop: StopInformation {
            version: 2,
            entry,
            proof,
        },
    })
}

async fn serve(config: ServeConfig) -> Result<(), String> {
    serve_until(config, shutdown_signal()).await
}

async fn serve_until<F>(config: ServeConfig, shutdown: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if config.remote.is_some() {
        serve_remote_until(config, shutdown).await
    } else {
        serve_local_until(config, shutdown).await
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                eprintln!("cannot install SIGTERM handler: {error}");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ServeExit {
    Shutdown,
    Client,
    Recorder,
    CheckpointWorker,
}

async fn wait_for_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn stop_admin_admission(tasks: Option<&AdminTaskTracker>) {
    if let Some(tasks) = tasks {
        tasks.stop_admission();
    }
}

async fn wait_for_admin_tasks(tasks: Option<&AdminTaskTracker>) {
    if let Some(tasks) = tasks {
        tasks.wait_for_idle().await;
    }
}

fn shutdown_deadline_error(timeout: Duration) -> String {
    format!(
        "shutdown did not complete within {} seconds; final checkpoint durability is unconfirmed",
        timeout.as_secs_f64()
    )
}

async fn before_shutdown_deadline<T>(
    deadline: tokio::time::Instant,
    timeout: Duration,
    future: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| shutdown_deadline_error(timeout))
}

fn remaining_shutdown_budget(deadline: tokio::time::Instant) -> Duration {
    deadline.saturating_duration_since(tokio::time::Instant::now())
}

fn pending_consensus_rpc_result(finished: bool) -> Result<(), String> {
    if finished {
        Ok(())
    } else {
        Err("consensus RPCs did not finish before the shutdown deadline".into())
    }
}

fn finish_pending_consensus_rpcs(
    runtime: &Arc<NodeRuntime>,
    timeout: Duration,
) -> Result<(), String> {
    let consensus = runtime.consensus();
    let finished = if matches!(
        tokio::runtime::Handle::try_current().map(|handle| handle.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    ) {
        tokio::task::block_in_place(|| consensus.finish_pending_rpcs(timeout))
    } else {
        consensus.finish_pending_rpcs(timeout)
    };
    pending_consensus_rpc_result(finished)
}

async fn serve_local_until<F>(config: ServeConfig, shutdown: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let node_config = config.node_config()?;
    let recorder = open_recorder(&config)?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut recorder_server =
        spawn_recorder_server(&config, recorder.clone(), shutdown_rx.clone()).await?;
    tokio::task::yield_now().await;

    let consensus = build_consensus(&config)?;
    let runtime_startup = open_runtime_with_retry(node_config, consensus, Vec::new());
    tokio::pin!(runtime_startup);
    let runtime = tokio::select! {
        result = &mut runtime_startup => result?,
        result = &mut recorder_server.0 => return Err(recorder_task_error(result)),
    };
    let client_listener = bind_client_listener(&config).await?;
    println!(
        "rhiza serving client={} recorder={}",
        config.client_listen,
        active_recorder_listen(&config)?
    );
    let mut materializer = materializer_worker(runtime.clone(), shutdown_rx.clone());

    let (app, admin_tasks) = match config.admin_config()? {
        Some(admin) => {
            let (router, tasks) =
                node_router_with_admin_and_tasks(runtime.clone(), recorder, admin)
                    .map_err(|error| error.to_string())?;
            (router, Some(tasks))
        }
        None => (node_router(runtime.clone(), recorder), None),
    };
    let mut client_server = AbortOnDrop(tokio::spawn(async move {
        axum::serve(client_listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
            .await
            .map_err(|error| format!("client server stopped: {error}"))
    }));
    tokio::pin!(shutdown);
    let (exit, result) = tokio::select! {
        biased;
        () = &mut shutdown => (ServeExit::Shutdown, Ok(())),
        result = &mut client_server.0 =>
            (ServeExit::Client, server_task_result(result, "client server")),
        result = &mut recorder_server.0 =>
            (ServeExit::Recorder, Err(recorder_task_error(result))),
    };
    stop_admin_admission(admin_tasks.as_ref());
    shutdown_tx.send_replace(true);
    runtime.cancel_operations();
    let deadline = tokio::time::Instant::now() + SERVE_SHUTDOWN_TIMEOUT;
    let drained = before_shutdown_deadline(deadline, SERVE_SHUTDOWN_TIMEOUT, async {
        let mut drained = Ok(());
        if exit != ServeExit::Client {
            retain_first_error(
                &mut drained,
                server_task_result((&mut client_server.0).await, "client server"),
            );
        }
        if exit != ServeExit::Recorder {
            retain_first_error(
                &mut drained,
                server_task_result((&mut recorder_server.0).await, "recorder server"),
            );
        }
        retain_first_error(
            &mut drained,
            task_result((&mut materializer.0).await, "materializer worker"),
        );
        wait_for_admin_tasks(admin_tasks.as_ref()).await;
        drained
    })
    .await
    .unwrap_or_else(Err);
    let mut shutdown_result = result;
    retain_first_error(&mut shutdown_result, drained);
    retain_first_error(
        &mut shutdown_result,
        finish_pending_consensus_rpcs(&runtime, remaining_shutdown_budget(deadline)),
    );
    shutdown_result
}

async fn serve_remote_until<F>(config: ServeConfig, shutdown: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let remote = config
        .remote
        .clone()
        .ok_or_else(|| "remote checkpoint configuration is missing".to_string())?;
    let store = open_object_store(&remote.object_store)?;
    if !store.supports_strong_cross_process_cas() {
        return Err("serving checkpoints require strong cross-process compare-and-swap".into());
    }
    let archive = ObjectArchiveStore::new_checkpoint(
        store,
        CheckpointIdentity::new(
            config.cluster_id.clone(),
            config.epoch,
            config.bundle.config_id,
            config.recovery_generation,
        ),
    )
    .map_err(|error| error.to_string())?;
    let authoritative_identity = archive
        .checkpoint_identity()
        .map_err(|error| error.to_string())?
        .clone();
    let node_config = config.node_config()?;
    let preparation = if config.bundle.predecessor.is_some() {
        require_successor_startup_mode(remote.startup)?;
        let restored =
            restore_successor_checkpoint_to_fresh_data_dir(archive.clone(), &node_config)
                .await
                .map_err(|error| error.to_string())?;
        if restored.requires_recorder_install() {
            install_successor_recorder_for_startup(&config)?;
            restored.complete().map_err(|error| error.to_string())?;
        }
        write_local_checkpoint_identity_marker(
            &config.data_dir,
            config.execution_profile,
            &authoritative_identity,
        )?;
        StartupPreparation::RecorderFirst
    } else {
        prepare_remote_startup(
            remote.startup,
            &archive,
            &config.data_dir,
            &config.node_id,
            config.execution_profile,
        )
        .await?
    };
    let recorder = open_recorder(&config)?;
    let mut recorder_server = match &preparation {
        StartupPreparation::RecorderFirst | StartupPreparation::VerifyLocalCheckpoint { .. } => {
            let server =
                spawn_recorder_server(&config, recorder.clone(), shutdown_rx.clone()).await?;
            tokio::task::yield_now().await;
            Some(server)
        }
        StartupPreparation::RuntimeFirstWithPeerCatchup { .. } => None,
    };

    let consensus = build_consensus(&config)?;
    let peer_candidates = match &preparation {
        StartupPreparation::RuntimeFirstWithPeerCatchup { .. } => build_log_peers(&config)?,
        StartupPreparation::RecorderFirst | StartupPreparation::VerifyLocalCheckpoint { .. } => {
            Vec::new()
        }
    };
    let runtime_startup = open_runtime_with_retry(node_config, consensus, peer_candidates);
    tokio::pin!(runtime_startup);
    let runtime = if let Some(server) = recorder_server.as_mut() {
        tokio::select! {
            result = &mut runtime_startup => result?,
            result = &mut server.0 => return Err(recorder_task_error(result)),
        }
    } else {
        runtime_startup.await?
    };
    if let StartupPreparation::VerifyLocalCheckpoint { identity, root } = &preparation {
        verify_local_rejoin_checkpoint(&runtime, identity, *root)?;
    }
    if let StartupPreparation::RuntimeFirstWithPeerCatchup { checkpoint_index } = &preparation {
        rehydrate_recorder_with_retry(runtime.clone(), recorder.clone(), *checkpoint_index).await?;
    }
    if recorder_server.is_none() {
        recorder_server =
            Some(spawn_recorder_server(&config, recorder.clone(), shutdown_rx.clone()).await?);
        tokio::task::yield_now().await;
    }

    let coordinator = Arc::new(
        CheckpointCoordinator::open_with_holder_and_options(
            archive,
            remote.durability.clone(),
            &config.node_id,
            CheckpointPublisherOptions::new(remote.lease_duration_ms),
        )
        .await
        .map_err(|error| error.to_string())?,
    );
    coordinator
        .note_recovered_committed(runtime.applied_index().map_err(|error| error.to_string())?);
    let client_listener = bind_client_listener(&config).await?;
    println!(
        "rhiza serving client={} recorder={} recovery_generation={}",
        config.client_listen,
        active_recorder_listen(&config)?,
        config.recovery_generation
    );
    let mut materializer = materializer_worker(runtime.clone(), shutdown_rx.clone());

    let (app, admin_tasks) = match config.admin_config()? {
        Some(admin) => {
            let (router, tasks) = node_router_with_checkpoint_and_admin_tasks(
                runtime.clone(),
                recorder,
                coordinator.clone(),
                admin,
            )
            .map_err(|error| error.to_string())?;
            (router, Some(tasks))
        }
        None => (
            node_router_with_checkpoint(runtime.clone(), recorder, coordinator.clone()),
            None,
        ),
    };
    let client_shutdown = shutdown_rx.clone();
    let mut client_server = AbortOnDrop(tokio::spawn(async move {
        axum::serve(client_listener, app)
            .with_graceful_shutdown(wait_for_shutdown(client_shutdown))
            .await
            .map_err(|error| format!("client server stopped: {error}"))
    }));
    let mut recorder_server = recorder_server.expect("remote recorder server started");
    let mut worker = checkpoint_worker(
        remote.durability,
        Arc::clone(&runtime),
        Arc::clone(&coordinator),
        shutdown_rx.clone(),
    );
    tokio::pin!(shutdown);
    let (exit, result) = if let Some(worker) = worker.as_mut() {
        tokio::select! {
            biased;
            () = &mut shutdown => (ServeExit::Shutdown, Ok(())),
            result = &mut client_server.0 => (ServeExit::Client, server_task_result(result, "client server")),
            result = &mut recorder_server.0 => (ServeExit::Recorder, Err(recorder_task_error(result))),
            result = &mut worker.0 => (ServeExit::CheckpointWorker, Err(checkpoint_worker_error(result))),
        }
    } else {
        tokio::select! {
            biased;
            () = &mut shutdown => (ServeExit::Shutdown, Ok(())),
            result = &mut client_server.0 => (ServeExit::Client, server_task_result(result, "client server")),
            result = &mut recorder_server.0 => (ServeExit::Recorder, Err(recorder_task_error(result))),
        }
    };
    stop_admin_admission(admin_tasks.as_ref());
    shutdown_tx.send_replace(true);
    runtime.cancel_operations();
    let deadline = tokio::time::Instant::now() + SERVE_SHUTDOWN_TIMEOUT;
    let drained = before_shutdown_deadline(deadline, SERVE_SHUTDOWN_TIMEOUT, async {
        let mut drained = Ok(());
        if exit != ServeExit::Client {
            retain_first_error(
                &mut drained,
                server_task_result((&mut client_server.0).await, "client server"),
            );
        }
        if exit != ServeExit::Recorder {
            retain_first_error(
                &mut drained,
                server_task_result((&mut recorder_server.0).await, "recorder server"),
            );
        }
        if exit != ServeExit::CheckpointWorker {
            if let Some(worker) = worker.as_mut() {
                retain_first_error(
                    &mut drained,
                    task_result((&mut worker.0).await, "checkpoint worker"),
                );
            }
        }
        retain_first_error(
            &mut drained,
            task_result((&mut materializer.0).await, "materializer worker"),
        );
        wait_for_admin_tasks(admin_tasks.as_ref()).await;
        drained
    })
    .await
    .unwrap_or_else(Err);
    let mut shutdown_result = result;
    retain_first_error(&mut shutdown_result, drained);
    finish_remote_shutdown(shutdown_result, runtime, coordinator, deadline).await
}

fn require_successor_startup_mode(mode: StartupMode) -> Result<(), String> {
    if mode == StartupMode::Rejoin {
        Ok(())
    } else {
        Err("successor startup requires rejoin mode".into())
    }
}

async fn finish_remote_shutdown(
    mut result: Result<(), String>,
    runtime: Arc<NodeRuntime>,
    coordinator: Arc<CheckpointCoordinator>,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    runtime.cancel_operations();
    let final_flush = before_shutdown_deadline(deadline, SERVE_SHUTDOWN_TIMEOUT, async {
        match runtime.applied_index() {
            Ok(applied_index) => coordinator
                .flush_runtime(&runtime, applied_index)
                .await
                .map(|_| ())
                .map_err(|error| {
                    format!(
                        "final checkpoint durability is unconfirmed because the flush failed: {error}"
                    )
                }),
            Err(error) => Err(format!(
                "final checkpoint durability is unconfirmed because the applied index is unavailable: {error}"
            )),
        }
    })
    .await
    .unwrap_or_else(Err);
    append_shutdown_error(&mut result, final_flush);
    append_shutdown_error(
        &mut result,
        finish_pending_consensus_rpcs(&runtime, remaining_shutdown_budget(deadline)),
    );
    result
}

fn install_successor_recorder_for_startup(config: &ServeConfig) -> Result<(), String> {
    let predecessor = config.bundle.require_predecessor()?;
    let recorder = RecorderFileStore::new_with_membership(
        config.data_dir.join("recorder"),
        config.node_id.clone(),
        config.cluster_id.clone(),
        config.epoch,
        predecessor.stop.entry.config_id,
        predecessor.membership.clone(),
    )
    .map_err(|error| error.to_string())?;
    recover_successor_recorder_after_checkpoint(
        &recorder,
        &config.node_config()?,
        config.bundle.config_id,
        config.bundle.membership.clone(),
        &predecessor.stop,
    )
    .map(|_| ())
    .map_err(|error| error.to_string())
}

fn open_object_store(config: &ObjStoreConfig) -> Result<ObjStore, String> {
    ObjStore::new(config.clone())
        .map_err(|error| redact_object_store_error(config, error.to_string()))
}

fn redact_object_store_error(config: &ObjStoreConfig, mut message: String) -> String {
    let secrets: Vec<&str> = match config {
        ObjStoreConfig::Local { .. } => Vec::new(),
        ObjStoreConfig::S3 {
            access_key,
            secret_key,
            ..
        } => access_key
            .iter()
            .chain(secret_key.iter())
            .map(String::as_str)
            .collect(),
        ObjStoreConfig::Gcs {
            service_account_key,
            ..
        } => service_account_key.iter().map(String::as_str).collect(),
        ObjStoreConfig::AzureBlob { access_key, .. } => {
            access_key.iter().map(String::as_str).collect()
        }
    };
    for secret in secrets.into_iter().filter(|secret| !secret.is_empty()) {
        message = message.replace(secret, "[redacted]");
    }
    message
}

fn open_recorder(config: &ServeConfig) -> Result<RecorderFileStore, String> {
    RecorderFileStore::new_with_membership(
        config.data_dir.join("recorder"),
        config.node_id.clone(),
        config.cluster_id.clone(),
        config.epoch,
        config.bundle.config_id,
        config.bundle.membership.clone(),
    )
    .map_err(|error| error.to_string())
}

fn active_recorder_listen(config: &ServeConfig) -> Result<&str, String> {
    match config.recorder_transport {
        RecorderTransport::Http => Ok(&config.recorder_listen),
        RecorderTransport::TcpPostcard => config
            .recorder_tcp
            .as_ref()
            .map(|tcp| tcp.listen.as_str())
            .ok_or_else(|| "recorder TCP configuration is missing".to_string()),
    }
}

async fn spawn_recorder_server(
    config: &ServeConfig,
    recorder: RecorderFileStore,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<AbortOnDrop<Result<(), String>>, String> {
    match config.recorder_transport {
        RecorderTransport::Http => {
            let listener = tokio::net::TcpListener::bind(&config.recorder_listen)
                .await
                .map_err(|error| format!("cannot bind recorder listener: {error}"))?;
            let app = recorder_router_for_generation(
                recorder,
                config.bundle.peers.clone(),
                config.recovery_generation,
            );
            Ok(AbortOnDrop(tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(wait_for_shutdown(shutdown))
                    .await
                    .map_err(|error| format!("recorder server stopped: {error}"))
            })))
        }
        RecorderTransport::TcpPostcard => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TCP listener: {error}"))?;
            let peers = config.bundle.peers.clone();
            let recovery_generation = config.recovery_generation;
            Ok(AbortOnDrop(tokio::spawn(async move {
                serve_recorder_tcp(
                    listener,
                    recorder,
                    peers,
                    recovery_generation,
                    wait_for_shutdown(shutdown),
                )
                .await
            })))
        }
    }
}

async fn bind_client_listener(config: &ServeConfig) -> Result<tokio::net::TcpListener, String> {
    tokio::net::TcpListener::bind(&config.client_listen)
        .await
        .map_err(|error| format!("cannot bind client listener: {error}"))
}

fn recorder_task_error(result: Result<Result<(), String>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Ok(())) => "recorder server stopped".into(),
        Ok(Err(error)) => error,
        Err(error) => format!("recorder server task failed: {error}"),
    }
}

fn checkpoint_worker(
    mode: DurabilityMode,
    runtime: Arc<NodeRuntime>,
    coordinator: Arc<CheckpointCoordinator>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Option<AbortOnDrop<()>> {
    let cadence = match mode {
        DurabilityMode::Sync => return None,
        DurabilityMode::Bounded { max_lag } => (max_lag / 2).min(Duration::from_secs(1)),
        DurabilityMode::Periodic { interval } => interval,
    };
    Some(AbortOnDrop(tokio::spawn(async move {
        loop {
            tokio::select! {
                () = wait_for_shutdown(shutdown.clone()) => return,
                () = tokio::time::sleep(cadence) => {}
            }
            tokio::select! {
                () = wait_for_shutdown(shutdown.clone()) => return,
                result = coordinator.flush_runtime(&runtime, u64::MAX) => {
                    if let Err(error) = result {
                        eprintln!("checkpoint flush failed; retrying after {cadence:?}: {error}");
                    }
                }
            }
        }
    })))
}

fn materializer_worker(
    runtime: Arc<NodeRuntime>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> AbortOnDrop<()> {
    AbortOnDrop(tokio::spawn(async move {
        if let Err(error) = runtime
            .run_background_materializer(Duration::from_millis(100), wait_for_shutdown(shutdown))
            .await
        {
            eprintln!("background materializer stopped: {error}");
        }
    }))
}

fn task_result<T>(result: Result<T, tokio::task::JoinError>, name: &str) -> Result<T, String> {
    result.map_err(|error| format!("{name} task failed: {error}"))
}

fn retain_first_error(current: &mut Result<(), String>, next: Result<(), String>) {
    if current.is_ok() && next.is_err() {
        *current = next;
    }
}

fn append_shutdown_error(current: &mut Result<(), String>, next: Result<(), String>) {
    let Err(next) = next else {
        return;
    };
    match current {
        Ok(()) => *current = Err(next),
        Err(current) => {
            current.push_str("; ");
            current.push_str(&next);
        }
    }
}

fn server_task_result(
    result: Result<Result<(), String>, tokio::task::JoinError>,
    name: &str,
) -> Result<(), String> {
    task_result(result, name)?
}

fn checkpoint_worker_error(result: Result<(), tokio::task::JoinError>) -> String {
    match result {
        Ok(()) => "checkpoint worker stopped".into(),
        Err(error) => format!("checkpoint worker failed: {error}"),
    }
}

fn build_consensus(config: &ServeConfig) -> Result<Arc<ThreeNodeConsensus>, String> {
    let local_token = config.local_peer_token()?.to_owned();
    let recorders = config
        .bundle
        .peers
        .iter()
        .enumerate()
        .map(|(index, peer)| {
            let client: Box<dyn RecorderRpc> = match config.recorder_transport {
                RecorderTransport::Http => Box::new(
                    HttpRecorderClient::new_with_recovery_generation(
                        peer.base_url(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )
                    .map_err(|error| error.to_string())?,
                ),
                RecorderTransport::TcpPostcard => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    Box::new(TcpPostcardRecorderClient::new(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )?)
                }
            };
            Ok((peer.node_id().to_owned(), client))
        })
        .collect::<Result<Vec<_>, String>>()?;
    ThreeNodeConsensus::from_recorders_with_ids(
        config.cluster_id.clone(),
        config.node_id.clone(),
        config.epoch,
        config.bundle.config_id,
        recorders,
    )
    .map(Arc::new)
    .map_err(|error| error.to_string())
}

fn build_log_peers(config: &ServeConfig) -> Result<Vec<HttpLogPeer>, String> {
    let local_token = config.local_peer_token()?.to_owned();
    config
        .bundle
        .peers
        .iter()
        .filter(|peer| peer.node_id() != config.node_id)
        .map(|peer| {
            HttpLogPeer::new_with_recovery_generation(
                peer.log_base_url(),
                config.node_id.clone(),
                local_token.clone(),
                config.recovery_generation,
            )
            .map_err(|error| error.to_string())
        })
        .collect()
}

async fn open_runtime_with_retry(
    node_config: NodeConfig,
    consensus: Arc<ThreeNodeConsensus>,
    peer_candidates: Vec<HttpLogPeer>,
) -> Result<Arc<NodeRuntime>, String> {
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    let mut last_retry_error = None;

    loop {
        let attempt_config = node_config.clone();
        let attempt_consensus = Arc::clone(&consensus);
        let attempt_peers = peer_candidates.clone();
        let result = tokio::task::spawn_blocking(move || {
            let peer_refs = attempt_peers
                .iter()
                .map(|peer| peer as &dyn LogPeer)
                .collect::<Vec<_>>();
            NodeRuntime::open(attempt_config, attempt_consensus, &peer_refs)
        })
        .await
        .map_err(|error| format!("runtime startup task failed: {error}"))?;

        match result {
            Ok(runtime) => return Ok(Arc::new(runtime)),
            Err(error @ (NodeError::Unavailable(_) | NodeError::Contention(_))) => {
                let message = error.to_string();
                if last_retry_error.as_deref() != Some(message.as_str()) {
                    eprintln!("runtime startup waiting for recorder quorum: {message}");
                    last_retry_error = Some(message);
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

async fn rehydrate_recorder_with_retry(
    runtime: Arc<NodeRuntime>,
    recorder: RecorderFileStore,
    checkpoint_index: u64,
) -> Result<(), String> {
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    loop {
        let attempt_runtime = runtime.clone();
        let attempt_recorder = recorder.clone();
        let result = tokio::task::spawn_blocking(move || {
            rehydrate_recorder_after_checkpoint(
                &attempt_runtime,
                &attempt_recorder,
                checkpoint_index,
            )
        })
        .await
        .map_err(|error| format!("recorder rehydration task failed: {error}"))?;
        match result {
            Ok(()) => return Ok(()),
            Err(NodeError::Unavailable(_) | NodeError::Contention(_)) => {
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

async fn run_init_checkpoint(config: CheckpointCommandConfig) -> Result<CheckpointTip, String> {
    initialize_empty_checkpoint(&config.archive()?).await
}

async fn run_roll_checkpoint(
    config: RollCheckpointConfig,
) -> Result<(CheckpointTip, CheckpointTip), String> {
    if config.from_generation.checked_add(1) != Some(config.to_generation) {
        return Err("target recovery generation must equal source generation + 1".into());
    }
    let source = config.archive_for_generation(config.from_generation)?;
    let target = config.archive_for_generation(config.to_generation)?;
    roll_checkpoint(&source, &target).await
}

async fn run_checkpoint_inspect(config: CheckpointCommandConfig) -> Result<String, String> {
    let loaded = config
        .archive()?
        .load_checkpoint()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "checkpoint is not initialized".to_string())?;
    serde_json::to_string(loaded.manifest()).map_err(|error| error.to_string())
}

async fn run_checkpoint_fork_successor(
    config: CheckpointForkSuccessorConfig,
) -> Result<String, String> {
    let target = config.target.archive()?;
    let mut source_config = config.target.clone();
    source_config.config_id = config.source_config_id;
    source_config.recovery_generation = config.source_generation;
    let source = source_config.archive()?;
    let loaded = source
        .fork_stopped_successor(&target, &config.stop_entry)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(loaded.manifest()).map_err(|error| error.to_string())
}

async fn run_checkpoint_compact(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Offline => {
            run_checkpoint_compact_offline(*required_serve_config(config.serve)?).await
        }
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id])?;
            let response: AdminCompactResponse = admin_post(
                &client,
                ADMIN_COMPACT_PATH,
                &AdminCompactRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: serve.bundle.config_id,
                    expected_recovery_generation: serve.recovery_generation,
                    expected_root: status.qlog_root,
                },
            )
            .await?;
            serde_json::to_string(&response).map_err(|error| error.to_string())
        }
    }
}

async fn run_checkpoint_compact_offline(config: ServeConfig) -> Result<String, String> {
    let remote = config
        .remote
        .as_ref()
        .ok_or_else(|| "checkpoint compact requires RHIZA_OBJECT_STORE".to_string())?;
    let store = open_object_store(&remote.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(
        store,
        CheckpointIdentity::new(
            config.cluster_id.clone(),
            config.epoch,
            config.bundle.config_id,
            config.recovery_generation,
        ),
    )
    .map_err(|error| error.to_string())?;
    let runtime = open_offline_runtime(&config)?;
    let coordinator =
        CheckpointCoordinator::open_with_holder(archive, DurabilityMode::Sync, &config.node_id)
            .await
            .map_err(|error| error.to_string())?;
    let anchor = runtime
        .checkpoint_compact(&coordinator)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&anchor).map_err(|error| error.to_string())
}

async fn run_gc_plan(config: GcPlanConfig) -> Result<String, String> {
    let archive = config.base.archive()?;
    let now = unix_time_ms()?;
    let identity = config.base.identity();
    archive
        .set_gc_root(identity.clone(), now)
        .await
        .map_err(|error| error.to_string())?;
    let plan = archive
        .plan_gc(
            GcPolicy::new(
                config.operation_id,
                identity,
                config.retain_generations,
                config.grace_ms,
                config.min_age_ms,
            ),
            now,
        )
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&plan).map_err(|error| error.to_string())
}

async fn run_gc_inspect(config: GcInspectConfig) -> Result<String, String> {
    let store = open_object_store(&config.base.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(store.clone(), config.base.identity())
        .map_err(|error| error.to_string())?;
    let plan_bytes = store
        .get(&archive.gc_plan_key(&config.plan_hash))
        .await
        .map_err(|error| redact_object_store_error(&config.base.object_store, error.to_string()))?;
    let plan: GcPlan =
        serde_json::from_slice(&plan_bytes).map_err(|error| format!("invalid GC plan: {error}"))?;
    validate_gc_plan_identity(&plan, &config.base.identity(), &config.plan_hash)?;
    let mut evidence = Vec::new();
    for key in store
        .list(&archive.gc_evidence_prefix(&config.plan_hash))
        .await
        .map_err(|error| redact_object_store_error(&config.base.object_store, error.to_string()))?
    {
        let bytes = store.get(&key).await.map_err(|error| {
            redact_object_store_error(&config.base.object_store, error.to_string())
        })?;
        evidence.push(
            serde_json::from_slice::<serde_json::Value>(&bytes)
                .map_err(|error| format!("invalid GC evidence: {error}"))?,
        );
    }
    serde_json::to_string(&serde_json::json!({"plan": plan, "evidence": evidence}))
        .map_err(|error| error.to_string())
}

async fn run_gc_apply(config: GcInspectConfig) -> Result<String, String> {
    let store = open_object_store(&config.base.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(store.clone(), config.base.identity())
        .map_err(|error| error.to_string())?;
    let plan_bytes = store
        .get(&archive.gc_plan_key(&config.plan_hash))
        .await
        .map_err(|error| redact_object_store_error(&config.base.object_store, error.to_string()))?;
    let plan: GcPlan =
        serde_json::from_slice(&plan_bytes).map_err(|error| format!("invalid GC plan: {error}"))?;
    validate_gc_plan_identity(&plan, &config.base.identity(), &config.plan_hash)?;
    let report = archive
        .execute_gc(&config.plan_hash, unix_time_ms()?)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&report).map_err(|error| error.to_string())
}

fn validate_gc_plan_identity(
    plan: &GcPlan,
    expected: &CheckpointIdentity,
    plan_hash: &str,
) -> Result<(), String> {
    if plan.plan_hash() != plan_hash {
        return Err("stored GC plan hash does not match --plan-hash".into());
    }
    if plan.root() != expected {
        return Err(
            "GC plan generation/config identity does not match the command environment".into(),
        );
    }
    Ok(())
}

fn unix_time_ms() -> Result<u64, String> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| "system time exceeds supported range".into())
}

fn open_offline_runtime(config: &ServeConfig) -> Result<NodeRuntime, String> {
    let node_config = config.node_config()?;
    let consensus = build_consensus(config)?;
    NodeRuntime::open(node_config, consensus, &[]).map_err(|error| error.to_string())
}

async fn run_membership_status(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Live(client) => {
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            serde_json::to_string(&status).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            let serve = required_serve_config(config.serve)?;
            let runtime = open_offline_runtime(&serve)?;
            let status = runtime.status().map_err(|error| error.to_string())?;
            serde_json::to_string(&status).map_err(|error| error.to_string())
        }
    }
}

async fn run_membership_stop(config: AdminCommandConfig) -> Result<String, String> {
    let successor = config.successor.clone().ok_or_else(|| {
        "membership stop requires RHIZA_SUCCESSOR_CONFIG_BUNDLE or RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE"
            .to_string()
    })?;
    match config.target {
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            if serve.bundle.predecessor.is_some() {
                return Err(
                    "cannot stop a successor bundle; use the active configuration bundle".into(),
                );
            }
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id])?;
            let successor = successor_descriptor(&serve, &successor)?;
            let response: AdminStopResponse = admin_post(
                &client,
                ADMIN_STOP_PATH,
                &AdminStopRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: serve.bundle.config_id,
                    successor,
                },
            )
            .await?;
            let material = PredecessorDocument {
                version: response.stop.version,
                members: serve.bundle.membership.members().to_vec(),
                stop_entry: response.stop.entry,
                stop_proof: response.stop.proof,
            };
            serde_json::to_string(&material).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            run_membership_stop_offline(*required_serve_config(config.serve)?, successor)
        }
    }
}

fn run_membership_stop_offline(
    config: ServeConfig,
    successor: ConfigurationBundle,
) -> Result<String, String> {
    if config.bundle.predecessor.is_some() {
        return Err("cannot stop a successor bundle; use the active configuration bundle".into());
    }
    successor_descriptor(&config, &successor)?;
    let runtime = open_offline_runtime(&config)?;
    let stop = runtime
        .stop_current_configuration_for_successor(&successor.membership)
        .map_err(|error| error.to_string())?;
    let material = PredecessorDocument {
        version: stop.version,
        members: config.bundle.membership.members().to_vec(),
        stop_entry: stop.entry,
        stop_proof: stop.proof,
    };
    serde_json::to_string(&material).map_err(|error| error.to_string())
}

fn successor_descriptor(
    predecessor: &ServeConfig,
    successor: &ConfigurationBundle,
) -> Result<AdminSuccessorBundle, String> {
    if successor.predecessor.is_some()
        || predecessor.bundle.config_id.checked_add(1) != Some(successor.config_id)
    {
        return Err(
            "successor bundle must be an unactivated config with config_id predecessor + 1".into(),
        );
    }
    Ok(AdminSuccessorBundle {
        config_id: successor.config_id,
        members: successor.membership.members().to_vec(),
        digest: successor.membership.digest(),
    })
}

async fn run_membership_install_successor(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            serve.node_config()?;
            let predecessor = serve.bundle.require_predecessor()?;
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(&status, &serve, &[predecessor.stop.entry.config_id])?;
            let response: AdminInstallSuccessorResponse = admin_post(
                &client,
                ADMIN_INSTALL_SUCCESSOR_PATH,
                &AdminInstallSuccessorRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: predecessor.stop.entry.config_id,
                    expected_stopped_anchor: LogAnchor::new(
                        predecessor.stop.entry.index,
                        predecessor.stop.entry.hash,
                    ),
                    old_members: predecessor.membership.members().to_vec(),
                    stop: predecessor.stop.clone(),
                    successor: AdminSuccessorBundle {
                        config_id: serve.bundle.config_id,
                        members: serve.bundle.membership.members().to_vec(),
                        digest: serve.bundle.membership.digest(),
                    },
                },
            )
            .await?;
            serde_json::to_string(&response).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            run_membership_install_successor_offline(*required_serve_config(config.serve)?)
        }
    }
}

fn run_membership_install_successor_offline(config: ServeConfig) -> Result<String, String> {
    config.node_config()?;
    let predecessor = config.bundle.require_predecessor()?;
    if predecessor.stop.entry.cluster_id != config.cluster_id
        || predecessor.stop.entry.epoch != config.epoch
        || predecessor.stop.entry.config_id.checked_add(1) != Some(config.bundle.config_id)
    {
        return Err(
            "predecessor certificate cluster/epoch/config does not match the successor bundle"
                .into(),
        );
    }
    let recorder = RecorderFileStore::new_with_membership(
        config.data_dir.join("recorder"),
        config.node_id.clone(),
        config.cluster_id,
        config.epoch,
        predecessor.stop.entry.config_id,
        predecessor.membership.clone(),
    )
    .map_err(|error| error.to_string())?;
    let state = install_successor_recorder(
        &recorder,
        config.bundle.config_id,
        config.bundle.membership.clone(),
        &predecessor.stop,
    )
    .map_err(|error| error.to_string())?;
    serde_json::to_string(&serde_json::json!({
        "config_id": state.config_id(),
        "config_digest": state.config_digest(),
        "activated": state.is_activated(),
        "predecessor_installed": state.predecessor().is_some(),
    }))
    .map_err(|error| error.to_string())
}

async fn run_membership_activate(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            let predecessor_config_id = serve.bundle.require_predecessor()?.stop.entry.config_id;
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(
                &status,
                &serve,
                &[predecessor_config_id, serve.bundle.config_id],
            )?;
            let response: AdminActivateResponse = admin_post(
                &client,
                ADMIN_ACTIVATE_PATH,
                &AdminActivateRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: serve.bundle.config_id,
                },
            )
            .await?;
            serde_json::to_string(&response).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            run_membership_activate_offline(*required_serve_config(config.serve)?)
        }
    }
}

fn run_membership_activate_offline(config: ServeConfig) -> Result<String, String> {
    config.bundle.require_predecessor()?;
    let runtime = open_offline_runtime(&config)?;
    let entry = runtime
        .activate_successor()
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&entry).map_err(|error| error.to_string())
}

fn required_serve_config(config: Option<Box<ServeConfig>>) -> Result<Box<ServeConfig>, String> {
    config.ok_or_else(|| "local serve configuration is required".into())
}

fn required_operation_id(operation_id: Option<String>) -> Result<String, String> {
    operation_id.ok_or_else(|| "admin operation id is required".into())
}

fn validate_live_admin_target(
    status: &AdminStatusResponse,
    serve: &ServeConfig,
    allowed_state_config_ids: &[u64],
) -> Result<(), String> {
    let expected_members = serve.bundle.membership.members();
    let actual_state_config_id = status.node.configuration_state.config_id();
    let mismatch = if status.cluster_id != serve.cluster_id {
        Some("cluster_id")
    } else if status.execution_profile != serve.execution_profile {
        Some("execution_profile")
    } else if status.epoch != serve.epoch {
        Some("epoch")
    } else if status.recovery_generation != serve.recovery_generation {
        Some("recovery_generation")
    } else if status.node.active_config_id != serve.bundle.config_id
        || !allowed_state_config_ids.contains(&actual_state_config_id)
    {
        Some("config_id")
    } else if status.members.as_slice() != expected_members
        || status.node.active_membership_digest != serve.bundle.membership.digest()
    {
        Some("membership")
    } else {
        None
    };
    match mismatch {
        Some(field) => Err(format!(
            "admin target fence mismatch for {field}; refusing mutating request"
        )),
        None => Ok(()),
    }
}

async fn admin_get<T: DeserializeOwned>(
    config: &AdminClientConfig,
    path: &str,
) -> Result<T, String> {
    let client = bounded_http_client(ADMIN_CONNECT_TIMEOUT, ADMIN_REQUEST_TIMEOUT)?;
    admin_get_with_client(config, path, &client).await
}

async fn admin_get_with_client<T: DeserializeOwned>(
    config: &AdminClientConfig,
    path: &str,
    client: &reqwest::Client,
) -> Result<T, String> {
    let response = client
        .get(admin_url(config, path))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(&config.token)
        .send()
        .await
        .map_err(admin_request_error)?;
    decode_admin_response(response).await
}

async fn admin_post<T: Serialize, R: DeserializeOwned>(
    config: &AdminClientConfig,
    path: &str,
    body: &T,
) -> Result<R, String> {
    let response = bounded_http_client(ADMIN_CONNECT_TIMEOUT, ADMIN_REQUEST_TIMEOUT)?
        .post(admin_url(config, path))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(&config.token)
        .json(body)
        .send()
        .await
        .map_err(admin_request_error)?;
    decode_admin_response(response).await
}

fn admin_url(config: &AdminClientConfig, path: &str) -> String {
    format!("{}{path}", config.url.trim_end_matches('/'))
}

fn admin_request_error(error: reqwest::Error) -> String {
    format!("admin request failed: {}", error.without_url())
}

fn bounded_http_client(
    connect_timeout: Duration,
    timeout: Duration,
) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(timeout)
        .build()
        .map_err(|error| format!("cannot build HTTP client: {error}"))
}

async fn decode_admin_response<T: DeserializeOwned>(response: Response) -> Result<T, String> {
    let status = response.status();
    if status.is_success() {
        return response
            .json::<T>()
            .await
            .map_err(|error| format!("invalid admin response: {error}"));
    }
    let code = response
        .json::<AdminErrorResponse>()
        .await
        .map(|error| format!("{:?}", error.code))
        .unwrap_or_else(|_| "unknown".into());
    Err(format!("admin request failed: status={status} code={code}"))
}

impl RollCheckpointConfig {
    fn archive_for_generation(&self, generation: u64) -> Result<ObjectArchiveStore, String> {
        let mut config = self.base.clone();
        config.recovery_generation = generation;
        config.archive()
    }
}

async fn initialize_empty_checkpoint(
    archive: &ObjectArchiveStore,
) -> Result<CheckpointTip, String> {
    let loaded = archive
        .initialize_checkpoint()
        .await
        .map_err(|error| error.to_string())?;
    if loaded.manifest().tip().index() != 0 || !loaded.manifest().segments().is_empty() {
        return Err("refusing to initialize a nonempty checkpoint".into());
    }
    Ok(*loaded.manifest().tip())
}

async fn roll_checkpoint(
    source: &ObjectArchiveStore,
    target: &ObjectArchiveStore,
) -> Result<(CheckpointTip, CheckpointTip), String> {
    let source_before = source
        .load_checkpoint()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "source checkpoint is not initialized".to_string())?;
    if source_before.manifest().base().snapshot().is_some() {
        let published = source
            .roll_recovery_generation(target)
            .await
            .map_err(|error| error.to_string())?;
        let source_tip = *source_before.manifest().tip();
        if published.manifest().tip() != &source_tip {
            return Err("target checkpoint does not exactly match source after roll".into());
        }
        return Ok((source_tip, *published.manifest().tip()));
    }
    let entries = source
        .restore_checkpoint()
        .await
        .map_err(|error| format!("source checkpoint verification failed: {error}"))?;
    let source_after = source
        .load_checkpoint()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "source checkpoint disappeared during roll".to_string())?;
    if source_before != source_after {
        return Err("source checkpoint changed during roll; old pods must be stopped".into());
    }
    target
        .initialize_checkpoint()
        .await
        .map_err(|error| error.to_string())?;
    let published = target
        .publish_committed(&entries)
        .await
        .map_err(|error| error.to_string())?;
    let target_entries = target
        .restore_checkpoint()
        .await
        .map_err(|error| format!("target checkpoint verification failed: {error}"))?;
    let source_tip = *source_after.manifest().tip();
    let target_tip = target_entries
        .last()
        .map(|entry| CheckpointTip::new(entry.index, entry.hash))
        .unwrap_or(source_tip);
    if target_entries != entries
        || target_tip != source_tip
        || *published.manifest().tip() != source_tip
    {
        return Err("target checkpoint does not exactly match source after roll".into());
    }
    Ok((source_tip, target_tip))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StartupPreparation {
    RecorderFirst,
    VerifyLocalCheckpoint {
        identity: CheckpointIdentity,
        root: LogAnchor,
    },
    RuntimeFirstWithPeerCatchup {
        checkpoint_index: u64,
    },
}

async fn prepare_remote_startup(
    mode: StartupMode,
    archive: &ObjectArchiveStore,
    data_dir: &Path,
    node_id: &str,
    execution_profile: ExecutionProfile,
) -> Result<StartupPreparation, String> {
    match mode {
        StartupMode::Bootstrap => {
            if !local_data_is_fresh(data_dir)? {
                return Err("bootstrap requires a fresh local data directory".into());
            }
            let loaded = archive
                .load_checkpoint()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "bootstrap requires an initialized empty checkpoint".to_string())?;
            if loaded.manifest().tip().index() != 0 || !loaded.manifest().segments().is_empty() {
                return Err("bootstrap requires an initialized empty checkpoint".into());
            }
            write_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                loaded.manifest().identity(),
            )?;
            Ok(StartupPreparation::RecorderFirst)
        }
        StartupMode::Rejoin if local_data_is_fresh(data_dir)? => {
            let tip =
                restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), data_dir, node_id)
                    .await
                    .map_err(|error| error.to_string())?;
            write_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                archive
                    .checkpoint_identity()
                    .map_err(|error| error.to_string())?,
            )?;
            if tip.index() == 0 {
                Ok(StartupPreparation::RecorderFirst)
            } else {
                Ok(StartupPreparation::RuntimeFirstWithPeerCatchup {
                    checkpoint_index: tip.index(),
                })
            }
        }
        StartupMode::Rejoin => {
            let loaded = archive
                .load_checkpoint()
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "rejoin requires an initialized checkpoint".to_string())?;
            read_and_validate_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                loaded.manifest().identity(),
            )?;
            Ok(StartupPreparation::VerifyLocalCheckpoint {
                identity: loaded.manifest().identity().clone(),
                root: LogAnchor::new(
                    loaded.manifest().tip().index(),
                    loaded.manifest().tip().hash(),
                ),
            })
        }
        StartupMode::Disaster => {
            if !local_data_is_fresh(data_dir)? {
                return Err("disaster startup requires a fresh local data directory".into());
            }
            restore_checkpoint_to_fresh_data_dir_for_node(archive.clone(), data_dir, node_id)
                .await
                .map_err(|error| error.to_string())?;
            write_local_checkpoint_identity_marker(
                data_dir,
                execution_profile,
                archive
                    .checkpoint_identity()
                    .map_err(|error| error.to_string())?,
            )?;
            Ok(StartupPreparation::RecorderFirst)
        }
    }
}

fn marker_from_identity(
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
) -> LocalCheckpointIdentityMarker {
    LocalCheckpointIdentityMarker {
        format_version: 1,
        cluster_id: identity.cluster_id().to_owned(),
        execution_profile,
        epoch: identity.epoch(),
        config_id: identity.config_id(),
        recovery_generation: identity.recovery_generation(),
    }
}

fn validate_local_checkpoint_identity_marker(
    marker: &LocalCheckpointIdentityMarker,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
) -> Result<(), String> {
    if marker.format_version != 1
        || marker.cluster_id != identity.cluster_id()
        || marker.execution_profile != execution_profile
        || marker.epoch != identity.epoch()
        || marker.config_id != identity.config_id()
        || marker.recovery_generation != identity.recovery_generation()
    {
        return Err(
            "local checkpoint identity marker does not exactly match the authoritative checkpoint"
                .into(),
        );
    }
    Ok(())
}

fn read_and_validate_local_checkpoint_identity_marker(
    data_dir: &Path,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
) -> Result<(), String> {
    let marker_path = data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE);
    let metadata = fs::symlink_metadata(&marker_path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            "nonfresh rejoin requires a local checkpoint identity marker".to_string()
        } else {
            format!("cannot inspect local checkpoint identity marker: {error}")
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("local checkpoint identity marker must be a regular file".into());
    }
    if metadata.len() == 0 || metadata.len() > MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES {
        return Err("local checkpoint identity marker has an invalid size".into());
    }
    let file = fs::File::open(&marker_path)
        .map_err(|error| format!("cannot open local checkpoint identity marker: {error}"))?;
    if !file
        .metadata()
        .map_err(|error| format!("cannot inspect open checkpoint identity marker: {error}"))?
        .is_file()
    {
        return Err("local checkpoint identity marker must remain a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read local checkpoint identity marker: {error}"))?;
    if bytes.len() as u64 > MAX_LOCAL_CHECKPOINT_IDENTITY_BYTES {
        return Err("local checkpoint identity marker has an invalid size".into());
    }
    let marker: LocalCheckpointIdentityMarker = serde_json::from_slice(&bytes)
        .map_err(|_| "local checkpoint identity marker is invalid".to_string())?;
    validate_local_checkpoint_identity_marker(&marker, execution_profile, identity)
}

fn write_local_checkpoint_identity_marker(
    data_dir: &Path,
    execution_profile: ExecutionProfile,
    identity: &CheckpointIdentity,
) -> Result<(), String> {
    fs::create_dir_all(data_dir)
        .map_err(|error| format!("cannot create local data directory: {error}"))?;
    let data_dir_metadata = fs::symlink_metadata(data_dir)
        .map_err(|error| format!("cannot inspect local data directory: {error}"))?;
    if data_dir_metadata.file_type().is_symlink() || !data_dir_metadata.is_dir() {
        return Err("local data directory must be a real directory".into());
    }

    let marker_path = data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE);
    if fs::symlink_metadata(&marker_path).is_ok() {
        return read_and_validate_local_checkpoint_identity_marker(
            data_dir,
            execution_profile,
            identity,
        );
    }
    let marker = marker_from_identity(execution_profile, identity);
    let bytes = serde_json::to_vec(&marker)
        .map_err(|error| format!("cannot encode local checkpoint identity marker: {error}"))?;
    let nonce = LOCAL_MARKER_NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let temporary = data_dir.join(format!(
        ".rhiza-checkpoint-identity.tmp-{}-{nonce}",
        std::process::id()
    ));
    let result = (|| -> Result<(), String> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|error| format!("cannot create checkpoint identity marker: {error}"))?;
        file.write_all(&bytes)
            .map_err(|error| format!("cannot write checkpoint identity marker: {error}"))?;
        file.sync_all()
            .map_err(|error| format!("cannot sync checkpoint identity marker: {error}"))?;
        match fs::hard_link(&temporary, &marker_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(format!(
                    "cannot atomically publish checkpoint identity marker: {error}"
                ))
            }
        }
        fs::remove_file(&temporary)
            .map_err(|error| format!("cannot remove checkpoint marker staging file: {error}"))?;
        fs::File::open(data_dir)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("cannot sync local data directory: {error}"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result?;
    read_and_validate_local_checkpoint_identity_marker(data_dir, execution_profile, identity)
}

static LOCAL_MARKER_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn verify_local_rejoin_checkpoint(
    runtime: &NodeRuntime,
    identity: &CheckpointIdentity,
    authoritative_root: LogAnchor,
) -> Result<(), String> {
    let config = runtime.config();
    let local_identity = CheckpointIdentity::new(
        config.cluster_id().to_owned(),
        config.epoch(),
        runtime.consensus().config_id(),
        config.recovery_generation(),
    );
    if &local_identity != identity {
        return Err(
            "nonfresh rejoin local qlog identity does not match the authoritative checkpoint"
                .into(),
        );
    }
    let expected_profile_prefix = format!("rhiza:{}:", config.execution_profile());
    if !identity.cluster_id().starts_with(&expected_profile_prefix) {
        return Err(
            "nonfresh rejoin execution profile does not match the checkpoint identity".into(),
        );
    }

    let state = runtime
        .log_store()
        .logical_state()
        .map_err(|error| error.to_string())?;
    let local_tip = state
        .tip
        .unwrap_or_else(|| LogAnchor::new(0, rhiza_core::LogHash::ZERO));
    if local_tip.index() < authoritative_root.index() {
        return Err(format!(
            "nonfresh rejoin local qlog tip {} is behind authoritative checkpoint {}",
            local_tip.index(),
            authoritative_root.index(),
        ));
    }
    if authoritative_root.index() == 0 {
        if authoritative_root.hash() != rhiza_core::LogHash::ZERO {
            return Err("authoritative checkpoint genesis hash is not zero".into());
        }
        return Ok(());
    }
    let local_hash = if state
        .anchor
        .as_ref()
        .is_some_and(|anchor| anchor.compacted().index() == authoritative_root.index())
    {
        state
            .anchor
            .as_ref()
            .map(|anchor| anchor.compacted().hash())
    } else if state
        .anchor
        .as_ref()
        .is_some_and(|anchor| anchor.compacted().index() > authoritative_root.index())
    {
        return Err(
            "nonfresh rejoin local qlog compacted past the authoritative checkpoint without exact inclusion evidence"
                .into(),
        );
    } else {
        runtime
            .log_store()
            .read(authoritative_root.index())
            .map_err(|error| error.to_string())?
            .map(|entry| entry.hash)
    };
    if local_hash != Some(authoritative_root.hash()) {
        return Err(format!(
            "nonfresh rejoin local qlog hash at index {} does not match the authoritative checkpoint",
            authoritative_root.index(),
        ));
    }
    Ok(())
}

fn local_data_is_fresh(data_dir: &Path) -> Result<bool, String> {
    for path in [
        data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE),
        data_dir.join("consensus/log"),
        data_dir.join("sqlite"),
        data_dir.join("ladybug"),
        data_dir.join("kv"),
        data_dir.join("recorder"),
        data_dir.join("consensus/recorder"),
    ] {
        if path_has_state(&path).map_err(|error| error.to_string())? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn path_has_state(path: &Path) -> Result<bool, std::io::Error> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Ok(true);
    }
    fs::read_dir(path)?
        .next()
        .transpose()
        .map(|entry| entry.is_some())
}

async fn request_write(args: &WriteArgs) -> Result<WriteResponse, String> {
    let request = WriteRequest {
        request_id: args.request_id.clone(),
        key: args.key.clone(),
        value: args.value.clone(),
    };
    client_json_request(&args.urls, &args.token, WRITE_PATH, &request, true).await
}

async fn request_read(args: &ReadArgs) -> Result<ReadResponse, String> {
    let request = ReadRequest {
        key: args.key.clone(),
        consistency: args.consistency,
    };
    client_json_request(
        &args.urls,
        &args.token,
        READ_PATH,
        &request,
        read_can_hedge(request.consistency),
    )
    .await
}

async fn request_sql_execute(args: &SqlExecuteArgs) -> Result<SqlExecuteResponse, String> {
    let request = SqlExecuteRequest {
        request_id: args.request_id.clone(),
        statements: vec![args.statement.clone()],
    };
    client_json_request(&args.urls, &args.token, SQL_EXECUTE_PATH, &request, true).await
}

async fn request_sql_query(args: &SqlQueryArgs) -> Result<SqlQueryResponse, String> {
    let request = SqlQueryRequest {
        statement: args.statement.clone(),
        consistency: args.consistency,
        max_rows: args.max_rows,
    };
    client_json_request(
        &args.urls,
        &args.token,
        SQL_QUERY_PATH,
        &request,
        read_can_hedge(request.consistency),
    )
    .await
}

const CLIENT_HEDGE_DELAY: Duration = Duration::from_millis(100);
const CLIENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CLIENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const CLIENT_OPERATION_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Copy)]
struct ClientPolicy {
    connect_timeout: Duration,
    attempt_timeout: Duration,
    operation_timeout: Duration,
    hedge_delay: Duration,
}

impl Default for ClientPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: CLIENT_CONNECT_TIMEOUT,
            attempt_timeout: CLIENT_ATTEMPT_TIMEOUT,
            operation_timeout: CLIENT_OPERATION_TIMEOUT,
            hedge_delay: CLIENT_HEDGE_DELAY,
        }
    }
}

#[cfg(test)]
impl ClientPolicy {
    fn test(hedge_delay: Duration, operation_timeout: Duration) -> Self {
        Self {
            connect_timeout: Duration::from_millis(20),
            attempt_timeout: operation_timeout,
            operation_timeout,
            hedge_delay,
        }
    }
}

fn read_can_hedge(consistency: Option<ReadConsistency>) -> bool {
    matches!(
        consistency,
        Some(ReadConsistency::Local | ReadConsistency::AppliedIndex(_))
    )
}

enum ClientAttemptError {
    Retryable(String),
    Fatal(String),
}

async fn client_json_request<B, T>(
    urls: &[String],
    token: &str,
    path: &str,
    body: &B,
    hedge: bool,
) -> Result<T, String>
where
    B: Serialize,
    T: DeserializeOwned + Send + 'static,
{
    client_json_request_with_policy(urls, token, path, body, hedge, ClientPolicy::default()).await
}

async fn client_json_request_with_policy<B, T>(
    urls: &[String],
    token: &str,
    path: &str,
    body: &B,
    hedge: bool,
    policy: ClientPolicy,
) -> Result<T, String>
where
    B: Serialize,
    T: DeserializeOwned + Send + 'static,
{
    let body = serde_json::to_vec(body).map_err(|_| "cannot encode request".to_string())?;
    let client = reqwest::Client::builder()
        .connect_timeout(policy.connect_timeout)
        .build()
        .map_err(|error| format!("cannot build HTTP client: {error}"))?;
    let mut attempts = tokio::task::JoinSet::new();
    let mut next = 0;
    let mut last_error = None;

    if let Some(url) = urls.get(next) {
        spawn_client_attempt(
            &mut attempts,
            &client,
            url,
            token,
            path,
            &body,
            policy.attempt_timeout,
        );
        next += 1;
    }

    let hedge_delay = tokio::time::sleep(policy.hedge_delay);
    let operation_deadline = tokio::time::sleep(policy.operation_timeout);
    tokio::pin!(hedge_delay, operation_deadline);

    loop {
        if attempts.is_empty() && next == urls.len() {
            return Err(last_error.unwrap_or_else(|| "missing request endpoint".into()));
        }

        tokio::select! {
            result = attempts.join_next(), if !attempts.is_empty() => {
                match result.expect("a nonempty attempt set must yield a result") {
                    Ok(Ok(response)) => {
                        attempts.abort_all();
                        return Ok(response);
                    }
                    Ok(Err(ClientAttemptError::Fatal(error))) => {
                        attempts.abort_all();
                        return Err(error);
                    }
                    Ok(Err(ClientAttemptError::Retryable(error))) => {
                        last_error = Some(error);
                        if let Some(url) = urls.get(next) {
                            spawn_client_attempt(
                                &mut attempts,
                                &client,
                                url,
                                token,
                                path,
                                &body,
                                policy.attempt_timeout,
                            );
                            next += 1;
                            hedge_delay.as_mut().reset(tokio::time::Instant::now() + policy.hedge_delay);
                        }
                    }
                    Err(error) => {
                        attempts.abort_all();
                        return Err(format!("request task failed: {error}"));
                    }
                }
            }
            () = &mut hedge_delay, if hedge && next < urls.len() => {
                spawn_client_attempt(
                    &mut attempts,
                    &client,
                    &urls[next],
                    token,
                    path,
                    &body,
                    policy.attempt_timeout,
                );
                next += 1;
                hedge_delay.as_mut().reset(tokio::time::Instant::now() + policy.hedge_delay);
            }
            () = &mut operation_deadline => {
                attempts.abort_all();
                return Err(last_error.unwrap_or_else(|| {
                    "request failed: operation deadline exceeded".into()
                }));
            }
        }
    }
}

fn spawn_client_attempt<T>(
    attempts: &mut tokio::task::JoinSet<Result<T, ClientAttemptError>>,
    client: &reqwest::Client,
    url: &str,
    token: &str,
    path: &str,
    body: &[u8],
    attempt_timeout: Duration,
) where
    T: DeserializeOwned + Send + 'static,
{
    let client = client.clone();
    let url = url.to_string();
    let token = token.to_string();
    let path = path.to_string();
    let body = body.to_vec();
    attempts.spawn(async move {
        tokio::time::timeout(attempt_timeout, async {
            let response = protocol_request(&client, Method::POST, &url, &path)
                .bearer_auth(token)
                .body(body)
                .send()
                .await
                .map_err(|error| ClientAttemptError::Retryable(request_error(error)))?;
            client_attempt_response(response).await
        })
        .await
        .unwrap_or_else(|_| {
            Err(ClientAttemptError::Retryable(
                "request failed: attempt deadline exceeded".into(),
            ))
        })
    });
}

async fn client_attempt_response<T: DeserializeOwned>(
    response: Response,
) -> Result<T, ClientAttemptError> {
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|error| ClientAttemptError::Retryable(request_error(error)))?;
    if status.is_success() {
        return serde_json::from_slice(&body)
            .map_err(|_| ClientAttemptError::Fatal("invalid JSON response".into()));
    }
    let Ok(error) = serde_json::from_slice::<ServerErrorResponse>(&body) else {
        return Err(ClientAttemptError::Fatal(format!("HTTP {status}")));
    };
    let retryable = error.retryable;
    let detail = server_error_detail(status, error);
    if retryable {
        Err(ClientAttemptError::Retryable(detail))
    } else {
        Err(ClientAttemptError::Fatal(detail))
    }
}

async fn request_health(args: &HealthArgs) -> Result<(), String> {
    let client = bounded_http_client(HEALTH_CONNECT_TIMEOUT, HEALTH_REQUEST_TIMEOUT)?;
    request_health_with_client(args, &client).await
}

async fn request_health_with_client(
    args: &HealthArgs,
    client: &reqwest::Client,
) -> Result<(), String> {
    let path = if args.ready { READYZ_PATH } else { LIVEZ_PATH };
    let response = protocol_request(client, Method::GET, &args.url, path)
        .send()
        .await
        .map_err(request_error)?;
    success_response(response).await.map(|_| ())
}

fn protocol_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    path: &str,
) -> RequestBuilder {
    client
        .request(method, endpoint(url, path))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(header::ACCEPT, "application/json")
        .header(header::CONTENT_TYPE, "application/json")
}

fn endpoint(url: &str, path: &str) -> String {
    format!("{}{path}", url.trim_end_matches('/'))
}

fn request_error(error: reqwest::Error) -> String {
    format!("request failed: {}", error.without_url())
}

#[derive(Deserialize)]
struct ServerErrorResponse {
    code: String,
    #[serde(default)]
    retryable: bool,
    #[serde(default)]
    message: Option<String>,
}

async fn success_response(response: Response) -> Result<Response, String> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let Ok(body) = response.bytes().await else {
        return Err(format!("HTTP {status}"));
    };
    let Ok(error) = serde_json::from_slice::<ServerErrorResponse>(&body) else {
        return Err(format!("HTTP {status}"));
    };
    Err(server_error_detail(status, error))
}

fn server_error_detail(status: reqwest::StatusCode, error: ServerErrorResponse) -> String {
    let mut detail = format!("HTTP {status} code={}", error.code);
    if let Some(message) = error.message.filter(|message| !message.is_empty()) {
        detail.push_str(" message=");
        detail.push_str(&message);
    }
    detail
}

fn finish_read(args: &ReadArgs, response: ReadResponse) -> i32 {
    if let Some(expected) = &args.expect {
        if response.value.as_deref() != Some(expected) {
            let actual = response.value.as_deref().unwrap_or("<missing>");
            eprintln!("read failed: expected {expected:?}, got {actual:?}");
            return 1;
        }
    }
    let value = response
        .value
        .as_ref()
        .map(|value| serde_json::to_string(value).expect("string serialization cannot fail"))
        .unwrap_or_else(|| "null".into());
    println!(
        "value={value} applied_index={} hash={}",
        response.applied_index,
        response.hash.to_hex()
    );
    0
}

fn fail(context: &str, error: impl std::fmt::Display) -> i32 {
    eprintln!("{context} failed: {error}");
    1
}

fn parse_object_store(value: &str) -> Result<ObjStoreConfig, String> {
    parse_object_store_with_lookup(value, true, &mut |name| env::var(name).ok())
}

fn parse_object_store_with_lookup(
    value: &str,
    allow_local: bool,
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<ObjStoreConfig, String> {
    if let Some(root) = value.strip_prefix("local:") {
        if !allow_local {
            return Err("local:<path> object stores are only supported by e2e".into());
        }
        if root.is_empty() {
            return Err("local object store path must not be empty".into());
        }
        return Ok(ObjStoreConfig::Local {
            root: PathBuf::from(root),
        });
    }
    match value {
        "s3" => {
            let endpoint = optional_env(lookup, "RHIZA_S3_ENDPOINT")?;
            let access_key = optional_env(lookup, "RHIZA_S3_ACCESS_KEY")?;
            let secret_key = optional_env(lookup, "RHIZA_S3_SECRET_KEY")?;
            if access_key.is_some() != secret_key.is_some() {
                return Err(
                    "RHIZA_S3_ACCESS_KEY and RHIZA_S3_SECRET_KEY must be set together".into(),
                );
            }
            Ok(ObjStoreConfig::S3 {
                endpoint,
                bucket: required_env(lookup, "RHIZA_S3_BUCKET")?,
                access_key,
                secret_key,
                region: lookup("RHIZA_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
                allow_http: parse_optional_bool(lookup, "RHIZA_S3_ALLOW_HTTP")?.unwrap_or(false),
            })
        }
        "gcs" => {
            let service_account_path = optional_env(lookup, "RHIZA_GCS_SERVICE_ACCOUNT_PATH")?;
            let service_account_key = optional_env(lookup, "RHIZA_GCS_SERVICE_ACCOUNT_KEY")?;
            if service_account_path.is_some() && service_account_key.is_some() {
                return Err(
                    "RHIZA_GCS_SERVICE_ACCOUNT_PATH and RHIZA_GCS_SERVICE_ACCOUNT_KEY are mutually exclusive"
                        .into(),
                );
            }
            Ok(ObjStoreConfig::Gcs {
                bucket: required_env(lookup, "RHIZA_GCS_BUCKET")?,
                service_account_path,
                service_account_key,
            })
        }
        "azure" => Ok(ObjStoreConfig::AzureBlob {
            account: required_env(lookup, "RHIZA_AZURE_ACCOUNT")?,
            container: required_env(lookup, "RHIZA_AZURE_CONTAINER")?,
            access_key: optional_env(lookup, "RHIZA_AZURE_ACCESS_KEY")?,
        }),
        _ => Err("RHIZA_OBJECT_STORE must be s3|gcs|azure (local:<path> is e2e-only)".into()),
    }
}

fn parse_optional_bool(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<bool>, String> {
    match lookup(name).as_deref() {
        None => Ok(None),
        Some("true" | "1") => Ok(Some(true)),
        Some("false" | "0") => Ok(Some(false)),
        Some(_) => Err(format!("{name} must be true|false|1|0")),
    }
}

const USAGE: &str = "usage:\n  rhiza status --url <url>\n  rhiza e2e [options]\n  rhiza serve\n  rhiza validate-config-bundle [--stdin]\n  rhiza init-checkpoint\n  rhiza roll-checkpoint [--from-generation N --to-generation N+1]\n  rhiza checkpoint inspect\n  rhiza checkpoint compact\n  rhiza gc plan --operation-id <id> [--retain-generations N --grace-ms N --min-age-ms N]\n  rhiza gc inspect|evidence --plan-hash <sha256>\n  rhiza gc apply --plan-hash <sha256> --confirm\n  rhiza membership status|stop|install-successor|activate [--offline]\n  rhiza write --url <preferred> [--url <fallback> ...] [--token <token>] --request-id <id> --key <key> --value <value>\n  rhiza read --url <preferred> [--url <fallback> ...] [--token <token>] --key <key> [--consistency local|read_barrier] [--expect <value>]\n  rhiza sql execute --url <preferred> [--url <fallback> ...] [--token <token>] --request-id <id> --sql <sql> [--params-json <json>]\n  rhiza sql query --url <preferred> [--url <fallback> ...] [--token <token>] --sql <sql> [--params-json <json>] [--consistency local|read_barrier] [--max-rows N]\n  rhiza health --url <url> [--ready]\n\nServe, checkpoint, recovery, GC, and offline membership commands require RHIZA_EXECUTION_PROFILE=sql|graph|kv and RHIZA_CONFIG_BUNDLE or RHIZA_CONFIG_BUNDLE_FILE. Repeat --url in preferred order. Idempotent operations hedge later endpoints after 100 ms; read_barrier operations retry sequentially. Every attempt reuses the exact request body, including write request IDs and read consistency. Client requests use a 2 s connect deadline, 5 s per-attempt deadline, and 15 s total operation deadline. Membership and checkpoint compact commands use the live admin API by default; pass --offline only as an explicit local fallback while the data root is not serving. gc plan is dry-run only; deletion requires gc apply with the exact plan hash and --confirm. roll-checkpoint performs explicit full-cluster disaster-recovery fencing; stop all old-generation pods before running it.";

fn usage() {
    eprintln!("{USAGE}");
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Arc, Condvar, Mutex,
        },
    };

    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
        Json, Router,
    };
    use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
    use rhiza_core::{
        ConfigChange, EntryType, LogAnchor, LogEntry, LogHash, RecoveryAnchor, SnapshotIdentity,
    };
    use rhiza_node::{
        NodeStatus, ReadRequest, RuntimeConfigurationStatus, WriteRequest, PROTOCOL_VERSION,
        VERSION_HEADER,
    };
    use rhiza_obj_store::{ObjStore, ObjStoreConfig};
    use rhiza_quepaxa::{AcceptedValue, RecordRequest, RecordSummary, RecorderReply};

    use super::*;

    #[test]
    fn e2e_parser_rejects_the_removed_verify_restore_alias() {
        assert!(matches!(parse_command(["e2e"]), Ok(Command::E2e(_))));
        assert_eq!(
            parse_command(["verify-restore"])
                .err()
                .expect("removed alias must be rejected"),
            "unknown command: verify-restore"
        );
        assert!(USAGE.contains("rhiza e2e [options]"));
        assert!(!USAGE.contains("verify-restore"));
    }

    #[test]
    fn write_parser_builds_request_when_all_flags_are_present() {
        let command = parse_command([
            "write",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--request-id",
            "request-1",
            "--key",
            "alpha",
            "--value",
            "one",
        ])
        .unwrap();

        let Command::Write(args) = command else {
            panic!("expected write command");
        };
        assert_eq!(args.urls, ["http://127.0.0.1:8080"]);
        assert_eq!(args.token, "client-secret");
        assert_eq!(args.request_id, "request-1");
        assert_eq!(args.key, "alpha");
        assert_eq!(args.value, "one");
    }

    #[test]
    fn client_parsers_preserve_ordered_repeated_endpoints() {
        let command = parse_command([
            "write",
            "--url",
            "http://preferred:8080",
            "--url",
            "http://fallback:8080",
            "--token",
            "client-secret",
            "--request-id",
            "request-1",
            "--key",
            "alpha",
            "--value",
            "one",
        ])
        .unwrap();

        let Command::Write(args) = command else {
            panic!("expected write command");
        };
        assert_eq!(args.urls, ["http://preferred:8080", "http://fallback:8080"]);

        let command = parse_command([
            "sql",
            "query",
            "--url",
            "http://preferred:8080",
            "--url",
            "http://fallback:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT 1",
            "--consistency",
            "read_barrier",
        ])
        .unwrap();

        let Command::SqlQuery(args) = command else {
            panic!("expected sql query command");
        };
        assert_eq!(args.urls, ["http://preferred:8080", "http://fallback:8080"]);
        assert_eq!(args.consistency, Some(ReadConsistency::ReadBarrier));
    }

    #[test]
    fn write_uses_environment_client_token_when_flag_is_absent() {
        let args = parse_write_with_lookup(
            [
                "--url",
                "http://127.0.0.1:8080",
                "--request-id",
                "request-1",
                "--key",
                "alpha",
                "--value",
                "one",
            ]
            .map(String::from),
            |name| (name == "RHIZA_CLIENT_TOKEN").then(|| "environment-secret".into()),
        )
        .unwrap();

        assert_eq!(args.token, "environment-secret");
    }

    #[test]
    fn read_uses_environment_client_token_when_flag_is_absent() {
        let args = parse_read_with_lookup(
            ["--url", "http://127.0.0.1:8080", "--key", "alpha"].map(String::from),
            |name| (name == "RHIZA_CLIENT_TOKEN").then(|| "environment-secret".into()),
        )
        .unwrap();

        assert_eq!(args.token, "environment-secret");
    }

    #[test]
    fn read_parser_rejects_unknown_consistency() {
        let error = parse_command([
            "read",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--key",
            "alpha",
            "--consistency",
            "eventual",
        ])
        .err()
        .expect("invalid consistency should fail");

        assert_eq!(error, "consistency must be `local` or `read_barrier`");
    }

    #[test]
    fn read_parsers_accept_only_canonical_read_barrier_consistency() {
        let consistency = "read_barrier";
        let read = parse_command([
            "read",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--key",
            "alpha",
            "--consistency",
            consistency,
        ])
        .unwrap();
        let Command::Read(read) = read else {
            panic!("expected read command");
        };
        assert_eq!(read.consistency, Some(ReadConsistency::ReadBarrier));

        let query = parse_command([
            "sql",
            "query",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT 1",
            "--consistency",
            consistency,
        ])
        .unwrap();
        let Command::SqlQuery(query) = query else {
            panic!("expected sql query command");
        };
        assert_eq!(query.consistency, Some(ReadConsistency::ReadBarrier));
        assert!(parse_read_consistency("barrier").is_err());
    }

    #[test]
    fn sql_parsers_preserve_typed_parameters_and_query_controls() {
        let execute = parse_command([
            "sql",
            "execute",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--request-id",
            "sql-1",
            "--sql",
            "INSERT INTO users(id, name) VALUES (?1, ?2)",
            "--params-json",
            r#"[{"type":"integer","value":1},{"type":"text","value":"Ada"}]"#,
        ])
        .unwrap();
        let Command::SqlExecute(execute) = execute else {
            panic!("expected sql execute command");
        };
        assert_eq!(execute.request_id, "sql-1");
        assert_eq!(
            execute.statement.parameters,
            [SqlValue::Integer(1), SqlValue::Text("Ada".into())]
        );

        let query = parse_command([
            "sql",
            "query",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT name FROM users",
            "--consistency",
            "read_barrier",
            "--max-rows",
            "25",
        ])
        .unwrap();
        let Command::SqlQuery(query) = query else {
            panic!("expected sql query command");
        };
        assert_eq!(query.consistency, Some(ReadConsistency::ReadBarrier));
        assert_eq!(query.max_rows, Some(25));
    }

    #[test]
    fn health_parser_selects_readiness_endpoint() {
        let command =
            parse_command(["health", "--url", "http://127.0.0.1:8080/", "--ready"]).unwrap();

        let Command::Health(args) = command else {
            panic!("expected health command");
        };
        assert_eq!(args.url, "http://127.0.0.1:8080/");
        assert!(args.ready);
    }

    #[test]
    fn status_parser_requires_url_and_selects_readiness_endpoint() {
        let error = parse_command(["status"])
            .err()
            .expect("status without a URL should fail");
        assert_eq!(error, "missing required argument: --url");

        let command = parse_command(["status", "--url", "http://127.0.0.1:8080/"]).unwrap();
        let Command::Status(args) = command else {
            panic!("expected status command");
        };
        assert_eq!(args.url, "http://127.0.0.1:8080/");
        assert!(args.ready);
    }

    #[tokio::test]
    async fn status_fails_when_readiness_endpoint_is_not_ready() {
        let app = Router::new().route(
            READYZ_PATH,
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let exit_code = run(["status", "--url", &format!("http://{address}")]
            .map(String::from)
            .into_iter())
        .await;

        server.abort();
        assert_eq!(exit_code, 1);
    }

    #[test]
    fn serve_config_uses_default_listeners_and_local_peer_token() {
        let values = HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", "sql"),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_NODE_ID", "node-2"),
            ("RHIZA_DATA_DIR", "/tmp/node-2"),
            ("RHIZA_EPOCH", "1"),
            ("RHIZA_CONFIG_BUNDLE", base_bundle_json()),
            ("RHIZA_CLIENT_TOKEN", "client-secret"),
        ]);

        let config =
            ServeConfig::from_lookup(|name| values.get(name).map(ToString::to_string)).unwrap();

        assert_eq!(config.client_listen, "0.0.0.0:8080");
        assert_eq!(config.execution_profile, ExecutionProfile::Sqlite);
        assert_eq!(config.cluster_id, "rhiza:sql:cluster-a");
        assert_eq!(config.recorder_listen, "0.0.0.0:8081");
        assert_eq!(config.local_peer_token().unwrap(), "peer-2-secret");
        assert_eq!(config.bundle.peers[0].base_url(), "http://node-1:8081");
        assert_eq!(config.bundle.peers[0].log_base_url(), "http://node-1:8080");
        assert_eq!(config.bundle.peers[1].log_base_url(), "http://node-2:8081");
        assert_eq!(config.recovery_generation, 1);
        assert!(config.remote.is_none());
        assert_eq!(config.recorder_transport, RecorderTransport::Http);
        assert!(config.recorder_tcp.is_none());
    }

    #[test]
    fn tcp_postcard_transport_requires_listener_and_all_peer_addresses_without_tls() {
        let mut values = base_serve_env();
        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-postcard");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("RHIZA_RECORDER_TCP_LISTEN"), "{error}");
        assert!(!error.contains("peer-1-secret"), "{error}");

        values.insert("RHIZA_RECORDER_TCP_LISTEN", "0.0.0.0:8082");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("recorder_tcp_addr"), "{error}");

        let bundle = serde_json::json!({
            "version": 1,
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "log_url":"http://node-1:8080", "recorder_tcp_addr":"node-1:8082", "token":"peer-1-secret"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "token":"peer-2-secret"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "token":"peer-3-secret"}
            ]
        })
        .to_string();
        values.insert("RHIZA_CONFIG_BUNDLE", &bundle);
        let config = parse_serve_env(&values).unwrap();
        assert_eq!(config.recorder_transport, RecorderTransport::TcpPostcard);
        assert_eq!(config.recorder_tcp.as_ref().unwrap().listen, "0.0.0.0:8082");

        values.insert("RHIZA_RECORDER_TLS_CA_FILE", "/removed/ca.pem");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("no longer supported"), "{error}");
        values.remove("RHIZA_RECORDER_TLS_CA_FILE");

        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-tls-postcard");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("http|tcp-postcard"), "{error}");
    }

    #[test]
    fn configuration_bundle_rejects_removed_tls_server_name() {
        let json = serde_json::json!({
            "version": 1,
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "recorder_tcp_addr":"node-1:8082", "recorder_tls_server_name":"node-1", "token":"t1"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "token":"t2"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "token":"t3"}
            ]
        });

        let error = parse_configuration_bundle(&json.to_string()).unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn configuration_bundle_canonicalizes_three_through_seven_members() {
        for count in 3..=7 {
            let members = (1..=count)
                .rev()
                .map(|index| {
                    serde_json::json!({
                        "node_id": format!("node-{index}"),
                        "url": format!("http://node-{index}:8081"),
                        "token": format!("peer-{index}-secret"),
                    })
                })
                .collect::<Vec<_>>();
            let bundle = parse_configuration_bundle(
                &serde_json::json!({
                    "version": 1,
                    "config_id": 7,
                    "members": members,
                })
                .to_string(),
            )
            .unwrap();

            assert_eq!(bundle.peers.len(), count);
            assert_eq!(bundle.membership.members()[0], "node-1");
            assert_eq!(
                bundle.membership.members()[count - 1],
                format!("node-{count}")
            );
            assert_eq!(bundle.configuration_state.config_id(), 7);
            assert_eq!(
                bundle.configuration_state.digest(),
                bundle.membership.digest()
            );
        }
    }

    #[test]
    fn configuration_bundle_rejects_bad_versions_counts_and_duplicate_members() {
        for json in [
            serde_json::json!({"version": 2, "config_id": 1, "members": []}),
            serde_json::json!({
                "version": 1,
                "config_id": 1,
                "members": [
                    {"node_id": "n1", "url": "http://n1", "token": "t1"},
                    {"node_id": "n2", "url": "http://n2", "token": "t2"},
                ],
            }),
            serde_json::json!({
                "version": 1,
                "config_id": 1,
                "members": [
                    {"node_id": "n1", "url": "http://n1", "token": "t1"},
                    {"node_id": "n1", "url": "http://n1-copy", "token": "t2"},
                    {"node_id": "n3", "url": "http://n3", "token": "t3"},
                ],
            }),
        ] {
            assert!(parse_configuration_bundle(&json.to_string()).is_err());
        }
    }

    #[test]
    fn bundle_source_is_exclusive_and_never_exposes_tokens() {
        let token = "bundle-token-that-must-stay-private";
        let json = serde_json::json!({
            "version": 1,
            "config_id": 7,
            "members": [
                {"node_id": "n1", "url": "http://n1", "token": token},
                {"node_id": "n2", "url": "http://n2", "token": "t2"},
                {"node_id": "n3", "url": "http://n3", "token": "t3"},
            ],
        })
        .to_string();
        let values = HashMap::from([
            ("RHIZA_CONFIG_BUNDLE", json.as_str()),
            ("RHIZA_CONFIG_BUNDLE_FILE", "/tmp/config.json"),
        ]);
        let error = load_configuration_bundle(
            |name| values.get(name).map(ToString::to_string),
            |_| unreachable!("exclusive sources fail before file access"),
        )
        .unwrap_err();
        assert!(!error.contains(token));
        assert!(error.contains("mutually exclusive"));

        let parsed = parse_configuration_bundle(&json).unwrap();
        assert!(!format!("{parsed:?}").contains(token));
    }

    #[test]
    fn configuration_bundle_loads_from_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("configuration.json");
        std::fs::write(
            &path,
            serde_json::json!({
                "version": 1,
                "config_id": 9,
                "members": [
                    {"node_id": "n3", "url": "http://n3", "token": "t3"},
                    {"node_id": "n1", "url": "http://n1", "token": "t1"},
                    {"node_id": "n2", "url": "http://n2", "token": "t2"},
                ],
            })
            .to_string(),
        )
        .unwrap();
        let path = path.to_string_lossy().into_owned();

        let bundle = load_configuration_bundle(
            |name| (name == "RHIZA_CONFIG_BUNDLE_FILE").then(|| path.clone()),
            |path| std::fs::read_to_string(path),
        )
        .unwrap();

        assert_eq!(bundle.config_id, 9);
        assert_eq!(bundle.membership.members(), ["n1", "n2", "n3"]);
    }

    #[test]
    fn removed_legacy_configuration_environment_is_rejected() {
        for (name, value) in [
            ("RHIZA_CONFIG_ID", "7"),
            ("RHIZA_PEER_1_ID", "node-1"),
            ("RHIZA_PEER_2_URL", "http://node-2:8081"),
        ] {
            let values = HashMap::from([(name, value)]);
            let error = load_configuration_bundle(
                |key| values.get(key).map(ToString::to_string),
                |_| unreachable!("legacy variables fail before file access"),
            )
            .unwrap_err();
            assert!(error.contains("unsupported"));
        }
    }

    #[test]
    fn serve_rejects_a_local_node_outside_the_bundle() {
        let mut values = base_serve_env();
        values.insert("RHIZA_NODE_ID", "node-9");

        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("local node_id"));
    }

    #[test]
    fn checkpoint_identity_uses_bundle_config_id() {
        let json = serde_json::json!({
            "version": 1,
            "config_id": 11,
            "members": [
                {"node_id": "n1", "url": "http://n1", "token": "t1"},
                {"node_id": "n2", "url": "http://n2", "token": "t2"},
                {"node_id": "n3", "url": "http://n3", "token": "t3"},
            ],
        })
        .to_string();

        let config_id =
            configuration_id(&mut |name| (name == "RHIZA_CONFIG_BUNDLE").then(|| json.clone()))
                .unwrap();

        assert_eq!(config_id, 11);
    }

    #[test]
    fn checkpoint_identity_uses_the_required_profile_and_canonical_cluster_id() {
        let values = HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", "graph"),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_EPOCH", "2"),
            ("RHIZA_CONFIG_BUNDLE", checkpoint_bundle_json()),
            ("RHIZA_RECOVERY_GENERATION", "3"),
            ("RHIZA_OBJECT_STORE", "s3"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
        ]);

        let config =
            CheckpointCommandConfig::from_lookup(|name| values.get(name).map(ToString::to_string))
                .unwrap();

        assert_eq!(config.identity().cluster_id(), "rhiza:graph:cluster-a");

        let error = match CheckpointCommandConfig::from_lookup(|name| {
            (name != "RHIZA_EXECUTION_PROFILE")
                .then(|| values.get(name))
                .flatten()
                .map(ToString::to_string)
        }) {
            Err(error) => error,
            Ok(_) => panic!("missing execution profile must be rejected"),
        };
        assert_eq!(error, "RHIZA_EXECUTION_PROFILE is required");
    }

    #[test]
    fn gc_apply_requires_an_exact_hash_and_explicit_confirmation() {
        assert!(parse_gc_apply_flags(["--plan-hash", "abc"].map(String::from)).is_err());
        assert!(parse_gc_apply_flags(["--confirm"].map(String::from)).is_err());
        assert!(parse_gc_apply_flags(
            ["--plan-hash", &"a".repeat(64), "--confirm"].map(String::from)
        )
        .is_ok());
        assert!(parse_gc_apply_flags(
            ["--plan-hash", &"z".repeat(64), "--confirm"].map(String::from)
        )
        .is_err());
    }

    #[test]
    fn membership_install_requires_predecessor_transition_material() {
        let json = serde_json::json!({
            "version": 1,
            "config_id": 2,
            "members": [
                {"node_id": "n1", "url": "http://n1", "token": "t1"},
                {"node_id": "n2", "url": "http://n2", "token": "t2"},
                {"node_id": "n4", "url": "http://n4", "token": "t4"},
            ],
        });
        let bundle = parse_configuration_bundle(&json.to_string()).unwrap();
        assert!(bundle.predecessor.is_none());
        assert!(bundle.require_predecessor().is_err());
    }

    fn exact_predecessor_bundle() -> serde_json::Value {
        predecessor_bundle_bound_to(["node-1", "node-2", "node-3"])
    }

    fn predecessor_bundle_bound_to<const N: usize>(
        successor_members: [&str; N],
    ) -> serde_json::Value {
        let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let command = ConfigChange::bound_stop(
            "rhiza-vind",
            3,
            predecessor.digest(),
            4,
            successor_members.into_iter().map(String::from).collect(),
        )
        .unwrap()
        .to_stored_command();
        let entry = LogEntry {
            cluster_id: "rhiza-vind".into(),
            epoch: 1,
            config_id: 3,
            index: 10,
            entry_type: command.entry_type,
            payload: command.payload.clone(),
            prev_hash: LogHash::ZERO,
            hash: LogEntry::calculate_hash(
                "rhiza-vind",
                10,
                1,
                3,
                command.entry_type,
                LogHash::ZERO,
                &command.payload,
            ),
        };
        let proposal = rhiza_quepaxa::Proposal::new(
            rhiza_quepaxa::ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("rhiza-vind", 10, 1, 3, LogHash::ZERO, &command),
        );
        let proof = DecisionProof::Phase2 {
            cluster_id: "rhiza-vind".into(),
            slot: 10,
            epoch: 1,
            config_id: 3,
            config_digest: predecessor.digest(),
            step: 6,
            proposal: proposal.clone(),
            summaries: ["node-1", "node-2"]
                .into_iter()
                .map(|id| rhiza_quepaxa::RecorderSummary {
                    recorder_id: id.into(),
                    slot: 10,
                    step: 6,
                    first_current: None,
                    aggregate_prior: Some(proposal.clone()),
                })
                .collect(),
        };
        serde_json::json!({
            "version": 1,
            "config_id": 4,
            "members": [
                {"node_id": "node-1", "url": "http://rhiza-sql-c4-0.rhiza-sql-c4:8081", "log_url": "http://rhiza-sql-c4-0.rhiza-sql-c4:8080", "token": "peer-1-secret"},
                {"node_id": "node-2", "url": "http://rhiza-sql-c4-1.rhiza-sql-c4:8081", "log_url": "http://rhiza-sql-c4-1.rhiza-sql-c4:8080", "token": "peer-2-secret"},
                {"node_id": "node-3", "url": "http://rhiza-sql-c4-2.rhiza-sql-c4:8081", "log_url": "http://rhiza-sql-c4-2.rhiza-sql-c4:8080", "token": "peer-3-secret"},
            ],
            "predecessor": {
                "version": 2,
                "members": ["node-1", "node-2", "node-3"],
                "stop_entry": entry,
                "stop_proof": proof,
            },
        })
    }

    #[test]
    fn validate_config_bundle_uses_the_production_parser() {
        let json = exact_predecessor_bundle().to_string();
        let values = HashMap::from([("RHIZA_CONFIG_BUNDLE", json.as_str())]);

        let command = parse_validate_config_bundle(
            |name| values.get(name).map(ToString::to_string),
            |_| unreachable!("inline bundle must not read a file"),
        )
        .unwrap();

        assert!(matches!(command, Command::ValidateConfigBundle(Some(4))));
        assert!(matches!(
            parse_command(["validate-config-bundle", "--stdin"]).unwrap(),
            Command::ValidateConfigBundle(None)
        ));
    }

    #[test]
    fn configuration_bundle_accepts_exact_predecessor_stop_material() {
        let json = exact_predecessor_bundle();

        let bundle = parse_configuration_bundle(&json.to_string()).unwrap();

        assert_eq!(bundle.config_id, 4);
        assert_eq!(bundle.configuration_state.config_id(), 3);
        assert_eq!(
            bundle.configuration_state.digest(),
            Membership::new(["node-1", "node-2", "node-3"])
                .unwrap()
                .digest()
        );
        assert_eq!(bundle.configuration_state.stop().unwrap().index(), 10);
        assert!(bundle.require_predecessor().is_ok());
    }

    #[test]
    fn predecessor_fixture_is_generated_by_and_accepted_by_production_types() {
        let fixture = include_str!("../../../scripts/test-fixtures/config-4-predecessor.json");

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(fixture).unwrap(),
            exact_predecessor_bundle()
        );
        assert!(parse_configuration_bundle(fixture).is_ok());
        let wrong_successor =
            include_str!("../../../scripts/test-fixtures/config-4-wrong-successor.json");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(wrong_successor).unwrap(),
            predecessor_bundle_bound_to(["node-1", "node-2", "node-4"])
        );
        assert!(parse_configuration_bundle(wrong_successor).is_err());
    }

    #[test]
    fn configuration_bundle_rejects_semantically_invalid_predecessor_material() {
        let valid = exact_predecessor_bundle();
        let mut mutations = Vec::new();

        let mut digest = valid.clone();
        digest["predecessor"]["stop_proof"]["Phase2"]["config_digest"][0] = serde_json::json!(1);
        mutations.push(digest);

        let mut entry_hash = valid.clone();
        entry_hash["predecessor"]["stop_entry"]["hash"][0] = serde_json::json!(1);
        mutations.push(entry_hash);

        let mut command_binding = valid.clone();
        command_binding["predecessor"]["stop_proof"]["Phase2"]["proposal"]["value"]
            ["command_hash"][0] = serde_json::json!(1);
        mutations.push(command_binding);

        mutations.push(predecessor_bundle_bound_to(["node-1", "node-2", "node-4"]));

        let mut phase2_maximum = valid;
        let low_priority = serde_json::json!([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1
        ]);
        let high_priority = serde_json::json!([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 2
        ]);
        phase2_maximum["predecessor"]["stop_proof"]["Phase2"]["proposal"]["priority"] =
            low_priority.clone();
        for summary in phase2_maximum["predecessor"]["stop_proof"]["Phase2"]["summaries"]
            .as_array_mut()
            .unwrap()
        {
            summary["aggregate_prior"]["priority"] = low_priority.clone();
        }
        phase2_maximum["predecessor"]["stop_proof"]["Phase2"]["summaries"][0]["aggregate_prior"]
            ["priority"] = high_priority;
        mutations.push(phase2_maximum);

        for mutation in mutations {
            assert!(parse_configuration_bundle(&mutation.to_string()).is_err());
        }
    }

    fn base_serve_env() -> HashMap<&'static str, &'static str> {
        HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", "sql"),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_NODE_ID", "node-2"),
            ("RHIZA_DATA_DIR", "/tmp/node-2"),
            ("RHIZA_EPOCH", "1"),
            ("RHIZA_CONFIG_BUNDLE", base_bundle_json()),
            ("RHIZA_CLIENT_TOKEN", "client-secret"),
        ])
    }

    fn base_bundle_json() -> &'static str {
        r#"{"version":1,"config_id":7,"members":[{"node_id":"node-1","url":"http://node-1:8081","log_url":"http://node-1:8080","token":"peer-1-secret"},{"node_id":"node-2","url":"http://node-2:8081","token":"peer-2-secret"},{"node_id":"node-3","url":"http://node-3:8081","token":"peer-3-secret"}]}"#
    }

    fn checkpoint_bundle_json() -> &'static str {
        r#"{"version":1,"config_id":11,"members":[{"node_id":"n1","url":"http://n1","token":"t1"},{"node_id":"n2","url":"http://n2","token":"t2"},{"node_id":"n3","url":"http://n3","token":"t3"}]}"#
    }

    fn parse_serve_env(values: &HashMap<&str, &str>) -> Result<ServeConfig, String> {
        ServeConfig::from_lookup(|name| values.get(name).map(ToString::to_string))
    }

    #[test]
    fn execution_profile_is_required_and_canonicalizes_the_cluster_id() {
        let mut values = base_serve_env();
        values.remove("RHIZA_EXECUTION_PROFILE");
        assert_eq!(
            parse_serve_env(&values).unwrap_err(),
            "RHIZA_EXECUTION_PROFILE is required"
        );

        for (value, profile, expected_cluster_id) in [
            ("sql", ExecutionProfile::Sqlite, "rhiza:sql:cluster-a"),
            ("graph", ExecutionProfile::Graph, "rhiza:graph:cluster-a"),
            ("kv", ExecutionProfile::Kv, "rhiza:kv:cluster-a"),
        ] {
            values.insert("RHIZA_EXECUTION_PROFILE", value);
            let config = parse_serve_env(&values).unwrap();
            assert_eq!(config.execution_profile, profile);
            assert_eq!(config.cluster_id, expected_cluster_id);
            assert_eq!(config.node_config().unwrap().execution_profile(), profile);
        }

        values.insert("RHIZA_EXECUTION_PROFILE", "sqlite");
        assert_eq!(
            parse_serve_env(&values).unwrap_err(),
            "RHIZA_EXECUTION_PROFILE must be sql|graph|kv"
        );

        values.insert("RHIZA_EXECUTION_PROFILE", "graph");
        values.insert("RHIZA_CLUSTER_ID", "rhiza:graph:cluster-a");
        let canonical = parse_serve_env(&values).unwrap();
        assert_eq!(canonical.logical_cluster_id, "cluster-a");
        assert_eq!(canonical.cluster_id, "rhiza:graph:cluster-a");
        assert_eq!(
            canonical.node_config().unwrap().execution_profile(),
            ExecutionProfile::Graph
        );
        values.insert("RHIZA_EXECUTION_PROFILE", "kv");
        assert!(parse_serve_env(&values).unwrap_err().contains("not kv"));
    }

    #[test]
    fn serve_admin_token_is_optional_nonempty_distinct_and_redacted() {
        let values = base_serve_env();
        assert!(parse_serve_env(&values).unwrap().admin_token.is_none());

        let mut values = base_serve_env();
        values.insert("RHIZA_ADMIN_TOKEN", "admin-secret");
        let config = parse_serve_env(&values).unwrap();
        assert_eq!(config.admin_token.as_deref(), Some("admin-secret"));
        assert!(!format!("{config:?}").contains("admin-secret"));

        values.insert("RHIZA_ADMIN_TOKEN", "");
        assert!(parse_serve_env(&values).is_err());
        values.insert("RHIZA_ADMIN_TOKEN", "client-secret");
        assert!(parse_serve_env(&values).unwrap_err().contains("distinct"));
        values.insert("RHIZA_ADMIN_TOKEN", "peer-2-secret");
        assert!(parse_serve_env(&values).unwrap_err().contains("distinct"));
        for invalid in [" admin ", "admin secret", "admin\tsecret", "café"] {
            values.insert("RHIZA_ADMIN_TOKEN", invalid);
            assert!(parse_serve_env(&values).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn client_and_admin_commands_reject_unsafe_auth_and_non_origin_admin_urls() {
        for invalid in [" secret ", "a b", "a\tb", "café"] {
            assert!(parse_write_with_lookup(
                [
                    "--url",
                    "http://127.0.0.1:8080",
                    "--token",
                    invalid,
                    "--request-id",
                    "r1",
                    "--key",
                    "k",
                    "--value",
                    "v",
                ]
                .map(String::from),
                |_| None,
            )
            .is_err());
            assert!(parse_admin_command_config(
                [
                    "--admin-url",
                    "http://127.0.0.1:8080",
                    "--admin-token",
                    invalid,
                ]
                .map(String::from)
                .into_iter(),
                false,
                false,
                |_| None,
            )
            .is_err());
        }

        for invalid in [
            "http://user@127.0.0.1:8080",
            "http://user:password@127.0.0.1:8080",
            "http://127.0.0.1:8080/prefix",
            "http://127.0.0.1:8080?query=1",
            "http://127.0.0.1:8080#fragment",
        ] {
            assert!(
                parse_admin_command_config(
                    ["--admin-url", invalid, "--admin-token", "admin-secret"]
                        .map(String::from)
                        .into_iter(),
                    false,
                    false,
                    |_| None,
                )
                .is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn membership_commands_default_to_live_admin_and_require_stable_operation_ids() {
        let admin_only = HashMap::from([
            ("RHIZA_ADMIN_URL", "http://127.0.0.1:8080"),
            ("RHIZA_ADMIN_TOKEN", "admin-secret"),
        ]);
        let status = parse_membership_command_with_lookup(
            ["status"].map(String::from).into_iter(),
            |name| admin_only.get(name).map(ToString::to_string),
        )
        .unwrap();
        let Command::MembershipStatus(status) = status else {
            panic!("expected membership status");
        };
        assert!(matches!(status.target, AdminTarget::Live(_)));
        assert!(status.serve.is_none());

        let mut live = base_serve_env();
        live.extend([
            ("RHIZA_ADMIN_URL", "http://127.0.0.1:8080"),
            ("RHIZA_ADMIN_TOKEN", "admin-secret"),
        ]);
        let missing_id =
            parse_membership_command_with_lookup(["stop"].map(String::from).into_iter(), |name| {
                live.get(name).map(ToString::to_string)
            })
            .err()
            .expect("missing operation id must fail");
        assert!(missing_id.contains("operation id"));
        live.insert("RHIZA_ADMIN_OPERATION_ID", "stop-001");
        let stop =
            parse_membership_command_with_lookup(["stop"].map(String::from).into_iter(), |name| {
                live.get(name).map(ToString::to_string)
            })
            .unwrap();
        let Command::MembershipStop(stop) = stop else {
            panic!("expected membership stop");
        };
        assert!(matches!(stop.target, AdminTarget::Live(_)));
        assert_eq!(stop.operation_id.as_deref(), Some("stop-001"));
        assert!(stop.serve.is_some());
    }

    #[test]
    fn live_admin_target_fence_requires_exact_profile_identity_and_configuration() {
        let serve = parse_serve_env(&base_serve_env()).unwrap();
        let state = serve.bundle.configuration_state.clone();
        let mut status = AdminStatusResponse {
            cluster_id: serve.cluster_id.clone(),
            execution_profile: serve.execution_profile,
            epoch: serve.epoch,
            node: NodeStatus {
                ready: true,
                configuration_status: RuntimeConfigurationStatus::Active,
                configuration_state: state,
                stop_anchor: None,
                active_config_id: serve.bundle.config_id,
                active_membership_digest: serve.bundle.membership.digest(),
            },
            members: serve.bundle.membership.members().to_vec(),
            recovery_generation: serve.recovery_generation,
            qlog_root: LogAnchor::new(0, LogHash::ZERO),
            checkpoint_root: None,
            stopped_transition: None,
        };
        assert!(validate_live_admin_target(&status, &serve, &[serve.bundle.config_id]).is_ok());

        status.execution_profile = ExecutionProfile::Kv;
        assert_eq!(
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id]).unwrap_err(),
            "admin target fence mismatch for execution_profile; refusing mutating request"
        );
        status.execution_profile = serve.execution_profile;
        status.node.active_config_id += 1;
        assert!(
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id])
                .unwrap_err()
                .contains("config_id")
        );
    }

    #[test]
    fn offline_membership_requires_explicit_flag_and_local_serve_config() {
        let values = base_serve_env();
        let command = parse_membership_command_with_lookup(
            ["status", "--offline"].map(String::from).into_iter(),
            |name| values.get(name).map(ToString::to_string),
        )
        .unwrap();
        let Command::MembershipStatus(config) = command else {
            panic!("expected membership status");
        };
        assert!(matches!(config.target, AdminTarget::Offline));
        assert!(config.serve.is_some());
    }

    #[test]
    fn remote_serve_parses_all_provider_modes_without_exposing_secrets() {
        let mut s3 = base_serve_env();
        s3.extend([
            ("RHIZA_OBJECT_STORE", "s3"),
            ("RHIZA_S3_ENDPOINT", "https://s3.example.test"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_ACCESS_KEY", "s3-access-secret"),
            ("RHIZA_S3_SECRET_KEY", "s3-key-secret"),
            ("RHIZA_DURABILITY_MODE", "sync"),
            ("RHIZA_RECOVERY_GENERATION", "4"),
            ("RHIZA_STARTUP_MODE", "bootstrap"),
        ]);
        let config = parse_serve_env(&s3).unwrap();
        assert_eq!(config.recovery_generation, 4);
        assert_eq!(config.node_config().unwrap().recovery_generation(), 4);
        let debug = format!("{config:?}");
        assert!(!debug.contains("s3-access-secret"));
        assert!(!debug.contains("s3-key-secret"));
        assert!(debug.contains("[redacted]"));
        assert!(matches!(
            config.remote.as_ref().unwrap().object_store,
            ObjStoreConfig::S3 { .. }
        ));

        let mut gcs = base_serve_env();
        gcs.extend([
            ("RHIZA_OBJECT_STORE", "gcs"),
            ("RHIZA_GCS_BUCKET", "checkpoints"),
            ("RHIZA_GCS_SERVICE_ACCOUNT_PATH", "/var/run/gcs.json"),
            ("RHIZA_DURABILITY_MODE", "bounded"),
            ("RHIZA_DURABILITY_MAX_LAG", "250ms"),
            ("RHIZA_RECOVERY_GENERATION", "2"),
            ("RHIZA_STARTUP_MODE", "rejoin"),
        ]);
        assert!(matches!(
            parse_serve_env(&gcs).unwrap().remote.unwrap().object_store,
            ObjStoreConfig::Gcs { .. }
        ));

        let mut azure = base_serve_env();
        azure.extend([
            ("RHIZA_OBJECT_STORE", "azure"),
            ("RHIZA_AZURE_ACCOUNT", "account"),
            ("RHIZA_AZURE_CONTAINER", "checkpoints"),
            ("RHIZA_AZURE_ACCESS_KEY", "azure-key-secret"),
            ("RHIZA_DURABILITY_MODE", "periodic"),
            ("RHIZA_DURABILITY_INTERVAL", "2m"),
            ("RHIZA_RECOVERY_GENERATION", "9"),
            ("RHIZA_STARTUP_MODE", "disaster"),
        ]);
        assert!(matches!(
            parse_serve_env(&azure)
                .unwrap()
                .remote
                .unwrap()
                .object_store,
            ObjStoreConfig::AzureBlob { .. }
        ));

        for secret in ["s3-access-secret", "s3-key-secret", "azure-key-secret"] {
            assert!(!parse_serve_env(&HashMap::from([
                ("RHIZA_CLUSTER_ID", "cluster-a"),
                ("RHIZA_OBJECT_STORE", "invalid"),
            ]))
            .unwrap_err()
            .contains(secret));
        }
    }

    #[test]
    fn duration_parser_accepts_only_strict_positive_supported_units() {
        assert_eq!(
            parse_positive_duration("1ms").unwrap(),
            std::time::Duration::from_millis(1)
        );
        assert_eq!(
            parse_positive_duration("2s").unwrap(),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            parse_positive_duration("3m").unwrap(),
            std::time::Duration::from_secs(180)
        );
        assert_eq!(
            parse_positive_duration("4h").unwrap(),
            std::time::Duration::from_secs(14_400)
        );
        for invalid in ["", "0ms", "1", "1.5s", " 1s", "1d", "-1s", "1 s"] {
            assert!(
                parse_positive_duration(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn object_store_errors_redact_provider_credentials() {
        let config = ObjStoreConfig::S3 {
            endpoint: Some("https://s3.example.test".into()),
            bucket: "checkpoints".into(),
            access_key: Some("access-secret".into()),
            secret_key: Some("private-secret".into()),
            region: "us-east-1".into(),
            allow_http: false,
        };
        let message = redact_object_store_error(
            &config,
            "failed with access-secret and private-secret".into(),
        );
        assert_eq!(message, "failed with [redacted] and [redacted]");
    }

    #[test]
    fn s3_configuration_supports_aws_discovery_and_rejects_partial_credentials() {
        let base = HashMap::from([
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_REGION", "ap-northeast-2"),
        ]);
        let discovered = parse_object_store_with_lookup("s3", false, &mut |name| {
            base.get(name).map(ToString::to_string)
        })
        .unwrap();
        assert!(matches!(
            discovered,
            ObjStoreConfig::S3 {
                endpoint: None,
                access_key: None,
                secret_key: None,
                ..
            }
        ));

        let full = HashMap::from([
            ("RHIZA_S3_ENDPOINT", "http://rustfs:9000"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_ACCESS_KEY", "access"),
            ("RHIZA_S3_SECRET_KEY", "secret"),
        ]);
        assert!(matches!(
            parse_object_store_with_lookup("s3", false, &mut |name| {
                full.get(name).map(ToString::to_string)
            })
            .unwrap(),
            ObjStoreConfig::S3 {
                endpoint: Some(_),
                access_key: Some(_),
                secret_key: Some(_),
                ..
            }
        ));

        for values in [
            HashMap::from([
                ("RHIZA_S3_BUCKET", "checkpoints"),
                ("RHIZA_S3_ACCESS_KEY", "access"),
            ]),
            HashMap::from([
                ("RHIZA_S3_BUCKET", "checkpoints"),
                ("RHIZA_S3_SECRET_KEY", "secret"),
            ]),
            HashMap::from([
                ("RHIZA_S3_BUCKET", "checkpoints"),
                ("RHIZA_S3_ACCESS_KEY", ""),
                ("RHIZA_S3_SECRET_KEY", "secret"),
            ]),
        ] {
            assert!(parse_object_store_with_lookup("s3", false, &mut |name| {
                values.get(name).map(ToString::to_string)
            })
            .is_err());
        }
    }

    #[test]
    fn remote_serve_rejects_missing_or_irrelevant_checkpoint_settings() {
        let mut values = base_serve_env();
        values.extend([
            ("RHIZA_OBJECT_STORE", "s3"),
            ("RHIZA_S3_ENDPOINT", "https://s3.example.test"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_ACCESS_KEY", "access"),
            ("RHIZA_S3_SECRET_KEY", "secret"),
            ("RHIZA_RECOVERY_GENERATION", "2"),
            ("RHIZA_STARTUP_MODE", "bootstrap"),
        ]);
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_DURABILITY_MODE"));

        values.insert("RHIZA_DURABILITY_MODE", "sync");
        values.insert("RHIZA_DURABILITY_MAX_LAG", "1s");
        assert!(parse_serve_env(&values).unwrap_err().contains("irrelevant"));

        values.insert("RHIZA_DURABILITY_MODE", "bounded");
        values.remove("RHIZA_DURABILITY_MAX_LAG");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_DURABILITY_MAX_LAG"));

        values.insert("RHIZA_DURABILITY_MAX_LAG", "1s");
        values.insert("RHIZA_DURABILITY_INTERVAL", "1m");
        assert!(parse_serve_env(&values).unwrap_err().contains("irrelevant"));

        values.insert("RHIZA_DURABILITY_MODE", "periodic");
        values.remove("RHIZA_DURABILITY_MAX_LAG");
        values.remove("RHIZA_DURABILITY_INTERVAL");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_DURABILITY_INTERVAL"));
    }

    #[test]
    fn remote_serve_requires_generation_and_startup_mode_and_rejects_local_store() {
        let mut values = base_serve_env();
        values.extend([
            ("RHIZA_OBJECT_STORE", "gcs"),
            ("RHIZA_GCS_BUCKET", "checkpoints"),
            ("RHIZA_DURABILITY_MODE", "sync"),
        ]);
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_RECOVERY_GENERATION"));
        values.insert("RHIZA_RECOVERY_GENERATION", "2");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_STARTUP_MODE"));
        values.insert("RHIZA_STARTUP_MODE", "resume");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("bootstrap|rejoin|disaster"));
        values.insert("RHIZA_OBJECT_STORE", "local:/tmp/checkpoints");
        values.insert("RHIZA_STARTUP_MODE", "rejoin");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("only supported by e2e"));
    }

    #[test]
    fn roll_parser_accepts_flags_or_env_and_requires_consecutive_generations() {
        let mut values = HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", "sql"),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_EPOCH", "1"),
            (
                "RHIZA_CONFIG_BUNDLE",
                r#"{"version":1,"config_id":1,"members":[{"node_id":"n1","url":"http://n1","token":"t1"},{"node_id":"n2","url":"http://n2","token":"t2"},{"node_id":"n3","url":"http://n3","token":"t3"}]}"#,
            ),
            ("RHIZA_OBJECT_STORE", "gcs"),
            ("RHIZA_GCS_BUCKET", "checkpoints"),
            ("RHIZA_FROM_GENERATION", "4"),
            ("RHIZA_TO_GENERATION", "5"),
        ]);
        let parsed = parse_roll_checkpoint_with_lookup(Vec::new(), |name| {
            values.get(name).map(ToString::to_string)
        })
        .unwrap();
        assert_eq!(parsed.from_generation, 4);
        assert_eq!(parsed.to_generation, 5);

        values.insert("RHIZA_TO_GENERATION", "6");
        assert!(parse_roll_checkpoint_with_lookup(Vec::new(), |name| {
            values.get(name).map(ToString::to_string)
        })
        .err()
        .expect("non-consecutive generations must fail")
        .contains("source generation + 1"));

        let parsed = parse_roll_checkpoint_with_lookup(
            ["--from-generation", "7", "--to-generation", "8"].map(String::from),
            |name| values.get(name).map(ToString::to_string),
        )
        .unwrap();
        assert_eq!((parsed.from_generation, parsed.to_generation), (7, 8));
    }

    fn local_checkpoint(root: &std::path::Path, generation: u64) -> ObjectArchiveStore {
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap();
        ObjectArchiveStore::new_checkpoint_for_single_process(
            store,
            CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, generation),
        )
    }

    fn entries(end: u64) -> Vec<LogEntry> {
        let mut previous = LogHash::ZERO;
        (1..=end)
            .map(|index| {
                let payload = format!("entry-{index}").into_bytes();
                let hash = LogEntry::calculate_hash(
                    "rhiza:sql:cluster-a",
                    index,
                    1,
                    1,
                    EntryType::Command,
                    previous,
                    &payload,
                );
                let entry = LogEntry {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    index,
                    entry_type: EntryType::Command,
                    payload,
                    prev_hash: previous,
                    hash,
                };
                previous = hash;
                entry
            })
            .collect()
    }

    fn runtime_for_final_flush(root: &Path) -> Arc<NodeRuntime> {
        Arc::new(
            NodeRuntime::open(
                NodeConfig::new(
                    "cluster-a",
                    "node-1",
                    root.join("node"),
                    1,
                    1,
                    [
                        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
                        PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
                        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
                    ],
                    "client-token",
                )
                .unwrap(),
                Arc::new(
                    ThreeNodeConsensus::from_recovered_tip(
                        "rhiza:sql:cluster-a",
                        "node-1",
                        1,
                        1,
                        [
                            root.join("recorders/node-1"),
                            root.join("recorders/node-2"),
                            root.join("recorders/node-3"),
                        ],
                        1,
                        LogHash::ZERO,
                    )
                    .unwrap(),
                ),
                &[],
            )
            .unwrap(),
        )
    }

    fn runtime_with_blocked_minority(
        root: &Path,
    ) -> (Arc<NodeRuntime>, mpsc::Receiver<()>, BlockingRelease) {
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
                    "rhiza:sql:cluster-a",
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
        let consensus = Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:sql:cluster-a",
                "node-1",
                1,
                1,
                recorders,
            )
            .unwrap(),
        );
        let runtime = Arc::new(
            NodeRuntime::open(
                NodeConfig::new(
                    "cluster-a",
                    "node-1",
                    root.join("node"),
                    1,
                    1,
                    [
                        PeerConfig::new("node-1", "http://node-1", "peer-token-1").unwrap(),
                        PeerConfig::new("node-2", "http://node-2", "peer-token-2").unwrap(),
                        PeerConfig::new("node-3", "http://node-3", "peer-token-3").unwrap(),
                    ],
                    "client-token",
                )
                .unwrap(),
                consensus,
                &[],
            )
            .unwrap(),
        );
        (runtime, started_rx, release)
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
            request: rhiza_quepaxa::RecorderRequest,
        ) -> rhiza_quepaxa::Result<RecorderReply> {
            self.inner.call(request)
        }

        fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
            let _ = self.started.send(());
            self.release.wait();
            self.inner.record(request)
        }

        fn install_decision_proof(
            &self,
            proof: DecisionProof,
            membership: &Membership,
        ) -> rhiza_quepaxa::Result<()> {
            self.inner.install_decision_proof(proof, membership)
        }

        fn inspect_decision_proof(
            &self,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
            self.inner.inspect_decision_proof(slot)
        }

        fn inspect_record_summary(
            &self,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            self.inner.inspect_record_summary(slot)
        }

        fn uses_typed_protocol(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn remote_shutdown_flushes_the_applied_tip_before_returning() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(&root.path().join("archive"), 1);
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = Arc::new(
            CheckpointCoordinator::open_with_holder(
                archive.clone(),
                DurabilityMode::Periodic {
                    interval: Duration::from_secs(3600),
                },
                "node-1",
            )
            .await
            .unwrap(),
        );
        let runtime = runtime_for_final_flush(root.path());
        let committed = runtime.write("request-1", "alpha", "one").unwrap();

        finish_remote_shutdown(
            Ok(()),
            Arc::clone(&runtime),
            Arc::clone(&coordinator),
            tokio::time::Instant::now() + SERVE_SHUTDOWN_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(coordinator.durable_tip().index(), committed.applied_index);
        assert_eq!(
            archive
                .load_checkpoint()
                .await
                .unwrap()
                .unwrap()
                .manifest()
                .tip()
                .index(),
            committed.applied_index
        );
    }

    #[tokio::test]
    async fn remote_shutdown_preserves_the_primary_error_after_the_final_flush() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(&root.path().join("archive"), 1);
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = Arc::new(
            CheckpointCoordinator::open_with_holder(
                archive,
                DurabilityMode::Periodic {
                    interval: Duration::from_secs(3600),
                },
                "node-1",
            )
            .await
            .unwrap(),
        );
        let runtime = runtime_for_final_flush(root.path());
        let committed = runtime.write("request-1", "alpha", "one").unwrap();

        let error = finish_remote_shutdown(
            Err("client server failed".into()),
            Arc::clone(&runtime),
            Arc::clone(&coordinator),
            tokio::time::Instant::now() + SERVE_SHUTDOWN_TIMEOUT,
        )
        .await
        .unwrap_err();

        assert_eq!(error, "client server failed");
        assert_eq!(coordinator.durable_tip().index(), committed.applied_index);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn remote_shutdown_reports_primary_durability_and_consensus_errors_together() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(&root.path().join("archive"), 1);
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = Arc::new(
            CheckpointCoordinator::open_with_holder(
                archive,
                DurabilityMode::Periodic {
                    interval: Duration::from_secs(3600),
                },
                "node-1",
            )
            .await
            .unwrap(),
        );
        let (runtime, started, release) = runtime_with_blocked_minority(root.path());
        runtime.write("request-1", "alpha", "one").unwrap();
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();

        let error = finish_remote_shutdown(
            Err("client server failed".into()),
            Arc::clone(&runtime),
            coordinator,
            tokio::time::Instant::now(),
        )
        .await
        .unwrap_err();
        release.release();
        finish_pending_consensus_rpcs(&runtime, SERVE_SHUTDOWN_TIMEOUT).unwrap();

        assert_eq!(
            error,
            format!(
                "client server failed; {}; consensus RPCs did not finish before the shutdown deadline",
                shutdown_deadline_error(SERVE_SHUTDOWN_TIMEOUT)
            )
        );
    }

    #[test]
    fn unfinished_consensus_rpcs_fail_an_otherwise_successful_shutdown() {
        assert!(pending_consensus_rpc_result(true).is_ok());
        assert!(pending_consensus_rpc_result(false)
            .unwrap_err()
            .contains("consensus RPCs did not finish"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn consensus_drain_uses_only_the_remaining_shutdown_budget() {
        let root = tempfile::tempdir().unwrap();
        let (runtime, started, release) = runtime_with_blocked_minority(root.path());
        runtime.write("request-1", "alpha", "one").unwrap();
        tokio::task::spawn_blocking(move || started.recv().unwrap())
            .await
            .unwrap();
        let exhausted_deadline = tokio::time::Instant::now();
        let prior_drain = before_shutdown_deadline(
            exhausted_deadline,
            Duration::ZERO,
            std::future::pending::<()>(),
        )
        .await;
        assert!(prior_drain.is_err());

        let error =
            finish_pending_consensus_rpcs(&runtime, remaining_shutdown_budget(exhausted_deadline))
                .unwrap_err();

        assert!(error.contains("consensus RPCs did not finish"));
        release.release();
        finish_pending_consensus_rpcs(&runtime, SERVE_SHUTDOWN_TIMEOUT).unwrap();
    }

    #[test]
    fn consensus_drain_is_not_queued_behind_a_saturated_blocking_pool() {
        const HANG_GUARD: Duration = Duration::from_secs(10);

        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().to_path_buf();
        let (blocker_started_tx, blocker_started_rx) = mpsc::channel();
        let (release_blocker_tx, release_blocker_rx) = mpsc::channel();
        let (drain_finished_tx, drain_finished_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let executor = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .enable_all()
                .build()
                .unwrap();
            executor.block_on(async move {
                let runtime = runtime_for_final_flush(&root_path);
                finish_pending_consensus_rpcs(&runtime, HANG_GUARD).expect(
                    "startup consensus RPCs must drain before saturating the blocking pool",
                );
                let blocker = tokio::task::spawn_blocking(move || {
                    blocker_started_tx.send(()).unwrap();
                    release_blocker_rx.recv().unwrap();
                });
                let result = finish_pending_consensus_rpcs(&runtime, Duration::ZERO);
                drain_finished_tx.send(result).unwrap();
                blocker.await.unwrap();
            });
        });
        blocker_started_rx
            .recv_timeout(HANG_GUARD)
            .expect("blocking-pool saturation must be established");

        let result = drain_finished_rx.recv_timeout(HANG_GUARD);
        release_blocker_tx.send(()).unwrap();
        worker.join().unwrap();

        result
            .expect("consensus drain must start immediately despite blocking-pool saturation")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_deadline_bounds_a_stalled_drain_or_final_flush() {
        let result = before_shutdown_deadline(
            tokio::time::Instant::now() + Duration::from_millis(10),
            Duration::from_millis(10),
            std::future::pending::<Result<(), String>>(),
        )
        .await;

        assert!(result
            .unwrap_err()
            .contains("final checkpoint durability is unconfirmed"));
    }

    #[tokio::test]
    async fn checkpoint_worker_stops_before_the_final_flush_phase() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(&root.path().join("archive"), 1);
        archive.initialize_checkpoint().await.unwrap();
        let coordinator = Arc::new(
            CheckpointCoordinator::open_with_holder(
                archive,
                DurabilityMode::Periodic {
                    interval: Duration::from_secs(3600),
                },
                "node-1",
            )
            .await
            .unwrap(),
        );
        let runtime = runtime_for_final_flush(root.path());
        let (shutdown, wait) = tokio::sync::watch::channel(false);
        let mut worker = checkpoint_worker(
            DurabilityMode::Periodic {
                interval: Duration::from_secs(3600),
            },
            runtime,
            coordinator,
            wait,
        )
        .unwrap();

        shutdown.send_replace(true);
        tokio::time::timeout(Duration::from_secs(1), &mut worker.0)
            .await
            .expect("checkpoint worker must stop before final flush")
            .unwrap();
    }

    #[test]
    fn usage_describes_live_admin_as_the_default_and_offline_as_explicit() {
        assert!(USAGE.contains("live admin API by default"));
        assert!(USAGE.contains("pass --offline only as an explicit local fallback"));
    }

    #[tokio::test]
    async fn init_checkpoint_is_idempotent_only_for_an_empty_matching_identity() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(root.path(), 1);
        assert_eq!(
            initialize_empty_checkpoint(&archive).await.unwrap().index(),
            0
        );
        assert_eq!(
            initialize_empty_checkpoint(&archive).await.unwrap().index(),
            0
        );
        archive.publish_committed(&entries(1)).await.unwrap();
        assert!(initialize_empty_checkpoint(&archive)
            .await
            .unwrap_err()
            .contains("nonempty"));
    }

    #[tokio::test]
    async fn roll_checkpoint_resumes_a_prefix_and_retries_a_complete_target() {
        let root = tempfile::tempdir().unwrap();
        let source = local_checkpoint(root.path(), 1);
        source.initialize_checkpoint().await.unwrap();
        source.publish_committed(&entries(3)).await.unwrap();
        let target = local_checkpoint(root.path(), 2);
        target.initialize_checkpoint().await.unwrap();
        target.publish_committed(&entries(1)).await.unwrap();

        let (old_tip, new_tip) = roll_checkpoint(&source, &target).await.unwrap();
        assert_eq!(old_tip.index(), 3);
        assert_eq!(new_tip, old_tip);
        assert_eq!(target.restore_checkpoint().await.unwrap(), entries(3));
        let retry = roll_checkpoint(&source, &target).await.unwrap();
        assert_eq!(retry, (old_tip, new_tip));
        assert_eq!(target.restore_checkpoint().await.unwrap(), entries(3));
    }

    #[tokio::test]
    async fn roll_checkpoint_rejects_a_divergent_target() {
        let root = tempfile::tempdir().unwrap();
        let source = local_checkpoint(root.path(), 1);
        source.initialize_checkpoint().await.unwrap();
        source.publish_committed(&entries(3)).await.unwrap();
        let target = local_checkpoint(root.path(), 2);
        target.initialize_checkpoint().await.unwrap();
        let mut divergent = entries(1);
        divergent[0].payload = b"divergent-entry".to_vec();
        divergent[0].hash = LogEntry::calculate_hash(
            "rhiza:sql:cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &divergent[0].payload,
        );
        target.publish_committed(&divergent).await.unwrap();

        assert!(roll_checkpoint(&source, &target)
            .await
            .unwrap_err()
            .contains("publication conflicts at index 1"));
    }

    #[tokio::test]
    async fn roll_checkpoint_preserves_compacted_v2_snapshot_and_suffix() {
        let root = tempfile::tempdir().unwrap();
        let source = local_checkpoint(root.path(), 1);
        let committed = entries(4);
        source.publish_committed(&committed[..2]).await.unwrap();
        source.publish_committed(&committed[2..]).await.unwrap();
        let bytes = b"cli-roll-snapshot";
        source
            .publish_checkpoint_snapshot(
                RecoveryAnchor::new(
                    "rhiza:sql:cluster-a",
                    1,
                    1,
                    1,
                    LogAnchor::new(2, committed[1].hash),
                    SnapshotIdentity::new(
                        "cli-roll-snapshot",
                        LogHash::digest(&[bytes]),
                        bytes.len() as u64,
                    ),
                ),
                bytes,
            )
            .await
            .unwrap();
        let target = local_checkpoint(root.path(), 2);

        let (source_tip, target_tip) = roll_checkpoint(&source, &target).await.unwrap();

        assert_eq!(source_tip, target_tip);
        let restored = target.restore_checkpoint_v2().await.unwrap();
        assert_eq!(restored.snapshot().unwrap().bytes(), bytes);
        assert_eq!(
            restored.snapshot().unwrap().anchor().recovery_generation(),
            2
        );
        assert_eq!(restored.suffix(), &committed[2..]);
    }

    #[tokio::test]
    async fn startup_preparation_enforces_bootstrap_rejoin_and_disaster_guards() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(&root.path().join("archive"), 1);
        let data_dir = root.path().join("node");

        assert!(prepare_remote_startup(
            StartupMode::Bootstrap,
            &archive,
            &data_dir,
            "node-1",
            ExecutionProfile::Sqlite,
        )
        .await
        .unwrap_err()
        .contains("initialized empty checkpoint"));
        archive.initialize_checkpoint().await.unwrap();

        assert_eq!(
            prepare_remote_startup(
                StartupMode::Bootstrap,
                &archive,
                &data_dir,
                "node-1",
                ExecutionProfile::Sqlite,
            )
            .await
            .unwrap(),
            StartupPreparation::RecorderFirst
        );
        let empty_rejoin_dir = root.path().join("empty-rejoin");
        assert_eq!(
            prepare_remote_startup(
                StartupMode::Rejoin,
                &archive,
                &empty_rejoin_dir,
                "node-1",
                ExecutionProfile::Sqlite,
            )
            .await
            .unwrap(),
            StartupPreparation::RecorderFirst
        );
        archive.publish_committed(&entries(2)).await.unwrap();
        let fresh_bootstrap_dir = root.path().join("fresh-bootstrap-nonempty");
        assert!(prepare_remote_startup(
            StartupMode::Bootstrap,
            &archive,
            &fresh_bootstrap_dir,
            "node-1",
            ExecutionProfile::Sqlite,
        )
        .await
        .unwrap_err()
        .contains("empty checkpoint"));

        let fresh_rejoin_dir = root.path().join("fresh-rejoin");
        assert_eq!(
            prepare_remote_startup(
                StartupMode::Rejoin,
                &archive,
                &fresh_rejoin_dir,
                "node-1",
                ExecutionProfile::Sqlite,
            )
            .await
            .unwrap(),
            StartupPreparation::RuntimeFirstWithPeerCatchup {
                checkpoint_index: 2
            }
        );
        assert!(fresh_rejoin_dir.join("consensus/log").exists());
        assert!(fresh_rejoin_dir
            .join(LOCAL_CHECKPOINT_IDENTITY_FILE)
            .is_file());
        assert!(matches!(
            prepare_remote_startup(
                StartupMode::Rejoin,
                &archive,
                &fresh_rejoin_dir,
                "node-1",
                ExecutionProfile::Sqlite,
            )
                .await
                .unwrap(),
            StartupPreparation::VerifyLocalCheckpoint { identity, root }
                if identity == *archive.checkpoint_identity().unwrap()
                    && root == LogAnchor::new(2, entries(2)[1].hash)
        ));

        let disaster_dir = root.path().join("disaster");
        std::fs::create_dir_all(disaster_dir.join("sqlite")).unwrap();
        std::fs::write(disaster_dir.join("sqlite/existing"), b"state").unwrap();
        assert!(prepare_remote_startup(
            StartupMode::Disaster,
            &archive,
            &disaster_dir,
            "node-1",
            ExecutionProfile::Sqlite,
        )
        .await
        .unwrap_err()
        .contains("fresh"));

        let fresh_disaster_dir = root.path().join("fresh-disaster");
        assert_eq!(
            prepare_remote_startup(
                StartupMode::Disaster,
                &archive,
                &fresh_disaster_dir,
                "node-1",
                ExecutionProfile::Sqlite,
            )
            .await
            .unwrap(),
            StartupPreparation::RecorderFirst
        );
        assert!(fresh_disaster_dir.join("consensus/log").exists());
    }

    #[tokio::test]
    async fn nonfresh_rejoin_rejects_missing_mismatched_and_torn_identity_markers() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(&root.path().join("archive"), 1);
        archive.initialize_checkpoint().await.unwrap();
        archive.publish_committed(&entries(1)).await.unwrap();

        let missing = root.path().join("missing");
        std::fs::create_dir_all(missing.join("sqlite")).unwrap();
        std::fs::write(missing.join("sqlite/existing"), b"state").unwrap();
        assert!(prepare_remote_startup(
            StartupMode::Rejoin,
            &archive,
            &missing,
            "node-1",
            ExecutionProfile::Sqlite,
        )
        .await
        .unwrap_err()
        .contains("requires a local checkpoint identity marker"));

        let mismatch = root.path().join("mismatch");
        write_local_checkpoint_identity_marker(
            &mismatch,
            ExecutionProfile::Sqlite,
            &CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 2),
        )
        .unwrap();
        assert!(prepare_remote_startup(
            StartupMode::Rejoin,
            &archive,
            &mismatch,
            "node-1",
            ExecutionProfile::Sqlite,
        )
        .await
        .unwrap_err()
        .contains("does not exactly match"));

        let torn = root.path().join("torn");
        std::fs::create_dir_all(&torn).unwrap();
        std::fs::write(
            torn.join(LOCAL_CHECKPOINT_IDENTITY_FILE),
            b"{\"format_version\":",
        )
        .unwrap();
        assert!(prepare_remote_startup(
            StartupMode::Rejoin,
            &archive,
            &torn,
            "node-1",
            ExecutionProfile::Sqlite,
        )
        .await
        .unwrap_err()
        .contains("marker is invalid"));
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_identity_marker_rejects_symlink_files_and_data_directories() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let identity = CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1);
        let data_dir = root.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let target = root.path().join("target.json");
        std::fs::write(&target, b"{}").unwrap();
        symlink(&target, data_dir.join(LOCAL_CHECKPOINT_IDENTITY_FILE)).unwrap();
        assert!(read_and_validate_local_checkpoint_identity_marker(
            &data_dir,
            ExecutionProfile::Sqlite,
            &identity,
        )
        .unwrap_err()
        .contains("regular file"));

        let real_dir = root.path().join("real");
        std::fs::create_dir_all(&real_dir).unwrap();
        let linked_dir = root.path().join("linked");
        symlink(&real_dir, &linked_dir).unwrap();
        assert!(write_local_checkpoint_identity_marker(
            &linked_dir,
            ExecutionProfile::Sqlite,
            &identity,
        )
        .unwrap_err()
        .contains("real directory"));
    }

    #[test]
    fn successor_startup_uses_rejoin_as_its_steady_mode() {
        assert!(require_successor_startup_mode(StartupMode::Rejoin).is_ok());
        assert!(require_successor_startup_mode(StartupMode::Bootstrap).is_err());
        assert!(require_successor_startup_mode(StartupMode::Disaster).is_err());
    }

    #[test]
    fn nonfresh_rejoin_accepts_a_local_suffix_only_after_exact_checkpoint_inclusion() {
        let root = tempfile::tempdir().unwrap();
        let runtime = runtime_for_final_flush(root.path());
        runtime.write("request-1", "key", "one").unwrap();
        let authoritative_root = runtime.log_root().unwrap();
        runtime.write("request-2", "key", "two").unwrap();
        let identity = CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1);

        write_local_checkpoint_identity_marker(
            &root.path().join("node"),
            ExecutionProfile::Sqlite,
            &identity,
        )
        .unwrap();
        read_and_validate_local_checkpoint_identity_marker(
            &root.path().join("node"),
            ExecutionProfile::Sqlite,
            &identity,
        )
        .unwrap();

        verify_local_rejoin_checkpoint(&runtime, &identity, authoritative_root).unwrap();

        let wrong_root = LogAnchor::new(authoritative_root.index(), LogHash::from_bytes([9; 32]));
        assert!(
            verify_local_rejoin_checkpoint(&runtime, &identity, wrong_root)
                .unwrap_err()
                .contains("hash at index")
        );
        let wrong_generation = CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 2);
        assert!(
            verify_local_rejoin_checkpoint(&runtime, &wrong_generation, authoritative_root,)
                .unwrap_err()
                .contains("identity")
        );
    }

    fn unused_local_address() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    async fn wait_for_tcp(address: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {address}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    async fn wait_until_ready(address: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let args = HealthArgs {
            url: format!("http://{address}"),
            ready: true,
        };
        loop {
            if request_health(&args).await.is_ok() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for readiness at {address}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sequential_http_cluster_start_reaches_readiness_without_process_restart() {
        sequential_cluster_start(RecorderTransport::Http).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sequential_tcp_postcard_cluster_commits_without_process_restart() {
        sequential_cluster_start(RecorderTransport::TcpPostcard).await;
    }

    async fn sequential_cluster_start(recorder_transport: RecorderTransport) {
        let temp = tempfile::tempdir().unwrap();
        let recorder_addresses = [
            unused_local_address(),
            unused_local_address(),
            unused_local_address(),
        ];
        let client_addresses = [
            unused_local_address(),
            unused_local_address(),
            unused_local_address(),
        ];
        let tcp_addresses = [
            unused_local_address(),
            unused_local_address(),
            unused_local_address(),
        ];
        let peers = [
            PeerConfig::new(
                "node-1",
                format!("http://{}", recorder_addresses[0]),
                "peer-1-secret",
            )
            .unwrap(),
            PeerConfig::new(
                "node-2",
                format!("http://{}", recorder_addresses[1]),
                "peer-2-secret",
            )
            .unwrap(),
            PeerConfig::new(
                "node-3",
                format!("http://{}", recorder_addresses[2]),
                "peer-3-secret",
            )
            .unwrap(),
        ];
        let configs: [ServeConfig; 3] = std::array::from_fn(|index| ServeConfig {
            execution_profile: ExecutionProfile::Sqlite,
            logical_cluster_id: "staggered-cluster".into(),
            cluster_id: "rhiza:sql:staggered-cluster".into(),
            node_id: format!("node-{}", index + 1),
            data_dir: temp.path().join(format!("node-{}", index + 1)),
            epoch: 1,
            bundle: ConfigurationBundle {
                config_id: 1,
                configuration_state: ConfigurationState::active(1, {
                    Membership::from_voters(peers.iter().map(|peer| peer.node_id().to_string()))
                        .unwrap()
                        .digest()
                }),
                membership: Membership::from_voters(
                    peers.iter().map(|peer| peer.node_id().to_string()),
                )
                .unwrap(),
                peers: peers.to_vec(),
                recorder_tcp_peers: tcp_addresses
                    .iter()
                    .map(|address| {
                        Some(RecorderTcpPeer {
                            address: address.clone(),
                        })
                    })
                    .collect(),
                predecessor: None,
            },
            client_token: "client-secret".into(),
            admin_token: None,
            client_listen: client_addresses[index].clone(),
            recorder_listen: recorder_addresses[index].clone(),
            recorder_transport,
            recorder_tcp: (recorder_transport == RecorderTransport::TcpPostcard).then(|| {
                RecorderTcpConfig {
                    listen: tcp_addresses[index].clone(),
                }
            }),
            recovery_generation: 1,
            remote: None,
        });

        let (first_shutdown, first_wait) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(serve_until(configs[0].clone(), async move {
            let _ = first_wait.await;
        }));
        let first_recorder_address = match recorder_transport {
            RecorderTransport::Http => &recorder_addresses[0],
            RecorderTransport::TcpPostcard => &tcp_addresses[0],
        };
        wait_for_tcp(first_recorder_address).await;
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(!first.is_finished());
        if recorder_transport == RecorderTransport::TcpPostcard {
            assert!(tokio::net::TcpStream::connect(&recorder_addresses[0])
                .await
                .is_err());
        }
        assert!(tokio::net::TcpStream::connect(&client_addresses[0])
            .await
            .is_err());

        let (second_shutdown, second_wait) = tokio::sync::oneshot::channel();
        let second = tokio::spawn(serve_until(configs[1].clone(), async move {
            let _ = second_wait.await;
        }));
        wait_until_ready(&client_addresses[0]).await;
        wait_until_ready(&client_addresses[1]).await;

        let (third_shutdown, third_wait) = tokio::sync::oneshot::channel();
        let third = tokio::spawn(serve_until(configs[2].clone(), async move {
            let _ = third_wait.await;
        }));
        for address in &client_addresses {
            wait_until_ready(address).await;
        }
        let committed = request_write(&WriteArgs {
            urls: vec![format!("http://{}", client_addresses[0])],
            token: "client-secret".into(),
            request_id: format!("integration-{recorder_transport:?}"),
            key: "transport".into(),
            value: "committed".into(),
        })
        .await
        .unwrap();
        assert!(committed.applied_index > 0);

        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
        let _ = third_shutdown.send(());
        let joined = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first, second, third)
        })
        .await
        .expect("servers must stop within the graceful shutdown bound");
        assert!(joined.0.unwrap().is_ok());
        assert!(joined.1.unwrap().is_ok());
        assert!(joined.2.unwrap().is_ok());
        for address in &recorder_addresses {
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
        for address in &tcp_addresses {
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
    }

    #[derive(Clone, Default)]
    struct CapturedWrite(Arc<Mutex<Option<(HeaderMap, WriteRequest)>>>);

    async fn capture_write(
        State(captured): State<CapturedWrite>,
        headers: HeaderMap,
        Json(request): Json<WriteRequest>,
    ) -> Json<serde_json::Value> {
        *captured.0.lock().unwrap() = Some((headers, request));
        Json(serde_json::json!({
            "applied_index": 1,
            "hash": vec![0_u8; 32],
        }))
    }

    #[tokio::test]
    async fn write_sends_protocol_json_and_bearer_headers() {
        let captured = CapturedWrite::default();
        let app = Router::new()
            .route("/v1/write", post(capture_write))
            .with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let response = request_write(&WriteArgs {
            urls: vec![format!("http://{address}")],
            token: "client-secret".into(),
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 1);
        let (headers, request) = captured.0.lock().unwrap().take().unwrap();
        assert_eq!(headers[VERSION_HEADER], PROTOCOL_VERSION);
        assert_eq!(headers["authorization"], "Bearer client-secret");
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(
            request,
            WriteRequest {
                request_id: "request-1".into(),
                key: "alpha".into(),
                value: "one".into(),
            }
        );
    }

    #[tokio::test]
    async fn write_retries_retryable_endpoint_with_the_same_request_id() {
        let first = CapturedWrite::default();
        let first_app = Router::new()
            .route(
                WRITE_PATH,
                post(
                    |State(captured): State<CapturedWrite>,
                     headers: HeaderMap,
                     Json(request): Json<WriteRequest>| async move {
                        *captured.0.lock().unwrap() = Some((headers, request));
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({
                                "code": "unavailable",
                                "retryable": true,
                                "message": "preferred proposer unavailable",
                            })),
                        )
                    },
                ),
            )
            .with_state(first.clone());
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let second = CapturedWrite::default();
        let second_app = Router::new()
            .route(WRITE_PATH, post(capture_write))
            .with_state(second.clone());
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let response = request_write(&WriteArgs {
            urls: vec![
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            token: "client-secret".into(),
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        let first_request = first.0.lock().unwrap().take().unwrap().1;
        let second_request = second.0.lock().unwrap().take().unwrap().1;
        assert_eq!(first_request, second_request);
        assert_eq!(first_request.request_id, "request-1");
    }

    #[tokio::test]
    async fn write_retries_the_next_endpoint_after_a_transport_failure() {
        let unavailable_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_address = unavailable_listener.local_addr().unwrap();
        drop(unavailable_listener);

        let fallback = CapturedWrite::default();
        let app = Router::new()
            .route(WRITE_PATH, post(capture_write))
            .with_state(fallback.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let response = request_write(&WriteArgs {
            urls: vec![
                format!("http://{unavailable_address}"),
                format!("http://{address}"),
            ],
            token: "client-secret".into(),
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 1);
        assert_eq!(
            fallback.0.lock().unwrap().take().unwrap().1.request_id,
            "request-1"
        );
    }

    #[tokio::test]
    async fn write_does_not_retry_a_non_retryable_endpoint_error() {
        let first_app = Router::new().route(
            WRITE_PATH,
            post(|| async {
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({
                        "code": "request_conflict",
                        "retryable": false,
                        "message": "request id has a different payload",
                    })),
                )
            }),
        );
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let fallback = CapturedWrite::default();
        let second_app = Router::new()
            .route(WRITE_PATH, post(capture_write))
            .with_state(fallback.clone());
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let error = request_write(&WriteArgs {
            urls: vec![
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            token: "client-secret".into(),
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap_err();

        first_server.abort();
        second_server.abort();
        assert_eq!(
            error,
            "HTTP 409 Conflict code=request_conflict message=request id has a different payload"
        );
        assert!(fallback.0.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn write_hedges_a_slow_preferred_endpoint_with_the_same_request_id() {
        let first = CapturedWrite::default();
        let first_app = Router::new()
            .route(
                WRITE_PATH,
                post(
                    |State(captured): State<CapturedWrite>,
                     headers: HeaderMap,
                     Json(request): Json<WriteRequest>| async move {
                        *captured.0.lock().unwrap() = Some((headers, request));
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        Json(serde_json::json!({
                            "applied_index": 2,
                            "hash": vec![0_u8; 32],
                        }))
                    },
                ),
            )
            .with_state(first.clone());
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let second = CapturedWrite::default();
        let second_app = Router::new()
            .route(WRITE_PATH, post(capture_write))
            .with_state(second.clone());
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            request_write(&WriteArgs {
                urls: vec![
                    format!("http://{first_address}"),
                    format!("http://{second_address}"),
                ],
                token: "client-secret".into(),
                request_id: "request-1".into(),
                key: "alpha".into(),
                value: "one".into(),
            }),
        )
        .await
        .expect("the fallback endpoint should be hedged before the preferred request finishes")
        .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        let first_request = first.0.lock().unwrap().take().unwrap().1;
        let second_request = second.0.lock().unwrap().take().unwrap().1;
        assert_eq!(first_request, second_request);
        assert_eq!(first_request.request_id, "request-1");
    }

    #[derive(Clone, Default)]
    struct CapturedRead(Arc<Mutex<Option<ReadRequest>>>);

    #[test]
    fn only_idempotent_reads_are_hedged() {
        assert!(!read_can_hedge(None));
        assert!(!read_can_hedge(Some(ReadConsistency::ReadBarrier)));
        assert!(read_can_hedge(Some(ReadConsistency::Local)));
        assert!(read_can_hedge(Some(ReadConsistency::AppliedIndex(7))));
    }

    #[tokio::test]
    async fn read_retry_preserves_requested_consistency() {
        let first_app = Router::new().route(
            READ_PATH,
            post(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "code": "unavailable",
                        "retryable": true,
                        "message": "preferred proposer unavailable",
                    })),
                )
            }),
        );
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let captured = CapturedRead::default();
        let second_app = Router::new()
            .route(
                READ_PATH,
                post(
                    |State(captured): State<CapturedRead>,
                     Json(request): Json<ReadRequest>| async move {
                        *captured.0.lock().unwrap() = Some(request);
                        Json(serde_json::json!({
                            "value": "one",
                            "applied_index": 1,
                            "hash": vec![0_u8; 32],
                        }))
                    },
                ),
            )
            .with_state(captured.clone());
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let response = request_read(&ReadArgs {
            urls: vec![
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            token: "client-secret".into(),
            key: "alpha".into(),
            consistency: Some(ReadConsistency::ReadBarrier),
            expect: None,
        })
        .await
        .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.value.as_deref(), Some("one"));
        assert_eq!(
            captured.0.lock().unwrap().take().unwrap().consistency,
            Some(ReadConsistency::ReadBarrier)
        );
    }

    #[tokio::test]
    async fn read_barrier_waits_for_the_current_attempt_before_retrying() {
        let first_app = Router::new().route(
            READ_PATH,
            post(|| async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                Json(serde_json::json!({
                    "value": "one",
                    "applied_index": 1,
                    "hash": vec![0_u8; 32],
                }))
            }),
        );
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let fallback_requests = Arc::new(AtomicUsize::new(0));
        let fallback_count = Arc::clone(&fallback_requests);
        let second_app = Router::new().route(
            READ_PATH,
            post(move || {
                let fallback_count = Arc::clone(&fallback_count);
                async move {
                    fallback_count.fetch_add(1, Ordering::SeqCst);
                    Json(serde_json::json!({
                        "value": "one",
                        "applied_index": 2,
                        "hash": vec![0_u8; 32],
                    }))
                }
            }),
        );
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let request = ReadRequest {
            key: "alpha".into(),
            consistency: Some(ReadConsistency::ReadBarrier),
        };
        let response: ReadResponse = client_json_request_with_policy(
            &[
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            "client-secret",
            READ_PATH,
            &request,
            read_can_hedge(request.consistency),
            ClientPolicy::test(Duration::from_millis(10), Duration::from_millis(200)),
        )
        .await
        .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        assert_eq!(fallback_requests.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn applied_index_read_hedges_and_preserves_consistency() {
        let first_app = Router::new().route(
            READ_PATH,
            post(|| async {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Json(serde_json::json!({
                    "value": "late",
                    "applied_index": 7,
                    "hash": vec![0_u8; 32],
                }))
            }),
        );
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let captured = CapturedRead::default();
        let second_app = Router::new()
            .route(
                READ_PATH,
                post(
                    |State(captured): State<CapturedRead>,
                     Json(request): Json<ReadRequest>| async move {
                        *captured.0.lock().unwrap() = Some(request);
                        Json(serde_json::json!({
                            "value": "one",
                            "applied_index": 7,
                            "hash": vec![0_u8; 32],
                        }))
                    },
                ),
            )
            .with_state(captured.clone());
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let request = ReadRequest {
            key: "alpha".into(),
            consistency: Some(ReadConsistency::AppliedIndex(7)),
        };
        let response: ReadResponse = client_json_request_with_policy(
            &[
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            "client-secret",
            READ_PATH,
            &request,
            read_can_hedge(request.consistency),
            ClientPolicy::test(Duration::from_millis(10), Duration::from_millis(500)),
        )
        .await
        .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.value.as_deref(), Some("one"));
        assert_eq!(
            captured.0.lock().unwrap().take().unwrap().consistency,
            Some(ReadConsistency::AppliedIndex(7))
        );
    }

    #[tokio::test]
    async fn attempt_deadline_retries_with_the_exact_serialized_body() {
        let bodies = Arc::new(Mutex::new(Vec::<String>::new()));
        let first_bodies = Arc::clone(&bodies);
        let first_app = Router::new().route(
            WRITE_PATH,
            post(move |body: String| {
                let first_bodies = Arc::clone(&first_bodies);
                async move {
                    first_bodies.lock().unwrap().push(body);
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Json(serde_json::json!({
                        "applied_index": 2,
                        "hash": vec![0_u8; 32],
                    }))
                }
            }),
        );
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let second_bodies = Arc::clone(&bodies);
        let second_app = Router::new().route(
            WRITE_PATH,
            post(move |body: String| {
                let second_bodies = Arc::clone(&second_bodies);
                async move {
                    second_bodies.lock().unwrap().push(body);
                    Json(serde_json::json!({
                        "applied_index": 1,
                        "hash": vec![0_u8; 32],
                    }))
                }
            }),
        );
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });

        let request = WriteRequest {
            request_id: "same-request-id".into(),
            key: "alpha".into(),
            value: "one".into(),
        };
        let response: WriteResponse = client_json_request_with_policy(
            &[
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            "client-secret",
            WRITE_PATH,
            &request,
            false,
            ClientPolicy {
                connect_timeout: Duration::from_millis(20),
                attempt_timeout: Duration::from_millis(40),
                operation_timeout: Duration::from_millis(300),
                hedge_delay: Duration::from_millis(10),
            },
        )
        .await
        .unwrap();

        first_server.abort();
        second_server.abort();
        assert_eq!(response.applied_index, 1);
        assert_eq!(
            *bodies.lock().unwrap(),
            vec![
                serde_json::to_string(&request).unwrap(),
                serde_json::to_string(&request).unwrap(),
            ]
        );
    }

    #[tokio::test]
    async fn operation_deadline_bounds_all_attempts() {
        let app = Router::new().route(
            WRITE_PATH,
            post(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Json(serde_json::json!({
                    "applied_index": 1,
                    "hash": vec![0_u8; 32],
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let request = WriteRequest {
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        };

        let error = client_json_request_with_policy::<_, WriteResponse>(
            &[format!("http://{address}")],
            "client-secret",
            WRITE_PATH,
            &request,
            false,
            ClientPolicy {
                connect_timeout: Duration::from_millis(20),
                attempt_timeout: Duration::from_secs(1),
                operation_timeout: Duration::from_millis(40),
                hedge_delay: Duration::from_millis(10),
            },
        )
        .await
        .unwrap_err();

        server.abort();
        assert_eq!(error, "request failed: operation deadline exceeded");
    }

    #[tokio::test]
    async fn operation_deadline_preserves_the_last_structured_server_error() {
        let first_app = Router::new().route(
            WRITE_PATH,
            post(|| async {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "code": "leader_unavailable",
                        "retryable": true,
                        "message": "preferred proposer unavailable",
                    })),
                )
            }),
        );
        let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_address = first_listener.local_addr().unwrap();
        let first_server =
            tokio::spawn(async move { axum::serve(first_listener, first_app).await });

        let second_app = Router::new().route(
            WRITE_PATH,
            post(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Json(serde_json::json!({
                    "applied_index": 1,
                    "hash": vec![0_u8; 32],
                }))
            }),
        );
        let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_address = second_listener.local_addr().unwrap();
        let second_server =
            tokio::spawn(async move { axum::serve(second_listener, second_app).await });
        let request = WriteRequest {
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        };

        let error = client_json_request_with_policy::<_, WriteResponse>(
            &[
                format!("http://{first_address}"),
                format!("http://{second_address}"),
            ],
            "client-secret",
            WRITE_PATH,
            &request,
            false,
            ClientPolicy {
                connect_timeout: Duration::from_millis(20),
                attempt_timeout: Duration::from_secs(1),
                operation_timeout: Duration::from_millis(50),
                hedge_delay: Duration::from_millis(10),
            },
        )
        .await
        .unwrap_err();

        first_server.abort();
        second_server.abort();
        assert_eq!(
            error,
            "HTTP 503 Service Unavailable code=leader_unavailable message=preferred proposer unavailable"
        );
    }

    #[tokio::test]
    async fn sql_execute_decodes_statement_and_returning_results() {
        let app = Router::new().route(
            SQL_EXECUTE_PATH,
            post(|| async {
                Json(serde_json::json!({
                    "version": 2,
                    "applied_index": 7,
                    "hash": vec![0_u8; 32],
                    "results": [{
                        "statement_index": 0,
                        "rows_affected": 1,
                        "returning": {
                            "columns": ["id"],
                            "rows": [[{"type": "integer", "value": 42}]]
                        }
                    }]
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let response = request_sql_execute(&SqlExecuteArgs {
            urls: vec![format!("http://{address}")],
            token: "client-secret".into(),
            request_id: "returning-1".into(),
            statement: SqlStatement {
                sql: "INSERT INTO items(id) VALUES (42) RETURNING id".into(),
                parameters: vec![],
            },
        })
        .await
        .unwrap();

        server.abort();
        assert_eq!(response.applied_index, 7);
        assert_eq!(response.results.len(), 1);
        assert_eq!(response.results[0].rows_affected, 1);
        assert_eq!(
            response.results[0].returning.as_ref().unwrap().rows,
            [vec![SqlValue::Integer(42)]]
        );
    }

    #[tokio::test]
    async fn request_errors_do_not_expose_client_token() {
        let app = Router::new().route(WRITE_PATH, post(|| async { StatusCode::UNAUTHORIZED }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let token = "client-secret-that-must-stay-private";

        let error = request_write(&WriteArgs {
            urls: vec![format!("http://{address}")],
            token: token.into(),
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap_err();

        server.abort();
        assert!(!error.contains(token));
    }

    #[tokio::test]
    async fn admin_transport_errors_do_not_expose_url_credentials() {
        let address = unused_local_address();
        let username = "url-user-that-must-stay-private";
        let password = "url-password-that-must-stay-private";
        let config = AdminClientConfig {
            url: format!("http://{username}:{password}@{address}"),
            token: "admin-secret".into(),
        };
        let client =
            bounded_http_client(Duration::from_millis(20), Duration::from_millis(40)).unwrap();

        let error = admin_get_with_client::<serde_json::Value>(&config, ADMIN_STATUS_PATH, &client)
            .await
            .unwrap_err();

        assert!(!error.contains(username));
        assert!(!error.contains(password));
        assert!(!error.contains(&address));
    }

    #[tokio::test]
    async fn health_request_respects_the_configured_deadline() {
        let app = Router::new().route(
            LIVEZ_PATH,
            get(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                StatusCode::OK
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client =
            bounded_http_client(Duration::from_millis(20), Duration::from_millis(40)).unwrap();
        let started = tokio::time::Instant::now();

        let error = request_health_with_client(
            &HealthArgs {
                url: format!("http://{address}"),
                ready: false,
            },
            &client,
        )
        .await
        .unwrap_err();

        server.abort();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(error.starts_with("request failed:"));
    }

    #[tokio::test]
    async fn admin_request_respects_the_configured_deadline() {
        let app = Router::new().route(
            ADMIN_STATUS_PATH,
            get(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Json(serde_json::json!({}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client =
            bounded_http_client(Duration::from_millis(20), Duration::from_millis(40)).unwrap();
        let started = tokio::time::Instant::now();

        let error = admin_get_with_client::<serde_json::Value>(
            &AdminClientConfig {
                url: format!("http://{address}"),
                token: "admin-secret".into(),
            },
            ADMIN_STATUS_PATH,
            &client,
        )
        .await
        .unwrap_err();

        server.abort();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(error.starts_with("admin request failed:"));
    }

    #[tokio::test]
    async fn request_surfaces_structured_json_server_errors() {
        let app = Router::new().route(
            WRITE_PATH,
            post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "code": "invalid_request",
                        "message": "request id is empty",
                    })),
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let error = request_write(&WriteArgs {
            urls: vec![format!("http://{address}")],
            token: "client-secret".into(),
            request_id: "request-1".into(),
            key: "alpha".into(),
            value: "one".into(),
        })
        .await
        .unwrap_err();

        server.abort();
        assert_eq!(
            error,
            "HTTP 400 Bad Request code=invalid_request message=request id is empty"
        );
    }
}
