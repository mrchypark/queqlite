use hiqlite::{Client, Params, Row};
use serde_json::{Value, json};
use std::env;
use std::error::Error;
use std::fmt::{self, Display};

#[derive(Debug)]
struct CliError(String);

impl Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for CliError {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sentinel {
    id: String,
    value: String,
}

impl From<&mut Row<'_>> for Sentinel {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            id: row.get("id"),
            value: row.get("value"),
        }
    }
}

struct Args {
    nodes: Vec<String>,
    secret: String,
    command: String,
    command_args: Vec<String>,
}

fn usage() -> &'static str {
    "usage: hiqlite-recovery-client --nodes host:port[,host:port] --secret SECRET \
<execute ID VALUE|reset|query-local ID|query-consistent ID|backup|metrics|verify-sentinel ID VALUE>"
}

fn parse_args() -> Result<Args, CliError> {
    let mut args = env::args().skip(1);
    let mut nodes = None;
    let mut secret = None;
    let mut command = None;
    let mut command_args = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--nodes" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError("--nodes requires a value".into()))?;
                nodes = Some(
                    value
                        .split(',')
                        .filter(|item| !item.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>(),
                );
            }
            "--secret" => {
                secret = Some(
                    args.next()
                        .ok_or_else(|| CliError("--secret requires a value".into()))?,
                );
            }
            "-h" | "--help" => return Err(CliError(usage().into())),
            value if command.is_none() => command = Some(value.to_owned()),
            value => command_args.push(value.to_owned()),
        }
    }

    let nodes = nodes.ok_or_else(|| CliError(format!("missing --nodes\n{}", usage())))?;
    if nodes.is_empty() {
        return Err(CliError("--nodes must contain at least one address".into()));
    }

    Ok(Args {
        nodes,
        secret: secret.ok_or_else(|| CliError(format!("missing --secret\n{}", usage())))?,
        command: command.ok_or_else(|| CliError(format!("missing command\n{}", usage())))?,
        command_args,
    })
}

fn require_command_args(args: &[String], count: usize, command: &str) -> Result<(), CliError> {
    if args.len() == count {
        Ok(())
    } else {
        Err(CliError(format!(
            "{command} expects {count} arguments, got {}",
            args.len()
        )))
    }
}

fn params(values: impl IntoIterator<Item = hiqlite::Param>) -> Params {
    values.into_iter().collect()
}

async fn ensure_schema(client: &Client) -> Result<(), hiqlite::Error> {
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS hiqlite_recovery_sentinel (\
             id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
            Params::new(),
        )
        .await?;
    Ok(())
}

async fn query_local(client: &Client, id: &str) -> Result<Option<Sentinel>, hiqlite::Error> {
    let mut rows: Vec<Sentinel> = client
        .query_map(
            "SELECT id, value FROM hiqlite_recovery_sentinel WHERE id = $1",
            params([id.into()]),
        )
        .await?;
    Ok(rows.pop())
}

async fn query_consistent(client: &Client, id: &str) -> Result<Option<Sentinel>, hiqlite::Error> {
    let mut rows: Vec<Sentinel> = client
        .query_consistent_map(
            "SELECT id, value FROM hiqlite_recovery_sentinel WHERE id = $1",
            params([id.into()]),
        )
        .await?;
    Ok(rows.pop())
}

fn sentinel_json(mode: &str, id: &str, sentinel: Option<Sentinel>) -> Value {
    match sentinel {
        Some(value) => json!({
            "command": mode,
            "found": true,
            "id": value.id,
            "value": value.value,
        }),
        None => json!({
            "command": mode,
            "found": false,
            "id": id,
        }),
    }
}

fn print_json(value: Value) -> Result<(), Box<dyn Error>> {
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let client = Client::remote(args.nodes, false, false, args.secret, true, None, None).await?;

    match args.command.as_str() {
        "execute" => {
            require_command_args(&args.command_args, 2, "execute")?;
            ensure_schema(&client).await?;
            let id = &args.command_args[0];
            let value = &args.command_args[1];
            let changed = client
                .execute(
                    "INSERT INTO hiqlite_recovery_sentinel (id, value) VALUES ($1, $2) \
                     ON CONFLICT(id) DO UPDATE SET value = excluded.value",
                    params([id.into(), value.into()]),
                )
                .await?;
            print_json(json!({
                "command": "execute",
                "acknowledged": true,
                "changed": changed,
                "id": id,
                "value": value,
            }))?;
        }
        "reset" => {
            require_command_args(&args.command_args, 0, "reset")?;
            ensure_schema(&client).await?;
            let changed = client
                .execute("DELETE FROM hiqlite_recovery_sentinel", Params::new())
                .await?;
            print_json(json!({
                "command": "reset",
                "acknowledged": true,
                "changed": changed,
            }))?;
        }
        "query-local" => {
            require_command_args(&args.command_args, 1, "query-local")?;
            let id = &args.command_args[0];
            print_json(sentinel_json("query-local", id, query_local(&client, id).await?))?;
        }
        "query-consistent" => {
            require_command_args(&args.command_args, 1, "query-consistent")?;
            let id = &args.command_args[0];
            print_json(sentinel_json(
                "query-consistent",
                id,
                query_consistent(&client, id).await?,
            ))?;
        }
        "backup" => {
            require_command_args(&args.command_args, 0, "backup")?;
            client.backup().await?;
            print_json(json!({
                "command": "backup",
                "triggered": true,
                "completed": false,
                "note": "S3 upload is asynchronous; verify the external object separately",
            }))?;
        }
        "metrics" => {
            require_command_args(&args.command_args, 0, "metrics")?;
            let metrics = client.metrics_db().await?;
            let mut voter_ids = metrics.membership_config.voter_ids().collect::<Vec<_>>();
            voter_ids.sort_unstable();
            let mut node_ids = metrics
                .membership_config
                .nodes()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            node_ids.sort_unstable();
            let learner_ids = node_ids
                .iter()
                .copied()
                .filter(|id| !voter_ids.contains(id))
                .collect::<Vec<_>>();
            print_json(json!({
                "command": "metrics",
                "node_id": metrics.id,
                "state": format!("{:?}", metrics.state),
                "running": metrics.running_state.is_ok(),
                "current_term": metrics.current_term,
                "current_leader": metrics.current_leader,
                "last_log_index": metrics.last_log_index,
                "last_applied": metrics.last_applied.map(|id| format!("{id:?}")),
                "voter_ids": voter_ids,
                "learner_ids": learner_ids,
                "node_ids": node_ids,
            }))?;
        }
        "verify-sentinel" => {
            require_command_args(&args.command_args, 2, "verify-sentinel")?;
            let expected = Sentinel {
                id: args.command_args[0].clone(),
                value: args.command_args[1].clone(),
            };
            let local = query_local(&client, &expected.id).await?;
            let consistent = query_consistent(&client, &expected.id).await?;
            if local.as_ref() != Some(&expected) || consistent.as_ref() != Some(&expected) {
                return Err(Box::new(CliError(format!(
                    "sentinel mismatch: expected={expected:?} local={local:?} consistent={consistent:?}"
                ))) as Box<dyn Error>);
            }
            print_json(json!({
                "command": "verify-sentinel",
                "verified": true,
                "id": expected.id,
                "value": expected.value,
                "local": true,
                "consistent": true,
            }))?;
        }
        command => return Err(Box::new(CliError(format!("unknown command: {command}")))),
    }

    Ok(())
}
