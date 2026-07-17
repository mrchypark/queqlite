use std::env;
use std::path::PathBuf;
use std::time::Duration;

pub const BACKEND_VERSIONS: &str = "turso=0.7.0;rusqlite=0.40.1;tokio=1.48.0";
pub const DURABILITY: &str = "synchronous=FULL;busy_timeout=0ms";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Args {
    pub db: PathBuf,
    pub scenario: String,
    pub count: usize,
    pub writers: usize,
}

impl Args {
    pub fn parse() -> Result<Self, String> {
        let mut db = None;
        let mut scenario = None;
        let mut count = 1_000;
        let mut writers = 1;
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| format!("missing value after {arg}"))?;
            match arg.as_str() {
                "--db" => db = Some(PathBuf::from(value)),
                "--scenario" => scenario = Some(value),
                "--count" => count = parse_positive("count", &value)?,
                "--writers" => writers = parse_positive("writers", &value)?,
                _ => return Err(format!("unknown argument: {arg}")),
            }
        }
        let parsed = Self {
            db: db.ok_or("--db is required")?,
            scenario: scenario.ok_or("--scenario is required")?,
            count,
            writers,
        };
        if !matches!(
            parsed.scenario.as_str(),
            "cold_open"
                | "warm_open"
                | "point_insert"
                | "point_update"
                | "point_read"
                | "ordered_scan"
                | "transaction_batch"
                | "multi_writer"
        ) {
            return Err(format!("unknown scenario: {}", parsed.scenario));
        }
        Ok(parsed)
    }
}

fn parse_positive(name: &str, value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|_| format!("invalid {name}: {value}"))?;
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

#[derive(Clone, Debug)]
pub struct Outcome {
    pub backend: &'static str,
    pub scenario: String,
    pub count: usize,
    pub writers: usize,
    pub runtime_init: Duration,
    pub open: Duration,
    pub setup: Duration,
    pub operation: Duration,
    pub checksum: u64,
    pub row_count: usize,
    pub successes: usize,
    pub errors: usize,
    pub busy: usize,
    pub journal_mode: String,
    pub synchronous: String,
}

impl Outcome {
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                "{{\"backend\":\"{}\",\"scenario\":\"{}\",\"count\":{},\"writers\":{},",
                "\"runtime_init_ns\":{},\"open_ns\":{},\"setup_ns\":{},\"operation_ns\":{},",
                "\"checksum\":{},\"row_count\":{},\"successes\":{},\"errors\":{},\"busy\":{},",
                "\"journal_mode\":\"{}\",\"synchronous\":\"{}\",",
                "\"durability_request\":\"{}\",\"versions\":\"{}\"}}"
            ),
            self.backend,
            self.scenario,
            self.count,
            self.writers,
            self.runtime_init.as_nanos(),
            self.open.as_nanos(),
            self.setup.as_nanos(),
            self.operation.as_nanos(),
            self.checksum,
            self.row_count,
            self.successes,
            self.errors,
            self.busy,
            escape_json(&self.journal_mode),
            escape_json(&self.synchronous),
            DURABILITY,
            BACKEND_VERSIONS,
        )
    }
}

fn escape_json(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

pub fn checksum_rows<I>(rows: I) -> u64
where
    I: IntoIterator<Item = (i64, String)>,
{
    let mut hash = 0xcbf29ce484222325_u64;
    for (id, value) in rows {
        for byte in id.to_le_bytes().into_iter().chain(value.bytes()) {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_changes_when_order_or_value_changes() {
        let rows = vec![(1, "a".to_owned()), (2, "b".to_owned())];
        assert_eq!(checksum_rows(rows.clone()), checksum_rows(rows.clone()));
        assert_ne!(
            checksum_rows(rows.clone()),
            checksum_rows(rows.into_iter().rev())
        );
        assert_ne!(
            checksum_rows(vec![(1, "a".to_owned())]),
            checksum_rows(vec![(1, "b".to_owned())])
        );
    }

    #[test]
    fn outcome_is_single_json_object_with_required_fields() {
        let output = Outcome {
            backend: "test",
            scenario: "cold_open".to_owned(),
            count: 1,
            writers: 1,
            runtime_init: Duration::ZERO,
            open: Duration::ZERO,
            setup: Duration::ZERO,
            operation: Duration::ZERO,
            checksum: 0,
            row_count: 0,
            successes: 1,
            errors: 0,
            busy: 0,
            journal_mode: "wal".to_owned(),
            synchronous: "2".to_owned(),
        }
        .to_json();
        assert!(output.starts_with('{') && output.ends_with('}'));
        for field in [
            "backend",
            "operation_ns",
            "checksum",
            "row_count",
            "errors",
            "busy",
        ] {
            assert!(output.contains(&format!("\"{field}\":")));
        }
    }
}
