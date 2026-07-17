use rusqlite::{Connection, ErrorCode, params};
use std::error::Error;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use turso_size_perf::common::{Args, Outcome, checksum_rows};

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> AnyResult<()> {
    let args = Args::parse().map_err(std::io::Error::other)?;
    let runtime_started = Instant::now();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()?;
    let runtime_init = runtime_started.elapsed();
    let mut outcome = run(&args)?;
    outcome.runtime_init = runtime_init;
    println!("{}", outcome.to_json());
    drop(runtime);
    Ok(())
}

fn open(path: &Path) -> AnyResult<(Connection, Duration, String, String)> {
    let started = Instant::now();
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.busy_timeout(Duration::ZERO)?;
    let elapsed = started.elapsed();
    let journal_mode = conn.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    let synchronous: i64 = conn.pragma_query_value(None, "synchronous", |row| row.get(0))?;
    Ok((conn, elapsed, journal_mode, synchronous.to_string()))
}

fn create_schema(conn: &Connection) -> AnyResult<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kv(id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
    )?;
    Ok(())
}

fn prefill(conn: &mut Connection, count: usize) -> AnyResult<()> {
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare("INSERT INTO kv(id,value) VALUES(?1,?2)")?;
        for id in 0..count {
            stmt.execute(params![id as i64, format!("value-{id:08}")])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn all_rows(conn: &Connection) -> AnyResult<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id,value FROM kv ORDER BY id")?;
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn observe(conn: &Connection) -> AnyResult<(u64, usize)> {
    let rows = all_rows(conn)?;
    let row_count = rows.len();
    Ok((checksum_rows(rows), row_count))
}

fn run(args: &Args) -> AnyResult<Outcome> {
    let (mut conn, mut open_elapsed, journal_mode, synchronous) = open(&args.db)?;
    let setup_started = Instant::now();
    let (operation, checksum, row_count, successes, errors, busy) = match args.scenario.as_str() {
        "cold_open" => {
            let started = Instant::now();
            create_schema(&conn)?;
            (started.elapsed(), checksum_rows([]), 0, 1, 0, 0)
        }
        "warm_open" => {
            create_schema(&conn)?;
            drop(conn);
            let reopened = open(&args.db)?;
            conn = reopened.0;
            open_elapsed = reopened.1;
            (Duration::ZERO, checksum_rows([]), 0, 1, 0, 0)
        }
        "point_insert" => {
            create_schema(&conn)?;
            let started = Instant::now();
            {
                let mut stmt = conn.prepare_cached("INSERT INTO kv(id,value) VALUES(?1,?2)")?;
                for id in 0..args.count {
                    stmt.execute(params![id as i64, format!("value-{id:08}")])?;
                }
            }
            let elapsed = started.elapsed();
            let (checksum, row_count) = observe(&conn)?;
            (elapsed, checksum, row_count, args.count, 0, 0)
        }
        "point_update" => {
            create_schema(&conn)?;
            prefill(&mut conn, args.count)?;
            let started = Instant::now();
            {
                let mut stmt = conn.prepare_cached("UPDATE kv SET value=?2 WHERE id=?1")?;
                for id in 0..args.count {
                    stmt.execute(params![id as i64, format!("updated-{id:08}")])?;
                }
            }
            let elapsed = started.elapsed();
            let (checksum, row_count) = observe(&conn)?;
            (elapsed, checksum, row_count, args.count, 0, 0)
        }
        "point_read" => {
            create_schema(&conn)?;
            prefill(&mut conn, args.count)?;
            let started = Instant::now();
            let mut observed = Vec::with_capacity(args.count);
            {
                let mut stmt = conn.prepare_cached("SELECT id,value FROM kv WHERE id=?1")?;
                for id in 0..args.count {
                    observed
                        .push(stmt.query_row([id as i64], |row| Ok((row.get(0)?, row.get(1)?)))?);
                }
            }
            let row_count = observed.len();
            (
                started.elapsed(),
                checksum_rows(observed),
                row_count,
                args.count,
                0,
                0,
            )
        }
        "ordered_scan" => {
            create_schema(&conn)?;
            prefill(&mut conn, args.count)?;
            let started = Instant::now();
            let rows = all_rows(&conn)?;
            let row_count = rows.len();
            (
                started.elapsed(),
                checksum_rows(rows),
                row_count,
                args.count,
                0,
                0,
            )
        }
        "transaction_batch" => {
            create_schema(&conn)?;
            let started = Instant::now();
            prefill(&mut conn, args.count)?;
            let elapsed = started.elapsed();
            let (checksum, row_count) = observe(&conn)?;
            (elapsed, checksum, row_count, args.count, 0, 0)
        }
        "multi_writer" => {
            create_schema(&conn)?;
            drop(conn);
            let (elapsed, successes, errors, busy) =
                multi_writer(&args.db, args.writers, args.count)?;
            conn = open(&args.db)?.0;
            let (checksum, row_count) = observe(&conn)?;
            if row_count != successes {
                return Err(format!(
                    "persisted row count {row_count} does not match {successes} successful writes"
                )
                .into());
            }
            (elapsed, checksum, row_count, successes, errors, busy)
        }
        _ => unreachable!(),
    };
    let setup = setup_started.elapsed().saturating_sub(operation);
    drop(conn);
    Ok(Outcome {
        backend: "rusqlite",
        scenario: args.scenario.clone(),
        count: args.count,
        writers: args.writers,
        runtime_init: Duration::ZERO,
        open: open_elapsed,
        setup,
        operation,
        checksum,
        row_count,
        successes,
        errors,
        busy,
        journal_mode,
        synchronous,
    })
}

fn multi_writer(
    path: &Path,
    writers: usize,
    count: usize,
) -> AnyResult<(Duration, usize, usize, usize)> {
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let path = path.to_owned();
        let barrier = barrier.clone();
        handles.push(thread::spawn(
            move || -> AnyResult<(usize, usize, usize)> {
                let (conn, _, _, _) = open(&path)?;
                let mut stmt = conn.prepare_cached("INSERT INTO kv(id,value) VALUES(?1,?2)")?;
                barrier.wait();
                let mut ok = 0;
                let mut errors = 0;
                let mut busy = 0;
                for index in 0..count {
                    let id = (writer * count + index) as i64;
                    match stmt.execute(params![id, format!("value-{id:08}")]) {
                        Ok(_) => ok += 1,
                        Err(error) => {
                            errors += 1;
                            if is_busy(&error) {
                                busy += 1;
                            }
                        }
                    }
                }
                Ok((ok, errors, busy))
            },
        ));
    }
    let started = Instant::now();
    barrier.wait();
    let mut totals = (0, 0, 0);
    for handle in handles {
        let result = handle.join().map_err(|_| "writer thread panicked")??;
        totals.0 += result.0;
        totals.1 += result.1;
        totals.2 += result.2;
    }
    Ok((started.elapsed(), totals.0, totals.1, totals.2))
}

fn is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}
