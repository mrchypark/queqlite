use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use turso::{Builder, Connection, Database, Error as TursoError, Value};
use turso_size_perf::common::{Args, Outcome, checksum_rows};

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> AnyResult<()> {
    let args = Args::parse().map_err(std::io::Error::other)?;
    let runtime_started = Instant::now();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .build()?;
    let runtime_init = runtime_started.elapsed();
    let mut outcome = runtime.block_on(run(&args))?;
    outcome.runtime_init = runtime_init;
    println!("{}", outcome.to_json());
    Ok(())
}

async fn open(path: &Path) -> AnyResult<(Database, Connection, Duration, String, String)> {
    let started = Instant::now();
    let db = Builder::new_local(path.to_str().ok_or("database path is not UTF-8")?)
        .build()
        .await?;
    let conn = db.connect()?;
    consume(conn.query("PRAGMA journal_mode = 'wal'", ()).await?).await?;
    consume(conn.query("PRAGMA synchronous = FULL", ()).await?).await?;
    conn.busy_timeout(Duration::ZERO)?;
    let elapsed = started.elapsed();
    let journal_mode = pragma_value(&conn, "journal_mode").await?;
    let synchronous = pragma_value(&conn, "synchronous").await?;
    Ok((db, conn, elapsed, journal_mode, synchronous))
}

async fn configure_connection(conn: &Connection) -> AnyResult<()> {
    consume(conn.query("PRAGMA journal_mode = 'wal'", ()).await?).await?;
    consume(conn.query("PRAGMA synchronous = FULL", ()).await?).await?;
    conn.busy_timeout(Duration::ZERO)?;
    Ok(())
}

async fn consume(mut rows: turso::Rows) -> AnyResult<()> {
    while rows.next().await?.is_some() {}
    Ok(())
}

async fn pragma_value(conn: &Connection, name: &str) -> AnyResult<String> {
    let mut rows = conn.query(format!("PRAGMA {name}"), ()).await?;
    let row = rows.next().await?.ok_or("pragma returned no row")?;
    Ok(match row.get_value(0)? {
        Value::Null => "null".to_owned(),
        Value::Integer(value) => value.to_string(),
        Value::Real(value) => value.to_string(),
        Value::Text(value) => value,
        Value::Blob(value) => format!("blob:{}", value.len()),
    })
}

async fn create_schema(conn: &Connection) -> AnyResult<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kv(id INTEGER PRIMARY KEY, value TEXT NOT NULL);",
    )
    .await?;
    Ok(())
}

async fn prefill(conn: &mut Connection, count: usize) -> AnyResult<()> {
    let tx = conn.transaction().await?;
    {
        let mut stmt = tx.prepare("INSERT INTO kv(id,value) VALUES(?1,?2)").await?;
        for id in 0..count {
            stmt.execute((id as i64, format!("value-{id:08}"))).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

async fn all_rows(conn: &Connection) -> AnyResult<Vec<(i64, String)>> {
    let mut rows = conn
        .query("SELECT id,value FROM kv ORDER BY id", ())
        .await?;
    let mut collected = Vec::new();
    while let Some(row) = rows.next().await? {
        collected.push((row.get(0)?, row.get(1)?));
    }
    Ok(collected)
}

async fn observe(conn: &Connection) -> AnyResult<(u64, usize)> {
    let rows = all_rows(conn).await?;
    let row_count = rows.len();
    Ok((checksum_rows(rows), row_count))
}

async fn run(args: &Args) -> AnyResult<Outcome> {
    let (mut db, mut conn, mut open_elapsed, journal_mode, synchronous) = open(&args.db).await?;
    let setup_started = Instant::now();
    let (operation, checksum, row_count, successes, errors, busy) = match args.scenario.as_str() {
        "cold_open" => {
            let started = Instant::now();
            create_schema(&conn).await?;
            (started.elapsed(), checksum_rows([]), 0, 1, 0, 0)
        }
        "warm_open" => {
            create_schema(&conn).await?;
            drop(conn);
            drop(db);
            let reopened = open(&args.db).await?;
            db = reopened.0;
            conn = reopened.1;
            open_elapsed = reopened.2;
            (Duration::ZERO, checksum_rows([]), 0, 1, 0, 0)
        }
        "point_insert" => {
            create_schema(&conn).await?;
            let started = Instant::now();
            {
                let mut stmt = conn
                    .prepare_cached("INSERT INTO kv(id,value) VALUES(?1,?2)")
                    .await?;
                for id in 0..args.count {
                    stmt.execute((id as i64, format!("value-{id:08}"))).await?;
                }
            }
            let elapsed = started.elapsed();
            let (checksum, row_count) = observe(&conn).await?;
            (elapsed, checksum, row_count, args.count, 0, 0)
        }
        "point_update" => {
            create_schema(&conn).await?;
            prefill(&mut conn, args.count).await?;
            let started = Instant::now();
            {
                let mut stmt = conn
                    .prepare_cached("UPDATE kv SET value=?2 WHERE id=?1")
                    .await?;
                for id in 0..args.count {
                    stmt.execute((id as i64, format!("updated-{id:08}")))
                        .await?;
                }
            }
            let elapsed = started.elapsed();
            let (checksum, row_count) = observe(&conn).await?;
            (elapsed, checksum, row_count, args.count, 0, 0)
        }
        "point_read" => {
            create_schema(&conn).await?;
            prefill(&mut conn, args.count).await?;
            let started = Instant::now();
            let mut observed = Vec::with_capacity(args.count);
            {
                let mut stmt = conn
                    .prepare_cached("SELECT id,value FROM kv WHERE id=?1")
                    .await?;
                for id in 0..args.count {
                    let mut rows = stmt.query([id as i64]).await?;
                    let row = rows.next().await?.ok_or("point row missing")?;
                    observed.push((row.get(0)?, row.get(1)?));
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
            create_schema(&conn).await?;
            prefill(&mut conn, args.count).await?;
            let started = Instant::now();
            let rows = all_rows(&conn).await?;
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
            create_schema(&conn).await?;
            let started = Instant::now();
            prefill(&mut conn, args.count).await?;
            let elapsed = started.elapsed();
            let (checksum, row_count) = observe(&conn).await?;
            (elapsed, checksum, row_count, args.count, 0, 0)
        }
        "multi_writer" => {
            create_schema(&conn).await?;
            drop(conn);
            let (elapsed, successes, errors, busy) =
                multi_writer(db.clone(), args.writers, args.count).await?;
            conn = db.connect()?;
            let (checksum, row_count) = observe(&conn).await?;
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
    drop(db);
    Ok(Outcome {
        backend: "turso",
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

async fn multi_writer(
    db: Database,
    writers: usize,
    count: usize,
) -> AnyResult<(Duration, usize, usize, usize)> {
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);
    for writer in 0..writers {
        let db = db.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            let conn = db.connect()?;
            configure_connection(&conn).await?;
            let mut stmt = conn
                .prepare_cached("INSERT INTO kv(id,value) VALUES(?1,?2)")
                .await?;
            barrier.wait().await;
            let mut ok = 0;
            let mut errors = 0;
            let mut busy = 0;
            for index in 0..count {
                let id = (writer * count + index) as i64;
                match stmt.execute((id, format!("value-{id:08}"))).await {
                    Ok(_) => ok += 1,
                    Err(error) => {
                        errors += 1;
                        if is_busy(&error) {
                            busy += 1;
                        }
                    }
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>((ok, errors, busy))
        }));
    }
    let started = Instant::now();
    barrier.wait().await;
    let mut totals = (0, 0, 0);
    for handle in handles {
        let result = handle.await??;
        totals.0 += result.0;
        totals.1 += result.1;
        totals.2 += result.2;
    }
    Ok((started.elapsed(), totals.0, totals.1, totals.2))
}

fn is_busy(error: &TursoError) -> bool {
    matches!(error, TursoError::Busy(_) | TursoError::BusySnapshot(_))
}
