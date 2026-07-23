use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

const STARTUP_GUARD: Duration = Duration::from_secs(10);
const IO_TIMEOUT: Duration = Duration::from_secs(2);
const READY_POLL_INTERVAL: Duration = Duration::from_millis(25);
const STDERR_LIMIT: usize = 8 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

struct ServerProcess {
    child: Child,
    address: SocketAddr,
    stderr: Arc<Mutex<Vec<u8>>>,
    stderr_drain: Option<thread::JoinHandle<()>>,
    stdout_drain: Option<thread::JoinHandle<()>>,
}

impl ServerProcess {
    fn start(data_dir: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_rhiza-basic-app-server"))
            .env("RHIZA_BIND_ADDR", "127.0.0.1:0")
            .env("RHIZA_DATA_DIR", data_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let (address_tx, address_rx) = mpsc::sync_channel(1);
        let stdout_drain = thread::spawn(move || {
            let mut announced = false;
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else {
                    break;
                };
                if !announced {
                    if let Some(address) = line.strip_prefix("RHIZA_LISTEN_ADDR=") {
                        let _ = address_tx.send(address.parse().map_err(|error| {
                            format!("invalid listener address {address:?}: {error}")
                        }));
                        announced = true;
                    }
                }
            }
            if !announced {
                let _ =
                    address_tx.send(Err("server exited without announcing its listener".into()));
            }
        });
        let stderr_output = Arc::new(Mutex::new(Vec::new()));
        let stderr_for_drain = Arc::clone(&stderr_output);
        let stderr_drain = thread::spawn(move || {
            let mut stderr = stderr;
            let mut chunk = [0; 1024];
            while let Ok(count) = stderr.read(&mut chunk) {
                if count == 0 {
                    break;
                }
                let mut output = stderr_for_drain.lock().unwrap();
                output.extend_from_slice(&chunk[..count]);
                if output.len() > STDERR_LIMIT {
                    let excess = output.len() - STDERR_LIMIT;
                    output.drain(..excess);
                }
            }
        });
        let mut server = Self {
            child,
            address: "127.0.0.1:0".parse().unwrap(),
            stderr: stderr_output,
            stderr_drain: Some(stderr_drain),
            stdout_drain: Some(stdout_drain),
        };
        let deadline = Instant::now() + STARTUP_GUARD;
        server.address = server
            .wait_for_listener(address_rx, deadline)
            .unwrap_or_else(|reason| {
                let stderr = server.stop_and_collect_output();
                panic!("{reason}; server stderr:\n{stderr}")
            });
        server.wait_until_ready(deadline).unwrap_or_else(|reason| {
            let stderr = server.stop_and_collect_output();
            panic!("{reason}; server stderr:\n{stderr}")
        });
        server
    }

    fn request(&self, request: &str) -> HttpResponse {
        http_request(self.address, request).unwrap_or_else(|error| {
            panic!(
                "request to {} failed: {error}; server stderr:\n{}",
                self.address,
                self.stderr_output()
            )
        })
    }

    fn wait_for_listener(
        &mut self,
        address_rx: mpsc::Receiver<Result<SocketAddr, String>>,
        deadline: Instant,
    ) -> Result<SocketAddr, String> {
        loop {
            let timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(READY_POLL_INTERVAL);
            if timeout.is_zero() {
                return Err(format!(
                    "server did not announce its listener within {STARTUP_GUARD:?}"
                ));
            }
            match address_rx.recv_timeout(timeout) {
                Ok(Ok(address)) => return Ok(address),
                Ok(Err(error)) => return Err(format!("listener handoff failed: {error}")),
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Some(status) = self.child.try_wait().unwrap() {
                        return Err(format!(
                            "server exited before announcing its listener ({status})"
                        ));
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err("listener handoff disconnected".into())
                }
            }
        }
    }

    fn wait_until_ready(&mut self, deadline: Instant) -> Result<(), String> {
        let request = format!(
            "GET /ready HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            self.address
        );
        let mut last_error = None;

        while Instant::now() < deadline {
            match http_request_until(self.address, &request, deadline) {
                Ok(response)
                    if response.status == 200
                        && serde_json::from_str::<serde_json::Value>(&response.body).ok()
                            == Some(serde_json::json!({"ready": true})) =>
                {
                    return Ok(())
                }
                Ok(response) => {
                    last_error = Some(format!(
                        "/ready returned HTTP {} with body {:?}",
                        response.status, response.body
                    ))
                }
                Err(error) => last_error = Some(error.to_string()),
            }
            if let Some(status) = self.child.try_wait().unwrap() {
                return Err(format!(
                    "server exited before becoming ready ({status}); last readiness result: {}",
                    last_error.as_deref().unwrap_or("none"),
                ));
            }
            thread::sleep(READY_POLL_INTERVAL);
        }

        Err(format!(
            "server did not become ready within {STARTUP_GUARD:?}; last readiness result: {}",
            last_error.as_deref().unwrap_or("none"),
        ))
    }

    fn stderr_output(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().unwrap()).into_owned()
    }

    fn stop_and_collect_output(&mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(stderr_drain) = self.stderr_drain.take() {
            let _ = stderr_drain.join();
        }
        if let Some(stdout_drain) = self.stdout_drain.take() {
            let _ = stdout_drain.join();
        }
        self.stderr_output()
    }

    fn sigkill(mut self) {
        self.child.kill().unwrap();
        let status = self.child.wait().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        self.stderr_drain.take().unwrap().join().unwrap();
        self.stdout_drain.take().unwrap().join().unwrap();
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(stderr_drain) = self.stderr_drain.take() {
            let _ = stderr_drain.join();
        }
        if let Some(stdout_drain) = self.stdout_drain.take() {
            let _ = stdout_drain.join();
        }
    }
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: String,
}

fn remaining_until(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(request_timeout());
    }
    Ok(remaining)
}

fn request_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "HTTP request exceeded its total time limit",
    )
}

fn tcp_stream(address: SocketAddr, deadline: Instant) -> io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&address, remaining_until(deadline)?)?;
    stream.set_read_timeout(Some(remaining_until(deadline)?))?;
    stream.set_write_timeout(Some(remaining_until(deadline)?))?;
    Ok(stream)
}

fn http_request(address: SocketAddr, request: &str) -> io::Result<HttpResponse> {
    http_request_until(address, request, Instant::now() + IO_TIMEOUT)
}

fn http_request_until(
    address: SocketAddr,
    request: &str,
    deadline: Instant,
) -> io::Result<HttpResponse> {
    let mut stream = tcp_stream(address, deadline)?;
    let mut request = request.as_bytes();
    while !request.is_empty() {
        stream.set_write_timeout(Some(remaining_until(deadline)?))?;
        let written = stream.write(request).map_err(|error| {
            if Instant::now() >= deadline {
                request_timeout()
            } else {
                error
            }
        })?;
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "connection closed while sending HTTP request",
            ));
        }
        request = &request[written..];
    }

    let mut response = Vec::new();
    let mut chunk = [0; 4096];
    loop {
        stream.set_read_timeout(Some(remaining_until(deadline)?))?;
        let count = stream.read(&mut chunk).map_err(|error| {
            if Instant::now() >= deadline {
                request_timeout()
            } else {
                error
            }
        })?;
        if count == 0 {
            break;
        }
        if response.len() + count > MAX_RESPONSE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("HTTP response exceeds {MAX_RESPONSE_BYTES} bytes"),
            ));
        }
        response.extend_from_slice(&chunk[..count]);
    }
    let response = String::from_utf8(response).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("HTTP response is not UTF-8: {error}"),
        )
    })?;
    let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "response is missing an HTTP body separator",
        )
    })?;
    let status = head
        .lines()
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "response is missing a status line",
            )
        })?
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "response status is malformed"))?
        .parse()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(HttpResponse {
        status,
        body: body.to_owned(),
    })
}

fn get(server: &ServerProcess, path: &str) -> HttpResponse {
    server.request(&format!(
        "GET {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        server.address
    ))
}

fn put(server: &ServerProcess, key: &str, request_id: &str, value: &str) -> HttpResponse {
    let body = serde_json::json!({"request_id": request_id, "value": value}).to_string();
    server.request(&format!(
        "PUT /items/{key} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        server.address,
        body.len()
    ))
}

#[test]
fn acknowledged_put_and_exact_replay_survive_sigkill_and_restart() {
    let data_dir = tempfile::tempdir().unwrap();
    let first = ServerProcess::start(data_dir.path());

    let acknowledged = put(&first, "greeting", "request-1", "hello");
    assert_eq!(acknowledged.status, 200);
    first.sigkill();

    let restarted = ServerProcess::start(data_dir.path());
    let read = get(&restarted, "/items/greeting");
    assert_eq!(read.status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&read.body).unwrap(),
        serde_json::json!({"key": "greeting", "value": "hello"})
    );
    let conflict = put(&restarted, "greeting", "request-1", "goodbye");
    assert_eq!(conflict.status, 409);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&conflict.body).unwrap()["error"],
        "request_conflict"
    );

    let replay = put(&restarted, "greeting", "request-1", "hello");
    assert_eq!(replay.status, acknowledged.status);
    assert_eq!(replay.body, acknowledged.body);
}

#[test]
fn disconnected_partial_request_leaves_ready_endpoint_healthy() {
    let data_dir = tempfile::tempdir().unwrap();
    let server = ServerProcess::start(data_dir.path());
    let mut client = tcp_stream(server.address, Instant::now() + IO_TIMEOUT).unwrap();
    write!(
        client,
        "PUT /items/partial HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{{\"request_id\":",
        server.address
    )
    .unwrap();
    client.shutdown(Shutdown::Both).unwrap();
    drop(client);

    let ready = server.request(&format!(
        "GET /ready HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        server.address
    ));
    assert_eq!(ready.status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&ready.body).unwrap(),
        serde_json::json!({"ready": true})
    );
}

#[test]
fn http_request_rejects_response_larger_than_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request);
        let _ = stream.write_all(&vec![b'x'; MAX_RESPONSE_BYTES + 1]);
    });

    let error = http_request(address, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("response exceeds"));
    server.join().unwrap();
}

#[test]
fn http_request_enforces_total_response_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0; 1024];
        let _ = stream.read(&mut request);
        for _ in 0..25 {
            if stream.write_all(b"x").is_err() {
                break;
            }
            let _ = stream.flush();
            thread::sleep(Duration::from_millis(100));
        }
    });

    let error = http_request(address, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n").unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    server.join().unwrap();
}
