// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Transport performance harness for detecting regressions across branches.
//!
//! This is a manual, environment-variable-driven load generator that connects to a real broker
//! and exercises the transport under a chosen workload, reporting latency percentiles and
//! throughput. It is intended to be run against the same broker on two builds (e.g. `main` vs.
//! a refactor branch) and the numbers compared by hand.
//!
//! # Why several modes
//!
//! Different regressions only surface under specific regimes, so pick the mode that stresses the
//! thing you care about:
//!
//! - `latency`  — serialized round-trips (one op in flight). Sensitive to `TCP_NODELAY`/Nagle and
//!   per-op overhead. Use a small payload and `INTERVAL_US=0` to keep the socket hot, or set an
//!   interval to model a steady drip (an idle socket resets the congestion window and coalescing
//!   behaves differently — that's the "hot socket" effect).
//! - `throughput` — many ops in flight (`INFLIGHT`). Sensitive to the crypto/copy data path. Use a
//!   large payload over TLS to catch the kernel-TLS-removal regression (extra userspace crypto CPU
//!   and copies). Watch CPU, not just msg/s (see below).
//! - `echo` — subscribe to the publish topic and measure full publish -> broker -> receive latency.
//!   Exercises both the writer and reader paths.
//!
//! # Isolating confounders (run these OUTSIDE the harness)
//!
//! - CPU cost: wrap the run in `/usr/bin/time -v` (look at "User time"/"System time") or `perf stat`
//!   to measure CPU-per-message. Throughput can look unchanged while CPU regresses.
//! - Noise: pin to isolated cores with `taskset -c 2,3`, disable turbo/frequency scaling, and run
//!   both builds back-to-back on the SAME machine, alternating, several trials each.
//! - RTT: a loopback broker hides Nagle/nodelay differences. Inject latency on the loopback with
//!   `tc qdisc add dev lo root netem delay 5ms` (remove with `tc qdisc del dev lo root`) so the
//!   `latency` mode actually exercises coalescing behavior.
//!
//! # Usage
//!
//! All configuration is via environment variables. From this crate directory, run
//! `cargo run --release -- --help` (or `HELP=1 cargo run --release`) to print this list.
//!
//! Connection:
//!   HOST         broker hostname                       (default: localhost)
//!   PORT         broker port                           (default: 1883, or 8883 when TRANSPORT=tls)
//!   TRANSPORT    tcp | tls                             (default: tcp)
//!   CLIENT_ID    MQTT client id                        (default: perf-harness-<pid>)
//!   USERNAME     MQTT username                         (optional)
//!   PASSWORD     MQTT password                         (optional)
//!   CA_FILE      PEM CA trust bundle path (TLS)        (optional; empty = use system defaults off)
//!   CERT_FILE    PEM client cert chain path (TLS)      (optional)
//!   KEY_FILE     PEM client private key path (TLS)     (optional; required if CERT_FILE set)
//!   CONNECT_TIMEOUT_SECS  transport+CONNECT timeout    (default: 30)
//!   KEEPALIVE_SECS        MQTT keepalive, 0 = infinite (default: 0)
//!   TCP_NODELAY  1/0 — only effective on branches whose API exposes it (ignored here)
//!
//! Workload:
//!   MODE         latency | throughput | echo          (default: latency)
//!   QOS          0 | 1                                 (default: 1; QoS 2 not implemented)
//!   TOPIC        topic to publish/subscribe            (default: perf/harness/<pid>)
//!   PAYLOAD_BYTES payload size in bytes                (default: 64)
//!   COUNT        measured operations                   (default: 10000)
//!   WARMUP       discarded warmup operations           (default: 1000)
//!   INFLIGHT     concurrent ops (throughput mode)      (default: 32)
//!   INTERVAL_US  sleep between ops (latency mode), us  (default: 0 = hot)
//!   LABEL        free-form tag echoed into output      (default: empty)
//!
//! Output: a human-readable summary plus a single machine-readable line prefixed `RESULT ` containing
//! a JSON object, so runs can be scraped and diffed.

// Benchmark utility: intentional lossy numeric casts (ns/us conversions, percentile indexing) and
// prose-heavy docs are fine here.
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::doc_markdown
)]

use std::num::NonZeroU16;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ms_mqtt_client::client::{
    Client, ClientOptions, ConnectResult, Connection, ConnectionTransportConfig,
    ConnectionTransportTlsConfig, ConnectionTransportType, DisconnectHandle, KeepAliveConfig,
    Receiver, new_client,
};
use ms_mqtt_client::packet::{
    ConnectProperties, DisconnectProperties, PublishProperties, QoS, RetainOptions,
    SubscribeProperties,
};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
use tokio::task::JoinSet;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Latency,
    Throughput,
    Echo,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Latency => "latency",
            Mode::Throughput => "throughput",
            Mode::Echo => "echo",
        }
    }
}

struct Config {
    // connection
    host: String,
    port: u16,
    tls: bool,
    client_id: String,
    username: Option<String>,
    password: Option<String>,
    ca: Option<Vec<u8>>,
    cert: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    connect_timeout: Duration,
    keepalive_secs: u16,
    // workload
    mode: Mode,
    qos: u8,
    topic: String,
    payload: Bytes,
    payload_bytes: usize,
    count: usize,
    warmup: usize,
    inflight: usize,
    interval_us: u64,
    label: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let tls = match env_str("TRANSPORT", "tcp").to_ascii_lowercase().as_str() {
            "tcp" => false,
            "tls" => true,
            other => return Err(format!("unknown TRANSPORT '{other}' (expected tcp|tls)")),
        };

        let mode = match env_str("MODE", "latency").to_ascii_lowercase().as_str() {
            "latency" => Mode::Latency,
            "throughput" => Mode::Throughput,
            "echo" => Mode::Echo,
            other => {
                return Err(format!(
                    "unknown MODE '{other}' (expected latency|throughput|echo)"
                ));
            }
        };

        let qos = env_u64("QOS", 1) as u8;
        if qos > 1 {
            return Err("QOS 2 is not implemented by this harness; use QOS=0 or QOS=1".to_string());
        }

        let pid = std::process::id();
        let payload_bytes = env_usize("PAYLOAD_BYTES", 64);
        let default_port: u16 = if tls { 8883 } else { 1883 };

        let ca = read_optional_file("CA_FILE")?;
        let cert = read_optional_file("CERT_FILE")?;
        let key = read_optional_file("KEY_FILE")?;
        if cert.is_some() != key.is_some() {
            return Err("CERT_FILE and KEY_FILE must be set together".to_string());
        }

        Ok(Self {
            host: env_str("HOST", "localhost"),
            port: env_u64("PORT", u64::from(default_port)) as u16,
            tls,
            client_id: env_str("CLIENT_ID", &format!("perf-harness-{pid}")),
            username: std::env::var("USERNAME").ok().filter(|s| !s.is_empty()),
            password: std::env::var("PASSWORD").ok().filter(|s| !s.is_empty()),
            ca,
            cert,
            key,
            connect_timeout: Duration::from_secs(env_u64("CONNECT_TIMEOUT_SECS", 30)),
            keepalive_secs: env_u64("KEEPALIVE_SECS", 0) as u16,
            mode,
            qos,
            topic: env_str("TOPIC", &format!("perf/harness/{pid}")),
            payload: Bytes::from(vec![b'x'; payload_bytes]),
            payload_bytes,
            count: env_usize("COUNT", 10_000),
            warmup: env_usize("WARMUP", 1_000),
            inflight: env_usize("INFLIGHT", 32).max(1),
            interval_us: env_u64("INTERVAL_US", 0),
            label: env_str("LABEL", ""),
        })
    }

    fn keep_alive(&self) -> KeepAliveConfig {
        match NonZeroU16::new(self.keepalive_secs) {
            None => KeepAliveConfig::Infinite,
            Some(ping_after) => KeepAliveConfig::Duration {
                ping_after,
                response_timeout: Duration::from_secs(u64::from(self.keepalive_secs)),
            },
        }
    }

    fn transport_type(&self) -> Result<ConnectionTransportType, String> {
        if self.tls {
            let client_cert = match (&self.cert, &self.key) {
                (Some(cert), Some(key)) => Some((cert.as_slice(), key.as_slice())),
                _ => None,
            };
            let ca = self.ca.as_deref().unwrap_or(&[]);
            let config = ConnectionTransportTlsConfig::from_pem(client_cert, ca)
                .map_err(|e| format!("failed to build TLS config: {e}"))?;
            Ok(ConnectionTransportType::Tls {
                hostname: self.host.clone(),
                port: self.port,
                config,
            })
        } else {
            Ok(ConnectionTransportType::Tcp {
                hostname: self.host.clone(),
                port: self.port,
            })
        }
    }
}

struct Report {
    cfg_summary: String,
    label: String,
    mode: &'static str,
    transport: &'static str,
    qos: u8,
    payload_bytes: usize,
    inflight: usize,
    interval_us: u64,
    count: usize,
    wall: Duration,
    latencies_ns: Vec<u64>,
}

impl Report {
    fn print(&self) {
        let wall_s = self.wall.as_secs_f64();
        let msgs_per_s = if wall_s > 0.0 {
            self.count as f64 / wall_s
        } else {
            0.0
        };
        let bytes = (self.count as f64) * (self.payload_bytes as f64);
        let mb_per_s = if wall_s > 0.0 {
            bytes / wall_s / (1024.0 * 1024.0)
        } else {
            0.0
        };

        let mut sorted = self.latencies_ns.clone();
        sorted.sort_unstable();
        let min = us(sorted.first().copied().unwrap_or(0));
        let max = us(sorted.last().copied().unwrap_or(0));
        let mean = if sorted.is_empty() {
            0.0
        } else {
            sorted.iter().sum::<u64>() as f64 / sorted.len() as f64 / 1000.0
        };
        let p50 = us(pct(&sorted, 50.0));
        let p90 = us(pct(&sorted, 90.0));
        let p99 = us(pct(&sorted, 99.0));
        let p999 = us(pct(&sorted, 99.9));

        println!();
        println!("==== network_bench result ====");
        if !self.label.is_empty() {
            println!("label:        {}", self.label);
        }
        println!("config:       {}", self.cfg_summary);
        println!("measured ops: {}", self.count);
        println!("wall time:    {wall_s:.3} s");
        println!("throughput:   {msgs_per_s:.1} msg/s   {mb_per_s:.2} MiB/s (payload only)");
        println!(
            "latency (us): min={min:.1}  p50={p50:.1}  p90={p90:.1}  p99={p99:.1}  \
             p99.9={p999:.1}  max={max:.1}  mean={mean:.1}"
        );
        println!("note:         measure CPU-per-msg externally, e.g. `/usr/bin/time -v ...`");
        println!("=============================");

        // Machine-readable line for scraping / diffing across branches.
        println!(
            "RESULT {{\"label\":\"{}\",\"mode\":\"{}\",\"transport\":\"{}\",\"qos\":{},\
             \"payload_bytes\":{},\"inflight\":{},\"interval_us\":{},\"count\":{},\
             \"wall_s\":{:.6},\"msgs_per_s\":{:.3},\"mib_per_s\":{:.3},\
             \"lat_us\":{{\"min\":{:.3},\"p50\":{:.3},\"p90\":{:.3},\"p99\":{:.3},\
             \"p999\":{:.3},\"max\":{:.3},\"mean\":{:.3}}}}}",
            self.label,
            self.mode,
            self.transport,
            self.qos,
            self.payload_bytes,
            self.inflight,
            self.interval_us,
            self.count,
            wall_s,
            msgs_per_s,
            mb_per_s,
            min,
            p50,
            p90,
            p99,
            p999,
            max,
            mean,
        );
    }
}

#[tokio::main]
async fn main() {
    if wants_help() {
        print_usage();
        return;
    }

    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("ERROR: {e}");
            eprintln!("(run with --help for usage)");
            std::process::exit(2);
        }
    };

    if let Err(e) = run(cfg).await {
        eprintln!("ERROR: {e}");
        std::process::exit(1);
    }
}

async fn run(cfg: Config) -> Result<(), String> {
    let options = ClientOptions {
        client_id: Some(cfg.client_id.clone()),
        // Keep queues comfortably above INFLIGHT so we measure the transport, not client-side
        // backpressure on the internal channels.
        publish_qos0_queue_size: cfg.inflight.saturating_mul(4).max(1024),
        publish_qos1_qos2_queue_size: cfg.inflight.saturating_mul(4).max(1024),
        ..Default::default()
    };
    let (client, connect_handle, receiver) = new_client(options);

    let transport = ConnectionTransportConfig {
        transport_type: cfg.transport_type()?,
        timeout: Some(cfg.connect_timeout),
    };

    eprintln!(
        "connecting to {}:{} ({})...",
        cfg.host,
        cfg.port,
        if cfg.tls { "tls" } else { "tcp" }
    );

    let connect_result = connect_handle
        .connect(
            transport,
            true, // clean_start
            cfg.keep_alive(),
            None, // will
            cfg.username.clone(),
            cfg.password.clone().map(Bytes::from),
            ConnectProperties::default(),
            Some(cfg.connect_timeout),
        )
        .await;

    let (connection, _connack, disconnect_handle): (Connection, _, DisconnectHandle) =
        match connect_result {
            ConnectResult::Success(connection, connack, disconnect_handle) => {
                (connection, connack, disconnect_handle)
            }
            ConnectResult::Failure(_handle, err) => {
                return Err(format!("connect failed: {err}"));
            }
        };

    eprintln!("connected; running '{}' workload...", cfg.mode.as_str());

    // Drive the connection future concurrently with the workload; the workload awaits completion
    // tokens/received messages, which only make progress while the connection is polled.
    let workload = run_workload(client, receiver, &cfg);
    tokio::pin!(workload);
    let conn_fut = connection.run_until_disconnect();
    tokio::pin!(conn_fut);

    let report = tokio::select! {
        _ = &mut conn_fut => {
            return Err("connection ended before the workload completed".to_string());
        }
        result = &mut workload => result?,
    };

    // Clean shutdown: request DISCONNECT and let the connection future flush it.
    let _ = disconnect_handle.disconnect(&DisconnectProperties::default());
    let _ = conn_fut.await;

    report.print();
    Ok(())
}

async fn run_workload(client: Client, receiver: Receiver, cfg: &Config) -> Result<Report, String> {
    let (latencies_ns, wall) = match cfg.mode {
        Mode::Latency => run_latency(&client, cfg).await?,
        Mode::Throughput => run_throughput(&client, cfg).await?,
        Mode::Echo => run_echo(&client, receiver, cfg).await?,
    };

    Ok(Report {
        cfg_summary: summarize(cfg),
        label: cfg.label.clone(),
        mode: cfg.mode.as_str(),
        transport: if cfg.tls { "tls" } else { "tcp" },
        qos: cfg.qos,
        payload_bytes: cfg.payload_bytes,
        inflight: if cfg.mode == Mode::Throughput {
            cfg.inflight
        } else {
            1
        },
        interval_us: cfg.interval_us,
        count: latencies_ns.len(),
        wall,
        latencies_ns,
    })
}

/// Serialized round-trips: one operation in flight at a time. Returns per-op latencies (excluding
/// any inter-op sleep) and the total wall time of the measured loop (including sleeps).
async fn run_latency(client: &Client, cfg: &Config) -> Result<(Vec<u64>, Duration), String> {
    for _ in 0..cfg.warmup {
        publish_once(client, cfg.qos, &cfg.topic, cfg.payload.clone()).await?;
    }

    let mut latencies = Vec::with_capacity(cfg.count);
    let start = Instant::now();
    for _ in 0..cfg.count {
        let op_start = Instant::now();
        publish_once(client, cfg.qos, &cfg.topic, cfg.payload.clone()).await?;
        latencies.push(elapsed_ns(op_start));
        if cfg.interval_us > 0 {
            tokio::time::sleep(Duration::from_micros(cfg.interval_us)).await;
        }
    }
    let wall = start.elapsed();
    Ok((latencies, wall))
}

/// Many operations in flight (bounded by `INFLIGHT`), measuring aggregate throughput. Per-op
/// latency is also recorded but includes client-side queueing at high concurrency.
async fn run_throughput(client: &Client, cfg: &Config) -> Result<(Vec<u64>, Duration), String> {
    // Warmup (not recorded).
    pipeline(client, cfg, cfg.warmup, None).await?;

    let mut latencies = Vec::with_capacity(cfg.count);
    let start = Instant::now();
    pipeline(client, cfg, cfg.count, Some(&mut latencies)).await?;
    let wall = start.elapsed();
    Ok((latencies, wall))
}

/// Full-path latency: publish to a topic we are subscribed to and measure until it is received.
async fn run_echo(
    client: &Client,
    mut receiver: Receiver,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration), String> {
    let filter =
        TopicFilter::new(cfg.topic.as_str()).map_err(|e| format!("invalid TOPIC filter: {e:?}"))?;
    let token = client
        .subscribe(
            filter,
            qos_enum(cfg.qos),
            false,
            RetainOptions::default(),
            SubscribeProperties::default(),
        )
        .await
        .map_err(|_| "subscribe rejected: client detached".to_string())?;
    token.await.map_err(|e| format!("SUBACK failed: {e}"))?;

    for _ in 0..cfg.warmup {
        publish_once(client, cfg.qos, &cfg.topic, cfg.payload.clone()).await?;
        receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during warmup".to_string())?;
    }

    let mut latencies = Vec::with_capacity(cfg.count);
    let start = Instant::now();
    for _ in 0..cfg.count {
        let op_start = Instant::now();
        publish_once(client, cfg.qos, &cfg.topic, cfg.payload.clone()).await?;
        receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during measurement".to_string())?;
        latencies.push(elapsed_ns(op_start));
    }
    let wall = start.elapsed();
    Ok((latencies, wall))
}

/// Runs `n` publishes with at most `cfg.inflight` outstanding, optionally recording per-op latency.
async fn pipeline(
    client: &Client,
    cfg: &Config,
    n: usize,
    mut record: Option<&mut Vec<u64>>,
) -> Result<(), String> {
    let mut set: JoinSet<Result<u64, String>> = JoinSet::new();
    let mut spawned = 0usize;
    let mut completed = 0usize;

    while completed < n {
        while spawned < n && set.len() < cfg.inflight {
            let client = client.clone();
            let topic = cfg.topic.clone();
            let payload = cfg.payload.clone();
            let qos = cfg.qos;
            set.spawn(async move {
                let start = Instant::now();
                publish_once(&client, qos, &topic, payload).await?;
                Ok(elapsed_ns(start))
            });
            spawned += 1;
        }

        if let Some(joined) = set.join_next().await {
            let latency = joined.map_err(|e| format!("task join error: {e}"))??;
            if let Some(v) = record.as_deref_mut() {
                v.push(latency);
            }
            completed += 1;
        }
    }
    Ok(())
}

/// Publishes a single message and awaits its completion (QoS 0 = sent on the wire; QoS 1 = PUBACK).
async fn publish_once(client: &Client, qos: u8, topic: &str, payload: Bytes) -> Result<(), String> {
    let topic_name = TopicName::new(topic).map_err(|e| format!("invalid TOPIC name: {e:?}"))?;
    match qos {
        0 => {
            let token = client
                .publish_qos0(topic_name, payload, false, PublishProperties::default())
                .await
                .map_err(|_| "publish_qos0 rejected: client detached".to_string())?;
            token
                .await
                .map_err(|e| format!("QoS0 completion failed: {e}"))?;
        }
        1 => {
            let token = client
                .publish_qos1(topic_name, payload, false, PublishProperties::default())
                .await
                .map_err(|_| "publish_qos1 rejected: client detached".to_string())?;
            token.await.map_err(|e| format!("PUBACK failed: {e}"))?;
        }
        _ => return Err("QoS 2 is not implemented by this harness".to_string()),
    }
    Ok(())
}

// ---- small helpers -------------------------------------------------------------------------

fn qos_enum(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        _ => QoS::AtLeastOnce,
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1000.0
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p / 100.0) * ((sorted.len() - 1) as f64);
    let idx = rank.round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn summarize(cfg: &Config) -> String {
    format!(
        "mode={} transport={} qos={} payload={}B count={} warmup={} inflight={} interval_us={} \
         host={}:{}",
        cfg.mode.as_str(),
        if cfg.tls { "tls" } else { "tcp" },
        cfg.qos,
        cfg.payload_bytes,
        cfg.count,
        cfg.warmup,
        cfg.inflight,
        cfg.interval_us,
        cfg.host,
        cfg.port,
    )
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn read_optional_file(key: &str) -> Result<Option<Vec<u8>>, String> {
    match std::env::var(key) {
        Ok(path) if !path.is_empty() => std::fs::read(&path)
            .map(Some)
            .map_err(|e| format!("failed to read {key}='{path}': {e}")),
        _ => Ok(None),
    }
}

fn wants_help() -> bool {
    std::env::args().any(|a| a == "--help" || a == "-h") || std::env::var("HELP").is_ok()
}

fn print_usage() {
    // The module-level doc comment is the canonical reference; reproduce the essentials here.
    print!(
        "\
network_bench — MQTT transport performance harness (env-var driven)

Run against the SAME broker on two builds and compare the `RESULT ...` JSON lines.

Connection:
  HOST, PORT, TRANSPORT(tcp|tls), CLIENT_ID, USERNAME, PASSWORD,
  CA_FILE, CERT_FILE, KEY_FILE, CONNECT_TIMEOUT_SECS, KEEPALIVE_SECS

Workload:
  MODE(latency|throughput|echo), QOS(0|1), TOPIC, PAYLOAD_BYTES,
  COUNT, WARMUP, INFLIGHT, INTERVAL_US, LABEL

Examples:
  # Small-payload round-trip latency on a hot socket (nodelay/Nagle sensitive)
  MODE=latency QOS=1 PAYLOAD_BYTES=32 COUNT=50000 HOST=broker cargo run --release

  # Large-payload sustained throughput over TLS (kTLS-removal / crypto-path sensitive)
  MODE=throughput QOS=0 PAYLOAD_BYTES=131072 INFLIGHT=64 COUNT=20000 \\
    TRANSPORT=tls PORT=8883 CA_FILE=ca.pem CERT_FILE=client.pem KEY_FILE=client.key \\
    HOST=broker cargo run --release

  # Full publish->receive path latency
  MODE=echo QOS=1 PAYLOAD_BYTES=256 COUNT=20000 HOST=broker cargo run --release

Tip: measure CPU-per-message with `/usr/bin/time -v` and inject RTT on loopback with `tc netem`.
"
    );
}
