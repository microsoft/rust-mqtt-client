// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Configuration: parses the environment into a [`Config`], and owns the CLI/usage surface.
//! This is the module that grows when adding new knobs or transports.

use std::num::NonZeroU16;
use std::time::Duration;

use bytes::Bytes;
use ms_mqtt_client::client::{
    ConnectionTransportTlsConfig, ConnectionTransportType, KeepAliveConfig,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Latency,
    Throughput,
    Inbound,
}

impl Mode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Mode::Latency => "latency",
            Mode::Throughput => "throughput",
            Mode::Inbound => "inbound",
        }
    }
}

pub(crate) struct Config {
    // connection
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) tls: bool,
    pub(crate) client_id: String,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) ca: Option<Vec<u8>>,
    pub(crate) cert: Option<Vec<u8>>,
    pub(crate) key: Option<Vec<u8>>,
    pub(crate) connect_timeout: Duration,
    pub(crate) keepalive_secs: u16,
    // workload
    pub(crate) mode: Mode,
    pub(crate) qos: u8,
    pub(crate) topic: String,
    pub(crate) payload: Bytes,
    pub(crate) payload_bytes: usize,
    pub(crate) count: usize,
    pub(crate) warmup: usize,
    pub(crate) inflight: usize,
    pub(crate) interval_us: u64,
    pub(crate) label: String,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self, String> {
        let tls = match env_str("TRANSPORT", "tcp").to_ascii_lowercase().as_str() {
            "tcp" => false,
            "tls" => true,
            other => return Err(format!("unknown TRANSPORT '{other}' (expected tcp|tls)")),
        };

        let mode = match env_str("MODE", "latency").to_ascii_lowercase().as_str() {
            "latency" => Mode::Latency,
            "throughput" => Mode::Throughput,
            "inbound" => Mode::Inbound,
            other => {
                return Err(format!(
                    "unknown MODE '{other}' (expected latency|throughput|inbound)"
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

    pub(crate) fn keep_alive(&self) -> KeepAliveConfig {
        match NonZeroU16::new(self.keepalive_secs) {
            None => KeepAliveConfig::Infinite,
            Some(ping_after) => KeepAliveConfig::Duration {
                ping_after,
                response_timeout: Duration::from_secs(u64::from(self.keepalive_secs)),
            },
        }
    }

    pub(crate) fn transport_type(&self) -> Result<ConnectionTransportType, String> {
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

    /// One-line, human-readable summary of the run configuration (for display).
    pub(crate) fn summary(&self) -> String {
        format!(
            "mode={} transport={} qos={} payload={}B count={} warmup={} inflight={} interval_us={} \
             host={}:{}",
            self.mode.as_str(),
            if self.tls { "tls" } else { "tcp" },
            self.qos,
            self.payload_bytes,
            self.count,
            self.warmup,
            self.inflight,
            self.interval_us,
            self.host,
            self.port,
        )
    }
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

pub(crate) fn wants_help() -> bool {
    std::env::args().any(|a| a == "--help" || a == "-h") || std::env::var("HELP").is_ok()
}

pub(crate) fn print_usage() {
    // The crate-level doc comment (see main.rs) is the canonical reference; reproduce the
    // essentials here.
    print!(
        "\
bench_client — MQTT transport performance harness (env-var driven)

Run against the SAME peer on two builds and compare the `RESULT ...` JSON lines.

Connection:
  HOST, PORT, TRANSPORT(tcp|tls), CLIENT_ID, USERNAME, PASSWORD,
  CA_FILE, CERT_FILE, KEY_FILE, CONNECT_TIMEOUT_SECS, KEEPALIVE_SECS

Workload:
  MODE(latency|throughput|inbound), QOS(0|1), TOPIC, PAYLOAD_BYTES,
  COUNT, WARMUP, INFLIGHT, INTERVAL_US, LABEL

Examples:
  # Small-payload round-trip latency on a hot socket (nodelay/Nagle sensitive)
  MODE=latency QOS=1 PAYLOAD_BYTES=32 COUNT=50000 HOST=peer cargo run -p bench_client --release

  # Large-payload sustained throughput over TLS (crypto path; run a `ROLE=sink TLS=1` peer)
  MODE=throughput QOS=0 PAYLOAD_BYTES=131072 INFLIGHT=64 COUNT=20000 \\
    TRANSPORT=tls PORT=8883 CA_FILE=certs/server.crt HOST=127.0.0.1 \\
    cargo run -p bench_client --release

  # Inbound receive throughput (start `bench_peer ROLE=feed` first, aim HOST/PORT at it)
  MODE=inbound PAYLOAD_BYTES=256 COUNT=100000 HOST=127.0.0.1 PORT=1883 cargo run -p bench_client --release

Note: the client-isolating workloads run against bench_peer, not a real broker
  (ROLE=feed for inbound; ROLE=sink for latency/throughput). See `cargo run -p bench_peer -- --help`.
Tip: measure CPU-per-message with `/usr/bin/time -v` and inject RTT on loopback with `tc netem`.
"
    );
}
