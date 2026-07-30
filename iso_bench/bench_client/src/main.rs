// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Transport performance harness for detecting regressions across builds.
//!
//! This is a manual, environment-variable-driven load generator that connects to a peer
//! (typically `bench_peer` from this workspace for isolation, but it can also target a real broker)
//! and exercises the transport under a chosen workload, reporting latency percentiles and
//! throughput. It is intended to be run against the same peer on two builds of the client and
//!
//! # Why several modes
//!
//! Different regressions only surface under specific regimes, so pick the mode that stresses the
//! thing you care about:
//!
//! - `pub-latency`  — serialized publish round-trips (one op in flight). Sensitive to
//!   `TCP_NODELAY`/Nagle and per-op overhead. Use a small payload and `INTERVAL_US=0` to keep the
//!   socket hot, or set an interval to model a steady drip (an idle socket resets the congestion
//!   window and coalescing behaves differently — the "hot socket" effect). `TARGET_RATE>0` switches
//!   to open-loop (coordinated-omission-correct latency under a fixed offered rate).
//! - `pub-throughput` — many publishes in flight (`INFLIGHT`). Sensitive to the crypto/copy data
//!   path. Use a large payload (and TLS) to stress per-byte CPU and copy costs. Watch CPU, not just
//!   msg/s (see below).
//! - `recv-throughput` — the client's *receive* throughput. An external peer (`bench_peer
//!   ROLE=feed`) firehoses PUBLISHes at the client; this drains the `Receiver` and reports receive
//!   throughput plus inter-arrival jitter, isolating the read/decode path from broker behavior.
//! - `recv-latency` — per-message *delivery* latency (wire → app). The peer stamps each publish's
//!   payload with its send time and the client records `now - stamp` at delivery. Unlike
//!   inter-arrival, this catches a uniform delivery delay that leaves the rate unchanged.
//!
//! # Isolating confounders (run these OUTSIDE the harness)
//!
//! - CPU cost: wrap the run in `/usr/bin/time -v` (look at "User time"/"System time") or `perf stat`
//!   to measure CPU-per-message. Throughput can look unchanged while CPU regresses.
//! - Noise: pin to isolated cores with `taskset -c 2,3`, disable turbo/frequency scaling, and run
//!   both builds back-to-back on the SAME machine, alternating, several trials each.
//! - RTT: a loopback broker hides Nagle/nodelay differences. Inject latency on the loopback with
//!   `tc qdisc add dev lo root netem delay 5ms` (remove with `tc qdisc del dev lo root`) so the
//!   `pub-latency` mode actually exercises coalescing behavior.
//!
//! # Usage
//!
//! All configuration is via environment variables. From the `iso_bench/` workspace, run
//! `cargo run -p bench_client --release -- --help` (or `HELP=1 cargo run -p bench_client --release`)
//! to print this list.
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
//!   TCP_NODELAY  1/0 — applied only if the client API exposes it (currently ignored)
//!
//! Workload:
//!   MODE         pub-latency | pub-throughput | recv-throughput | recv-latency (default: pub-latency)
//!   QOS          0 | 1                                 (default: 1; QoS 2 not implemented)
//!   TOPIC        topic to publish/subscribe            (default: perf/harness/<pid>)
//!   PAYLOAD_BYTES payload size in bytes                (default: 64)
//!   COUNT        measured operations                   (default: 10000)
//!   WARMUP       discarded warmup operations           (default: 1000)
//!   INFLIGHT     concurrent ops (pub-throughput mode)  (default: 32)
//!   INTERVAL_US  sleep between ops (pub-latency), us    (default: 0 = hot)
//!   TARGET_RATE  open-loop pub-latency rate, msg/s      (default: 0 = closed-loop/sequential)
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

mod config;
mod report;
mod workload;

use bytes::Bytes;
use ms_mqtt_client::client::{
    ClientOptions, ConnectResult, Connection, ConnectionTransportConfig, DisconnectHandle,
    new_client,
};
use ms_mqtt_client::packet::{ConnectProperties, DisconnectProperties};

use crate::config::{Config, print_usage, wants_help};
use crate::workload::run_workload;

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

    // Drive the connection on its own task so the reader and the workload run concurrently.
    // This matters for the `recv-*` modes: the incoming-publish channel is unbounded, so a reader
    // co-scheduled with the consumer in a single `select!` task can starve it under a firehose
    // and grow memory without bound.
    let conn_task = tokio::spawn(async move { connection.run_until_disconnect().await });

    let report = run_workload(client, receiver, &cfg).await?;

    // Clean shutdown: request DISCONNECT and let the connection task flush it.
    let _ = disconnect_handle.disconnect(&DisconnectProperties::default());
    let _ = conn_task.await;

    report.print();
    Ok(())
}
