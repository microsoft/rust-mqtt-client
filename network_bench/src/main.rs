// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Transport performance harness for detecting regressions across builds.
//!
//! This is a manual, environment-variable-driven load generator that connects to a real broker
//! and exercises the transport under a chosen workload, reporting latency percentiles and
//! throughput. It is intended to be run against the same broker on two builds of the client and
//! the numbers compared by hand.
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
//!   large payload (and TLS) to stress per-byte CPU and copy costs. Watch CPU, not just msg/s
//!   (see below).
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
//!   TCP_NODELAY  1/0 — applied only if the client API exposes it (currently ignored)
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
