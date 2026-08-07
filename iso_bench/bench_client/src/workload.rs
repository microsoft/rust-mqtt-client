// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Workload execution: drives the chosen `MODE` against the transport and records per-op
//! latencies. This is the module that grows when adding modes (a new [`Mode`] variant maps to
//! one new `run_*` function here).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use ms_mqtt_client::client::{Client, ManualAcknowledgement, Receiver};
use ms_mqtt_client::packet::{PubAckProperties, PublishProperties};
use ms_mqtt_client::topic::TopicName;
use tokio::task::JoinSet;

use crate::config::{Config, Mode};
use crate::report::Report;
use crate::usage::{Usage, Window};

pub(crate) async fn run_workload(
    client: Client,
    receiver: Receiver,
    cfg: &Config,
) -> Result<Report, String> {
    let (latencies_ns, wall, usage) = match cfg.mode {
        Mode::PubLatency => run_pub_latency(&client, cfg).await?,
        Mode::PubThroughput => run_pub_throughput(&client, cfg).await?,
        Mode::RecvThroughput => run_recv_throughput(receiver, cfg).await?,
        Mode::RecvLatency => run_recv_latency(receiver, cfg).await?,
    };

    Ok(Report {
        cfg_summary: cfg.summary(),
        label: cfg.label.clone(),
        mode: cfg.mode.as_str(),
        transport: if cfg.tls { "tls" } else { "tcp" },
        qos: cfg.qos,
        payload_bytes: cfg.payload_bytes,
        inflight: if cfg.mode == Mode::PubThroughput {
            cfg.inflight
        } else {
            1
        },
        interval_us: cfg.interval_us,
        target_rate: cfg.target_rate,
        latency_kind: match cfg.mode {
            Mode::RecvThroughput => "inter-arrival",
            Mode::RecvLatency => "delivery latency",
            _ => "op latency",
        },
        count: latencies_ns.len(),
        wall,
        usage,
        latencies_ns,
    })
}

/// Publish round-trips. Closed-loop (`TARGET_RATE=0`) or open-loop (`TARGET_RATE>0`).
async fn run_pub_latency(
    client: &Client,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration, Usage), String> {
    for _ in 0..cfg.warmup {
        publish_once(client, cfg.qos, &cfg.topic, cfg.payload.clone()).await?;
    }
    if cfg.target_rate > 0.0 {
        run_latency_open_loop(client, cfg).await
    } else {
        run_latency_closed_loop(client, cfg).await
    }
}

/// Serialized round-trips: one operation in flight at a time. Returns per-op latencies (excluding
/// any inter-op sleep) and the total wall time of the measured loop (including sleeps).
async fn run_latency_closed_loop(
    client: &Client,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration, Usage), String> {
    let mut latencies = Vec::with_capacity(cfg.count);
    let win = Window::open();
    for _ in 0..cfg.count {
        let op_start = Instant::now();
        publish_once(client, cfg.qos, &cfg.topic, cfg.payload.clone()).await?;
        latencies.push(elapsed_ns(op_start));
        if cfg.interval_us > 0 {
            tokio::time::sleep(Duration::from_micros(cfg.interval_us)).await;
        }
    }
    let (wall, usage) = win.close();
    Ok((latencies, wall, usage))
}

/// Open-loop round-trips: publishes are issued on a fixed schedule at `cfg.target_rate` msgs/sec,
/// concurrently (a fresh task per op) so a slow response never throttles the send cadence. Latency
/// is measured from each op's INTENDED send time, so stalls aren't under-sampled (coordinated-
/// omission correct). In-flight grows without bound if the target rate exceeds what the client can
/// sustain -- that is the intended overload signal.
async fn run_latency_open_loop(
    client: &Client,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration, Usage), String> {
    let mut set: JoinSet<Result<u64, String>> = JoinSet::new();
    let mut latencies = Vec::with_capacity(cfg.count);
    let win = Window::open();

    // The open-loop schedule is anchored to the window's own start, so `intended` and `wall` share
    // one origin -- a separate Instant::now() here would drift from it by the sampling cost.
    let start = win.started();
    for i in 0..cfg.count {
        let intended = start + Duration::from_secs_f64(i as f64 / cfg.target_rate);
        // tokio's timer is ~1ms-granular, which would swamp us-scale latencies; sleep the coarse
        // part, then busy-spin the last <=~2ms to hit `intended` accurately. The margin must exceed
        // the timer granularity so the coarse sleep can't overshoot past `intended`.
        let spin_margin = Duration::from_millis(2);
        loop {
            let Some(remaining) = intended.checked_duration_since(Instant::now()) else {
                break;
            };
            if remaining > spin_margin {
                tokio::time::sleep(remaining - spin_margin).await;
            } else {
                std::hint::spin_loop();
            }
        }

        let client = client.clone();
        let topic = cfg.topic.clone();
        let payload = cfg.payload.clone();
        let qos = cfg.qos;
        set.spawn(async move {
            publish_once(&client, qos, &topic, payload).await?;
            let latency = Instant::now().saturating_duration_since(intended);
            Ok(u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX))
        });

        // Reap completed ops so the JoinSet tracks only in-flight, not every op ever spawned.
        while let Some(joined) = set.try_join_next() {
            latencies.push(joined.map_err(|e| format!("task join error: {e}"))??);
        }
    }

    while let Some(joined) = set.join_next().await {
        latencies.push(joined.map_err(|e| format!("task join error: {e}"))??);
    }
    let (wall, usage) = win.close();
    Ok((latencies, wall, usage))
}

/// Many operations in flight (bounded by `INFLIGHT`), measuring aggregate throughput. Per-op
/// latency is also recorded but includes client-side queueing at high concurrency.
async fn run_pub_throughput(
    client: &Client,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration, Usage), String> {
    // Warmup (not recorded).
    pipeline(client, cfg, cfg.warmup, None).await?;

    let mut latencies = Vec::with_capacity(cfg.count);
    let win = Window::open();
    pipeline(client, cfg, cfg.count, Some(&mut latencies)).await?;
    let (wall, usage) = win.close();
    Ok((latencies, wall, usage))
}

/// Inbound receive throughput: drain the `Receiver` and record inter-arrival gaps. The producer is
/// an external peer (`bench_peer ROLE=feed`), so this measures only the client's receive path
/// (read → decode → deliver), not broker behavior. QoS 1 publishes are PUBACK'd (the receive-side
/// ack path); QoS 0 need no ack.
async fn run_recv_throughput(
    mut receiver: Receiver,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration, Usage), String> {
    for _ in 0..cfg.warmup {
        let (_publish, ack) = receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during warmup".to_string())?;
        ack_incoming(ack).await?;
    }

    let mut gaps = Vec::with_capacity(cfg.count);
    let win = Window::open();
    let mut last = win.started();
    for _ in 0..cfg.count {
        let (_publish, ack) = receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during measurement".to_string())?;
        let now = Instant::now();
        gaps.push(u64::try_from(now.duration_since(last).as_nanos()).unwrap_or(u64::MAX));
        last = now;
        ack_incoming(ack).await?;
    }
    let (wall, usage) = win.close();
    Ok((gaps, wall, usage))
}

/// Receive-path delivery latency: the peer stamps each publish's payload with its send time (epoch
/// nanos, see bench_peer `STAMP`), and we record `now - stamp` at delivery. Unlike inter-arrival,
/// this catches a *uniform* added delivery delay (buffering/scheduling) that leaves the rate
/// unchanged. Relies on a shared wall clock, i.e. a single host -- which the whole tool assumes.
async fn run_recv_latency(
    mut receiver: Receiver,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration, Usage), String> {
    if cfg.payload_bytes < 8 {
        return Err(
            "recv-latency needs PAYLOAD_BYTES >= 8 (payload carries an 8-byte send stamp)"
                .to_string(),
        );
    }
    for _ in 0..cfg.warmup {
        let (_publish, ack) = receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during warmup".to_string())?;
        ack_incoming(ack).await?;
    }

    let mut latencies = Vec::with_capacity(cfg.count);
    let win = Window::open();
    for _ in 0..cfg.count {
        let (publish, ack) = receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during measurement".to_string())?;
        let now = epoch_nanos();
        let stamp = publish
            .payload
            .get(..8)
            .map(|b| u64::from_le_bytes(b.try_into().expect("slice of 8 is 8 bytes")))
            .ok_or_else(|| "received payload too small for send stamp".to_string())?;
        latencies.push(now.saturating_sub(stamp));
        ack_incoming(ack).await?;
    }
    let (wall, usage) = win.close();
    Ok((latencies, wall, usage))
}

/// Wall-clock nanoseconds since the Unix epoch -- comparable across processes on one host, so the
/// peer's send stamp and the client's receive time can be differenced.
fn epoch_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// PUBACKs a received QoS 1 publish (QoS 0 needs nothing). Unacked QoS 1 holds receive-side session
/// state, so the harness must ack to avoid unbounded growth. Acking is serial on purpose: the client
/// sends PUBACKs through a single connection + capacity-1 channel anyway, so harness-side ack
/// parallelism can't parallelize it -- it only floods the runtime and starves that connection task.
async fn ack_incoming(ack: ManualAcknowledgement) -> Result<(), String> {
    match ack {
        ManualAcknowledgement::QoS0 => {}
        ManualAcknowledgement::QoS1(token) => {
            token
                .accept(PubAckProperties::default())
                .await
                .map_err(|e| format!("PUBACK accept failed: {e:?}"))?;
        }
        ManualAcknowledgement::QoS2(_) => {
            return Err("QoS 2 inbound is not supported by this harness".to_string());
        }
    }
    Ok(())
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
            // QoS 0 token resolves at queue admission, before encode + write (see bench.sh: no QoS 0
            // latency config).
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

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
