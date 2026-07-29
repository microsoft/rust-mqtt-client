// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Workload execution: drives the chosen `MODE` against the transport and records per-op
//! latencies. This is the module that grows when adding modes (a new [`Mode`] variant maps to
//! one new `run_*` function here).

use std::time::{Duration, Instant};

use bytes::Bytes;
use ms_mqtt_client::client::{Client, Receiver};
use ms_mqtt_client::packet::PublishProperties;
use ms_mqtt_client::topic::TopicName;
use tokio::task::JoinSet;

use crate::config::{Config, Mode};
use crate::report::Report;

pub(crate) async fn run_workload(
    client: Client,
    receiver: Receiver,
    cfg: &Config,
) -> Result<Report, String> {
    let (latencies_ns, wall) = match cfg.mode {
        Mode::Latency => run_latency(&client, cfg).await?,
        Mode::Throughput => run_throughput(&client, cfg).await?,
        Mode::Inbound => run_inbound(receiver, cfg).await?,
    };

    Ok(Report {
        cfg_summary: cfg.summary(),
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
        target_rate: cfg.target_rate,
        latency_kind: match cfg.mode {
            Mode::Inbound => "inter-arrival",
            _ => "op latency",
        },
        count: latencies_ns.len(),
        wall,
        latencies_ns,
    })
}

/// Latency round-trips. Closed-loop (`TARGET_RATE=0`) or open-loop (`TARGET_RATE>0`).
async fn run_latency(client: &Client, cfg: &Config) -> Result<(Vec<u64>, Duration), String> {
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
) -> Result<(Vec<u64>, Duration), String> {
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

/// Open-loop round-trips: publishes are issued on a fixed schedule at `cfg.target_rate` msgs/sec,
/// concurrently (a fresh task per op) so a slow response never throttles the send cadence. Latency
/// is measured from each op's INTENDED send time, so stalls aren't under-sampled (coordinated-
/// omission correct). In-flight grows without bound if the target rate exceeds what the client can
/// sustain -- that is the intended overload signal.
async fn run_latency_open_loop(
    client: &Client,
    cfg: &Config,
) -> Result<(Vec<u64>, Duration), String> {
    let mut set: JoinSet<Result<u64, String>> = JoinSet::new();
    let mut latencies = Vec::with_capacity(cfg.count);
    let start = Instant::now();

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

/// Inbound receive throughput: drain the `Receiver` and record inter-arrival gaps. The producer is
/// an external peer (`bench_peer ROLE=feed`), so this measures only the client's receive path
/// (read → decode → deliver), not broker behavior.
async fn run_inbound(mut receiver: Receiver, cfg: &Config) -> Result<(Vec<u64>, Duration), String> {
    for _ in 0..cfg.warmup {
        receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during warmup".to_string())?;
    }

    let mut gaps = Vec::with_capacity(cfg.count);
    let start = Instant::now();
    let mut last = start;
    for _ in 0..cfg.count {
        receiver
            .recv()
            .await
            .ok_or_else(|| "receiver closed during measurement".to_string())?;
        let now = Instant::now();
        gaps.push(u64::try_from(now.duration_since(last).as_nanos()).unwrap_or(u64::MAX));
        last = now;
    }
    let wall = start.elapsed();
    Ok((gaps, wall))
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

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
