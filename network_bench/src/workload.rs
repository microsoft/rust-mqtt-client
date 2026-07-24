// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Workload execution: drives the chosen `MODE` against the transport and records per-op
//! latencies. This is the module that grows when adding modes (a new [`Mode`] variant maps to
//! one new `run_*` function here).

use std::time::{Duration, Instant};

use bytes::Bytes;
use ms_mqtt_client::client::{Client, Receiver};
use ms_mqtt_client::packet::{PublishProperties, QoS, RetainOptions, SubscribeProperties};
use ms_mqtt_client::topic::{TopicFilter, TopicName};
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
        Mode::Echo => run_echo(&client, receiver, cfg).await?,
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

fn qos_enum(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        _ => QoS::AtLeastOnce,
    }
}

fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}
