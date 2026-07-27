// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Output: latency/jitter percentiles and throughput, rendered as a human summary plus a single
//! machine-readable `RESULT ` line. This is the module that grows when adding metrics or
//! output formats.

use std::time::Duration;

pub(crate) struct Report {
    pub(crate) cfg_summary: String,
    pub(crate) label: String,
    pub(crate) mode: &'static str,
    pub(crate) transport: &'static str,
    pub(crate) qos: u8,
    pub(crate) payload_bytes: usize,
    pub(crate) inflight: usize,
    pub(crate) interval_us: u64,
    /// What the percentile numbers represent for this mode ("op latency" vs "inter-arrival").
    pub(crate) latency_kind: &'static str,
    pub(crate) count: usize,
    pub(crate) wall: Duration,
    pub(crate) latencies_ns: Vec<u64>,
}

impl Report {
    pub(crate) fn print(&self) {
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
            "{kind} (us): min={min:.1}  p50={p50:.1}  p90={p90:.1}  p99={p99:.1}  \
             p99.9={p999:.1}  max={max:.1}  mean={mean:.1}",
            kind = self.latency_kind
        );
        println!("note:         measure CPU-per-msg externally, e.g. `/usr/bin/time -v ...`");
        println!("=============================");

        // Machine-readable line for scraping / diffing across runs. Raw-string pieces keep the JSON
        // quotes literal; `concat!` joins them at compile time into one format template.
        println!(
            concat!(
                r#"RESULT {{"label":"{}","mode":"{}","transport":"{}","qos":{},"#,
                r#""payload_bytes":{},"inflight":{},"interval_us":{},"count":{},"#,
                r#""wall_s":{:.6},"msgs_per_s":{:.3},"mib_per_s":{:.3},"lat_kind":"{}","#,
                r#""lat_us":{{"min":{:.3},"p50":{:.3},"p90":{:.3},"p99":{:.3},"#,
                r#""p999":{:.3},"max":{:.3},"mean":{:.3}}}}}"#,
            ),
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
            self.latency_kind,
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
