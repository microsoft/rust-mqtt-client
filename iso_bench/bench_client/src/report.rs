// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Output: latency/jitter percentiles and throughput, rendered as a human summary plus a single
//! machine-readable `RESULT ` line. This is the module that grows when adding metrics or
//! output formats.

use std::time::Duration;

use hdrhistogram::Histogram;

use crate::usage::Usage;

pub(crate) struct Report {
    pub(crate) cfg_summary: String,
    pub(crate) label: String,
    pub(crate) mode: &'static str,
    pub(crate) transport: &'static str,
    pub(crate) qos: u8,
    pub(crate) payload_bytes: usize,
    pub(crate) inflight: usize,
    pub(crate) interval_us: u64,
    /// Open-loop offered rate in msgs/sec (0 = closed-loop / sequential).
    pub(crate) target_rate: f64,
    /// What the percentile numbers represent for this mode ("op latency" vs "inter-arrival").
    pub(crate) latency_kind: &'static str,
    pub(crate) count: usize,
    pub(crate) wall: Duration,
    /// CPU and peak RSS for the measured window only -- see [`crate::usage`].
    pub(crate) usage: Usage,
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

        // Log-spaced buckets of the raw samples, so a histogram can be reconstructed offline.
        let hist_ns = log_buckets_json(&self.latencies_ns);

        println!();
        println!("==== iso_bench result ====");
        if !self.label.is_empty() {
            println!("label:        {}", self.label);
        }
        println!("config:       {}", self.cfg_summary);
        println!("measured ops: {}", self.count);
        println!("wall time:    {wall_s:.3} s");
        if self.target_rate > 0.0 {
            println!(
                "offered rate: {msgs_per_s:.1} msg/s achieved (open-loop; ~= TARGET_RATE unless \
                 overloaded)   {mb_per_s:.2} MiB/s"
            );
        } else {
            println!("throughput:   {msgs_per_s:.1} msg/s   {mb_per_s:.2} MiB/s (payload only)");
        }
        println!(
            "{kind} (us): min={min:.1}  p50={p50:.1}  p90={p90:.1}  p99={p99:.1}  \
             p99.9={p999:.1}  max={max:.1}  mean={mean:.1}",
            kind = self.latency_kind
        );
        // CPU attributable to the measured window (see crate::usage for why this is not taken from
        // /usr/bin/time). Divided by the SAME op count the window measured, so numerator and
        // denominator finally describe the same span.
        let user_s = self.usage.user_us as f64 / 1e6;
        let sys_s = self.usage.sys_us as f64 / 1e6;
        let cpu_us_per_msg = if self.count > 0 {
            (self.usage.user_us + self.usage.sys_us) as f64 / self.count as f64
        } else {
            0.0
        };
        println!(
            "cpu (window): {user_s:.3} s user + {sys_s:.3} s sys = {cpu_us_per_msg:.3} us/msg   \
             peak rss {rss} kB{caveat}",
            rss = self.usage.peak_rss_kb,
            caveat = if self.usage.peak_rss_windowed {
                ""
            } else {
                " (PROCESS-LIFETIME: kernel refused the peak reset)"
            }
        );
        println!("=============================");

        // Machine-readable line for scraping / diffing across runs. Raw-string pieces keep the JSON
        // quotes literal; `concat!` joins them at compile time into one format template.
        //
        // `cpu` covers the MEASURED window only, unlike the /usr/bin/time figures bench-once.sh also
        // emits (as proc_*), which span the whole process including warm-up.
        println!(
            concat!(
                r#"RESULT {{"label":"{}","mode":"{}","transport":"{}","qos":{},"#,
                r#""payload_bytes":{},"inflight":{},"interval_us":{},"target_rate":{:.3},"count":{},"#,
                r#""wall_s":{:.6},"msgs_per_s":{:.3},"mib_per_s":{:.3},"lat_kind":"{}","#,
                r#""cpu":{{"user_s":{:.6},"sys_s":{:.6},"cpu_us_per_msg":{:.3},"#,
                r#""max_rss_kb":{},"windowed_rss":{}}},"#,
                r#""lat_us":{{"min":{:.3},"p50":{:.3},"p90":{:.3},"p99":{:.3},"#,
                r#""p999":{:.3},"max":{:.3},"mean":{:.3}}},"hist_ns":{}}}"#,
            ),
            self.label,
            self.mode,
            self.transport,
            self.qos,
            self.payload_bytes,
            self.inflight,
            self.interval_us,
            self.target_rate,
            self.count,
            wall_s,
            msgs_per_s,
            mb_per_s,
            self.latency_kind,
            user_s,
            sys_s,
            cpu_us_per_msg,
            self.usage.peak_rss_kb,
            self.usage.peak_rss_windowed,
            min,
            p50,
            p90,
            p99,
            p999,
            max,
            mean,
            hist_ns,
        );
    }
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1000.0
}

/// Records the samples into an HdrHistogram and emits `[[upper_bound_ns, count], ...]` over
/// log2 buckets (1 us base). Deterministic bucket bounds let reps be summed by an offline renderer.
fn log_buckets_json(latencies: &[u64]) -> String {
    let mut hist = match Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3) {
        Ok(h) => h,
        Err(_) => return "[]".to_string(),
    };
    for &v in latencies {
        let _ = hist.record(v.max(1));
    }
    let mut out = String::from("[");
    let mut first = true;
    for iv in hist.iter_log(1000, 2.0) {
        let count = iv.count_since_last_iteration();
        if count == 0 {
            continue;
        }
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str(&format!("[{},{}]", iv.value_iterated_to(), count));
    }
    out.push(']');
    out
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p / 100.0) * ((sorted.len() - 1) as f64);
    let idx = rank.round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
