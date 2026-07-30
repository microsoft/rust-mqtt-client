#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Human-readable renderer for a JSONL results file (written by bench-workload.sh / bench.sh). It is
# the "read the results" tool: an overview, then per-config statistic tables (median / mean / min /
# max / CV%), an A/B comparison when a config has >= 2 labels, and a text histogram of the latency /
# inter-arrival distribution (summed from the per-rep `hist_ns` buckets).
#
# Grouping is by CONFIG, so this works unchanged for a single config (bench-workload.sh scopes to
# its own with --config) or a whole suite (bench.sh writes every config to one file).
#
# Usage:
#   report.py [results.jsonl] [--config NAME] [--label NAME] [--no-hist | --hist-only]
#
#   --config NAME   only this config          --no-hist    tables only (no histograms)
#   --label NAME    only this build label      --hist-only  histograms only (no tables)
import argparse
import collections
import json
import statistics as st
import sys

# (json key, display label, unit, "up"|"down" = which direction is BETTER for A/B verdicts)
METRICS = [
    ("msgs_per_s", "throughput", "msg/s", "up"),
    ("mib_per_s", "throughput", "MiB/s", "up"),
    ("lat_p50", "p50", "us", "down"),
    ("lat_p90", "p90", "us", "down"),
    ("lat_p99", "p99", "us", "down"),
    ("lat_p999", "p99.9", "us", "down"),
    ("lat_max", "max", "us", "down"),
    ("cpu_us_per_msg", "cpu/msg", "us", "down"),
    ("max_rss_kb", "max rss", "KB", "down"),
]


def config_of(r):
    return r.get("config") or (
        f"{r.get('mode')}-{r.get('transport')}-q{r.get('qos')}-{r.get('payload_bytes')}b"
    )


def ordered(seq):
    """Distinct values, preserving first-seen order (labels/configs keep run order)."""
    out = []
    for x in seq:
        if x not in out:
            out.append(x)
    return out


def cv(xs):
    """Coefficient of variation (%) -- run-to-run noise. 0 for < 2 samples."""
    if len(xs) < 2:
        return 0.0
    mean = st.mean(xs)
    return 0.0 if mean == 0 else st.pstdev(xs) / mean * 100.0


def fmt_num(x):
    ax = abs(x)
    if ax >= 1000:
        return f"{x:,.0f}"
    if ax >= 100:
        return f"{x:.1f}"
    return f"{x:.2f}"


def rule(char="=", width=74):
    return char * width


def config_meta(rows):
    """One-line descriptor + latency-kind label for a config, from a representative row."""
    r = rows[0]
    parts = [r.get("mode", "?"), r.get("transport", "?"), f"qos{r.get('qos')}"]
    parts.append(f"{r.get('payload_bytes', '?')}B")
    if (r.get("target_rate") or 0) > 0:
        parts.append(f"open-loop {fmt_num(r['target_rate'])}/s")
    kind = r.get("lat_kind", "latency")
    return ", ".join(str(p) for p in parts), kind


def print_overview(rows, configs):
    labels = ordered(r["label"] for r in rows)
    print(rule())
    print(f" iso_bench results  ({len(rows)} records)")
    print(rule())
    print(f" builds (labels): {', '.join(labels)}")
    print(f" configs:         {len(configs)}")

    # Per-build provenance -- so cross-branch comparisons are auditable. A/B assumes the harness /
    # workload is identical across labels; differing toolchain or host is a confound worth flagging.
    prov = {lbl: first_with(rows, lbl, "git_sha") for lbl in labels}
    if any(prov.values()):
        print()
        for lbl in labels:
            r = prov.get(lbl) or {}
            sha = r.get("git_sha", "?")
            if r.get("git_dirty"):
                sha += "-dirty"
            print(f"   [{lbl:<12}] sha={sha:<16} rustc={r.get('rustc', '?'):<9} host={r.get('host', '?')}")
        for field, what in (("rustc", "toolchain"), ("host", "host")):
            vals = {(prov.get(l) or {}).get(field) for l in labels if prov.get(l)}
            if len(vals) > 1:
                print(f"   !! {what} differs across builds ({', '.join(sorted(map(str, vals)))}) -- a confound")

    print()
    print(f" {'config':<12}{'build':<14}{'reps':>5}   description")
    for c in configs:
        crows = [r for r in rows if config_of(r) == c]
        desc, _ = config_meta(crows)
        for lbl in ordered(r["label"] for r in crows):
            n = sum(1 for r in crows if r["label"] == lbl)
            print(f" {c:<12}{lbl:<14}{n:>5}   {desc}")


def first_with(rows, label, key):
    """First record for a label that carries `key` (provenance is identical across a label's reps)."""
    for r in rows:
        if r.get("label") == label and key in r:
            return r
    return None


# Workload params that MUST match across labels for an A/B to be valid (the instrument, not the
# specimen). If any differ, the two labels measured different things -- flag loudly, don't compare.
DRIFT_KEYS = ["mode", "transport", "qos", "payload_bytes", "count", "inflight", "target_rate"]


def print_drift(rows, config, labels):
    """Warn if the workload definition drifted across labels. Returns True if drift was found."""
    drifted = []
    for key in DRIFT_KEYS:
        per_label = {}
        for lbl in labels:
            vals = [r[key] for r in rows if config_of(r) == config and r["label"] == lbl and key in r]
            if vals:
                per_label[lbl] = vals[0]
        if len({str(v) for v in per_label.values()}) > 1:
            drifted.append((key, per_label))
    if drifted:
        print("\n  !! WORKLOAD DRIFT across labels -- this A/B compares different workloads:")
        for key, per_label in drifted:
            cells = "  ".join(f"[{l}]={v}" for l, v in per_label.items())
            print(f"       {key}: {cells}")
        print("     (same harness/config assumed; verdicts below are NOT trustworthy)")
    return bool(drifted)


def series(rows, config, label, metric):
    return [
        r[metric]
        for r in rows
        if config_of(r) == config and r["label"] == label and r.get(metric) is not None
    ]


def print_summary_table(rows, config, labels, kind):
    for label in labels:
        n = sum(1 for r in rows if config_of(r) == config and r["label"] == label)
        print(f"\n  [{label}]  {n} reps   (latency rows = {kind})")
        print(
            f"  {'metric':<12}{'unit':>7}{'median':>13}{'mean':>13}"
            f"{'min':>13}{'max':>13}{'cv%':>7}"
        )
        print(f"  {rule('-', 78)}")
        for key, disp, unit, _ in METRICS:
            xs = series(rows, config, label, key)
            if not xs:
                continue
            print(
                f"  {disp:<12}{unit:>7}{fmt_num(st.median(xs)):>13}{fmt_num(st.mean(xs)):>13}"
                f"{fmt_num(min(xs)):>13}{fmt_num(max(xs)):>13}{cv(xs):>7.1f}"
            )


def print_comparison(rows, config, labels):
    base, latest = labels[0], labels[-1]
    print(f"\n  A/B comparison  (median; baseline=[{base}], delta=[{latest}])")
    header = "".join(f"{('[' + l + ']'):>14}" for l in labels)
    print(f"  {'metric':<12}{header}{'delta%':>10}{'verdict':>11}")
    print(f"  {rule('-', 12 + 14 * len(labels) + 21)}")
    rep = next((r for r in rows if config_of(r) == config), {})
    # Metrics that must NOT be gated as pass/fail for this config (shown as 'info'):
    #   lat_max            -- a single worst sample per rep, far too heavy-tailed to judge.
    #   QoS 0 pub tput/p50 -- no wire-completion signal, so these time queue admission +
    #                         scheduler interleaving, not send cost (read cpu/msg + p99 instead).
    info_only = {"lat_max"}
    if rep.get("mode") == "pub-throughput" and rep.get("qos") in (0, "0"):
        info_only |= {"msgs_per_s", "mib_per_s", "lat_p50"}
    for key, disp, _, better in METRICS:
        bxs = series(rows, config, base, key)
        if not bxs:
            continue
        bmed, bcv = st.median(bxs), cv(bxs)
        cells = "".join(
            f"{(fmt_num(st.median(series(rows, config, l, key))) if series(rows, config, l, key) else '-'):>14}"
            for l in labels
        )
        lxs = series(rows, config, latest, key)
        if lxs and bmed:
            d = (st.median(lxs) - bmed) / bmed * 100.0
            if key in info_only:
                verdict = "info"
            elif abs(d) <= max(bcv, 1.0):
                verdict = "~noise"
            else:
                improved = (d > 0) if better == "up" else (d < 0)
                verdict = "better" if improved else "WORSE"
            print(f"  {disp:<12}{cells}{d:>9.1f}%{verdict:>11}")
        else:
            print(f"  {disp:<12}{cells}{'-':>10}{'-':>11}")


def print_histogram(rows, config, label, kind):
    selected = [
        r for r in rows if config_of(r) == config and r["label"] == label and "hist_ns" in r
    ]
    if not selected:
        return
    buckets = collections.defaultdict(int)
    for r in selected:
        for upper_ns, count in r["hist_ns"]:
            buckets[upper_ns] += count
    if not buckets:
        return
    uppers = sorted(buckets)
    total = sum(buckets.values())
    peak = max(buckets.values())
    width = 28

    print(f"\n  histogram  [{label}]  {kind}  reps={len(selected)}  samples={total:,}")
    print(f"  {'bucket (us)':>17}  {'count':>10}  {'pct':>6}  {'cum':>6}")
    lower = 0.0
    cum = 0
    for upper in uppers:
        count = buckets[upper]
        cum += count
        bar = "#" * round(count / peak * width) if peak else ""
        upper_us = upper / 1000.0
        print(
            f"  {lower:8.1f}-{upper_us:8.1f}  {count:>10,}  "
            f"{count / total * 100:5.1f}%  {cum / total * 100:5.1f}%  {bar}"
        )
        lower = upper_us


def main():
    ap = argparse.ArgumentParser(
        description="Render an iso_bench results.jsonl file for human reading."
    )
    ap.add_argument("path", nargs="?", default="results.jsonl", help="results JSONL file")
    ap.add_argument("--config", help="only this config")
    ap.add_argument("--label", help="only this build label")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--no-hist", action="store_true", help="tables only (no histograms)")
    g.add_argument("--hist-only", action="store_true", help="histograms only (no tables)")
    args = ap.parse_args()

    try:
        rows = [json.loads(line) for line in open(args.path) if line.strip()]
    except FileNotFoundError:
        sys.exit(f"no such results file: {args.path}")
    if args.config:
        rows = [r for r in rows if config_of(r) == args.config]
    if args.label:
        rows = [r for r in rows if r.get("label") == args.label]
    if not rows:
        sys.exit("no results for that selection")

    configs = ordered(config_of(r) for r in rows)

    if not args.hist_only:
        print_overview(rows, configs)

    any_ab = False
    for config in configs:
        crows = [r for r in rows if config_of(r) == config]
        labels = ordered(r["label"] for r in crows)
        desc, kind = config_meta(crows)

        print(f"\n{rule()}")
        print(f" config: {config}   ({desc})")
        print(rule())

        if not args.hist_only:
            print_summary_table(rows, config, labels, kind)
            if len(labels) >= 2:
                any_ab = True
                print_drift(rows, config, labels)
                print_comparison(rows, config, labels)

        if not args.no_hist:
            for label in labels:
                print_histogram(rows, config, label, kind)

    if any_ab and not args.hist_only:
        print(f"\n{rule('-')}")
        print(" Reading A/B: latency_* / cpu_us_per_msg UP = regression; throughput DOWN = regression.")
        print(" 'verdict' compares the LATEST label to the baseline and flags deltas larger than the")
        print(" baseline's run-to-run CV (a rough signal, not a formal significance test).")
        print(" 'info' = shown for context, never a verdict (heavy-tailed max; QoS 0 throughput/p50")
        print("          measure queue admission, not send cost -- read cpu/msg + p99 there).")


if __name__ == "__main__":
    main()
