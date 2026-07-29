#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Aggregates a JSONL results file (written by bench-workload.sh / bench.sh) and prints, per
# CONFIG, a per-label summary (median / mean / min / max / CV%). When a config has >= 2 labels it
# also prints an A/B comparison (median deltas of the latest label vs. the baseline, flagged against
# the baseline's run-to-run CV). Grouping by config means this works unchanged for a single config
# (bench-workload.sh) or a whole suite (bench.sh).
#
# Usage: aggregate.py [results.jsonl]
import json
import statistics as st
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "results.jsonl"
# Optional 2nd arg: restrict output to one config (bench-workload.sh scopes to its own; omit for all).
config_filter = sys.argv[2] if len(sys.argv) > 2 else None
rows = [json.loads(line) for line in open(path) if line.strip()]
if not rows:
    sys.exit("no results recorded")

METRICS = ["msgs_per_s", "mib_per_s", "lat_p50", "lat_p99", "lat_p999", "cpu_us_per_msg", "max_rss_kb"]


def config_of(r):
    return r.get("config") or (
        f"{r.get('mode')}-{r.get('transport')}-q{r.get('qos')}-{r.get('payload_bytes')}b"
    )


if config_filter:
    rows = [r for r in rows if config_of(r) == config_filter]
    if not rows:
        sys.exit(f"no results for config '{config_filter}'")


def ordered(seq):
    out = []
    for x in seq:
        if x not in out:
            out.append(x)
    return out


def series(config, label, metric):
    return [
        r[metric]
        for r in rows
        if config_of(r) == config and r["label"] == label and r.get(metric) is not None
    ]


def cv(xs):
    if len(xs) < 2:
        return 0.0
    mean = st.mean(xs)
    return 0.0 if mean == 0 else st.pstdev(xs) / mean * 100.0


def fmt(x):
    return f"{x:.3f}" if abs(x) < 100 else f"{x:.1f}"


configs = ordered(config_of(r) for r in rows)

for config in configs:
    labels = ordered(r["label"] for r in rows if config_of(r) == config)
    print(f"\n########## config: {config} ##########")
    for label in labels:
        n = sum(1 for r in rows if config_of(r) == config and r["label"] == label)
        print(f"--- [{label}] over {n} reps ---")
        print(f"{'metric':<16}{'median':>12}{'mean':>12}{'min':>12}{'max':>12}{'cv%':>8}")
        for m in METRICS:
            xs = series(config, label, m)
            if not xs:
                continue
            print(
                f"{m:<16}{fmt(st.median(xs)):>12}{fmt(st.mean(xs)):>12}"
                f"{fmt(min(xs)):>12}{fmt(max(xs)):>12}{cv(xs):>8.1f}"
            )

    if len(labels) >= 2:
        base, latest = labels[0], labels[-1]
        print(f"--- comparison (median; baseline={base}, delta={latest}) ---")
        header = "".join(f"{l:>14}" for l in labels)
        print(f"{'metric':<16}{header}{'delta%':>10}{'note':>9}")
        for m in METRICS:
            bxs = series(config, base, m)
            if not bxs:
                continue
            bmed, bcv = st.median(bxs), cv(bxs)
            cells = "".join(
                f"{(fmt(st.median(series(config, l, m))) if series(config, l, m) else '-'):>14}"
                for l in labels
            )
            lxs = series(config, latest, m)
            if lxs and bmed:
                d = (st.median(lxs) - bmed) / bmed * 100.0
                note = ">noise" if abs(d) > max(bcv, 1.0) else "~noise"
                print(f"{m:<16}{cells}{d:>9.1f}%{note:>9}")
            else:
                print(f"{m:<16}{cells}{'-':>10}{'-':>9}")

if any(len(ordered(r['label'] for r in rows if config_of(r) == c)) >= 2 for c in configs):
    print("\nRead latency_* and cpu_us_per_msg going UP as regressions, msgs/mib_per_s going DOWN.")
    print("'note' flags whether a delta exceeds the baseline's run-to-run CV (a rough signal, not a")
    print("formal significance test). delta%/note compare the LATEST label to the baseline.")
