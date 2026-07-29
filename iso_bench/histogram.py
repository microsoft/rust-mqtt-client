#!/usr/bin/env python3
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Renders a text histogram of the latency/inter-arrival distribution from results.jsonl, summing the
# per-rep log-spaced buckets (`hist_ns`, emitted by bench_client via HdrHistogram). Bucket bounds are
# deterministic, so reps sum cleanly.
#
# Usage: histogram.py [results.jsonl] [config] [label]
#   config / label optional; omit to include everything with histogram data.
import collections
import json
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "results.jsonl"
config = sys.argv[2] if len(sys.argv) > 2 else None
label = sys.argv[3] if len(sys.argv) > 3 else None

rows = [json.loads(line) for line in open(path) if line.strip()]


def config_of(r):
    return r.get("config") or (
        f"{r.get('mode')}-{r.get('transport')}-q{r.get('qos')}-{r.get('payload_bytes')}b"
    )


selected = [
    r
    for r in rows
    if "hist_ns" in r
    and (config is None or config_of(r) == config)
    and (label is None or r.get("label") == label)
]
if not selected:
    sys.exit("no histogram data for that selection (need records with 'hist_ns')")

# Sum bucket counts across the selected reps, keyed by the bucket's upper bound (ns).
buckets = collections.defaultdict(int)
for r in selected:
    for upper_ns, count in r["hist_ns"]:
        buckets[upper_ns] += count

uppers = sorted(buckets)
total = sum(buckets.values())
peak = max(buckets.values())
width = 50

kinds = {r.get("lat_kind", "latency") for r in selected}
print(
    f"histogram  config={config or 'all'}  label={label or 'all'}  "
    f"reps={len(selected)}  samples={total}  ({'/'.join(sorted(kinds))})"
)
print(f"{'bucket (us)':>22}  {'count':>10}  {'pct':>6}")
lower = 0.0
for upper in uppers:
    count = buckets[upper]
    bar = "#" * round(count / peak * width) if peak else ""
    upper_us = upper / 1000.0
    print(f"{lower:9.1f}-{upper_us:9.1f}  {count:>10}  {count / total * 100:5.1f}%  {bar}")
    lower = upper_us
