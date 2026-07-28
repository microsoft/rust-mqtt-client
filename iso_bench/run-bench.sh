#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Repetition + aggregation over single-run.sh -- the primary entry point for benchmarking.
#
# Wall-clock benchmarks are noisy, so a single run is not trustworthy. This runs REPS independent
# reps of the CURRENT build/config, records each RESULT + CPU to a JSONL file, and prints per-label
# summary statistics (median / mean / min / max / CV%). When the results file holds >= 2 labels it
# also prints an A/B comparison (median deltas vs. the baseline label, flagged against the
# baseline's run-to-run noise). For a single ad-hoc run, use single-run.sh directly.
#
# A/B workflow (build each git ref, then compare):
#   RESET=1 LABEL=main     REPS=8 MODE=latency QOS=1 ./run-bench.sh
#   git checkout my-refactor
#           LABEL=refactor REPS=8 MODE=latency QOS=1 ./run-bench.sh   # prints the comparison
#
# Establish the noise floor first by running the SAME build twice under two labels; only trust
# deltas larger than that spread.
#
# Accepts every single-run.sh env var (MODE QOS TRANSPORT PAYLOAD_BYTES COUNT WARMUP INFLIGHT
# INTERVAL_US CLIENT_CORES PEER_CORES NETEM_DELAY ...), plus:
#   REPS          repetitions                       (default 8)
#   LABEL         tag for this build                (default: git short SHA)
#   RESULTS_FILE  JSONL accumulator                 (default: ./results.jsonl)
#   RESET         1 = truncate RESULTS_FILE first   (default 0)
set -euo pipefail

self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
script_dir="$(dirname "$self")"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    while IFS= read -r line; do
        [[ "$line" == '#'* ]] || break
        line="${line#\#}"
        printf '%s\n' "${line# }"
    done < <(tail -n +4 "$self")
    exit 0
fi

cd "$script_dir"

command -v python3 >/dev/null || {
    echo "ERROR: python3 is required for aggregation" >&2
    exit 1
}

REPS="${REPS:-8}"
LABEL="${LABEL:-$(git rev-parse --short HEAD 2>/dev/null || echo run)}"
RESULTS_FILE="${RESULTS_FILE:-$script_dir/results.jsonl}"
RESET="${RESET:-0}"
export LABEL # so single-run.sh -> bench_client tags the RESULT line with it

[[ "$RESET" == "1" ]] && : >"$RESULTS_FILE"

echo "== iso_bench repeat: label='$LABEL' reps=$REPS -> $RESULTS_FILE ==" >&2

for ((r = 1; r <= REPS; r++)); do
    printf '[%s] rep %d/%d ... ' "$LABEL" "$r" "$REPS" >&2
    err_log="$(mktemp)"
    if ! out="$(./single-run.sh 2>"$err_log")"; then
        echo "FAILED" >&2
        cat "$err_log" >&2
        rm -f "$err_log"
        exit 1
    fi
    rm -f "$err_log"

    result_line="$(grep '^RESULT ' <<<"$out" || true)"
    cpu_line="$(grep '^CPU ' <<<"$out" || true)"
    if [[ -z "$result_line" ]]; then
        echo "no RESULT line produced" >&2
        echo "$out" >&2
        exit 1
    fi
    msgs="$(sed -n 's/.*"msgs_per_s":\([0-9.]*\).*/\1/p' <<<"$result_line")"
    printf 'msgs_per_s=%s\n' "${msgs:-?}" >&2

    RESULT_JSON="${result_line#RESULT }" CPU_JSON="${cpu_line#CPU }" \
        REC_LABEL="$LABEL" REP="$r" OUT_FILE="$RESULTS_FILE" \
        python3 - <<'PY'
import json, os
res = json.loads(os.environ["RESULT_JSON"])
cpu = json.loads(os.environ.get("CPU_JSON") or "{}")
rec = {"label": os.environ["REC_LABEL"], "rep": int(os.environ["REP"])}
for k in ("mode", "transport", "qos", "payload_bytes", "count", "msgs_per_s", "mib_per_s"):
    if k in res:
        rec[k] = res[k]
for k, v in (res.get("lat_us") or {}).items():
    rec["lat_" + k] = v
for k in ("cpu_us_per_msg", "max_rss_kb", "user_s", "sys_s"):
    if k in cpu:
        rec[k] = cpu[k]
with open(os.environ["OUT_FILE"], "a") as f:
    f.write(json.dumps(rec) + "\n")
PY
done

# ---- aggregate + compare ------------------------------------------------------------------------
RESULTS_FILE="$RESULTS_FILE" python3 - <<'PY'
import json, os, statistics as st

rows = [json.loads(l) for l in open(os.environ["RESULTS_FILE"]) if l.strip()]
if not rows:
    raise SystemExit("no results recorded")

labels = []
for r in rows:
    if r["label"] not in labels:
        labels.append(r["label"])

metrics = ["msgs_per_s", "mib_per_s", "lat_p50", "lat_p99", "lat_p999", "cpu_us_per_msg", "max_rss_kb"]

def series(label, m):
    return [r[m] for r in rows if r["label"] == label and r.get(m) is not None]

def cv(xs):
    if len(xs) < 2:
        return 0.0
    m = st.mean(xs)
    return 0.0 if m == 0 else st.pstdev(xs) / m * 100.0

def fmt(x):
    return f"{x:.3f}" if abs(x) < 100 else f"{x:.1f}"

# Warn if the accumulated results mix different configs (likely a forgotten RESET).
cfgs = {(r.get("mode"), r.get("transport"), r.get("qos"), r.get("payload_bytes")) for r in rows}
if len(cfgs) > 1:
    print("WARNING: results.jsonl mixes different configs (mode/transport/qos/payload).")
    print("         Use RESET=1 to start a clean comparison.\n")

for label in labels:
    n = sum(1 for r in rows if r["label"] == label)
    print(f"=== [{label}] over {n} reps ===")
    print(f"{'metric':<16}{'median':>12}{'mean':>12}{'min':>12}{'max':>12}{'cv%':>8}")
    for m in metrics:
        xs = series(label, m)
        if not xs:
            continue
        print(f"{m:<16}{fmt(st.median(xs)):>12}{fmt(st.mean(xs)):>12}"
              f"{fmt(min(xs)):>12}{fmt(max(xs)):>12}{cv(xs):>8.1f}")
    print()

if len(labels) >= 2:
    base = labels[0]
    latest = labels[-1]
    print(f"=== comparison (median; baseline={base}, delta={latest}) ===")
    print(f"{'metric':<16}" + "".join(f"{l:>14}" for l in labels) + f"{'delta%':>10}{'note':>9}")
    for m in metrics:
        bxs = series(base, m)
        if not bxs:
            continue
        bmed, bcv = st.median(bxs), cv(series(base, m))
        cells = "".join(f"{(fmt(st.median(series(l, m))) if series(l, m) else '-'):>14}" for l in labels)
        lxs = series(latest, m)
        if lxs and bmed:
            d = (st.median(lxs) - bmed) / bmed * 100.0
            note = ">noise" if abs(d) > max(bcv, 1.0) else "~noise"
            print(f"{m:<16}{cells}{d:>9.1f}%{note:>9}")
        else:
            print(f"{m:<16}{cells}{'-':>10}{'-':>9}")
    print("\nRead latency_* and cpu_us_per_msg going UP as regressions, msgs/mib_per_s going DOWN.")
    print("'note' flags whether the delta exceeds the baseline's run-to-run CV (a rough signal,")
    print("not a formal significance test). delta%/note compare the LAST label to the baseline.")
PY

echo "results: $RESULTS_FILE" >&2
