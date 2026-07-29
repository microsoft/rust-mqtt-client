#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Repetition + aggregation over bench-once.sh for ONE config. For the full regression suite across
# all workloads, use bench.sh (which calls this per config).
#
# Wall-clock benchmarks are noisy, so a single run is not trustworthy. This runs REPS independent
# reps of the CURRENT build/config, records each RESULT + CPU to a JSONL file, and prints per-label
# summary statistics (median / mean / min / max / CV%). When the results file holds >= 2 labels it
# also prints an A/B comparison (median deltas vs. the baseline label, flagged against the
# baseline's run-to-run noise). For a single ad-hoc run, use bench-once.sh directly.
#
# A/B workflow (build each git ref, then compare):
#   RESET=1 LABEL=main     REPS=8 MODE=latency QOS=1 ./bench-workload.sh
#   git checkout my-refactor
#           LABEL=refactor REPS=8 MODE=latency QOS=1 ./bench-workload.sh   # prints the comparison
#
# Establish the noise floor first by running the SAME build twice under two labels; only trust
# deltas larger than that spread.
#
# Accepts every bench-once.sh env var (MODE QOS TRANSPORT PAYLOAD_BYTES COUNT WARMUP INFLIGHT
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
CONFIG="${CONFIG:-}"
RESULTS_FILE="${RESULTS_FILE:-$script_dir/results.jsonl}"
RESET="${RESET:-0}"
export LABEL # so bench-once.sh -> bench_client tags the RESULT line with it

[[ "$RESET" == "1" ]] && : >"$RESULTS_FILE"

echo "== iso_bench: label='$LABEL' config='${CONFIG:-auto}' reps=$REPS -> $RESULTS_FILE ==" >&2

# Build provenance -- stamped into every record so two results files are self-describing and any
# instrument/toolchain drift between branches is visible after the fact (see report.py drift check).
prov_sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
prov_dirty=0
[[ -n "$(git status --porcelain 2>/dev/null)" ]] && prov_dirty=1
prov_rustc="$(rustc --version 2>/dev/null | awk '{print $2}')"
prov_host="$(hostname 2>/dev/null || echo unknown)"

this_config=""
for ((r = 1; r <= REPS; r++)); do
    printf '[%s] rep %d/%d ... ' "$LABEL" "$r" "$REPS" >&2
    err_log="$(mktemp)"
    if ! out="$(./bench-once.sh 2>"$err_log")"; then
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

    this_config="$(
        RESULT_JSON="${result_line#RESULT }" CPU_JSON="${cpu_line#CPU }" \
            REC_LABEL="$LABEL" REC_CONFIG="$CONFIG" REP="$r" OUT_FILE="$RESULTS_FILE" \
            PROV_SHA="$prov_sha" PROV_DIRTY="$prov_dirty" PROV_RUSTC="$prov_rustc" PROV_HOST="$prov_host" \
            python3 - <<'PY'
import json, os
res = json.loads(os.environ["RESULT_JSON"])
cpu = json.loads(os.environ.get("CPU_JSON") or "{}")
rec = {"label": os.environ["REC_LABEL"], "rep": int(os.environ["REP"])}
for k in ("mode", "transport", "qos", "payload_bytes", "count", "inflight", "target_rate", "msgs_per_s", "mib_per_s", "lat_kind", "hist_ns"):
    if k in res:
        rec[k] = res[k]
rec["config"] = os.environ.get("REC_CONFIG") or (
    f"{res.get('mode')}-{res.get('transport')}-q{res.get('qos')}-{res.get('payload_bytes')}b"
)
for k, v in (res.get("lat_us") or {}).items():
    rec["lat_" + k] = v
for k in ("cpu_us_per_msg", "max_rss_kb", "user_s", "sys_s"):
    if k in cpu:
        rec[k] = cpu[k]
rec["git_sha"] = os.environ.get("PROV_SHA") or "unknown"
rec["git_dirty"] = os.environ.get("PROV_DIRTY") == "1"
rec["rustc"] = os.environ.get("PROV_RUSTC") or "unknown"
rec["host"] = os.environ.get("PROV_HOST") or "unknown"
with open(os.environ["OUT_FILE"], "a") as f:
    f.write(json.dumps(rec) + "\n")
print(rec["config"])
PY
    )"
done

# Summarize just THIS config (across its reps, plus any earlier label for it -> A/B).
python3 "$script_dir/report.py" "$RESULTS_FILE" --config "$this_config" --no-hist

echo "results: $RESULTS_FILE" >&2
