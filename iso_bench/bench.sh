#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
#
# Runs the FULL curated benchmark suite -- the main entry point for regression detection.
#
# It runs a hand-picked set of representative configs (latency / throughput / inbound, each over TCP
# and TLS), REPS reps each, via bench-workload.sh -- each config prints its OWN summary + A/B as it
# finishes. The suite is CURATED (not a full cross-product) to keep the
# runtime bounded; each config is sized per workload (latency for tail stability, throughput/inbound
# for a steady window) rather than a single shared COUNT.
#
# A/B workflow (build each git ref, then compare):
#   RESET=1 LABEL=main     ./bench.sh
#   git checkout my-refactor
#           LABEL=refactor ./bench.sh   # prints the per-config comparison
#
# Env:
#   REPS          reps per config                   (default 10)
#   LABEL         tag for this build                (default: git short SHA)
#   RESULTS_FILE  JSONL accumulator                 (default: ./results.jsonl)
#   RESET         1 = truncate RESULTS_FILE first   (default 0; use on the first build of an A/B)
#   CLIENT_CORES / PEER_CORES / NETEM_DELAY / CERT_DIR   (passed through to bench-once.sh)
#
# For a single ad-hoc config use bench-workload.sh; for one un-aggregated run use bench-once.sh.
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

REPS="${REPS:-10}"
LABEL="${LABEL:-$(git rev-parse --short HEAD 2>/dev/null || echo run)}"
RESULTS_FILE="${RESULTS_FILE:-$script_dir/results.jsonl}"
RESET="${RESET:-0}"

# Curated suite. Each entry is a config name plus the bench-workload.sh env for that config. Payloads:
# small (64 B) for latency to isolate per-op cost; large (16 KiB) for throughput/inbound to expose
# per-byte crypto/copy cost. COUNTs: latency ~1e5 (stable p99); throughput/inbound ~3e5 (>=~1 s
# steady window). Edit this list to change what the gate covers.
suite=(
    "CONFIG=lat-tcp  MODE=latency    QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=64    COUNT=100000"
    "CONFIG=lat-tls  MODE=latency    QOS=1 TRANSPORT=tls PAYLOAD_BYTES=64    COUNT=100000"
    "CONFIG=tput-tcp MODE=throughput QOS=1 TRANSPORT=tcp PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000"
    "CONFIG=tput-tls MODE=throughput QOS=1 TRANSPORT=tls PAYLOAD_BYTES=16384 INFLIGHT=64 COUNT=300000"
    "CONFIG=in-tcp   MODE=inbound          TRANSPORT=tcp PAYLOAD_BYTES=16384 COUNT=300000"
    "CONFIG=in-tls   MODE=inbound          TRANSPORT=tls PAYLOAD_BYTES=16384 COUNT=300000"
)

[[ "$RESET" == "1" ]] && : >"$RESULTS_FILE"

echo "== iso_bench SUITE: label='$LABEL' reps=$REPS configs=${#suite[@]} -> $RESULTS_FILE ==" >&2

i=0
for cfg in "${suite[@]}"; do
    i=$((i + 1))
    echo "" >&2
    echo ">>> config [$i/${#suite[@]}]: $cfg" >&2
    # Word-splitting of $cfg into KEY=VAL args is intentional.
    # shellcheck disable=SC2086
    env $cfg RESET=0 REPS="$REPS" LABEL="$LABEL" RESULTS_FILE="$RESULTS_FILE" \
        ./bench-workload.sh
done

echo "" >&2
echo "== suite done: label='$LABEL' -> $RESULTS_FILE ==" >&2
echo "   (full human report incl. histograms: python3 report.py $RESULTS_FILE)" >&2
